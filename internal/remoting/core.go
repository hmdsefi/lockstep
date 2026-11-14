// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package remoting

import (
	"cmp"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math/rand/v2"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/hmdsefi/lockstep/codec"
	"github.com/hmdsefi/lockstep/internal/engine"
)

// Timer labels of the core (ENG-070; sim/engine.go gives each its class).
const (
	labelTick        = "lockstep/remote/tick"
	labelRedial      = "lockstep/remote/redial"
	labelDialTimeout = "lockstep/remote/dial-timeout"
)

// Record kinds of the core (§8).
const (
	recordStart  = "lockstep.remote.start"
	recordDial   = "lockstep.remote.dial"
	recordAccept = "lockstep.remote.accept"
	recordUp     = "lockstep.remote.up"
	recordReject = "lockstep.remote.reject"
	recordDown   = "lockstep.remote.down"
	recordRedial = "lockstep.remote.redial"
)

// The reasons a session goes down, as the down record names them.
const (
	DownIO          = "io"           // the driver reported the link closed or a dial failed
	DownIdle        = "idle"         // nothing was received for IdleTimeout; CLOSE idle was sent
	DownReplaced    = "replaced"     // a newer HELLO from the same peer replaced the session
	DownShutdown    = "shutdown"     // this node shut remoting down; CLOSE shutdown was sent
	DownProtocol    = "protocol"     // a received frame broke the protocol; CLOSE protocol was sent
	DownClosed      = "closed"       // the peer sent CLOSE
	DownRejected    = "rejected"     // the HELLO was answered with REJECT
	DownWrongNode   = "wrong-node"   // the WELCOME came from another node than the one dialed
	DownDialTimeout = "dial-timeout" // no WELCOME arrived within DialTimeout
)

// Settings are the protocol settings of a Core, the fields of package remote's Config that the
// protocol uses, with every default already filled in.
type Settings struct {
	// Registry holds the message types that cross nodes.
	Registry *codec.Registry
	// Codec encodes messages; its name is announced in HELLO and WELCOME and must match the
	// peer's. Required.
	Codec codec.Codec
	// MaxFrame is the largest frame this node accepts, announced in HELLO and WELCOME, from
	// MinFrameLimit to MaxFrameLimit.
	MaxFrame int
	// OutboundQueue is the number of frames per peer that are not yet written.
	OutboundQueue int
	// Heartbeat is the tick interval: idle checks and pings.
	Heartbeat time.Duration
	// IdleTimeout is how long a session may receive nothing before it is closed.
	IdleTimeout time.Duration
	// DialTimeout bounds the time from a dial to its WELCOME.
	DialTimeout time.Duration
	// RedialMin and RedialMax bound the wait before a redial.
	RedialMin, RedialMax time.Duration
	// Advertise is the address announced in HELLO and WELCOME, at most MaxAdvertise bytes; it may
	// be empty.
	Advertise string
	// Listen is the address the driver listens on, for the start record; empty in simulation.
	Listen string
}

// Config is what a Core needs: the identity of its node, the engine's services and the protocol
// settings. Package remote and the simulation fill it from what the system gives its transport.
type Config struct {
	// Node is this node's name. Required.
	Node string
	// Incarnation is this node's incarnation, never 0. The core reads it once, in New.
	Incarnation uint32
	// Clock is the engine's clock: the core's only source of time and timers. Required.
	Clock engine.Clock
	// Entropy gives the core its random stream, "lockstep/<node>/<incarnation>/$remote". The core
	// draws from it only when it arms a redial. Required.
	Entropy engine.Entropy
	// Tracer receives the core's records. Nil, or a tracer that is not enabled, means none.
	Tracer engine.Tracer
	// Failer reports failures that end a simulation seed. Nil means none. The core does not use
	// it yet: the receive path will.
	Failer engine.Failer
	// Log receives the core's warnings. Nil means none.
	Log *slog.Logger
	// Driver opens the core's connections. Required.
	Driver Driver
	// Settings are the protocol settings.
	Settings Settings
}

// Core is the remoting protocol of one node, shared by the production and the simulation
// drivers. It keeps one outgoing session per peer, which it dials on demand, and accepts incoming
// sessions; it handshakes, sends heartbeats, closes idle sessions and redials with seeded
// backoff. It guards its state with a mutex, starts no goroutine and reads time only from its
// Clock, so the same events give the same frames and records in every run.
type Core struct {
	mu sync.Mutex

	node      string
	inc       uint32
	codecName string
	set       Settings
	clock     engine.Clock
	rand      *rand.Rand
	tracer    engine.Tracer // nil when records are off
	log       *slog.Logger
	driver    Driver

	started bool
	tick    engine.Cancel // the armed heartbeat tick; nil before Start
	peers   map[string]*peer
	links   map[Link]*session
	serial  uint64 // sessions created so far
}

// New checks cfg and returns a Core that has not started. Every error text starts with
// "remoting: invalid config: ".
func New(cfg Config) (*Core, error) {
	s := cfg.Settings
	const p = "remoting: invalid config: "
	switch {
	case !engine.ValidNodeName(cfg.Node):
		return nil, fmt.Errorf(p+"node name %q is not valid", cfg.Node)
	case cfg.Incarnation == 0:
		return nil, errors.New(p + "incarnation is 0")
	case cfg.Clock == nil:
		return nil, errors.New(p + "no clock")
	case cfg.Entropy == nil:
		return nil, errors.New(p + "no entropy")
	case cfg.Driver == nil:
		return nil, errors.New(p + "no driver")
	case s.Codec == nil:
		return nil, errors.New(p + "no codec")
	case len(s.Codec.Name()) < 1 || len(s.Codec.Name()) > MaxCodecName:
		return nil, fmt.Errorf(p+"codec name %q is not 1 to %d bytes", s.Codec.Name(), MaxCodecName)
	case s.MaxFrame < MinFrameLimit || s.MaxFrame > MaxFrameLimit:
		return nil, fmt.Errorf(p+"MaxFrame %d out of range [%d, %d]", s.MaxFrame, MinFrameLimit, MaxFrameLimit)
	case s.Heartbeat <= 0 || s.IdleTimeout <= 0 || s.DialTimeout <= 0 || s.RedialMin <= 0:
		return nil, errors.New(p + "Heartbeat, IdleTimeout, DialTimeout and RedialMin must be positive")
	case s.IdleTimeout/2 < s.Heartbeat: // IdleTimeout < 2*Heartbeat without overflow
		return nil, fmt.Errorf(p+"IdleTimeout %s is less than twice Heartbeat %s", s.IdleTimeout, s.Heartbeat)
	case s.RedialMax < s.RedialMin:
		return nil, fmt.Errorf(p+"RedialMax %s is less than RedialMin %s", s.RedialMax, s.RedialMin)
	case len(s.Advertise) > MaxAdvertise:
		return nil, fmt.Errorf(p+"advertise address of %d bytes exceeds %d", len(s.Advertise), MaxAdvertise)
	}
	c := &Core{
		node:      cfg.Node,
		inc:       cfg.Incarnation,
		codecName: s.Codec.Name(),
		set:       s,
		clock:     cfg.Clock,
		rand:      cfg.Entropy.Rand("lockstep/" + cfg.Node + "/" + strconv.FormatUint(uint64(cfg.Incarnation), 10) + "/$remote"),
		log:       cfg.Log,
		driver:    cfg.Driver,
		peers:     make(map[string]*peer),
		links:     make(map[Link]*session),
	}
	if cfg.Tracer != nil && cfg.Tracer.Enabled() {
		c.tracer = cfg.Tracer
	}
	if c.log == nil {
		c.log = slog.New(slog.DiscardHandler)
	}
	return c, nil
}

// Start records the start of remoting on this node and arms the first heartbeat tick. Calls
// after the first do nothing. Call it in engine context.
func (c *Core) Start() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started {
		return
	}
	c.started = true
	if c.tracer != nil {
		attrs := []engine.Attr{
			{Key: "node", Value: c.node},
			{Key: "inc", Value: strconv.FormatUint(uint64(c.inc), 10)},
			{Key: "codec", Value: c.codecName},
			{Key: "max-frame", Value: strconv.Itoa(c.set.MaxFrame)},
		}
		if c.set.Listen != "" {
			attrs = append(attrs, engine.Attr{Key: "listen", Value: c.set.Listen})
		}
		c.tracer.Record(recordStart, attrs...)
	}
	c.armTick()
}

// Connect makes sure an outgoing session to node exists or is on its way: it dials now unless a
// session is up, a dial is in progress or a redial is armed. Until a session to node comes up,
// node has work waiting, so a dial that fails is retried with backoff; a session that is already
// up adds no such work. Connect returns an error wrapping ErrUnknownNode when the driver knows no
// address for node, and an error for this node's own name or an invalid one. Call it in engine
// context.
func (c *Core) Connect(node string) error {
	if node == c.node || !engine.ValidNodeName(node) {
		return fmt.Errorf("remoting: cannot connect to node %q", node)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	_, known := c.peers[node]
	p := c.peer(node)
	if p.out != nil && p.out.up {
		return nil
	}
	if p.out != nil || p.redial != nil {
		p.want = true
		return nil
	}
	if err := c.dial(p); err != nil {
		if errors.Is(err, ErrUnknownNode) {
			if !known {
				delete(c.peers, node)
			}
			return err
		}
		p.want = true
		c.dialFailed(p, err)
		return nil
	}
	p.want = true
	return nil
}

// Accepted hands the core a link that a peer opened to this node. The core waits DialTimeout for
// its HELLO; a link without one by then is closed, without REJECT, and goes down with reason
// "dial-timeout". Call it in engine context.
func (c *Core) Accepted(link Link) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.links[link]; ok {
		return
	}
	s := c.newSession(link, dirIn, nil)
	if c.tracer != nil {
		c.tracer.Record(recordAccept, engine.Attr{Key: "conn", Value: s.connText()})
	}
	c.armDialTimeout(s)
}

// Writable tells the core that link has room again after its Write returned false. The core
// writes the frames the link did not take, in order, and resumes writing. Call it in engine
// context.
func (c *Core) Writable(link Link) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if s := c.links[link]; s != nil {
		c.flushBlocked(s)
	}
}

// Frame hands the core one frame, length prefix included, that arrived on link. A driver that
// cannot cut a frame out of its stream, because the length prefix is out of range, passes the
// bytes it has, and the core closes the session as a protocol error. The core does not keep b
// after Frame returns. Frames on a link the core does not know, or no longer knows, are ignored.
// Call it in engine context.
func (c *Core) Frame(link Link, b []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.links[link]
	if s == nil {
		return
	}
	s.lastRecv = c.clock.Now()
	if s.dir == dirIn && s.peer == nil {
		c.acceptHello(s, b)
		return
	}
	f, err := ParseFrame(b, c.set.MaxFrame)
	if err != nil {
		c.protocolError(s, err)
		return
	}
	if s.dir == dirOut && !s.up {
		c.handshakeReply(s, &f)
		return
	}
	c.sessionFrame(s, &f)
}

// Closed tells the core that link ended: a read or write failed, the peer closed it, or a dial
// did not connect. err says why, for the log; nil means the peer closed it cleanly. The session
// goes down with reason "io". Call it in engine context.
func (c *Core) Closed(link Link, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.links[link]
	if s == nil {
		return
	}
	if err != nil {
		c.log.Info("remoting: link closed", "node", c.node, "peer", s.peerName(), "conn", s.conn, "err", err)
	}
	c.down(s, DownIO)
}

// peer returns the peer named name, adding it if it is new.
func (c *Core) peer(name string) *peer {
	p := c.peers[name]
	if p == nil {
		p = &peer{name: name}
		c.peers[name] = p
	}
	return p
}

// newSession adds a session on link.
func (c *Core) newSession(link Link, d dir, p *peer) *session {
	c.serial++
	now := c.clock.Now()
	s := &session{serial: c.serial, link: link, conn: link.ID(), dir: d, peer: p, lastRecv: now, lastSend: now}
	c.links[link] = s
	return s
}

// sessions returns the live sessions in creation order.
func (c *Core) sessions() []*session {
	return slices.SortedFunc(maps.Values(c.links), func(a, b *session) int {
		return cmp.Compare(a.serial, b.serial)
	})
}

// armTick arms the next heartbeat tick.
func (c *Core) armTick() {
	c.tick = c.clock.AfterFunc(c.set.Heartbeat, labelTick, c.onTick)
}

// onTick runs once per Heartbeat (REM-090): an up session that received nothing for IdleTimeout
// is closed with CLOSE idle; an up outgoing session that wrote or received nothing for Heartbeat
// writes PING, unless its link still holds frames it did not take. Sessions in their handshake are
// left to the dial timeout.
func (c *Core) onTick() {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.clock.Now()
	for _, s := range c.sessions() {
		if c.links[s.link] != s || !s.up {
			continue
		}
		if now.Sub(s.lastRecv) >= c.set.IdleTimeout {
			c.closeSession(s, CloseIdle, DownIdle)
			continue
		}
		if s.dir == dirOut && len(s.blocked) == 0 &&
			(now.Sub(s.lastSend) >= c.set.Heartbeat || now.Sub(s.lastRecv) >= c.set.Heartbeat) {
			c.write(s, &Frame{Kind: KindPing})
		}
	}
	c.armTick()
}
