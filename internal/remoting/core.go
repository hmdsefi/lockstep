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
	"sync/atomic"
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
	// Failer receives the frames the core could not apply as they were: one that does not decode,
	// a value its recipient does not take, a reply whose answer could not be sent, and one that
	// breaks the protocol (the session closes with CLOSE protocol, or a first frame is answered
	// with REJECT version, kind or payload). The simulation fails the seed with them; production
	// ignores them. Nil means none.
	Failer engine.Failer
	// Log receives the core's warnings. Nil means none.
	Log *slog.Logger
	// Host is the system the core belongs to: the core hands it the envelopes it receives, and
	// fails asks whose requests it could not send, through Host.Inbound. Required.
	Host engine.Host
	// System is the *lockstep.System that Host belongs to, which the core passes to the runtime
	// hooks to check what it receives against the system's actors and asks. Required.
	System any
	// Driver opens the core's connections. Required.
	Driver Driver
	// Errors builds the errors the core returns and fails asks with. Required.
	Errors Errors
	// DeadLetter makes msg a dead letter from from to to with reason, as the system's own dead
	// letters are. The core calls it in engine context without holding its lock, so it may send.
	// Required.
	DeadLetter func(from, to engine.Address, reason string, msg any)
	// Settings are the protocol settings.
	Settings Settings
	// Simulated marks the core of a simulated system, which has one from its boot: package
	// remote's Enable then replaces its settings once with SetSettings instead of building a core.
	Simulated bool

	// Metrics are the counters the core increments; the zero value counts nothing.
	Metrics Metrics
}

// Core is the remoting protocol of one node, shared by the production and the simulation
// drivers. It keeps one outgoing session per peer, which it dials on demand, and accepts incoming
// sessions; it handshakes, sends heartbeats, closes idle sessions and redials with seeded
// backoff. It serves the death watches and lookups of other nodes, keeps this node's watches of
// their actors across reconnects, ends what a restarted peer's old incarnation left behind, and
// shuts down cleanly. It guards its state with a mutex, starts no goroutine and reads time only from its
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
	host      engine.Host
	sys       any
	failer    engine.Failer // nil when none
	driver    Driver
	errs      Errors
	dead      func(from, to engine.Address, reason string, msg any)

	simulated bool // Config.Simulated
	replaced  bool // SetSettings replaced the settings
	started   bool
	shut      bool          // Shutdown ran
	tick      engine.Cancel // the armed heartbeat tick; nil before Start and after Shutdown
	peers     map[string]*peer
	links     map[Link]*session
	serial    uint64 // sessions created so far
	// closed holds the last up incoming session of each peer that went down, by link, until the
	// driver reports the link closed: frames still handed over for it are discarded (REM-250).
	closed map[Link]*session

	// deferred holds the calls that must run after the lock is released, in order: dead letters,
	// which may send and so call Deliver again, and log calls, whose handlers may block.
	deferred []func()
	// failTimers holds the armed lockstep/remote/fail timers, in arming order, so a shutdown can
	// stop them.
	failTimers []*failTimer
	// maxWatchers bounds the watches of one peer's actors on local actors (REM-280): maxWatchers,
	// lowered by tests.
	maxWatchers int
	// met are the counters, nil functions being off; up is the number of up sessions.
	met Metrics
	up  atomic.Int64
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
	case cfg.Host == nil:
		return nil, errors.New(p + "no host")
	case cfg.System == nil:
		return nil, errors.New(p + "no system")
	case cfg.Driver == nil:
		return nil, errors.New(p + "no driver")
	case cfg.Errors == nil:
		return nil, errors.New(p + "no errors")
	case cfg.DeadLetter == nil:
		return nil, errors.New(p + "no dead-letter function")
	}
	if err := checkSettings(s); err != nil {
		return nil, err
	}
	c := &Core{
		node:      cfg.Node,
		inc:       cfg.Incarnation,
		codecName: s.Codec.Name(),
		set:       s,
		clock:     cfg.Clock,
		rand:      cfg.Entropy.Rand("lockstep/" + cfg.Node + "/" + strconv.FormatUint(uint64(cfg.Incarnation), 10) + "/$remote"),
		log:       cfg.Log,
		host:      cfg.Host,
		sys:       cfg.System,
		failer:    cfg.Failer,
		driver:    cfg.Driver,
		errs:      cfg.Errors,
		dead:      cfg.DeadLetter,
		peers:     make(map[string]*peer),
		links:     make(map[Link]*session),
		closed:    make(map[Link]*session),
		simulated: cfg.Simulated,

		maxWatchers: maxWatchers,
		met:         cfg.Metrics,
	}
	if cfg.Tracer != nil && cfg.Tracer.Enabled() {
		c.tracer = cfg.Tracer
	}
	if c.log == nil {
		c.log = slog.New(slog.DiscardHandler)
	}
	return c, nil
}

// checkSettings checks the protocol settings s for New and SetSettings. Every error text starts
// with "remoting: invalid config: ".
func checkSettings(s Settings) error {
	const p = "remoting: invalid config: "
	switch {
	case s.Registry == nil:
		return errors.New(p + "no registry")
	case s.Codec == nil:
		return errors.New(p + "no codec")
	case len(s.Codec.Name()) < 1 || len(s.Codec.Name()) > MaxCodecName:
		return fmt.Errorf(p+"codec name %q is not 1 to %d bytes", s.Codec.Name(), MaxCodecName)
	case s.MaxFrame < MinFrameLimit || s.MaxFrame > MaxFrameLimit:
		return fmt.Errorf(p+"MaxFrame %d out of range [%d, %d]", s.MaxFrame, MinFrameLimit, MaxFrameLimit)
	case s.OutboundQueue < 1:
		return fmt.Errorf(p+"OutboundQueue %d is less than 1", s.OutboundQueue)
	case s.Heartbeat <= 0 || s.IdleTimeout <= 0 || s.DialTimeout <= 0 || s.RedialMin <= 0:
		return errors.New(p + "Heartbeat, IdleTimeout, DialTimeout and RedialMin must be positive")
	case s.IdleTimeout/2 < s.Heartbeat: // IdleTimeout < 2*Heartbeat without overflow
		return fmt.Errorf(p+"IdleTimeout %s is less than twice Heartbeat %s", s.IdleTimeout, s.Heartbeat)
	case s.RedialMax < s.RedialMin:
		return fmt.Errorf(p+"RedialMax %s is less than RedialMin %s", s.RedialMax, s.RedialMin)
	case len(s.Advertise) > MaxAdvertise:
		return fmt.Errorf(p+"advertise address of %d bytes exceeds %d", len(s.Advertise), MaxAdvertise)
	}
	return nil
}

// ErrCodecInUse is SetSettings's error for settings that change the codec or the registry while
// the core has a connection or a queued frame.
var ErrCodecInUse = errors.New("remoting: the codec and registry cannot change while connections are open or frames are queued")

// Simulated reports whether the core was built with Config.Simulated.
func (c *Core) Simulated() bool { return c.simulated }

// Settable reports whether SetSettings may replace the core's settings: the core is simulated
// (Config.Simulated) and its settings have not been replaced yet.
func (c *Core) Settable() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.simulated && !c.replaced
}

// SetSettings replaces the protocol settings of a simulated core, once: package
// remote's Enable on a simulated system calls it. From then on the core announces s in the
// handshakes it writes and applies s to the frames it builds, queues and receives, to its timers
// and to its redials. Sessions already up are not handshaken again: they keep the frame limit
// their peer announced, and their peer keeps the one this core announced then, so a lower MaxFrame
// can make the peer's next large frame a protocol error. They also keep the codec agreed at their
// handshake: s may name a codec of another name or another registry only while the core has no
// connection, up or in its handshake, and no frame queued, which was encoded with the old codec.
// A started core's heartbeat tick is armed again, one new Heartbeat from now. The start record is
// not repeated.
//
// It returns an error and changes nothing when s fails New's checks, when the core is not
// simulated or its settings were replaced already, when s changes the codec or registry while a
// connection is open or a frame queued, and, wrapping lockstep.ErrStopped, after Shutdown. The
// send and receive paths read the settings without the lock, so SetSettings is only for a
// simulated core, whose calls all come from the simulation's one goroutine. Call it in engine
// context or from that goroutine.
func (c *Core) SetSettings(s Settings) error {
	if err := checkSettings(s); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.unlock()
	switch {
	case !c.simulated:
		return errors.New("remoting: the settings of a core that is not simulated cannot be replaced")
	case c.replaced:
		return errors.New("remoting: the settings were replaced already")
	case c.shut:
		return errShutDown
	case (s.Codec.Name() != c.codecName || s.Registry != c.set.Registry) && c.open():
		return ErrCodecInUse
	}
	c.replaced = true
	c.set = s
	c.codecName = s.Codec.Name()
	if c.tick != nil {
		c.tick()
		c.armTick()
	}
	return nil
}

// open reports whether the core has a connection, up or in its handshake, or a frame queued for a
// peer.
func (c *Core) open() bool {
	if len(c.links) > 0 {
		return true
	}
	for _, p := range c.peers { //lockstep:maporder the result is the same in any order
		if len(p.queue) > 0 {
			return true
		}
	}
	return false
}

// Start records the start of remoting on this node and arms the first heartbeat tick. Calls
// after the first, and calls after Shutdown, do nothing. Call it in engine context.
func (c *Core) Start() {
	c.mu.Lock()
	defer c.unlock()
	if c.started || c.shut {
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

// warn logs a warning once the lock is released; args are evaluated now.
func (c *Core) warn(msg string, args ...any) {
	c.deferred = append(c.deferred, func() { c.log.Warn(msg, args...) })
}

// info logs at level info once the lock is released; args are evaluated now.
func (c *Core) info(msg string, args ...any) {
	c.deferred = append(c.deferred, func() { c.log.Info(msg, args...) })
}

// Connect makes sure an outgoing session to node exists or is on its way: it dials now unless a
// session is up, a dial is in progress or a redial is armed. Until a session to node comes up,
// node has work waiting, so a dial that fails is retried with backoff; a session that is already
// up adds no such work. Connect returns an error wrapping ErrUnknownNode when the driver knows no
// address for node, an error for this node's own name or an invalid one, and an error after
// Shutdown. Call it in engine context.
func (c *Core) Connect(node string) error {
	if node == c.node || !engine.ValidNodeName(node) {
		return fmt.Errorf("remoting: cannot connect to node %q", node)
	}
	c.mu.Lock()
	defer c.unlock()
	if c.shut {
		return errShutDown
	}
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
			c.tidy(p)
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
// "dial-timeout". After Shutdown the core closes link at once. Call it in engine context.
func (c *Core) Accepted(link Link) {
	c.mu.Lock()
	defer c.unlock()
	if c.shut {
		link.Close()
		return
	}
	if _, ok := c.links[link]; ok {
		return
	}
	c.forgetClosed(link)
	s := c.newSession(link, dirIn, nil)
	if c.tracer != nil {
		c.tracer.Record(recordAccept, engine.Attr{Key: "conn", Value: s.connText()})
	}
	c.armDialTimeout(s)
}

// Writable tells the core that link has room again after its Write returned false. The core
// writes the frames the link did not take, in order, then the data frames queued for the peer,
// and resumes writing. Call it in engine context.
func (c *Core) Writable(link Link) {
	c.mu.Lock()
	defer c.unlock()
	if s := c.links[link]; s != nil {
		s.stalled = false
		c.flushBlocked(s)
		if s.dir == dirOut && s.up {
			c.flush(s)
		}
	}
}

// Frame hands the core one frame, length prefix included, that arrived on link. A driver that
// cannot cut a frame out of its stream, because the length prefix is out of range, passes the
// bytes it has, and the core closes the session as a protocol error. The core does not keep b
// after Frame returns.
//
// The envelopes a session receives go to the Host, in the order their frames arrived, after
// Frame has released the core's lock. A frame that arrives on the link of an incoming session the
// core closed, before the driver reports the link closed, is discarded with a record; frames on
// any other link the core does not know, or no longer knows, are ignored. Call it in engine
// context.
func (c *Core) Frame(link Link, b []byte) {
	// Parsing and decoding read only settings that do not change, so they run without the lock.
	// A frame that turns out to be dropped (a link the core does not know, a session not up, a
	// late or misrouted frame) is decoded for nothing; knowing that first would take the lock
	// twice for every frame, and such frames are rare.
	f, err := ParseFrame(b, c.set.MaxFrame)
	var in *inbound
	if err == nil {
		in = c.decodeFrame(&f)
	}
	c.mu.Lock()
	defer c.unlock()
	s := c.links[link]
	if s == nil {
		if old := c.closed[link]; old != nil && err == nil {
			c.discard(old, &f, DiscardClosedSession)
		}
		return
	}
	c.received(len(b))
	s.lastRecv = c.clock.Now()
	if s.dir == dirIn && s.peer == nil {
		c.acceptHello(s, b)
		return
	}
	if err != nil {
		c.protocolError(s, err)
		return
	}
	if s.dir == dirOut && !s.up {
		c.handshakeReply(s, &f)
		return
	}
	c.sessionFrame(s, &f, in)
}

// Closed tells the core that link ended: a read or write failed, the peer closed it, or a dial
// did not connect. err says why, for the log; nil means the peer closed it cleanly. The session
// goes down with reason "io". Call it in engine context.
func (c *Core) Closed(link Link, err error) {
	c.mu.Lock()
	defer c.unlock()
	s := c.links[link]
	if s == nil {
		c.forgetClosed(link)
		return
	}
	if err != nil {
		c.info("remoting: link closed", "node", c.node, "peer", s.peerName(), "conn", s.conn, "err", err)
	}
	c.down(s, DownIO)
	// The driver hands over no more frames for link.
	c.forgetClosed(link)
}

// tidy forgets p when it holds nothing: no session, no queued frame, no armed redial, nothing
// waiting, and no incarnation, which only a handshake gives. A peer that completed a handshake is
// kept for its incarnation and advertise address, which restart detection and later dials need, so
// the peers kept are the nodes that handshook with this one plus the ones with work.
func (c *Core) tidy(p *peer) {
	if p.out == nil && p.in == nil && p.redial == nil && p.inc == 0 && !c.waiting(p) && c.peers[p.name] == p {
		delete(c.peers, p.name)
	}
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
// left to the dial timeout. The lookups whose asks are no longer pending are forgotten (REM-290).
func (c *Core) onTick() {
	c.mu.Lock()
	defer c.unlock()
	if c.shut {
		return
	}
	now := c.clock.Now()
	for _, s := range c.sessions() {
		if c.links[s.link] != s || !s.up {
			continue
		}
		if now.Sub(s.lastRecv) >= c.set.IdleTimeout {
			c.closeSession(s, CloseIdle, DownIdle)
			continue
		}
		if s.dir == dirOut && !s.held() &&
			(now.Sub(s.lastSend) >= c.set.Heartbeat || now.Sub(s.lastRecv) >= c.set.Heartbeat) {
			c.write(s, &Frame{Kind: KindPing})
		}
	}
	c.pruneLookups()
	c.armTick()
}
