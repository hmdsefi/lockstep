// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package remote

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/remoting"
)

// labelRecv is the engine callback that hands the core what the connections delivered (REM-150).
const labelRecv = "lockstep/remote/recv"

const (
	// closeLinger bounds the wait, after a link wrote its last frame and closed its writing side,
	// for the peer to close its side, so that the peer reads every frame before the connection
	// goes away.
	closeLinger = time.Second
	// readBuffer is the size of each connection's read buffer, and the most a reader allocates
	// for a frame before its bytes have arrived.
	readBuffer = 64 << 10
	// drainBatch is the most events one drain callback hands to the core.
	drainBatch = 1024
	// maxAcceptDelay bounds the wait before accepting again after Accept failed.
	maxAcceptDelay = time.Second
)

// errStopped is Dial's error once remoting is shutting down.
var errStopped = errors.New("remote: remoting is shutting down")

// newListener wraps ln with TLS when cfg is set.
func newListener(ln net.Listener, cfg *tls.Config) net.Listener {
	if cfg == nil {
		return ln
	}
	return tls.NewListener(ln, cfg)
}

// driver is the TCP remoting.Driver of one system. It owns every goroutine and connection of
// remoting: one goroutine that accepts, and per connection one that reads and one that writes (a
// dialed connection's writer dials first). Those goroutines never call the core: they post what
// happened to the driver's event queue, and an engine callback (labelRecv) hands the events to the
// core in the order they were posted, so the core sees each link's events in the order they
// happened, in engine context.
type driver struct {
	ln          net.Listener
	peers       map[string]string
	resolve     func(string) (string, error)
	tls         *tls.Config
	dialer      net.Dialer
	dialTimeout time.Duration
	writeWait   time.Duration // the write deadline: IdleTimeout
	maxFrame    int
	bufLimit    int
	inLimit     int
	clock       engine.Clock // set by Enable before any goroutine starts
	node        string
	log         *slog.Logger
	ctx         context.Context // cancelled by stop: ends dials and handshakes
	cancel      context.CancelFunc
	nextID      atomic.Uint64
	core        atomic.Pointer[remoting.Core]

	mu      sync.Mutex
	stopped bool
	links   map[*link]struct{} // links whose Closed is not yet posted
	// wg counts every goroutine; Add runs under mu while !stopped, or from a counted goroutine.
	wg sync.WaitGroup

	qmu    sync.Mutex
	events []event // posted, not yet handed to the core
	armed  bool    // a drain callback is armed or running
}

// newDriver returns the driver of a validated c listening on ln.
func newDriver(c Config, ln net.Listener) *driver {
	ctx, cancel := context.WithCancel(context.Background())
	return &driver{
		ln:          ln,
		peers:       maps.Clone(c.Peers),
		resolve:     c.Resolve,
		tls:         c.TLS,
		dialer:      net.Dialer{Timeout: c.DialTimeout},
		dialTimeout: c.DialTimeout,
		writeWait:   c.IdleTimeout,
		maxFrame:    c.MaxFrame,
		bufLimit:    linkBuffer,
		inLimit:     inboundLimit,
		log:         discardLog,
		ctx:         ctx,
		cancel:      cancel,
		links:       make(map[*link]struct{}),
	}
}

// setCore makes core the driver's core. It reports false when stop has run already.
func (d *driver) setCore(core *remoting.Core) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.stopped {
		return false
	}
	d.core.Store(core)
	return true
}

// startAccepting starts the goroutine that accepts connections, unless stop has run.
func (d *driver) startAccepting() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.stopped {
		return
	}
	d.wg.Add(1)
	go d.accept()
}

// stop is the transport's stop function, run by the system's Shutdown after its last actor
// stopped (REM-296): the core's shutdown writes CLOSE shutdown on every session and closes their
// links, then the listener and every other link close, and stop returns once every goroutine of
// the driver has exited.
func (d *driver) stop() {
	d.mu.Lock()
	if d.stopped {
		d.mu.Unlock()
		return
	}
	d.stopped = true
	d.mu.Unlock()
	if core := d.core.Load(); core != nil {
		core.Shutdown()
	}
	_ = d.ln.Close()
	d.cancel()
	d.mu.Lock()
	open := slices.Collect(maps.Keys(d.links))
	d.mu.Unlock()
	for _, l := range open {
		l.Close()
	}
	d.wg.Wait()
}

// track counts n goroutines of l and lists l until its Closed is posted. It reports false, and
// does neither, once stop has run.
func (d *driver) track(l *link, n int) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.stopped {
		return false
	}
	d.links[l] = struct{}{}
	d.wg.Add(n)
	return true
}

func (d *driver) untrack(l *link) {
	d.mu.Lock()
	delete(d.links, l)
	d.mu.Unlock()
}

// accept accepts connections until the listener closes. Each accepted connection is handed to
// the core with Accepted, which waits DialTimeout for its HELLO; with TLS its reader runs the
// handshake first, within DialTimeout.
func (d *driver) accept() {
	defer d.wg.Done()
	var delay time.Duration
	for {
		conn, err := d.ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) || d.isStopped() {
				return
			}
			delay = min(max(2*delay, 5*time.Millisecond), maxAcceptDelay)
			d.log.Warn("remote: accept failed", "node", d.node, "err", err, "retry", delay)
			t := time.NewTimer(delay)
			select {
			case <-t.C:
			case <-d.ctx.Done():
				t.Stop()
				return
			}
			continue
		}
		delay = 0
		l := d.newLink()
		l.ctx, l.cancel = context.WithCancel(d.ctx)
		if _, ok := conn.(*tls.Conn); ok {
			l.handshook = make(chan struct{})
		}
		if !d.track(l, 2) {
			l.cancel()
			_ = conn.Close()
			return
		}
		l.report(event{kind: eventAccepted, link: l})
		go l.read(conn)
		go func() {
			defer d.wg.Done()
			defer l.cancel()
			l.write(conn)
		}()
	}
}

func (d *driver) isStopped() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.stopped
}

// Dial returns a link to node at once and connects it on a goroutine of its own, which then
// writes the frames the core wrote meanwhile (remoting.Driver). The address is the one in Peers,
// else the one Resolve returns, else advertise, the address node announced in its HELLO.
func (d *driver) Dial(node, advertise string) (remoting.Link, error) {
	addr, err := d.address(node, advertise)
	if err != nil {
		return nil, err
	}
	l := d.newLink()
	l.ctx, l.cancel = context.WithCancel(d.ctx)
	if !d.track(l, 1) {
		l.cancel()
		return nil, errStopped
	}
	go l.dial(addr)
	return l, nil
}

// address returns the address to dial node at (REM-100), or an error wrapping
// remoting.ErrUnknownNode when there is none.
func (d *driver) address(node, advertise string) (string, error) {
	if a := d.peers[node]; a != "" {
		return a, nil
	}
	var rerr error
	if d.resolve != nil {
		a, err := d.resolve(node)
		if err == nil && a != "" {
			return a, nil
		}
		rerr = err
	}
	if advertise != "" {
		return advertise, nil
	}
	if rerr != nil {
		return "", fmt.Errorf("%w: %q: %w", remoting.ErrUnknownNode, node, rerr)
	}
	return "", fmt.Errorf("%w: %q", remoting.ErrUnknownNode, node)
}

// clientTLS returns the TLS configuration of a dial to addr: the configured one, with ServerName
// set to addr's host when it is empty.
func (d *driver) clientTLS(addr string) *tls.Config {
	cfg := d.tls.Clone()
	if cfg.ServerName == "" {
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			host = addr
		}
		cfg.ServerName = host
	}
	return cfg
}

func (d *driver) newLink() *link {
	l := &link{d: d, id: d.nextID.Add(1)}
	l.cond.L = &l.mu
	return l
}

// eventKind says which core call an event is.
type eventKind uint8

const (
	eventAccepted eventKind = iota + 1
	eventFrame
	eventWritable
	eventClosed
)

// event is one call into the core that a connection's goroutine posted.
type event struct {
	kind  eventKind
	link  *link
	frame []byte // eventFrame
	err   error  // eventClosed
}

// post queues e and arms the drain callback unless it is armed.
func (d *driver) post(e event) {
	d.qmu.Lock()
	d.events = append(d.events, e)
	arm := !d.armed
	d.armed = true
	d.qmu.Unlock()
	if arm {
		d.clock.AfterFunc(0, labelRecv, d.drain)
	}
}

// drain hands the queued events to the core, in order, in engine context. Only one drain is armed
// or running at a time, so the events reach the core one at a time. It hands over at most
// drainBatch events and leaves the rest, and those posted meanwhile, to another callback, so a
// busy network does not hold a worker indefinitely. Once the engine stops, the callback no longer
// runs: the core has shut down by then.
func (d *driver) drain() {
	d.qmu.Lock()
	n := min(len(d.events), drainBatch)
	batch := d.events[:n:n]
	d.events = d.events[n:]
	d.qmu.Unlock()
	core := d.core.Load()
	for i := range batch {
		e := &batch[i]
		switch e.kind {
		case eventAccepted:
			core.Accepted(e.link)
		case eventFrame:
			core.Frame(e.link, e.frame)
			e.link.taken(len(e.frame))
		case eventWritable:
			core.Writable(e.link)
		case eventClosed:
			core.Closed(e.link, e.err)
		}
		*e = event{}
	}
	d.qmu.Lock()
	if len(d.events) == 0 {
		d.events = nil
		d.armed = false
		d.qmu.Unlock()
		return
	}
	d.qmu.Unlock()
	d.clock.AfterFunc(0, labelRecv, d.drain)
}

// link is one TCP connection: a remoting.Link. Its writer goroutine writes the frames the core
// hands it, from a buffer of at most bufLimit bytes; its reader goroutine cuts the stream into
// frames. Every event about the link is posted under its mutex, and none after Closed, so the core
// gets them in order and Closed last.
type link struct {
	d  *driver
	id uint64
	// ctx is cancelled by Close and by the driver's stop: it ends a dial and a TLS handshake.
	ctx    context.Context
	cancel context.CancelFunc
	// handshook, for an accepted TLS connection, is closed once its reader's handshake ended;
	// the writer writes nothing before, so it never waits on a handshake.
	handshook chan struct{}

	mu       sync.Mutex
	cond     sync.Cond // on mu: signals the writer
	buf      [][]byte  // frames taken and not yet written
	size     int       // bytes in buf
	refused  bool      // Write refused a frame: Writable is due once there is room
	closing  bool      // Close ran: write buf, then close
	ended    bool      // the connection failed or ended: nothing more is written
	reported bool      // Closed was posted: nothing more is posted
	pending  int       // bytes of frames posted that the core has not taken yet
}

func (l *link) ID() uint64 { return l.id }

// Write takes frame unless the buffer holds frames and frame does not fit in it. A link that is
// closing or ended takes every frame and drops it.
func (l *link) Write(frame []byte) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closing || l.ended {
		return true
	}
	if len(l.buf) > 0 && l.size+len(frame) > l.d.bufLimit {
		l.refused = true
		return false
	}
	l.buf = append(l.buf, frame)
	l.size += len(frame)
	l.cond.Broadcast() // the reader may wait on cond too
	return true
}

// Close makes the writer write the frames taken so far, then close the connection's writing side;
// the reader closes the connection once the peer has closed its side, or after closeLinger. A
// dial or a TLS handshake still in progress is abandoned.
func (l *link) Close() {
	l.mu.Lock()
	if l.closing {
		l.mu.Unlock()
		return
	}
	l.closing = true
	l.cond.Broadcast()
	l.mu.Unlock()
	l.cancel()
}

// taken tells the reader that the core has taken n bytes of its frames.
func (l *link) taken(n int) {
	l.mu.Lock()
	l.pending -= n
	l.cond.Broadcast()
	l.mu.Unlock()
}

// post posts frame, then waits while the frames the core has not taken exceed inLimit bytes,
// unless the link is closing or ended.
func (l *link) post(frame []byte) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.reported {
		return
	}
	l.pending += len(frame)
	l.d.post(event{kind: eventFrame, link: l, frame: frame})
	for l.pending > l.d.inLimit && !l.closing && !l.ended {
		l.cond.Wait()
	}
}

// report posts e unless Closed was posted already.
func (l *link) report(e event) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.reported {
		return
	}
	if e.kind == eventClosed {
		l.reported = true
		l.ended = true
		l.buf, l.size = nil, 0
		l.cond.Broadcast()
		l.d.untrack(l)
	}
	l.d.post(e)
}

// end posts Closed for err, nil when the peer closed the connection cleanly, and closes conn.
func (l *link) end(conn net.Conn, err error) {
	if errors.Is(err, io.EOF) {
		err = nil
	}
	l.report(event{kind: eventClosed, link: l, err: err})
	_ = conn.Close()
}

// dial connects l to addr, with TLS when configured, then reads and writes it. A dial or a
// handshake that fails ends the link: the core takes its session down with reason io.
func (l *link) dial(addr string) {
	defer l.d.wg.Done()
	defer l.cancel()
	conn, err := l.d.dialer.DialContext(l.ctx, "tcp", addr)
	if err == nil && l.d.tls != nil {
		tc := tls.Client(conn, l.d.clientTLS(addr))
		ctx, cancel := context.WithTimeout(l.ctx, l.d.dialTimeout)
		err = tc.HandshakeContext(ctx)
		cancel()
		if err != nil {
			_ = conn.Close()
		} else {
			conn = tc
		}
	}
	if err != nil {
		l.report(event{kind: eventClosed, link: l, err: err})
		return
	}
	// This goroutine is counted, so the count is above zero and Add may race with stop's Wait.
	l.d.wg.Add(1)
	go l.read(conn)
	l.write(conn)
}

// read cuts conn's stream into frames and posts each one until the connection ends (REM-385). A
// length prefix out of range is posted as it is, which the core answers with CLOSE protocol;
// what follows it is dropped.
func (l *link) read(conn net.Conn) {
	defer l.d.wg.Done()
	if l.handshook != nil {
		err := l.handshake(conn)
		close(l.handshook)
		if err != nil {
			l.end(conn, err)
			return
		}
	}
	r := bufio.NewReaderSize(conn, readBuffer)
	var prefix [4]byte
	for {
		if _, err := io.ReadFull(r, prefix[:]); err != nil {
			l.end(conn, err)
			return
		}
		if _, _, err := remoting.SplitFrame(prefix[:], l.d.maxFrame); err != nil {
			l.post(slices.Clone(prefix[:]))
			_, err := io.Copy(io.Discard, r)
			l.end(conn, err)
			return
		}
		frame, err := readBody(r, prefix, int(binary.BigEndian.Uint32(prefix[:])))
		if err != nil {
			l.end(conn, err)
			return
		}
		l.post(frame)
	}
}

// handshake runs the server side of an accepted TLS connection's handshake, within DialTimeout,
// and abandons it when the link closes or the driver stops.
func (l *link) handshake(conn net.Conn) error {
	tc, ok := conn.(*tls.Conn)
	if !ok {
		return nil
	}
	ctx, cancel := context.WithTimeout(l.ctx, l.d.dialTimeout)
	defer cancel()
	return tc.HandshakeContext(ctx)
}

// readBody reads the n bytes after prefix and returns the whole frame. It allocates as the bytes
// arrive, at most readBuffer ahead of them, so a peer cannot make it allocate a large frame it
// never sends.
func readBody(r io.Reader, prefix [4]byte, n int) ([]byte, error) {
	total := len(prefix) + n
	frame := make([]byte, 0, min(total, readBuffer))
	frame = append(frame, prefix[:]...)
	for len(frame) < total {
		k := min(total-len(frame), readBuffer)
		frame = slices.Grow(frame, k)
		m, err := io.ReadFull(r, frame[len(frame):len(frame)+k])
		frame = frame[:len(frame)+m]
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = io.ErrUnexpectedEOF
			}
			return nil, err
		}
	}
	return frame, nil
}

// write writes the frames the link takes, in order, each batch within the write deadline, until
// the link closes or ends; on an accepted TLS connection it first waits for the handshake to end.
// A refused frame makes it post Writable once it has taken the buffer. A write error ends the
// link. After Close it closes the connection's writing side and gives the reader closeLinger to
// see the peer close its side.
func (l *link) write(conn net.Conn) {
	if l.handshook != nil {
		<-l.handshook
	}
	l.mu.Lock()
	for {
		for len(l.buf) == 0 && !l.closing && !l.ended {
			l.cond.Wait()
		}
		if l.ended || len(l.buf) == 0 {
			break
		}
		batch := net.Buffers(l.buf)
		l.buf, l.size = nil, 0
		if l.refused {
			l.refused = false
			l.d.post(event{kind: eventWritable, link: l})
		}
		l.mu.Unlock()
		err := conn.SetWriteDeadline(time.Now().Add(l.d.writeWait))
		if err == nil {
			_, err = batch.WriteTo(conn)
		}
		if err != nil {
			l.end(conn, err)
			return
		}
		l.mu.Lock()
	}
	ended := l.ended
	l.mu.Unlock()
	if ended {
		_ = conn.Close()
		return
	}
	cw, ok := conn.(interface{ CloseWrite() error })
	if !ok || conn.SetWriteDeadline(time.Now().Add(l.d.writeWait)) != nil || cw.CloseWrite() != nil ||
		conn.SetReadDeadline(time.Now().Add(closeLinger)) != nil {
		_ = conn.Close()
	}
}
