// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package remote_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/codec"
	"github.com/hmdsefi/lockstep/internal/remoting"
	"github.com/hmdsefi/lockstep/remote"
)

// The message types that cross nodes in these tests, registered in newRegistry.
type (
	Note struct {
		N     int
		Items []int
	}
	Echo struct {
		Text  string
		Reply lockstep.ReplyTo[string]
	}
	Seq  struct{ N int }
	Halt struct{}
	// unsupported has a field the JSON codec cannot encode.
	unsupported struct{ F func() }
)

// newRegistry returns a private registry with the test message types.
func newRegistry() *codec.Registry {
	r := codec.NewRegistry()
	codec.RegisterIn[Note](r, "remotetest.Note")
	codec.RegisterIn[Echo](r, "remotetest.Echo")
	codec.RegisterIn[Seq](r, "remotetest.Seq")
	codec.RegisterIn[Halt](r, "remotetest.Halt")
	return r
}

// logRecord is one record a capture kept: its message and its attributes as text.
type logRecord struct {
	msg   string
	attrs map[string]string
}

// capture is a slog.Handler, at every level, that keeps every record.
type capture struct {
	mu    *sync.Mutex
	recs  *[]logRecord
	attrs []slog.Attr
}

func newCapture() *capture { return &capture{mu: &sync.Mutex{}, recs: new([]logRecord)} }

func (*capture) Enabled(context.Context, slog.Level) bool { return true }

func (c *capture) Handle(_ context.Context, r slog.Record) error {
	rec := logRecord{msg: r.Message, attrs: make(map[string]string)}
	for _, a := range c.attrs {
		rec.attrs[a.Key] = a.Value.String()
	}
	r.Attrs(func(a slog.Attr) bool {
		rec.attrs[a.Key] = a.Value.String()
		return true
	})
	c.mu.Lock()
	*c.recs = append(*c.recs, rec)
	c.mu.Unlock()
	return nil
}

func (c *capture) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &capture{mu: c.mu, recs: c.recs, attrs: append(append([]slog.Attr(nil), c.attrs...), attrs...)}
}

func (c *capture) WithGroup(string) slog.Handler { return c }

// find returns the records with message msg whose attributes hold every key and value of want.
func (c *capture) find(msg string, want map[string]string) []logRecord {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []logRecord
	for _, r := range *c.recs {
		ok := r.msg == msg
		for k, v := range want {
			ok = ok && r.attrs[k] == v
		}
		if ok {
			out = append(out, r)
		}
	}
	return out
}

// node is a system with remoting enabled on loopback.
type node struct {
	sys  *lockstep.System
	log  *capture
	addr string // the address it listens on
}

// newNode returns a system named name, with a capturing logger, that shuts down when the test
// ends.
func newNode(t *testing.T, name string) *node {
	t.Helper()
	log := newCapture()
	sys := lockstep.New(lockstep.WithNodeName(name), lockstep.WithLogger(slog.New(log)))
	t.Cleanup(func() { shutdown(t, sys) })
	return &node{sys: sys, log: log}
}

// startNode returns a node named name with remoting enabled on 127.0.0.1:0 with reg, short redial
// delays and whatever edit changes.
func startNode(t *testing.T, name string, reg *codec.Registry, edit func(*remote.Config)) *node {
	t.Helper()
	n := newNode(t, name)
	c := remote.Config{Listen: "127.0.0.1:0", Registry: reg, RedialMin: 10 * time.Millisecond, RedialMax: 100 * time.Millisecond}
	if edit != nil {
		edit(&c)
	}
	n.enable(t, c)
	return n
}

// enable enables remoting on n with c and learns the address it listens on.
func (n *node) enable(t *testing.T, c remote.Config) {
	t.Helper()
	if err := remote.Enable(n.sys, c); err != nil {
		t.Fatalf("Enable on %s: %v", n.sys.Node(), err)
	}
	recs := n.log.find("remote: listening", map[string]string{"node": n.sys.Node()})
	if len(recs) != 1 {
		t.Fatalf("Enable on %s logged %d listening records, want 1", n.sys.Node(), len(recs))
	}
	n.addr = recs[0].attrs["addr"]
}

func shutdown(t *testing.T, sys *lockstep.System) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := sys.Shutdown(ctx); err != nil {
		t.Errorf("Shutdown of %s: %v", sys.Node(), err)
	}
}

// spawn spawns an actor named name with a large mailbox.
func spawn[M any](t *testing.T, sys *lockstep.System, name string, a lockstep.Actor[M]) lockstep.PID[M] {
	t.Helper()
	pid, err := lockstep.Spawn(sys, name, func() lockstep.Actor[M] { return a }, lockstep.WithMailbox(4096, lockstep.FailSend))
	if err != nil {
		t.Fatal(err)
	}
	return pid
}

// forward hands every message it receives to ch.
type forward[M any] struct{ ch chan<- M }

func (f forward[M]) Receive(_ *lockstep.Context[M], m M) { f.ch <- m }

// echo answers an Echo with its text and the actor's tag.
type echo struct{ tag string }

func (e echo) Receive(ctx *lockstep.Context[Echo], m Echo) { m.Reply.Send(ctx, m.Text+" from "+e.tag) }

// halter stops when it receives Halt.
type halter struct{}

func (halter) Receive(ctx *lockstep.Context[Halt], _ Halt) { ctx.Stop() }

// watchCmd makes a watcher watch addr and, with halt, then send Halt to addr's path.
type watchCmd struct {
	addr lockstep.Address
	halt bool
}

// watcher watches what it is told to and hands every Terminated it gets to ch.
type watcher struct{ ch chan<- lockstep.Terminated }

func (w watcher) Receive(ctx *lockstep.Context[watchCmd], cmd watchCmd) {
	if err := lockstep.Watch(ctx, cmd.addr); err != nil {
		panic(err)
	}
	if cmd.halt {
		if err := lockstep.Send(ctx, lockstep.PIDAt[Halt](cmd.addr.Node(), cmd.addr.Path()), Halt{}); err != nil {
			panic(err)
		}
	}
}

func (w watcher) Signal(_ *lockstep.Context[watchCmd], sig lockstep.Signal) {
	if term, ok := sig.(lockstep.Terminated); ok {
		w.ch <- term
	}
}

// recv returns the next value of ch, or fails the test after 10 s.
func recv[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(10 * time.Second):
		var zero T
		t.Fatalf("no %T within 10 s", zero)
		return zero
	}
}

// ask asks the echo actor on node b of from with text and returns the answer.
func ask(from *lockstep.System, text string, timeout time.Duration) *lockstep.Reply[string] {
	return lockstep.Ask(from, lockstep.PIDAt[Echo]("b", "/user/echo"), func(rt lockstep.ReplyTo[string]) Echo {
		return Echo{Text: text, Reply: rt}
	}, lockstep.WithTimeout(timeout))
}

// directory is a Config.Resolve whose answers a test changes.
type directory struct {
	mu    sync.Mutex
	addrs map[string]string
}

func (d *directory) set(node, addr string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.addrs == nil {
		d.addrs = make(map[string]string)
	}
	d.addrs[node] = addr
}

func (d *directory) resolve(node string) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if a, ok := d.addrs[node]; ok {
		return a, nil
	}
	return "", fmt.Errorf("directory: no address for %q", node)
}

// TestATREM02SendAndAsk checks two systems over loopback TCP: a Send arrives as a decoded copy of
// the message, and an Ask is answered. B knows no address of A: it answers over the session it
// dials to the address A announced in its HELLO.
func TestATREM02SendAndAsk(t *testing.T) {
	reg := newRegistry()
	b := startNode(t, "b", reg, nil)
	a := startNode(t, "a", reg, func(c *remote.Config) { c.Peers = map[string]string{"b": b.addr} })
	notes := make(chan Note, 1)
	spawn[Note](t, b.sys, "notes", forward[Note]{notes})
	spawn[Echo](t, b.sys, "echo", echo{tag: "b"})

	items := []int{1, 2, 3}
	if err := lockstep.Send(a.sys, lockstep.PIDAt[Note]("b", "/user/notes"), Note{N: 7, Items: items}); err != nil {
		t.Fatal(err)
	}
	items[0] = 100 // the message was encoded when it was sent
	got := recv(t, notes)
	if got.N != 7 || len(got.Items) != 3 || got.Items[0] != 1 || got.Items[2] != 3 {
		t.Errorf("received %+v, want {N:7 Items:[1 2 3]}", got)
	}
	if &got.Items[0] == &items[0] {
		t.Error("the received message shares the sender's slice")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	v, err := ask(a.sys, "hi", 10*time.Second).Await(ctx)
	if err != nil || v != "hi from b" {
		t.Errorf("Ask = %q, %v; want %q", v, err, "hi from b")
	}
}

// longName is the JSON codec under a name longer than HELLO can carry.
type longName struct{ codec.JSON }

func (longName) Name() string { return strings.Repeat("j", 65) }

// TestATREM03EnableErrors checks Enable's errors in their order (REM-010): already enabled,
// Validate, Listen required, the registry's check, the codec name and the advertise address that
// HELLO must carry, the listen error, and a stopped system. Each case's config also breaks every
// later check, so the error shows which check runs first. After any of them nothing is open, and
// the system can still be enabled.
func TestATREM03EnableErrors(t *testing.T) {
	reg := newRegistry()
	bad := codec.NewRegistry()
	codec.RegisterIn[unsupported](bad, "remotetest.Unsupported")
	checkErr := bad.Check(codec.JSON{})
	if checkErr == nil {
		t.Fatal("the registry with a func field passed Check")
	}
	busy := rawListener(t)

	enabled := startNode(t, "e", reg, nil)
	if err := remote.Enable(enabled.sys, remote.Config{MaxFrame: 1}); !errors.Is(err, remote.ErrAlreadyEnabled) {
		t.Errorf("second Enable = %v, want ErrAlreadyEnabled", err)
	}

	n := newNode(t, "a")
	for _, tc := range []struct {
		name string
		cfg  remote.Config
		want string // the error text, or its prefix when it ends with ": "
	}{
		{"Validate", remote.Config{MaxFrame: 1, Registry: bad},
			"remote: invalid config: MaxFrame 1 out of range [4096, 67108864]"},
		{"ListenRequired", remote.Config{Registry: bad}, "remote: Listen is required"},
		{"RegistryCheck", remote.Config{Registry: bad, Codec: longName{}, Advertise: strings.Repeat("x", 256), Listen: busy.Addr().String()},
			bad.Check(longName{}).Error()},
		{"CodecName", remote.Config{Registry: reg, Codec: longName{}, Advertise: strings.Repeat("x", 256), Listen: busy.Addr().String()},
			`remote: codec name "` + longName{}.Name() + `" is not 1 to 64 bytes`},
		{"Advertise", remote.Config{Registry: reg, Advertise: strings.Repeat("x", 256), Listen: busy.Addr().String()},
			"remote: advertise address of 256 bytes exceeds 255"},
		{"Listen", remote.Config{Registry: reg, Listen: busy.Addr().String()}, "remote: listen " + busy.Addr().String() + ": "},
	} {
		err := remote.Enable(n.sys, tc.cfg)
		switch {
		case err == nil:
			t.Errorf("%s: Enable succeeded", tc.name)
		case strings.HasSuffix(tc.want, ": ") && !strings.HasPrefix(err.Error(), tc.want),
			!strings.HasSuffix(tc.want, ": ") && err.Error() != tc.want:
			t.Errorf("%s: Enable = %q, want %q", tc.name, err, tc.want)
		}
	}
	// None of the failures left anything behind: the system enables now.
	n.enable(t, remote.Config{Listen: "127.0.0.1:0", Registry: reg})

	// On a stopped system the attach fails after the listen: the listener is closed again.
	stopped := newNode(t, "s")
	shutdown(t, stopped.sys)
	free := rawListener(t)
	addr := free.Addr().String()
	_ = free.Close()
	if err := remote.Enable(stopped.sys, remote.Config{Listen: addr, Registry: reg}); !errors.Is(err, lockstep.ErrStopped) {
		t.Errorf("Enable on a stopped system = %v, want ErrStopped", err)
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Errorf("the failed Enable left %s open: %v", addr, err)
	} else {
		_ = ln.Close()
	}
}

// TestATCOD14UnsupportedTypeFailsEnable checks the production half of AT-COD-14: a registered
// type that the codec cannot encode makes Enable fail with the registry check's error, before
// anything listens.
func TestATCOD14UnsupportedTypeFailsEnable(t *testing.T) {
	bad := newRegistry()
	codec.RegisterIn[unsupported](bad, "remotetest.Unsupported")
	want := bad.Check(codec.JSON{})
	if want == nil || !strings.Contains(want.Error(), "remotetest.Unsupported") && !strings.Contains(want.Error(), "unsupported") {
		t.Fatalf("Check = %v, want an error naming the unsupported type", want)
	}
	n := newNode(t, "a")
	for _, c := range []codec.Codec{nil, codec.JSON{}} {
		err := remote.Enable(n.sys, remote.Config{Listen: "127.0.0.1:0", Registry: bad, Codec: c})
		if err == nil || err.Error() != want.Error() {
			t.Errorf("Enable with codec %v = %v, want %v", c, err, want)
		}
	}
	n.enable(t, remote.Config{Listen: "127.0.0.1:0", Registry: newRegistry()})
}

// TestATREM09FIFO checks that 1,000 messages from one system to an actor on another arrive once
// each and in order: with the default limits; with a connection buffer so small that every frame
// waits for the one before it to be written (back-pressure through Writable); and with an inbound
// limit so small that the reader waits for the core to take every frame before it reads the next.
func TestATREM09FIFO(t *testing.T) {
	for _, tc := range []struct {
		name            string
		buffer, inbound int
	}{{"Default", 0, 0}, {"BackPressure", 1, 0}, {"InboundBackPressure", 0, 1}} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.buffer > 0 {
				defer remote.SetLinkBuffer(tc.buffer)()
			}
			if tc.inbound > 0 {
				defer remote.SetInboundLimit(tc.inbound)()
			}
			reg := newRegistry()
			b := startNode(t, "b", reg, nil)
			a := startNode(t, "a", reg, func(c *remote.Config) {
				c.Peers = map[string]string{"b": b.addr}
				c.OutboundQueue = 4096
			})
			seqs := make(chan Seq, 2048)
			spawn[Seq](t, b.sys, "seq", forward[Seq]{seqs})
			to := lockstep.PIDAt[Seq]("b", "/user/seq")
			const n = 1000
			for i := range n {
				if err := lockstep.Send(a.sys, to, Seq{N: i}); err != nil {
					t.Fatalf("Send %d: %v", i, err)
				}
			}
			if err := lockstep.Send(a.sys, to, Seq{N: -1}); err != nil {
				t.Fatal(err)
			}
			for i := range n {
				if got := recv(t, seqs); got.N != i {
					t.Fatalf("message %d is %d", i, got.N)
				}
			}
			if got := recv(t, seqs); got.N != -1 {
				t.Fatalf("after the 1,000 messages came %d, want the end marker", got.N)
			}
		})
	}
}

// TestATREM14WatchRemote checks remote watches over TCP: a watch of an actor that is not alive on
// the other node gives Terminated with reason no-actor, and a watched actor that stops gives
// Terminated with its reason.
func TestATREM14WatchRemote(t *testing.T) {
	reg := newRegistry()
	b := startNode(t, "b", reg, nil)
	a := startNode(t, "a", reg, func(c *remote.Config) { c.Peers = map[string]string{"b": b.addr} })
	spawn[Halt](t, b.sys, "w", halter{})
	terms := make(chan lockstep.Terminated, 4)
	w := spawn[watchCmd](t, a.sys, "watcher", watcher{terms})

	missing := lockstep.PIDAt[Halt]("b", "/user/none").Address()
	if err := lockstep.Send(a.sys, w, watchCmd{addr: missing}); err != nil {
		t.Fatal(err)
	}
	if got := recv(t, terms); got.Addr != missing || got.Reason != lockstep.NoActor {
		t.Errorf("watch of a missing actor: %+v, want Terminated{%v, no-actor}", got, missing)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pid, err := remote.Lookup[Halt](ctx, a.sys, "b", "/user/w")
	if err != nil {
		t.Fatal(err)
	}
	// The WATCH and the Halt go over the same session, in this order.
	if err := lockstep.Send(a.sys, w, watchCmd{addr: pid.Address(), halt: true}); err != nil {
		t.Fatal(err)
	}
	if got := recv(t, terms); got.Addr != pid.Address() || got.Reason != lockstep.Normal {
		t.Errorf("watch across a stop: %+v, want Terminated{%v, normal}", got, pid.Address())
	}
}

// TestATREM22Shutdown checks that Shutdown writes CLOSE shutdown on every session, outgoing,
// incoming and one still waiting for its WELCOME, that the frames still queued become dead letters
// "shutdown", and that every goroutine of remoting has exited when Shutdown returns.
func TestATREM22Shutdown(t *testing.T) {
	leaks := goleak.IgnoreCurrent()
	reg := newRegistry()
	rLn, qLn := rawListener(t), rawListener(t)
	log := newCapture()
	sys := lockstep.New(lockstep.WithNodeName("a"), lockstep.WithLogger(slog.New(log)))
	t.Cleanup(func() { shutdown(t, sys) })
	a := &node{sys: sys, log: log}
	a.enable(t, remote.Config{
		Listen: "127.0.0.1:0", Registry: reg, DialTimeout: time.Minute,
		Peers: map[string]string{"r": rLn.Addr().String(), "q": qLn.Addr().String()},
	})

	// r's incoming session to a.
	in, err := net.Dial("tcp", a.addr)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	writeFrame(t, in, helloFrame(remoting.KindHello, "r", "a", ""))
	mustRead(t, in, remoting.KindWelcome)

	// a's outgoing session to r, which carries a message.
	if err := lockstep.Send(sys, lockstep.PIDAt[Note]("r", "/user/x"), Note{N: 1}); err != nil {
		t.Fatal(err)
	}
	out := acceptWithin(t, rLn, 10*time.Second)
	if out == nil {
		t.Fatal("a did not dial r")
	}
	mustRead(t, out, remoting.KindHello)
	writeFrame(t, out, helloFrame(remoting.KindWelcome, "r", "a", ""))
	mustRead(t, out, remoting.KindMsg)

	// a's session to q never gets its WELCOME, so the messages to q stay queued.
	for i := range 3 {
		if err := lockstep.Send(sys, lockstep.PIDAt[Note]("q", "/user/x"), Note{N: i}); err != nil {
			t.Fatal(err)
		}
	}
	pending := acceptWithin(t, qLn, 10*time.Second)
	if pending == nil {
		t.Fatal("a did not dial q")
	}
	mustRead(t, pending, remoting.KindHello)

	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		done <- sys.Shutdown(ctx)
	}()
	for _, c := range []struct {
		name string
		conn net.Conn
	}{{"outgoing", out}, {"incoming", in}, {"waiting for WELCOME", pending}} {
		f := mustRead(t, c.conn, remoting.KindClose)
		if string(f.Payload) != remoting.CloseShutdown {
			t.Errorf("%s session: CLOSE %q, want %q", c.name, f.Payload, remoting.CloseShutdown)
		}
		if _, err := readFrame(t, c.conn); !errors.Is(err, io.EOF) {
			t.Errorf("%s session: after CLOSE read %v, want EOF", c.name, err)
		}
		_ = c.conn.Close()
	}
	if err := <-done; err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if got := log.find("dead letter", map[string]string{"reason": "shutdown"}); len(got) != 3 {
		t.Errorf("%d dead letters with reason shutdown, want 3: %v", len(got), got)
	}
	goleak.VerifyNone(t, leaks)
}

// TestATREM23PeerRestart checks a peer restart over TCP: an ask in flight when b stops fails with
// an error matching ErrPeerRestarted and lockstep.ErrTerminated once a reaches b's new
// incarnation, which a dials again at its new address; then traffic resumes.
func TestATREM23PeerRestart(t *testing.T) {
	reg := newRegistry()
	var dir directory
	a := startNode(t, "a", reg, func(c *remote.Config) { c.Resolve = dir.resolve })
	b1 := startNode(t, "b", reg, nil)
	dir.set("b", b1.addr)
	spawn[Echo](t, b1.sys, "echo", echo{tag: "b1"})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if v, err := ask(a.sys, "x", 10*time.Second).Await(ctx); err != nil || v != "x from b1" {
		t.Fatalf("ask before the restart = %q, %v", v, err)
	}

	shutdown(t, b1.sys)
	lost := ask(a.sys, "lost", 20*time.Second)

	b2 := startNode(t, "b", reg, nil)
	if b2.sys.Incarnation() == b1.sys.Incarnation() {
		t.Skip("the new system drew the old incarnation")
	}
	spawn[Echo](t, b2.sys, "echo", echo{tag: "b2"})
	dir.set("b", b2.addr)

	// The lost ask's frame either waits for a redial or went out on the session b1 closed; a
	// message to b makes a dial to b2 either way.
	var err error
	for {
		if err = lockstep.Send(a.sys, lockstep.PIDAt[Note]("b", "/user/none"), Note{}); err != nil {
			t.Fatal(err)
		}
		short, stop := context.WithTimeout(ctx, 50*time.Millisecond)
		_, err = lost.Await(short)
		stop()
		if !errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
			break
		}
	}
	if !errors.Is(err, remote.ErrPeerRestarted) || !errors.Is(err, lockstep.ErrTerminated) {
		t.Fatalf("the ask in flight across the restart: %v, want ErrPeerRestarted and ErrTerminated", err)
	}
	if v, err := ask(a.sys, "y", 10*time.Second).Await(ctx); err != nil || v != "y from b2" {
		t.Errorf("ask after the restart = %q, %v; want %q", v, err, "y from b2")
	}
}

// TestATREM33AddressOrder checks that dialing is lazy and that a peer's address is the one in
// Peers, else the one Resolve returns, else the one the peer announced in its HELLO; with none, a
// message is a dead letter "unknown-node" and an ask fails with ErrUnreachable.
func TestATREM33AddressOrder(t *testing.T) {
	reg := newRegistry()
	viaPeers, viaResolveB, viaResolveC, viaHello := rawListener(t), rawListener(t), rawListener(t), rawListener(t)
	resolve := map[string]string{"b": viaResolveB.Addr().String(), "c": viaResolveC.Addr().String()}
	a := startNode(t, "a", reg, func(c *remote.Config) {
		c.Peers = map[string]string{"b": viaPeers.Addr().String()}
		c.Resolve = func(node string) (string, error) {
			if addr, ok := resolve[node]; ok {
				return addr, nil
			}
			return "", errors.New("no such node")
		}
	})

	if conn := acceptWithin(t, viaPeers, 200*time.Millisecond); conn != nil {
		t.Fatal("a dialed b before sending to it")
	}
	send := func(node string) {
		t.Helper()
		if err := lockstep.Send(a.sys, lockstep.PIDAt[Note](node, "/user/x"), Note{}); err != nil {
			t.Fatal(err)
		}
	}
	dialed := func(ln net.Listener, node string) {
		t.Helper()
		conn := acceptWithin(t, ln, 10*time.Second)
		if conn == nil {
			t.Fatalf("a did not dial %s at %s", node, ln.Addr())
		}
		f := mustRead(t, conn, remoting.KindHello)
		if h, err := remoting.ParseHello(f.Payload); err != nil || h.Receiver != node || h.Sender != "a" {
			t.Errorf("HELLO to %s: %+v, %v", node, h, err)
		}
	}

	send("b") // in Peers and resolvable: Peers wins
	dialed(viaPeers, "b")
	send("c") // resolvable only
	dialed(viaResolveC, "c")
	if conn := acceptWithin(t, viaResolveB, 100*time.Millisecond); conn != nil {
		t.Error("a dialed b at the address Resolve gives, not the one in Peers")
	}

	// d is in neither, but connects to a announcing its address.
	in, err := net.Dial("tcp", a.addr)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	writeFrame(t, in, helloFrame(remoting.KindHello, "d", "a", viaHello.Addr().String()))
	mustRead(t, in, remoting.KindWelcome)
	send("d")
	dialed(viaHello, "d")

	// e has no address at all.
	send("e")
	deadline := time.Now().Add(10 * time.Second)
	for len(a.log.find("dead letter", map[string]string{"reason": "unknown-node"})) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no dead letter unknown-node for the message to e")
		}
		time.Sleep(10 * time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r := lockstep.Ask(a.sys, lockstep.PIDAt[Echo]("e", "/user/echo"), func(rt lockstep.ReplyTo[string]) Echo {
		return Echo{Reply: rt}
	})
	if _, err := r.Await(ctx); !errors.Is(err, remote.ErrUnreachable) {
		t.Errorf("ask to a node without an address: %v, want ErrUnreachable", err)
	}
}
