// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package remote_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/codec"
	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/hooks"
	"github.com/hmdsefi/lockstep/internal/remoting"
	"github.com/hmdsefi/lockstep/remote"
)

// sinkLink is a Link that keeps the frames written on it.
type sinkLink struct {
	id     uint64
	mu     *sync.Mutex
	frames *[]remoting.Frame
	wrote  chan struct{}
}

func (l *sinkLink) ID() uint64 { return l.id }

func (l *sinkLink) Write(b []byte) bool {
	f, err := remoting.ParseFrame(b, remoting.MaxFrameLimit)
	if err != nil {
		panic(err)
	}
	l.mu.Lock()
	*l.frames = append(*l.frames, f)
	l.mu.Unlock()
	select {
	case l.wrote <- struct{}{}:
	default:
	}
	return true
}

func (*sinkLink) Close() {}

// sinkDriver dials sinkLinks.
type sinkDriver struct{ link *sinkLink }

func (d sinkDriver) Dial(string, string) (remoting.Link, error) { return d.link, nil }

// lookupErrors is the Errors of the test core.
type lookupErrors struct{}

func (lookupErrors) Remote(node string, code uint8, msg string) error {
	return &remote.Error{Node: node, Code: remote.Code(code), Message: msg}
}
func (lookupErrors) Unreachable(node string) error {
	return fmt.Errorf("%w: %s", remote.ErrUnreachable, node)
}
func (lookupErrors) PeerRestarted(node string) error { return remote.ErrPeerRestarted }
func (lookupErrors) FrameTooLarge(n, m int) error {
	return fmt.Errorf("%w: %d > %d", remote.ErrFrameTooLarge, n, m)
}
func (lookupErrors) Code(error) uint8 { return 0 }

// coreTransport is the system's transport: the core, once it is built.
type coreTransport struct{ core *remoting.Core }

func (t *coreTransport) Deliver(env engine.Envelope) error { return t.core.Deliver(env) }

// remoteSystem returns a system of node a with a remoting core registered for it, whose driver
// keeps what the core writes to node b.
func remoteSystem(t *testing.T) (*lockstep.System, *remoting.Core, *sinkLink) {
	t.Helper()
	sys := newSystem(t)
	tr := &coreTransport{}
	att, err := hooks.AttachTransport(sys, tr, func() { tr.core.Shutdown() })
	if err != nil {
		t.Fatal(err)
	}
	link := &sinkLink{id: 1, mu: &sync.Mutex{}, frames: new([]remoting.Frame), wrote: make(chan struct{}, 1)}
	core, err := remoting.New(remoting.Config{
		Node: att.Node, Incarnation: att.Incarnation, Clock: att.Clock, Entropy: att.Entropy, Host: att.Host,
		System: sys, Driver: sinkDriver{link}, Errors: lookupErrors{}, DeadLetter: att.DeadLetter,
		Settings: remoting.Settings{
			Registry: codec.Default(), Codec: codec.JSON{}, MaxFrame: remote.DefaultMaxFrame, OutboundQueue: 16,
			Heartbeat: time.Hour, IdleTimeout: 2 * time.Hour, DialTimeout: time.Hour, RedialMin: time.Second, RedialMax: time.Second,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	tr.core = core
	if !remoting.Register(core) {
		t.Fatal("Register failed")
	}
	return sys, core, link
}

// welcome dials b and brings the core's outgoing session up.
func welcome(core *remoting.Core, link *sinkLink) {
	if err := core.Connect("b"); err != nil {
		panic(err)
	}
	core.Frame(link, remoting.AppendFrame(nil, &remoting.Frame{Kind: remoting.KindWelcome, Payload: remoting.AppendHello(nil, &remoting.Hello{
		Sender: "b", Receiver: "a", Incarnation: 1, FrameLimit: remoting.MinFrameLimit, Codec: "json",
	})}))
}

// lookupFrame waits for the LOOKUP to recipient that the core writes and returns it.
func lookupFrame(t *testing.T, link *sinkLink, recipient string) remoting.Frame {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		link.mu.Lock()
		for _, f := range *link.frames {
			if f.Kind == remoting.KindLookup && f.Recipient == recipient {
				link.mu.Unlock()
				return f
			}
		}
		link.mu.Unlock()
		select {
		case <-link.wrote:
		case <-deadline:
			t.Fatalf("no LOOKUP to %s written", recipient)
		}
	}
}

// answer hands the core f, numbered seq, on b's incoming session, which the first call opens.
func answer(core *remoting.Core, seq uint64, f remoting.Frame) {
	if seq == 1 {
		core.Accepted(incoming)
		core.Frame(incoming, remoting.AppendFrame(nil, &remoting.Frame{Kind: remoting.KindHello, Payload: remoting.AppendHello(nil, &remoting.Hello{
			Sender: "b", Receiver: "a", Incarnation: 1, FrameLimit: remoting.MinFrameLimit, Codec: "json",
		})}))
	}
	f.Seq = seq
	core.Frame(incoming, remoting.AppendFrame(nil, &f))
}

// incoming is b's incoming link.
var incoming = &sinkLink{id: 2, mu: &sync.Mutex{}, frames: new([]remoting.Frame), wrote: make(chan struct{}, 1)}

// TestLookupRemoteNode checks Lookup on another node: it sends LOOKUP with M's type key from a
// reply address of the system, and the answer, a lockstep.Address, becomes the PID with its uid;
// a FAIL becomes the *Error of its code.
func TestLookupRemoteNode(t *testing.T) {
	sys, core, link := remoteSystem(t)
	welcome(core, link)
	type result struct {
		pid lockstep.PID[ping]
		err error
	}
	run := func(path string) chan result {
		ch := make(chan result, 1)
		go func() {
			pid, err := remote.Lookup[ping](context.Background(), sys, "b", path)
			ch <- result{pid, err}
		}()
		return ch
	}

	ch := run("/user/p")
	f := lookupFrame(t, link, "lockstep://b/user/p#0")
	if f.Type != "github.com/hmdsefi/lockstep/remote_test.ping" {
		t.Errorf("LOOKUP type %q, want the type key of ping", f.Type)
	}
	var a lockstep.Address
	if err := a.UnmarshalText([]byte("lockstep://b/user/p#2a")); err != nil {
		t.Fatal(err)
	}
	name, data, err := codec.Default().Encode(codec.JSON{}, a)
	if err != nil {
		t.Fatal(err)
	}
	answer(core, 1, remoting.Frame{Kind: remoting.KindReply, Sender: a.String(), Recipient: f.Sender, Type: name, Payload: data})
	if r := <-ch; r.err != nil || r.pid.Address() != a {
		t.Errorf("Lookup = %v, %v; want %s", r.pid, r.err, a)
	}

	ch = run("/user/none")
	f = lookupFrame(t, link, "lockstep://b/user/none#0")
	answer(core, 2, remoting.Frame{Kind: remoting.KindFail, Sender: f.Recipient, Recipient: f.Sender,
		Payload: remoting.AppendFailure(nil, &remoting.Failure{Code: uint8(remote.CodeNotFound), Message: "not found"})})
	r := <-ch
	var re *remote.Error
	if !errors.As(r.err, &re) || re.Code != remote.CodeNotFound || !errors.Is(r.err, lockstep.ErrNotFound) {
		t.Errorf("Lookup of a missing path = %v, want a *remote.Error of CodeNotFound", r.err)
	}
}

// TestLookupRemoteTimeout checks that Lookup on another node waits no longer than ctx's deadline:
// the ask times out with lockstep.ErrTimeout, and a deadline that has passed returns
// context.DeadlineExceeded without sending.
func TestLookupRemoteTimeout(t *testing.T) {
	sys, core, link := remoteSystem(t)
	welcome(core, link)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := remote.Lookup[ping](ctx, sys, "b", "/user/p"); !errors.Is(err, lockstep.ErrTimeout) && !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Lookup = %v, want a timeout", err)
	}
	past, cancel2 := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel2()
	if _, err := remote.Lookup[ping](past, sys, "b", "/user/q"); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Lookup with a passed deadline = %v, want context.DeadlineExceeded", err)
	}
}
