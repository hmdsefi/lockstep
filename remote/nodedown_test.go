// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package remote_test

import (
	"errors"
	"net"
	"testing"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/internal/hooks"
	"github.com/hmdsefi/lockstep/internal/remoting"
	"github.com/hmdsefi/lockstep/remote"
)

// CLU §4.1: hooks.NodeDown of a peer that is in fact up, as after a false positive of the
// membership protocol, ends a's watch of the peer's actor with Failed(reason) and writes the peer
// an UNWATCH with the watch's number, so that the peer drops the registration. The core keeps no
// outgoing watch either: when the session is lost and a dials r again for a MSG, the new session
// writes no WATCH ahead of it (REM-284).
func TestNodeDownWritesUnwatch(t *testing.T) {
	ln := rawListener(t)
	a := startNode(t, "a", newRegistry(), func(c *remote.Config) { c.Peers = map[string]string{"r": ln.Addr().String()} })
	terms := make(chan lockstep.Terminated, 1)
	d := spawn[do](t, a.sys, "d", doer{terms: terms})
	x := lockstep.PIDAt[Note]("r", "/user/x").Address()
	errDown := errors.New("declared down")

	runIn(t, a.sys, d, func(ctx *lockstep.Context[do]) {
		if err := lockstep.Watch(ctx, x); err != nil {
			t.Error(err)
		}
	})
	out := acceptRaw(t, ln)
	out.welcome("a")
	_, f := out.expect(remoting.KindWatch)
	n, err := remoting.ParseWatch(f.Payload)
	if err != nil || f.Sender != d.String() || f.Recipient != x.String() {
		t.Fatalf("WATCH %s -> %s #%d, %v; want %v -> %v", f.Sender, f.Recipient, n, err, d, x)
	}

	t.Cleanup(func() { hooks.NodeUp(a.sys, "r") })
	runIn(t, a.sys, d, func(*lockstep.Context[do]) { hooks.NodeDown(a.sys, "r", errDown) })
	_, f = out.expect(remoting.KindUnwatch)
	if m, err := remoting.ParseWatch(f.Payload); err != nil || m != n || f.Sender != d.String() || f.Recipient != x.String() {
		t.Errorf("UNWATCH %s -> %s #%d, %v; want %v -> %v #%d", f.Sender, f.Recipient, m, err, d, x, n)
	}
	if got := recv(t, terms); got.Addr != x || got.Reason.Kind != lockstep.StopFailed || !errors.Is(got.Reason.Err, errDown) {
		t.Errorf("Terminated %+v, want %v failed with %v", got, x, errDown)
	}

	// r resets the session, which a logs; a dials again only for the MSG, and writes it first.
	if tc, ok := out.conn.(*net.TCPConn); ok {
		_ = tc.SetLinger(0)
	}
	_ = out.conn.Close()
	waitLogs(t, a, "remoting: link closed", map[string]string{"peer": "r"}, 1)
	if err := lockstep.Send(a.sys, lockstep.PIDAt[Note]("r", "/user/x"), Note{N: 1}); err != nil {
		t.Fatal(err)
	}
	out = acceptRaw(t, ln)
	out.welcome("a")
	out.expect(remoting.KindMsg)
}
