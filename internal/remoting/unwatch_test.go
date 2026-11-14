// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package remoting

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/hooks"
)

// watchFrames returns the WATCH and UNWATCH frames written on l, as "<seq> <KIND> #<n>".
func watchFrames(l *fakeLink) []string {
	var out []string
	for _, f := range l.frames {
		if f.Kind == KindWatch || f.Kind == KindUnwatch {
			n, _ := ParseWatch(f.Payload)
			out = append(out, fmt.Sprintf("%d %s #%d", f.Seq, f.Kind, n))
		}
	}
	return out
}

// TestUnwatchResent checks that an UNWATCH that never reached the peer after its WATCH was written
// is written by the next session: one that became a dead letter queue-full or unknown-node. A pair
// that is watched again needs none, since its WATCH replaces the peer's registration, and neither
// does a watch whose WATCH was never written, one the peer ended with TERMINATED, or one of an
// incarnation that restarted.
func TestUnwatchResent(t *testing.T) {
	env := func(k hooks.RemoteKind, n uint64) engine.Envelope {
		return engine.Envelope{From: sender, To: target, Payload: hooks.RemotePayload{Kind: k, Seq: n}}
	}
	const deadUnwatch = "lockstep://a/user/x#1 -> lockstep://b/user/y#2 <nil>"
	t.Run("queue full", func(t *testing.T) {
		h := newSendHarness(t, func(c *Config) { c.Settings.OutboundQueue = 1 })
		h.deliver(env(hooks.RemoteWatch, 1))
		l := h.up()
		l.full = true
		h.deliver(msgEnv(ping{1}))             // stays queued: the link refuses it
		h.deliver(env(hooks.RemoteUnwatch, 1)) // queue-full
		same(t, "dead letters", h.takeDead(), []string{"queue-full " + deadUnwatch})
		h.core.Closed(l, nil) // the queued message makes the core dial again at once
		l2 := h.up()
		same(t, "first session", watchFrames(l), []string{"1 WATCH #1"})
		same(t, "second session", data(l2), []string{"UNWATCH seq=1", "MSG seq=2"})
		same(t, "second session", watchFrames(l2), []string{"1 UNWATCH #1"})
		// The UNWATCH was written: a third session writes nothing of the watch.
		h.core.Closed(l2, nil)
		h.deliver(msgEnv(ping{2}))
		same(t, "third session", data(h.up()), []string{"MSG seq=1"})
	})
	t.Run("unknown node", func(t *testing.T) {
		h := newSendHarness(t, nil)
		h.deliver(env(hooks.RemoteWatch, 1))
		l := h.up()
		h.core.Closed(l, nil) // the outgoing watch makes the core dial again at once
		h.drv.errs["b"] = fmt.Errorf("no address: %w", ErrUnknownNode)
		h.core.Closed(h.link(), errors.New("refused")) // a redial is armed
		h.deliver(env(hooks.RemoteUnwatch, 1))         // queued for the redial
		h.eng.Advance(h.core.set.RedialMax)            // the redial finds no address
		same(t, "dead letters", h.takeDead(), []string{"unknown-node " + deadUnwatch})
		delete(h.drv.errs, "b")
		h.deliver(msgEnv(ping{1}))
		same(t, "next session", data(h.up()), []string{"UNWATCH seq=1", "MSG seq=2"})
	})
	t.Run("watched again", func(t *testing.T) {
		h := newSendHarness(t, func(c *Config) { c.Settings.OutboundQueue = 1 })
		h.deliver(env(hooks.RemoteWatch, 1))
		l := h.up()
		l.full = true
		h.deliver(msgEnv(ping{1}))
		h.deliver(env(hooks.RemoteUnwatch, 1)) // queue-full
		h.deliver(env(hooks.RemoteWatch, 2))   // queue-full; the outgoing watch is 2
		h.takeDead()
		h.core.Closed(l, nil)
		same(t, "next session", watchFrames(h.up()), []string{"1 WATCH #2"})
	})
	t.Run("never written", func(t *testing.T) {
		h := newSendHarness(t, func(c *Config) { c.Settings.OutboundQueue = 1 })
		h.deliver(msgEnv(ping{1}))             // queued while dialing
		h.deliver(env(hooks.RemoteWatch, 1))   // queue-full
		h.deliver(env(hooks.RemoteUnwatch, 1)) // queue-full
		h.takeDead()
		same(t, "first session", data(h.up()), []string{"MSG seq=1"})
	})
	t.Run("ended by the peer", func(t *testing.T) {
		h := newWatchHarness(t)
		watched := engine.Address{Node: "b", Path: "/user/v", UID: 4}
		h.deliver(engine.Envelope{From: aY, To: watched, Payload: hooks.RemotePayload{Kind: hooks.RemoteWatch, Seq: 5}})
		l := h.outLink()
		h.peerFrame(KindTerminated, addrText(watched), addrText(aY), "", AppendTerminated(nil, &Terminated{Watch: 5, Kind: StopNormal}))
		h.core.Closed(l, nil)
		h.deliver(msgEnv(ping{1}))
		same(t, "next session", watchFrames(h.outLink()), nil)
	})
	t.Run("restart", func(t *testing.T) {
		h := newSendHarness(t, func(c *Config) { c.Settings.OutboundQueue = 1 })
		h.deliver(env(hooks.RemoteWatch, 1))
		l := h.up()
		l.full = true
		h.deliver(msgEnv(ping{1}))
		h.deliver(env(hooks.RemoteUnwatch, 1)) // queue-full
		h.core.Closed(l, nil)
		// The peer restarted: it holds no watch of this node any more.
		h.core.Frame(h.link(), welcome("b", "a", 8, "json"))
		same(t, "next session", watchFrames(h.link()), nil)
	})
}

// TestRemoteWatchRefusedWarnsOnce checks that the refusals of a peer's WATCH frames at the bound
// log one warning each time the peer reaches the bound, not one per frame.
func TestRemoteWatchRefusedWarnsOnce(t *testing.T) {
	h := newWatchHarness(t)
	h.core.maxWatchers = 1
	warnings := func() int { return strings.Count(h.log.String(), "remoting: watch refused") }
	h.watch(false, bX, aY, 1)
	h.watch(false, bX, aZ, 2) // refused: warns
	h.watch(false, bW, aY, 3) // refused: quiet
	h.watch(false, bW, aZ, 4) // refused: quiet
	if n := warnings(); n != 1 {
		t.Errorf("%d warnings for three refusals, want 1", n)
	}
	if n := len(terminated(t, h.outLink())); n != 3 {
		t.Errorf("%d TERMINATED frames, want one per refused WATCH", n)
	}
	h.watch(true, bX, aY, 1)  // below the bound again
	h.watch(false, bX, aZ, 5) // taken
	h.watch(false, bW, aY, 6) // refused: warns again
	h.watch(false, bW, aZ, 7) // refused: quiet
	if n := warnings(); n != 2 {
		t.Errorf("%d warnings, want 2: one each time the peer reached the bound", n)
	}
}
