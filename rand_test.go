// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	crand "crypto/rand"
	"encoding/binary"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"testing/cryptotest"
	"time"

	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/engine/enginetest"
)

// countingEntropy wraps an engine.Entropy and records the label of every Rand call.
type countingEntropy struct {
	engine.Entropy
	labels []string
}

func (c *countingEntropy) Rand(label string) *rand.Rand {
	c.labels = append(c.labels, label)
	return c.Entropy.Rand(label)
}

// newCountingSystem returns a manual system on node "local" (epoch 1) whose Entropy is a
// countingEntropy around the manual engine's.
func newCountingSystem(t *testing.T, opts ...Option) (*System, *enginetest.Engine, *countingEntropy) {
	t.Helper()
	eng := enginetest.New("local", 1)
	bundle := eng.Engine()
	ce := &countingEntropy{Entropy: bundle.Entropy}
	bundle.Entropy = ce
	sys, err := newSystem(bundle, opts)
	if err != nil {
		t.Fatalf("newSystem: %v", err)
	}
	return sys, eng, ce
}

// userLabels returns the labels of ce that name actors under /user: it drops the system's own
// "$uid" stream.
func userLabels(ce *countingEntropy) []string {
	var out []string
	for _, l := range ce.labels {
		if strings.HasPrefix(l, "lockstep/local/1/user/") {
			out = append(out, l)
		}
	}
	return out
}

// randActor calls ctx.Rand in the handlers selected by use and keeps every pointer it got, per
// incarnation number. Every incarnation is the same value.
type randActor struct {
	useStarted, useReceive, useStopped func(inc uint32) bool
	got                                map[uint32][]*rand.Rand
}

func (a *randActor) draw(ctx *Context[any], use func(uint32) bool) {
	if use != nil && use(ctx.inc) {
		if a.got == nil {
			a.got = make(map[uint32][]*rand.Rand)
		}
		a.got[ctx.inc] = append(a.got[ctx.inc], ctx.Rand())
	}
}

func (a *randActor) Started(ctx *Context[any])               { a.draw(ctx, a.useStarted) }
func (a *randActor) Stopped(ctx *Context[any], _ StopReason) { a.draw(ctx, a.useStopped) }

func (a *randActor) Receive(ctx *Context[any], msg any) {
	a.draw(ctx, a.useReceive)
	if s, _ := msg.(string); s == "panic" {
		panic("panic")
	}
}

func always(uint32) bool { return true }

// AT-TMR-14: TMR-120. Started, Receive and Stopped of one incarnation get the same *rand.Rand, a
// restart gets a new one, and Entropy.Rand is called once per incarnation that used Rand and not
// at all for one that did not.
func TestATTMR14OneStreamPerIncarnation(t *testing.T) {
	sys, eng, ce := newCountingSystem(t, WithGuardianStrategy(r0()))
	// Incarnations 1 and 3 use Rand in all three handlers; incarnation 2 never does.
	use := func(inc uint32) bool { return inc != 2 }
	a := &randActor{useStarted: use, useReceive: use, useStopped: use}
	p, err := Spawn(sys, "r", actorOf[any](a))
	if err != nil {
		t.Fatal(err)
	}
	send(t, sys, p, any("panic")) // ends incarnation 1
	eng.RunUntilIdle()
	send(t, sys, p, any("panic")) // incarnation 2 ends; it never called Rand
	eng.RunUntilIdle()
	send(t, sys, p, any("a"))
	eng.RunUntilIdle()
	if err := Stop(sys, p.Address()); err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()

	for _, inc := range []uint32{1, 3} {
		got := a.got[inc]
		if len(got) != 3 {
			t.Fatalf("incarnation %d: %d Rand calls recorded, want 3 (Started, Receive, Stopped)", inc, len(got))
		}
		if got[0] == nil || got[0] != got[1] || got[1] != got[2] {
			t.Errorf("incarnation %d: Rand pointers %p %p %p, want one non-nil pointer", inc, got[0], got[1], got[2])
		}
	}
	if len(a.got[2]) != 0 {
		t.Errorf("incarnation 2 recorded %d Rand calls, want 0", len(a.got[2]))
	}
	if a.got[1][0] == a.got[3][0] {
		t.Error("incarnations 1 and 3 got the same *rand.Rand, want a new one after a restart")
	}
	want := []string{"lockstep/local/1/user/r/1", "lockstep/local/1/user/r/3"}
	if got := userLabels(ce); !slices.Equal(got, want) {
		t.Errorf("Entropy.Rand labels %q, want %q (one per incarnation that used Rand)", got, want)
	}
}

// TMR-110: the label is lockstep/<node>/<epoch>/<path>/<inc>, from the node name, the system's
// incarnation and the actor's path, and a child has its own.
func TestTMR110Label(t *testing.T) {
	sys, eng, ce := newCountingSystem(t, WithGuardianStrategy(r0()))
	parent := &randActor{useStarted: always, useReceive: always}
	childActor := &randActor{useStarted: always}
	pid, err := Spawn(sys, "r", actorOf[any](&starterFunc{Actor: parent, start: func(ctx *Context[any]) {
		parent.Started(ctx)
		if ctx.inc == 1 {
			if _, err := Spawn(ctx, "c", actorOf[any](childActor)); err != nil {
				t.Errorf("Spawn child: %v", err)
			}
		}
	}}))
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	send(t, sys, pid, any("panic")) // restarts r: incarnation 2
	eng.RunUntilIdle()

	got := userLabels(ce)
	want := []string{"lockstep/local/1/user/r/1", "lockstep/local/1/user/r/c/1", "lockstep/local/1/user/r/2"}
	if !slices.Equal(got, want) {
		t.Errorf("Entropy.Rand labels %q, want %q", got, want)
	}
}

// starterFunc is an Actor whose Started is start.
type starterFunc struct {
	Actor[any]
	start func(*Context[any])
}

func (s *starterFunc) Started(ctx *Context[any]) { s.start(ctx) }

// TMR-120: Rand panics outside a turn like every other Context method that needs one (ACT-120).
func TestRandOutsideTurnPanics(t *testing.T) {
	sys, eng, _ := newCountingSystem(t)
	var saved *Context[any]
	if _, err := Spawn(sys, "r", actorOf[any](&starterFunc{Actor: &randActor{}, start: func(ctx *Context[any]) { saved = ctx }})); err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	defer func() {
		if recover() == nil {
			t.Error("Rand outside its turn did not panic")
		}
	}()
	saved.Rand()
}

// leBytes reads two little-endian words from b like the local engine's seeding (ENG-440).
func leBytes(b [16]byte) (uint64, uint64) {
	return binary.LittleEndian.Uint64(b[:8]), binary.LittleEndian.Uint64(b[8:])
}

// AT-TMR-15: TMR-130. The local engine seeds each incarnation's stream from crypto/rand: with the
// global source fixed, the first draw is the first draw of a PCG seeded with the first 16 bytes
// it yields, and without it two actors' first draws differ. It cannot run in parallel: it
// replaces the process's crypto/rand source.
func TestATTMR15ProductionSeeding(t *testing.T) {
	firstDraw := func(t *testing.T, sys *System, name string) uint64 {
		t.Helper()
		out := make(chan uint64, 1)
		_, err := Spawn(sys, name, actorOf[any](&starterFunc{Actor: &randActor{}, start: func(ctx *Context[any]) {
			out <- ctx.Rand().Uint64()
		}}))
		if err != nil {
			t.Fatalf("Spawn %q: %v", name, err)
		}
		select {
		case v := <-out:
			return v
		case <-time.After(10 * time.Second):
			t.Fatalf("actor %q did not draw within 10 s", name)
			return 0
		}
	}

	t.Run("fixed source", func(t *testing.T) {
		cryptotest.SetGlobalRandom(t, 7)
		var b [16]byte
		if _, err := crand.Read(b[:]); err != nil {
			t.Fatal(err)
		}
		lo, hi := leBytes(b)
		want := rand.New(rand.NewPCG(lo, hi)).Uint64() //nolint:gosec // G404: reproduces the engine's seeded stream

		// The engine reads its epoch from crypto/rand in New, so build the system first and reset
		// the source after: Rand's seed is then the first read after the reset.
		sys := newLocalSystem(t)
		cryptotest.SetGlobalRandom(t, 7)
		if got := firstDraw(t, sys, "a"); got != want {
			t.Errorf("first draw %#x, want %#x", got, want)
		}
	})

	t.Run("unfixed source", func(t *testing.T) {
		sys := newLocalSystem(t)
		x, y := firstDraw(t, sys, "a"), firstDraw(t, sys, "b")
		if x == y {
			t.Errorf("two actors drew %#x first, want different values", x)
		}
	})
}
