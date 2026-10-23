// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim

import (
	"context"
	"fmt"
	"math/rand/v2"
	"reflect"
	"strings"
	"time"

	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/overload"
)

// turnLabel is the label of the kernel event that runs one turn (SIM-150).
const turnLabel = "lockstep/turn"

// simEngine is the simulation engine of one boot of a lockstep node: it is bound to the node and
// to the incarnation it was created for (SIM §5.3). It implements the engine seam's Scheduler,
// Clock, Transport, Effects, Entropy, Tracer, Failer and Waiter.
type simEngine struct {
	s      *Sim
	n      *kernel.Node
	inc    uint32         // the node incarnation the engine belongs to
	gate   *overload.Gate // the bundle's Admission (SFT-150)
	bundle engine.Engine

	// The transport's state (SIM-160, SIM-170), read by key only. seq holds the last number given
	// to an envelope for each destination node; next holds the next expected number of each
	// stream key that arrived at this boot. host is the system's inbound side.
	seq  map[kernel.NodeID]uint64
	next map[stream]uint64
	host engine.Host

	dead bool // the engine ignores every seam call (SIM-120)
}

// newEngine returns the engine of incarnation inc of node n, with its bundle (SIM-070, SIM §5.3).
func newEngine(s *Sim, n *kernel.Node, inc uint32) *simEngine {
	e := &simEngine{s: s, n: n, inc: inc, gate: overload.NewGate()}
	e.bundle = engine.Engine{
		Node:       n.Name(),
		Epoch:      inc,
		Blocking:   false, // SIM-220
		Scheduler:  e,
		Clock:      e,
		Transport:  e, // the simnet transport (SIM-160)
		Effects:    e,
		Entropy:    e,
		Tracer:     e,
		Admission:  e.gate, // a fresh gate per boot (SFT-150)
		Failer:     e,
		Waiter:     e,
		Interposer: nil, // SFT-070
		Stop:       e.stop,
	}
	return e
}

// canSchedule reports whether e may schedule a node event now: it is not dead, its incarnation is
// the node's current one and the node is not down, so a system of an earlier incarnation never
// schedules work on a later one (SIM-120).
func (e *simEngine) canSchedule() bool {
	return !e.dead && e.inc == e.n.Incarnation() && e.n.State() != kernel.NodeDown
}

// Ready queues one turn of a as one kernel event, if a's slot says so (SIM-150, ENG-050).
func (e *simEngine) Ready(a engine.ActorRef) {
	if e.canSchedule() && a.Slot().Ready() {
		e.postTurn(a)
	}
}

// postTurn posts the kernel event that runs one turn of a with budget 1 and queues the next turn
// when Slot.End says so; the runtime does not call Ready for it (ENG-350).
func (e *simEngine) postTurn(a engine.ActorRef) {
	e.n.Post(turnLabel, func() {
		slot := a.Slot()
		slot.Begin()
		more := a.RunTurn(1)
		if slot.End(more) && e.canSchedule() {
			e.postTurn(a)
		}
	})
}

// stop is the bundle's Stop, which System.Shutdown calls after every actor stopped (ENG-140): it
// marks the engine dead (SIM-195, SIM-120). The node stays up, and System keeps returning the
// stopped system until the next boot.
func (e *simEngine) stop() { e.dead = true }

// InTurn reports whether the caller runs inside a kernel callback (SIM-195, ENG-055).
func (e *simEngine) InTurn() bool { return kernel.Active() != nil }

// Now returns the node's local clock (SIM-155).
func (e *simEngine) Now() time.Time { return e.n.Now() }

// AfterFunc schedules fn as a node event after the local duration d, labeled with the caller's
// label (SIM-155, ENG-070). The Cancel it returns cancels that event.
func (e *simEngine) AfterFunc(d time.Duration, label string, fn func()) engine.Cancel {
	if !e.canSchedule() {
		return func() bool { return false }
	}
	id := e.n.After(d, label, fn)
	return func() bool { return e.n.Cancel(id) }
}

// Rand returns the kernel's stream for label, which starts with "lockstep/" (SIM-180).
func (e *simEngine) Rand(label string) *rand.Rand {
	if !strings.HasPrefix(label, "lockstep/") {
		panic(fmt.Sprintf("sim: Entropy label %q does not start with \"lockstep/\"", label))
	}
	return e.s.w.Sim.Rand(label)
}

// Enabled reports true: the simulation keeps every record (SIM-185).
func (e *simEngine) Enabled() bool { return true }

// Record emits a runtime record as a kernel record of the node, with the text of its owner's
// template (SIM-185). A dead engine records nothing (SIM-120).
func (e *simEngine) Record(kind string, attrs ...engine.Attr) {
	if e.dead {
		return
	}
	e.emit(kind, recordText(kind, attrs), attrs)
}

// Describe returns kernel.Describe(v) (SIM-185).
func (e *simEngine) Describe(v any) string { return kernel.Describe(v) }

// emit emits one record of the node through the kernel and counts it (SIM-185). Records sim
// emits itself come here with their own text.
func (e *simEngine) emit(kind, text string, attrs []engine.Attr) {
	var ka []kernel.Attr
	if len(attrs) > 0 {
		ka = make([]kernel.Attr, len(attrs))
		for i, a := range attrs {
			ka[i] = kernel.Attr{Key: a.Key, Value: a.Value}
		}
	}
	e.s.w.Sim.Emit(kernel.Record{Node: e.n.ID(), Kind: kind, Text: text, Attrs: ka})
	e.s.counts.add(kind, attrs)
}

// recordText returns the text of a runtime record: its owner's template filled from the
// attributes (ENG §8, EFF §8), or the kind without its "lockstep." prefix.
func recordText(kind string, attrs []engine.Attr) string {
	switch kind {
	case "lockstep.turn":
		return "turn " + attrValue(attrs, "actor")
	case "lockstep.effect.start":
		return "effect start " + attrValue(attrs, "name")
	case "lockstep.effect.done":
		return "effect " + attrValue(attrs, "outcome") + " " + attrValue(attrs, "name")
	}
	return strings.TrimPrefix(kind, "lockstep.")
}

// attrValue returns the value of the first attribute named key, or "".
func attrValue(attrs []engine.Attr, key string) string {
	for _, a := range attrs {
		if a.Key == key {
			return a.Value
		}
	}
	return ""
}

// Fail reports misuse the runtime detected (SIM-190, ENG-130).
func (e *simEngine) Fail(err error) { e.misuse(err) }

// misuse emits lockstep.misuse with err's text and stores err in the lockstep/misuse slot unless
// a failure is stored there already, so the first misuse fails the seed (SIM-190).
func (e *simEngine) misuse(err error) {
	e.emit("lockstep.misuse", "misuse", []engine.Attr{{Key: "error", Value: err.Error()}})
	e.s.store(checkMisuse, err)
}

// closed reports, without blocking, whether done is closed. done is only ever closed, never
// sent on, so a receive that can complete at once means it is closed. Simulation code has no
// select statement (DET lint), so the non-blocking receive goes through reflect.
func closed(done <-chan struct{}) bool {
	v, _ := reflect.ValueOf(done).TryRecv()
	return v.IsValid()
}

// Wait never waits and never drives the run (SIM-210): it returns nil if done is closed, and
// otherwise fails the seed with a misuse failure and returns it.
func (e *simEngine) Wait(_ context.Context, done <-chan struct{}) error {
	if closed(done) {
		return nil
	}
	err := fmt.Errorf("lockstep: sim: wait on a pending reply from the test body; use sim.Ask: %w", lockstep.ErrAwaitInActor)
	e.misuse(err)
	return err
}
