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

// labelClass is the class of the label of an event sim schedules: how a pending event with that
// label counts for quiescence (ENG-070, SIM-250).
type labelClass uint8

const (
	classActivity   labelClass = iota // lockstep activity: keeps the world busy
	classAsk                          // an ask timeout: one per pending ask (MSG-320), not activity
	classBackground                   // not lockstep activity, such as periodic housekeeping: ignored
)

// labelClasses holds the class of every label sim schedules events with: its own node events,
// ENG-070's AfterFunc labels and SFT's events. A label it does not list starts with "lockstep/"
// and is lockstep activity (SIM §3). It is read by key only.
var labelClasses = map[string]labelClass{
	turnLabel:                      classActivity, // SIM-150
	sendLabel:                      classActivity, // SIM-160
	effectLabel:                    classActivity, // EFF-430
	"lockstep/timer":               classActivity,
	"lockstep/ask-timeout":         classAsk,
	"lockstep/backoff":             classActivity,
	"lockstep/effect-timeout":      classActivity,
	"lockstep/overload-sample":     classBackground,
	"lockstep/remote/tick":         classBackground,
	"lockstep/remote/redial":       classBackground,
	"lockstep/remote/dial-timeout": classActivity,
	"lockstep/remote/fail":         classActivity,
	"lockstep/remote/recv":         classActivity,
	"lockstep/delay":               classActivity,   // SFT-090: a held message's release
	"lockstep/pressure":            classBackground, // SFT pressure is not lockstep activity
}

// classOf returns the class of label (ENG-070): its entry in labelClasses, or activity.
func classOf(label string) labelClass {
	if c, ok := labelClasses[label]; ok {
		return c
	}
	return classActivity
}

// eventCounts counts pending events by label and by class (SIM-250). byLabel is read by key only.
type eventCounts struct {
	byLabel  map[string]int
	activity int // events of class activity
	asks     int // ask timeouts, so pending asks
}

// add adds n, which may be negative, to the count of label and of its class. It panics if the
// count of label would go below zero: every event is counted once and subtracted once.
func (c *eventCounts) add(label string, n int) {
	if c.byLabel[label]+n < 0 {
		panic(fmt.Sprintf("lockstep: sim: pending %s events below zero", label)) // unreachable
	}
	if c.byLabel == nil {
		c.byLabel = make(map[string]int)
	}
	c.byLabel[label] += n
	switch classOf(label) {
	case classActivity:
		c.activity += n
	case classAsk:
		c.asks += n
	case classBackground:
	}
}

// simEngine is the simulation engine of one boot of a lockstep node: it is bound to the node and
// to the incarnation it was created for (SIM §5.3). It implements the engine seam's Scheduler,
// Clock, Transport, Effects, Entropy, Tracer, Failer and Waiter.
type simEngine struct {
	s      *Sim
	n      *kernel.Node
	inc    uint32         // the node incarnation the engine belongs to
	gate   *overload.Gate // the bundle's Admission (SFT-150)
	bundle engine.Engine

	// The transport's state (SIM-160, SIM-170); the maps are read by key only. seq holds the last
	// number given to an envelope for each destination node; next holds the next expected number
	// of each stream key that arrived at this boot. deferred holds the frames that wait for their
	// lockstep/send event, oldest first. host is the system's inbound side.
	seq      map[kernel.NodeID]uint64
	next     map[stream]uint64
	deferred []deferredFrame
	host     engine.Host

	// dead is set when the node crashes (SIM-110) or the system stops (SIM-195): the engine then
	// ignores every seam call (SIM-120). The kernel itself discards the events a crashed
	// incarnation scheduled, so its turns, timers and deferred sends never run, even after a
	// restart.
	dead bool

	// events counts the node events this engine scheduled that have neither run nor been
	// cancelled, and that its node's crash has not dropped (SIM-250); labels lists each label
	// events has counted, in the order it was first counted, so dropPending walks the counts in a
	// fixed order (SIM-330). The Sim's node counts are the sums over its engines. dropped is set
	// once dropPending has subtracted every event still pending.
	events  eventCounts
	labels  []string
	dropped bool
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
		Stop:       e.kill,
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
	e.post(turnLabel, func() {
		slot := a.Slot()
		slot.Begin()
		more := a.RunTurn(1)
		if slot.End(more) && e.canSchedule() {
			e.postTurn(a)
		}
	})
}

// pendingEvent is one counted event: its label, and whether it has been subtracted from the
// counts. Each counted event is subtracted exactly once: when it runs, when its Cancel returns
// true, or, for a node event, by its node's crash (SIM-250).
type pendingEvent struct {
	label   string
	settled bool
}

// post is schedule with d = 0 for an event nobody cancels: it posts fn as an event of e's node at
// the current time, as kernel.Node.Post does, and counts it.
func (e *simEngine) post(label string, fn func()) { e.schedule(0, label, fn) }

// schedule schedules fn as an event of e's node, labeled label, after the local duration d, and
// returns the Cancel of that event. It is the one counted path for node events: every node event
// sim schedules goes through it, so the event counts as pending, by its label's class, in e's
// counts and the Sim's until it runs, its Cancel returns true or its node crashes (SIM-250). The
// caller has checked canSchedule; on a down node nothing is scheduled or counted.
func (e *simEngine) schedule(d time.Duration, label string, fn func()) engine.Cancel {
	ev := &pendingEvent{label: label}
	id := e.n.After(d, label, func() {
		e.settle(ev)
		fn()
	})
	if id == 0 { // the node is down: nothing was scheduled
		return notPending
	}
	e.count(label, 1)
	return func() bool {
		if !e.n.Cancel(id) {
			return false
		}
		e.settle(ev)
		return true
	}
}

// count adds n pending events labeled label to e's counts and the Sim's node counts.
func (e *simEngine) count(label string, n int) {
	if _, ok := e.events.byLabel[label]; !ok {
		e.labels = append(e.labels, label)
	}
	e.events.add(label, n)
	e.s.countNode(label, n)
}

// settle subtracts ev from the counts because it runs or was cancelled, unless it was subtracted
// already, by an earlier settle or by dropPending.
func (e *simEngine) settle(ev *pendingEvent) {
	if ev.settled || e.dropped {
		return
	}
	ev.settled = true
	e.count(ev.label, -1)
}

// dropPending subtracts every event e still has pending from the counts, and makes a later settle
// of those events subtract nothing. The crash hook calls it: the kernel drops a crashed
// incarnation's events without running them (SIM-250).
func (e *simEngine) dropPending() {
	for _, label := range e.labels {
		if n := e.events.byLabel[label]; n > 0 {
			e.count(label, -n)
		}
	}
	e.dropped = true
}

// countNode adds n pending node events labeled label to the Sim's node counts. A change in the
// number of ask timeouts is a change in the set of pending asks, which starts a new generation of
// the wait-for graph (SIM-260).
func (s *Sim) countNode(label string, n int) {
	s.events.add(label, n)
	if classOf(label) == classAsk {
		s.askGen++
	}
}

// worldAt schedules fn as a world event labeled label at virtual time t, as w.Sim.At does, and
// returns its Cancel. worldAtFront is the same with w.Sim.AtFront. They are the counted path for
// world events: a world event sim schedules with a "lockstep/" label goes through one of them, so
// it counts as pending, by its label's class, in the Sim's world counts until it runs or its
// Cancel returns true (SIM-250). A world event survives crashes, so the crash hook never touches
// these counts.
func (s *Sim) worldAt(t kernel.Time, label string, fn func()) engine.Cancel {
	return s.scheduleWorld(s.w.Sim.At, t, label, fn)
}

// worldAtFront is worldAt with w.Sim.AtFront: the event runs before the seeded events at t and
// takes no scheduler draw.
func (s *Sim) worldAtFront(t kernel.Time, label string, fn func()) engine.Cancel {
	return s.scheduleWorld(s.w.Sim.AtFront, t, label, fn)
}

// scheduleWorld schedules fn with at, w.Sim.At or w.Sim.AtFront, and counts it in s.world until
// it runs or its Cancel returns true.
func (s *Sim) scheduleWorld(at func(kernel.Time, string, func()) kernel.EventID, t kernel.Time, label string, fn func()) engine.Cancel {
	ev := &pendingEvent{label: label}
	id := at(t, label, func() {
		s.settleWorld(ev)
		fn()
	})
	s.world.add(label, 1)
	return func() bool {
		if !s.w.Sim.Cancel(id) {
			return false
		}
		s.settleWorld(ev)
		return true
	}
}

// settleWorld subtracts the world event ev from s.world, unless it was subtracted already.
func (s *Sim) settleWorld(ev *pendingEvent) {
	if ev.settled {
		return
	}
	ev.settled = true
	s.world.add(ev.label, -1)
}

// kill marks the engine dead, so it ignores every seam call from now on (SIM-120). It is the one
// way an engine dies: the crash hook calls it when the node crashes, before the system is
// discarded (SIM-110), and it is the bundle's Stop, which System.Shutdown calls after every actor
// stopped (ENG-140, SIM-195). After a Stop the node stays up, and System keeps returning the
// stopped system until the next boot.
func (e *simEngine) kill() { e.dead = true }

// InTurn reports whether the caller runs inside a kernel callback (SIM-195, ENG-055).
func (e *simEngine) InTurn() bool { return kernel.Active() != nil }

// Now returns the node's local clock (SIM-155).
func (e *simEngine) Now() time.Time { return e.n.Now() }

// AfterFunc schedules fn as a node event after the local duration d, labeled with the caller's
// label (SIM-155, ENG-070). The Cancel it returns cancels that event.
func (e *simEngine) AfterFunc(d time.Duration, label string, fn func()) engine.Cancel {
	if !e.canSchedule() {
		return notPending
	}
	return e.schedule(d, label, fn)
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
// emits itself for a node come here with their own text.
func (e *simEngine) emit(kind, text string, attrs []engine.Attr) {
	e.s.emit(e.n.ID(), kind, text, attrs)
}

// emit emits one record of node, or of no node when node is 0, through the kernel and counts it
// (SIM-185).
func (s *Sim) emit(node kernel.NodeID, kind, text string, attrs []engine.Attr) {
	var ka []kernel.Attr
	if len(attrs) > 0 {
		ka = make([]kernel.Attr, len(attrs))
		for i, a := range attrs {
			ka[i] = kernel.Attr{Key: a.Key, Value: a.Value}
		}
	}
	s.w.Sim.Emit(kernel.Record{Node: node, Kind: kind, Text: text, Attrs: ka})
	s.counts.add(kind, attrs)
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

// Fail reports misuse the runtime detected (SIM-190, ENG-130). A dead engine ignores it
// (SIM-120): the system of a crashed or stopped node makes no more calls that count.
func (e *simEngine) Fail(err error) { e.liveMisuse(err) }

// liveMisuse is misuse unless e is dead: a dead engine records and stores nothing (SIM-120). Fail,
// Wait and the effect handlers' misuse (EFF-440, EFF-460) go through it.
func (e *simEngine) liveMisuse(err error) {
	if !e.dead {
		e.misuse(err)
	}
}

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
// otherwise fails the seed with a misuse failure and returns it. A dead engine returns the error
// without recording or storing it (SIM-120). Through the runtime that case does not occur: a crash
// or a stop resolves every pending ask of the system before its engine is used again, and an ask
// made afterwards is resolved at once, so a body's Await on one of its replies returns without
// waiting.
func (e *simEngine) Wait(_ context.Context, done <-chan struct{}) error {
	if closed(done) {
		return nil
	}
	err := fmt.Errorf("lockstep: sim: wait on a pending reply from the test body; use sim.Ask: %w", lockstep.ErrAwaitInActor)
	e.liveMisuse(err)
	return err
}
