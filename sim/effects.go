// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"time"

	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/hooks"
)

// effectLabel is the label of the node event that delivers a handler's result (EFF-430).
const effectLabel = "lockstep/effect"

// EffectCall describes one Do call to an effect handler.
type EffectCall struct {
	Name  string            // the effect's name
	Node  string            // name of the node the actor runs on
	Actor lockstep.Address  // the actor that called Do
	ID    lockstep.EffectID // the ID Do returned

	// unexported: the effect value, the node and the stream
	value  any
	node   *kernel.Node
	stream *rand.Rand
}

// Effect returns the Effect value passed to Do; type-assert it to read its fields.
func (c EffectCall) Effect() any { return c.value }

// Rand returns the stream "lockstep/effect/<node>/<epoch>/<name>" of this call's node, node
// incarnation and effect name. Handlers draw all their randomness from it.
func (c EffectCall) Rand() *rand.Rand { return c.stream }

// Now returns the node's local time, as ctx.Now() does in the actor.
func (c EffectCall) Now() time.Time { return c.node.Now() }

// EffectResult is what a handler returns.
type EffectResult struct {
	Value   any           // of the effect's type T, or nil for the zero T
	Err     error         // passed to done unchanged
	Latency time.Duration // local time on the actor's node until the result arrives; 0 or more
}

// HandleEffect installs h as the handler of every effect named name, replacing any previous
// handler; a nil h removes it. It may be called from the body and from kernel callbacks, and
// applies to Do calls made after it. It panics if name is invalid or starts with "lockstep.".
func (s *Sim) HandleEffect(name string, h func(c EffectCall) EffectResult) {
	s.check()
	if !engine.ValidEffectName(name) {
		panic(fmt.Sprintf("lockstep: sim: HandleEffect: invalid effect name %q", name))
	}
	if strings.HasPrefix(name, "lockstep.") {
		panic(fmt.Sprintf("lockstep: sim: HandleEffect: name %q is reserved for lockstep's built-in effects", name))
	}
	if h == nil {
		delete(s.handlers, name)
		return
	}
	if s.handlers == nil {
		s.handlers = make(map[string]func(c EffectCall) EffectResult)
	}
	s.handlers[name] = h
}

// Start is the engine's Effects. For an inline internal effect it runs req.Run in a node event of
// its own (startInline). For any other effect it never calls req.Run (EFF-400): it calls the Sim's
// handler for req.Name inside this call, so inside the turn that called Do, and schedules the
// handler's result as one lockstep/effect node event after the handler's latency, which calls done
// (EFF-430). The Cancel it returns cancels that event. The handler runs with no runtime lock held,
// so it may call HandleEffect, sim.Send and the kernel and simnet APIs (EFF-470).
//
// Start schedules nothing and returns a Cancel that reports true the first time it is called,
// and done is never called, when there is no handler (EFF-450: the failure goes to the
// lockstep/no-effect-handler slot and no lockstep.misuse is recorded), when the handler panics
// (EFF-460) and when it returns a negative latency (EFF-440); the last two fail the seed through
// lockstep/misuse, and the calling actor does not fail. A dead engine, and an engine the handler
// killed, schedule nothing and return a Cancel that reports false (SIM-120).
func (e *simEngine) Start(req engine.EffectRequest, done func(v any, err error)) engine.Cancel {
	if !e.canSchedule() {
		return notPending
	}
	if ie, ok := req.Value.(hooks.InternalEffect); ok && ie.Inline {
		return e.startInline(req, done)
	}
	h := e.s.handlers[req.Name]
	if h == nil {
		e.s.store(checkNoEffectHandler, fmt.Errorf("%w: effect %q called by %s", lockstep.ErrNoEffectHandler, req.Name, req.Actor))
		return cancelOnce()
	}
	res, ok := e.callHandler(h, e.effectCall(req))
	if !ok {
		return cancelOnce()
	}
	if res.Latency < 0 {
		e.liveMisuse(fmt.Errorf("lockstep: effect %q: handler returned negative latency %s", req.Name, res.Latency))
		return cancelOnce()
	}
	if !e.canSchedule() { // the handler crashed the node or stopped its system
		return notPending
	}
	actor, held := req.Actor.String(), &heldEffect{id: req.ID, name: req.Name}
	cancel := e.schedule(res.Latency, effectLabel, func() {
		e.release(actor, held)
		// The kernel never runs an event of an earlier incarnation; a system stopped since Do
		// cancelled its effects, so this guard only keeps a dead engine silent (SIM-120).
		if e.canSchedule() {
			done(res.Value, res.Err)
		}
	})
	// The effect is held once its result event is scheduled, which the canSchedule check above
	// guarantees: an effect never scheduled is never held, so it can never be reported as a leak
	// (SIM-290). The event runs after Start returns, so the hold always precedes its release.
	e.hold(actor, held)
	return func() bool {
		cancelled := cancel()
		if cancelled {
			e.release(actor, held)
		}
		return cancelled
	}
}

// startInline runs the inline internal effect req (PER-520): it schedules a lockstep/effect event
// at the current time that calls req.Run with a background context, outside any turn, and then
// schedules a second lockstep/effect event at the same time that calls done with Run's result.
// A panic in Run becomes the error the other engines give it (ENG-430), never a seed failure.
// Handlers registered with HandleEffect are never consulted. Both events go through the counted
// schedule path, so they are lockstep activity (SIM-250), and a node crash discards them. The
// effect is held for the leak check from Start until done is called or the Cancel succeeds
// (SIM-290). The Cancel cancels whichever of the two events is pending and reports true unless
// done has been called or the effect was cancelled already; a cancelled effect never calls done.
func (e *simEngine) startInline(req engine.EffectRequest, done func(v any, err error)) engine.Cancel {
	actor, held := req.Actor.String(), &heldEffect{id: req.ID, name: req.Name}
	var cancelled, finished bool
	var pending engine.Cancel // the Cancel of the event to run next
	pending = e.schedule(0, effectLabel, func() {
		if cancelled || !e.canSchedule() {
			return
		}
		v, err := runInline(req)
		if cancelled || !e.canSchedule() { // the effect was cancelled, or the node crashed, during Run
			return
		}
		pending = e.schedule(0, effectLabel, func() {
			if cancelled || !e.canSchedule() {
				return
			}
			finished = true
			e.release(actor, held)
			done(v, err)
		})
	})
	e.hold(actor, held)
	return func() bool {
		if cancelled || finished {
			return false
		}
		cancelled = true
		pending()
		e.release(actor, held)
		return true
	}
}

// runInline calls req.Run with a background context and turns a panic into an error with the
// text of the other engines (ENG-430).
func runInline(req engine.EffectRequest) (v any, err error) {
	defer func() {
		if p := recover(); p != nil {
			v, err = nil, fmt.Errorf("lockstep: effect %q panicked: %v", req.Name, p)
		}
	}()
	return req.Run(context.Background())
}

// effectCall returns the EffectCall of req on e's node and incarnation (EFF-400, EFF-420).
func (e *simEngine) effectCall(req engine.EffectRequest) EffectCall {
	actor, _ := hooks.Address(req.Actor).(lockstep.Address)
	node := e.n.Name()
	return EffectCall{
		Name:   req.Name,
		Node:   node,
		Actor:  actor,
		ID:     lockstep.EffectID(req.ID),
		value:  req.Value,
		node:   e.n,
		stream: e.s.w.Sim.Rand("lockstep/effect/" + node + "/" + strconv.FormatUint(uint64(e.inc), 10) + "/" + req.Name), // EFF-420
	}
}

// callHandler calls h with c and reports whether h returned. A panic in h is recovered here, not
// by the actor's guarded call, and fails the seed with EFF-460's misuse failure, unless h killed
// the engine. runtime.Goexit is not stopped.
func (e *simEngine) callHandler(h func(c EffectCall) EffectResult, c EffectCall) (res EffectResult, ok bool) {
	defer func() {
		if ok {
			return
		}
		if v := recover(); v != nil {
			e.liveMisuse(fmt.Errorf("lockstep: effect handler %q panicked: %s", c.Name, kernel.Describe(v)))
		}
	}()
	return h(c), true
}

// notPending is the Cancel of work a dead engine never scheduled: an AfterFunc event or an effect
// it never started (SIM-120).
func notPending() bool { return false }

// cancelOnce returns the Cancel of an effect for which nothing was scheduled: it reports true the
// first time it is called and false after that.
func cancelOnce() engine.Cancel {
	cancelled := false
	return func() bool {
		if cancelled {
			return false
		}
		cancelled = true
		return true
	}
}
