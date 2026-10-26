// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"fmt"

	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/hooks"
)

// controlled is implemented by every *Context[M]: controlHook reaches the controls of a context
// through it without knowing M.
type controlled interface {
	control() hooks.ActorControl
}

// control returns the controls of c's incarnation (PER §4.3). It panics on a nil *Context.
func (c *Context[M]) control() hooks.ActorControl {
	c.mustNotNil()
	return actorControl[M]{ctx: c}
}

// controlHook is hooks.Control (PER §4.3): the controls of ctx, a *Context[M] of any M. It panics
// if ctx has another type.
func controlHook(ctx any) hooks.ActorControl {
	cc, ok := ctx.(controlled)
	if !ok {
		panic(fmt.Sprintf("lockstep: hooks.Control: ctx has type %T, want a *lockstep.Context", ctx))
	}
	return cc.control()
}

// actorControl is the hooks.ActorControl of one incarnation: the one whose context ctx is.
type actorControl[M any] struct {
	ctx *Context[M]
}

// Address returns the PID's String() (ACT-410).
func (a actorControl[M]) Address() string { return a.ctx.cell.addr.String() }

// StartInternal starts e for this incarnation (PER-240). It is not implemented yet.
func (actorControl[M]) StartInternal(hooks.InternalEffect) (cancel func()) {
	panic("lockstep: StartInternal: not implemented")
}

// PauseUser stops (true) or resumes (false) the delivery of user messages to the incarnation from
// its next user message on (PER-250, MBX-190). Pausing twice is pausing once, and resuming twice
// is resuming once. It panics unless a handler of the incarnation is running (ACT-120): the
// paused flag is turn-owned. A resume during a turn needs no Ready: the turn loop pops the
// queued user messages next, or RunTurn reports more work (ENG-320, ENG-350).
func (a actorControl[M]) PauseUser(paused bool) {
	a.ctx.mustInTurn()
	a.ctx.cell.paused = paused
}

// TraceEnabled reports the engine Tracer's Enabled (ENG-110).
func (a actorControl[M]) TraceEnabled() bool { return a.ctx.cell.sys.eng.Tracer.Enabled() }

// Record emits Tracer.Record(kind, attrs...) with kv as the attributes' keys and values,
// alternating, when the tracer is enabled. It panics if kv has an odd length.
func (a actorControl[M]) Record(kind string, kv ...string) {
	if len(kv)%2 != 0 {
		panic(fmt.Sprintf("lockstep: ActorControl.Record(%q): odd number of key-value arguments", kind))
	}
	tr := a.ctx.cell.sys.eng.Tracer
	if !tr.Enabled() {
		return
	}
	attrs := make([]engine.Attr, 0, len(kv)/2)
	for i := 0; i < len(kv); i += 2 {
		attrs = append(attrs, engine.Attr{Key: kv[i], Value: kv[i+1]})
	}
	tr.Record(kind, attrs...)
}

// signalHook is hooks.Signal (PER §4.3): it queues sig, a Signal, on the system queue of the live
// actor at to on sys, a *System, and reports true. The signal is delivered as SUP delivers
// signals (SUP-320), except that no watch or subscription filters it: a Running incarnation that
// implements SignalReceiver gets it in Signal. It reports false, and queues nothing, when sys is
// discarded or no actor at to is alive: to names another node, a guardian, no registered actor,
// or an actor that completed its final stop. It panics if sys or sig has another type.
func signalHook(sys any, to engine.Address, sig any) bool {
	s := hookSystem("Signal", sys)
	sg, ok := sig.(Signal)
	if !ok {
		panic(fmt.Sprintf("lockstep: hooks.Signal: sig has type %T, want a lockstep.Signal", sig))
	}
	a := addressOf(to)
	if s.discarded.Load() || a.IsZero() || a.node != s.node || s.isGuardian(a) {
		return false
	}
	c, _, _ := s.resolve(a, nil)
	if c == nil {
		return false
	}
	return c.offerSystem(signalMsg{sig: sg, direct: true}, nil)
}
