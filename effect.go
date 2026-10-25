// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"time"

	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/supervise"
)

// ErrNoEffectHandler is the error that fails a simulation seed when an actor starts an effect and
// the simulation has no handler for the effect's name. The failure wraps it, so
// errors.Is(err, ErrNoEffectHandler) holds, and its text names the effect and the actor.
var ErrNoEffectHandler = errors.New("lockstep: no effect handler")

// DefaultEffectTimeout is the timeout of an effect started without WithEffectTimeout.
const DefaultEffectTimeout = 30 * time.Second

// effectTimeoutLabel is the label of the engine timer that times an effect out (ENG-070).
const effectTimeoutLabel = "lockstep/effect-timeout"

// The outcomes of a lockstep.effect.done record (EFF §8).
const (
	outcomeOK         = "ok"
	outcomeError      = "error"
	outcomeTimeout    = "timeout"
	outcomeOverloaded = "overloaded"
	outcomeRefused    = "refused"
	outcomeCancelled  = "cancelled"
)

// Effect is an operation that waits: a network call, file I/O, a database query, CPU work over a
// budget. An actor starts it with Do and gets its outcome back as a message.
type Effect[T any] interface {
	// Name returns the effect's stable name: 1 to 64 characters from [A-Za-z0-9._-], starting
	// with a letter or a digit. Simulation finds the effect's handler by this name, and traces
	// show it. Names starting with "lockstep." are reserved for lockstep's built-in effects.
	Name() string

	// Run performs the operation and returns its result. A System built by New calls it on a
	// goroutine of its own; simulation never calls it. Run should return soon after ctx is done,
	// which happens when the effect times out or is cancelled.
	Run(ctx context.Context) (T, error)
}

// EffectID identifies one Do call within a System. IDs start at 1 and increase by one per Do
// call that starts an effect. 0 means that no effect was started.
type EffectID uint64

// EffectOption configures one Do call.
type EffectOption func(*effectConfig)

// effectConfig is what the options of one Do call set. hasBad and badTimeout hold the first
// non-positive timeout an option was given: Do panics for it after it has applied every option.
type effectConfig struct {
	timeout    time.Duration
	hasBad     bool
	badTimeout time.Duration
}

// WithEffectTimeout sets the effect's timeout, measured on the engine's clock from the Do call.
// Do panics if d is not positive.
func WithEffectTimeout(d time.Duration) EffectOption {
	return func(c *effectConfig) {
		if d <= 0 {
			if !c.hasBad {
				c.hasBad, c.badTimeout = true, d
			}
			return
		}
		c.timeout = d
	}
}

// Do starts effect e for the actor of ctx and returns its ID. When the effect's outcome is
// delivered, a later turn of the actor calls done with the result, or with the zero T and
// ErrTimeout or ErrOverloaded, and passes done's return value to Receive. done runs on the
// actor's turn, never concurrently with Receive. If the incarnation ends or CancelEffect cancels
// the effect first, done is never called.
//
// Do panics if e is nil, e.Name() is invalid, done is nil, or an option is invalid.
func Do[T, M any](ctx *Context[M], e Effect[T], done func(T, error) M, opts ...EffectOption) EffectID {
	ctx.mustInTurn()
	// EFF-010's checks, in its order.
	if e == nil {
		panic("lockstep: Do: nil effect")
	}
	name := e.Name()
	if !engine.ValidEffectName(name) {
		panic(fmt.Sprintf("lockstep: Do: invalid effect name %q", name))
	}
	if done == nil {
		panic("lockstep: Do: nil done")
	}
	cfg := effectConfig{timeout: DefaultEffectTimeout}
	for _, o := range opts {
		if o == nil {
			panic("lockstep: Do: nil option")
		}
		o(&cfg)
	}
	if cfg.hasBad {
		panic("lockstep: Do: effect timeout must be positive, got " + cfg.badTimeout.String())
	}

	c := ctx.cell
	if c.ending() {
		c.recordEffectDone(ctx.inc, 0, name, outcomeRefused, nil) // EFF-030
		return 0
	}

	// EFF-020: the ID, the start record, the timeout timer, the engine.
	id := EffectID(c.sys.effectSeq.Add(1))
	want := reflect.TypeFor[T]()
	pe := &pendingEffect[M]{
		id:   id,
		name: name,
		inc:  ctx.inc,
		// bind turns the engine's result into the call of done, or reports a value that is not a T
		// (EFF-070). A nil v is the zero T (EFF-060).
		bind: func(v any, err error) (func() M, error) {
			var val T
			if v != nil {
				tv, ok := v.(T)
				if !ok {
					return nil, fmt.Errorf("lockstep: effect %q: engine returned %s, want %s", name, reflect.TypeOf(v).String(), want.String())
				}
				val = tv
			}
			return func() M { return done(val, err) }, nil
		},
	}
	eng := c.sys.eng
	if eng.Tracer.Enabled() {
		eng.Tracer.Record("lockstep.effect.start",
			engine.Attr{Key: "actor", Value: c.addr.String()},
			engine.Attr{Key: "inc", Value: strconv.FormatUint(uint64(ctx.inc), 10)},
			engine.Attr{Key: "effect", Value: strconv.FormatUint(uint64(id), 10)},
			engine.Attr{Key: "name", Value: name},
			engine.Attr{Key: "timeout", Value: cfg.timeout.String()},
		)
	}
	// The effect is in the table before the timer or the engine can call back, which on the local
	// engine may be another goroutine before this one returns from Start. The timer's callback
	// never reads cancelTimer, and the engine's done function cannot run before Start, so the
	// timer is armed outside the lock and cancelTimer needs none.
	c.emu.Lock()
	if c.effects == nil {
		c.effects = make(map[EffectID]*pendingEffect[M])
	}
	c.effects[id] = pe
	c.emu.Unlock()
	pe.cancelTimer = eng.Clock.AfterFunc(cfg.timeout, effectTimeoutLabel, func() { c.effectTimedOut(pe) })

	cancel := eng.Effects.Start(engine.EffectRequest{
		ID:    uint64(id),
		Name:  name,
		Actor: c.addr.engine(),
		Value: e,
		Run: func(rctx context.Context) (any, error) {
			v, err := e.Run(rctx)
			return v, err
		},
	}, func(v any, err error) { c.effectDone(pe, v, err) })

	c.emu.Lock()
	if c.effects[id] == pe {
		pe.cancelRun = cancel
		c.emu.Unlock()
		return id
	}
	c.emu.Unlock()
	// The effect's outcome came in before Start returned. After a result, Cancel does nothing. A
	// timeout that fired this early had no Cancel to call: it delivered ErrTimeout, and the Cancel
	// that ends the engine's work follows here. That is the one case in which done's message can
	// precede the engine's Cancel; otherwise EFF-090's order holds.
	cancel()
	return id
}

// pendingEffect is one effect of a cell that has no outcome yet (EFF §3, §11). It belongs to the
// incarnation inc that called Do. id, name, inc and bind never change; cancelRun is guarded by the
// cell's emu, and the effect is pending exactly while the cell's table holds it. Do sets
// cancelTimer before it calls Effects.Start, and it is read only after Start: by effectDone, which
// the engine calls after Start, and by cancelEffect, which runs in the cell's turn after Do has
// returned.
type pendingEffect[M any] struct {
	id   EffectID
	name string
	inc  uint32
	bind func(v any, err error) (call func() M, misuse error)

	cancelTimer engine.Cancel // of the timeout timer
	cancelRun   engine.Cancel // the Cancel that Effects.Start returned; nil until Start has returned
}

// effectResult is the msg of the Self entry that carries a delivered outcome to the actor's turn
// (EFF-080): call runs the user's done with the result and returns the message for Receive, and
// res is the error when the outcome has one, otherwise the value, which the drop record of an
// ended incarnation describes (MBX §8).
type effectResult[M any] struct {
	res  any
	call func() M
}

// takeEffect removes pe from the table and returns the engine's Cancel, nil before Start has
// returned, or ok false when pe is no longer pending: the first of the engine's result and the
// timeout to take it is the effect's one outcome. It never runs user code.
func (c *cell[M]) takeEffect(pe *pendingEffect[M]) (cancelRun engine.Cancel, ok bool) {
	c.emu.Lock()
	defer c.emu.Unlock()
	if c.effects[pe.id] != pe {
		return nil, false
	}
	delete(c.effects, pe.id)
	return pe.cancelRun, true
}

// cancelEffect cancels pe if it is still pending (EFF-100, EFF-110) and reports whether it did: it
// cancels the timeout timer, calls the engine's Cancel, then records outcome=cancelled. done is
// never called for it, and an engine result that arrives anyway is ignored (EFF-060). Only the
// cell's turn calls it, so Do has returned and cancelTimer and cancelRun are set. It runs no user
// code.
func (c *cell[M]) cancelEffect(pe *pendingEffect[M]) bool {
	cancelRun, ok := c.takeEffect(pe)
	if !ok {
		return false
	}
	pe.cancelTimer()
	if cancelRun != nil {
		cancelRun()
	}
	c.recordEffectDone(pe.inc, pe.id, pe.name, outcomeCancelled, nil)
	return true
}

// cancelEffects cancels every pending effect of the cell, in ID order, as the incarnation ends
// (EFF-100). The table holds the live incarnation's effects only: an earlier incarnation's were
// cancelled when it ended.
func (c *cell[M]) cancelEffects() {
	c.emu.Lock()
	pes := make([]*pendingEffect[M], 0, len(c.effects))
	for _, pe := range c.effects { //lockstep:maporder the effects are sorted by ID below
		pes = append(pes, pe)
	}
	c.emu.Unlock()
	slices.SortFunc(pes, func(a, b *pendingEffect[M]) int { return cmp.Compare(a.id, b.id) })
	for _, pe := range pes {
		c.cancelEffect(pe)
	}
}

// effectDone is the done function that Do gives the engine (EFF-060). It runs in engine context,
// possibly on another goroutine than the turn. A call for an effect that is no longer pending is
// ignored without a record. Otherwise the timeout timer is cancelled and the outcome follows err:
// ErrOverloaded when the engine refused the effect, an error when err is not nil, ok otherwise.
func (c *cell[M]) effectDone(pe *pendingEffect[M], v any, err error) {
	if _, ok := c.takeEffect(pe); !ok {
		return
	}
	pe.cancelTimer()
	outcome := outcomeOK
	switch {
	case errors.Is(err, engine.ErrOverloaded):
		v, err, outcome = nil, ErrOverloaded, outcomeOverloaded
	case err != nil:
		outcome = outcomeError
	}
	c.deliverEffect(pe, v, err, outcome)
}

// effectTimedOut is the callback of the effect's timeout timer (EFF-090), which runs in engine
// context. If the effect is still pending it calls the engine's Cancel, then delivers ErrTimeout.
// A timer that fires before Start has returned has no Cancel to call: ErrTimeout is delivered
// first, and Do calls the Cancel when Start returns.
func (c *cell[M]) effectTimedOut(pe *pendingEffect[M]) {
	cancelRun, ok := c.takeEffect(pe)
	if !ok {
		return
	}
	if cancelRun != nil {
		cancelRun()
	}
	c.deliverEffect(pe, nil, ErrTimeout, outcomeTimeout)
}

// deliverEffect ends pe with outcome: it records lockstep.effect.done and enqueues the result as a
// Self entry of the effect's incarnation, which the mailbox always admits (MBX-035). A value that
// is not a T is misuse (EFF-070): done is not called, Failer.Fail gets the error and the record
// says outcome=error. If the incarnation has ended and no entry can be enqueued, the result is
// recorded as dropped (MBX-170). It runs no lock and no user code under a lock.
func (c *cell[M]) deliverEffect(pe *pendingEffect[M], v any, err error, outcome string) {
	call, misuse := pe.bind(v, err)
	if misuse != nil {
		c.sys.eng.Failer.Fail(misuse)
		c.recordEffectDone(pe.inc, pe.id, pe.name, outcomeError, misuse)
		return
	}
	c.recordEffectDone(pe.inc, pe.id, pe.name, outcome, err)
	res := effectResult[M]{res: v, call: call}
	if err != nil {
		res.res = err
	}
	if !c.offerSelf(entry{msg: res, kind: entrySelf, inc: pe.inc}) {
		c.sys.recordDropEnded(c.addr, pe.inc, res.res)
	}
}

// processEffectResult handles the effect result entry e that the turn popped (EFF-080). A result
// of an incarnation that has ended is dropped (MBX-170). Otherwise done and Receive are one
// guarded handler call of this turn with source receive, so a panic or a Fail in either fails the
// actor (SUP-140). It returns the message that the turn's lockstep.turn record describes: the
// message done returned, none when the entry was dropped or done panicked.
func (c *cell[M]) processEffectResult(e entry, r effectResult[M]) (msg any, ok bool) {
	if e.inc != c.inc {
		c.sys.recordDropEnded(c.addr, e.inc, r.res)
		return nil, false
	}
	c.callHandler(supervise.InReceive, func() {
		m := r.call()
		msg, ok = m, true
		c.actor.Receive(c.ctx, m)
	})
	return msg, ok
}

// recordEffectDone emits lockstep.effect.done (EFF §8) for effect id of incarnation inc, with
// outcome and err; the attribute err is empty without an error.
func (c *cell[M]) recordEffectDone(inc uint32, id EffectID, name, outcome string, err error) {
	tr := c.sys.eng.Tracer
	if !tr.Enabled() {
		return
	}
	desc := ""
	if err != nil {
		desc = tr.Describe(err)
	}
	tr.Record("lockstep.effect.done",
		engine.Attr{Key: "actor", Value: c.addr.String()},
		engine.Attr{Key: "inc", Value: strconv.FormatUint(uint64(inc), 10)},
		engine.Attr{Key: "effect", Value: strconv.FormatUint(uint64(id), 10)},
		engine.Attr{Key: "name", Value: name},
		engine.Attr{Key: "outcome", Value: outcome},
		engine.Attr{Key: "err", Value: desc},
	)
}
