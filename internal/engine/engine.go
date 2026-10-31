// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package engine is the seam between the lockstep runtime and the engines that run it
// (architecture §12, RFC-0001). It holds types and interfaces, plus the Slot state machine every
// engine shares. Non-test files import only the standard library.
package engine

import (
	"context"
	"errors"
	"math/rand/v2"
	"strconv"
	"time"
)

// ErrOverloaded is returned by Admission and passed to effect callbacks when an engine refuses
// work. Package lockstep re-exports it as lockstep.ErrOverloaded (the same value).
var ErrOverloaded = errors.New("lockstep: overloaded")

// Address identifies one actor incarnation. lockstep.Address converts to and from it (ACT).
type Address struct {
	Node string // node name
	Path string // actor path starting with "/", for example "/user/orders/o-17"
	UID  uint64 // unique per spawn on Node
}

// String returns "<zero>" for the zero Address, and otherwise
// "lockstep://" + Node + Path + "#" + UID in lowercase hexadecimal (ACT-410).
func (a Address) String() string {
	if a == (Address{}) {
		return "<zero>"
	}
	return "lockstep://" + a.Node + a.Path + "#" + strconv.FormatUint(a.UID, 16)
}

// Attr is one trace attribute.
type Attr struct {
	Key   string // non-empty
	Value string
}

// Record is one trace record as a Tracer receives it.
type Record struct {
	Kind  string // "lockstep.<event>"
	Attrs []Attr
}

// Cancel stops a pending callback. It returns true if this call stopped the callback from ever
// running, and false if the callback already ran, is running, or was cancelled before.
type Cancel func() bool

// Header is one instrumentation header of a message (OTL-050). It has the layout of
// lockstep.Header, so the runtime converts []Header and lockstep.Headers without copying.
type Header struct {
	Key   string
	Value string
}

// Envelope carries one message between systems on different nodes.
type Envelope struct {
	From    Address  // sender; for a *System sender its user guardian's address (ACT)
	To      Address  // receiver
	Seq     uint64   // set by the transport (ENG-080); 0 on the sending side
	Headers []Header // set by the runtime (OTL-050, Phase 4), carried unchanged (ENG-085); nil when tracing is off
	Payload any      // runtime-defined; engines read it only to describe it in traces
}

// ActorRef is the runtime's handle on one actor cell, as engines see it.
type ActorRef interface {
	// Address returns the actor's address. It is constant for the cell's lifetime.
	Address() Address
	// Slot returns the cell's scheduling state. It returns the same pointer on every call.
	Slot() *Slot
	// RunTurn runs one turn (§5.3) with budget >= 1 and reports whether the actor still has work.
	RunTurn(budget int) (more bool)
}

// Scheduler runs turns.
type Scheduler interface {
	// Ready reports that a has work. It may be called from any goroutine at any time, including
	// from inside a's own RunTurn, and never blocks.
	Ready(a ActorRef)
	// InTurn reports whether the caller runs in this engine's engine context.
	InTurn() bool
}

// LocalScheduler is a Scheduler that can place a turn near the actor whose turn caused it. The
// runtime calls ReadyFrom instead of Ready when it enqueues during from's turn, if the engine's
// Scheduler implements this interface.
type LocalScheduler interface {
	Scheduler
	ReadyFrom(a, from ActorRef)
}

// Clock is the engine's time.
type Clock interface {
	// Now returns the engine's current time: wall time on the local and production engines, the
	// node's local clock in simulation.
	Now() time.Time
	// AfterFunc runs fn in engine context once d has passed on this clock (d <= 0: as soon as
	// possible), never inside the AfterFunc call itself. label names the timer's kind (ENG-070).
	AfterFunc(d time.Duration, label string, fn func()) Cancel
}

// Deadlines arms ask timeouts: many short-lived timers that mostly stop before they expire. The
// engine keeps them in one structure of its own, so arming and stopping one allocates nothing.
type Deadlines interface {
	// Arm makes x.Expire run in engine context once d has passed on the engine's clock (d <= 0:
	// as soon as possible), exactly once unless Disarm stops it, and never inside the Arm call.
	// key spreads concurrent callers over the engine's structure; callers pass a value that
	// differs between concurrent calls. Arm returns the deadline's handle, never 0, or 0 when the
	// engine arms nothing because it is stopping.
	Arm(key uint64, d time.Duration, x Expirer) uint64
	// Disarm stops the deadline with handle h. It returns true if this call kept Expire from ever
	// running, and false if Expire has started, the deadline was disarmed before, or h is 0.
	Disarm(h uint64) bool
}

// Expirer is what a deadline runs when it expires.
type Expirer interface {
	Expire()
}

// Transport carries envelopes to systems on other nodes.
type Transport interface {
	// Deliver sends env to the node env.To.Node. It never blocks. It returns an error only when
	// env cannot be sent at all for a synchronous, local reason (its message type is not
	// registered, encoding failed, the frame is too large) and then sends nothing; loss on the
	// network is never reported (ENG-080, R2).
	Deliver(env Envelope) error
}

// Host is the runtime side of one system that engines call into.
type Host interface {
	// Inbound hands the runtime an envelope that arrived for this system's node (ENG-080).
	Inbound(env Envelope)
}

// EffectRequest describes one effect started by an actor (EFF). Value is the user's Effect[T] value
// for a Do effect, or the hooks.InternalEffect of an internal effect, which simulation runs inline
// when its Inline is set (PER-240, PER-520).
type EffectRequest struct {
	ID    uint64                                 // unique per system, assigned by the runtime
	Name  string                                 // Effect.Name()
	Actor Address                                // the actor that started it
	Value any                                    // the Effect[T] or hooks.InternalEffect value
	Run   func(ctx context.Context) (any, error) // the production implementation
}

// ValidEffectName reports whether name is a valid effect name: it matches
// ^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$ (EFF-010). Do and the simulation's HandleEffect both check
// names with it (EFF-010, EFF-410). Reserved names starting with "lockstep." are valid here.
func ValidEffectName(name string) bool {
	if len(name) < 1 || len(name) > 64 {
		return false
	}
	for i := range len(name) {
		b := name[i]
		switch {
		case b >= 'a' && b <= 'z', b >= 'A' && b <= 'Z', b >= '0' && b <= '9':
		case i > 0 && (b == '.' || b == '_' || b == '-'):
		default:
			return false
		}
	}
	return true
}

// Effects runs effects.
type Effects interface {
	// Start starts req. The engine calls done at most once, in engine context, never inside the
	// Start call itself. If the returned Cancel reports true, done is never called and the
	// context passed to req.Run (if the engine called it) is cancelled.
	Start(req EffectRequest, done func(v any, err error)) Cancel
}

// Entropy hands out random streams.
type Entropy interface {
	// Rand returns a random stream for label, which starts with "lockstep/" (TMR, EFF).
	Rand(label string) *rand.Rand
}

// Tracer receives the runtime's trace records.
type Tracer interface {
	// Enabled reports whether Record keeps records. It returns the same value for the engine's
	// lifetime; the runtime builds no record when it is false.
	Enabled() bool
	// Record emits one record. kind starts with "lockstep.". Safe for concurrent use.
	Record(kind string, attrs ...Attr)
	// Describe returns a description of v that is the same in every run.
	Describe(v any) string
}

// The Admission declarations below are OVL §4.3's, copied verbatim (R23).

// AdmissionSource is one reason admission can stop (OVL §3).
type AdmissionSource uint8

const (
	SourceMemory   AdmissionSource = iota + 1 // production live heap (OVL-320)
	SourceBudget                              // simulation count budgets (SFT-140)
	SourcePressure                            // simulation memory-pressure fault (SFT-125)
)

// String returns "memory", "budget", "pressure", or "AdmissionSource(<n>)".
func (s AdmissionSource) String() string {
	switch s {
	case SourceMemory:
		return "memory"
	case SourceBudget:
		return "budget"
	case SourcePressure:
		return "pressure"
	}
	return "AdmissionSource(" + strconv.FormatUint(uint64(s), 10) + ")"
}

// AdmissionChange is one source transition (OVL-150).
type AdmissionChange struct {
	Source    AdmissionSource
	On        bool   // the source's new state
	Live      uint64 // measured bytes at the change; 0 for SourcePressure
	High, Low uint64 // thresholds in force; 0 for SourcePressure
	Shedding  bool   // the combined state after the change: true while any source is on
}

// AdmissionError is the error AdmitSpawn and AdmitSend return while the system sheds.
type AdmissionError struct {
	Source AdmissionSource // the source that turned shedding on (OVL-100)
}

// Error returns "lockstep: overloaded (<source>)".
func (e *AdmissionError) Error() string {
	return "lockstep: overloaded (" + e.Source.String() + ")"
}

// Is reports whether target is ErrOverloaded, so errors.Is(err, ErrOverloaded) holds.
func (e *AdmissionError) Is(target error) bool {
	return target == ErrOverloaded
}

// Usage is what the runtime reports to admission about one system.
type Usage struct {
	Actors int   // live actors (OVL-020)
	Bytes  int64 // the system's estimate (OVL §3); 0 while estimates are off (OVL-400)
}

// Admission decides whether the system takes on more work (architecture §15, OVL).
type Admission interface {
	// AdmitSpawn returns nil or an *AdmissionError. reserve is true for reserve spawns (OVL §3),
	// which admission never refuses.
	AdmitSpawn(reserve bool) error
	// AdmitSend returns nil or an *AdmissionError. The runtime calls it only for user messages to
	// user actors (OVL-110).
	AdmitSend() error
	// Bind connects admission to its system. The runtime calls it exactly once, while the system
	// is built (New or hooks.NewSystem), before the first spawn. The engine reports no change
	// before Bind. It then calls onChange once per source transition, in transition order, never
	// concurrently with itself and without holding a lock that AdmitSpawn or AdmitSend takes; on
	// the simulation engine inside the kernel event that caused the change. usage returns the
	// system's current usage; it is safe to call from engine context, and engines that need no
	// usage ignore it.
	Bind(onChange func(AdmissionChange), usage func() Usage)
}

// Failer receives misuse the runtime detects. The simulation engine fails the seed with err;
// other engines may ignore it. The runtime still returns or traces the error itself.
type Failer interface {
	Fail(err error)
}

// Waiter makes an external caller wait (Reply.Await, System.Shutdown).
type Waiter interface {
	// Wait returns nil once done is closed, ctx.Err() if ctx ends first, or an engine error if
	// the engine can never close done (ENG-135). The runtime calls it only when InTurn is false.
	Wait(ctx context.Context, done <-chan struct{}) error
}

// The Interposer declaration below is SFT §12.2's, copied verbatim.

// Interposer lets the simulation engine hold or drop a sent message before the runtime offers it
// to the receiver's mailbox (SFT-070).
type Interposer interface {
	// Interpose is called in engine context, before the runtime offers a Send or Ask request
	// message, local (MBX-090) or inbound (MBX-120), to the receiver's queue. It returns false to
	// let the runtime offer it now. It returns true when the engine took the message: the runtime
	// then offers nothing and Send returns nil, and the engine calls offer later, at most once and
	// in engine context, to offer the message without interposition.
	Interpose(from, to Address, msg any, offer func()) bool
}

// Engine bundles what one engine provides to one system.
type Engine struct {
	Node       string     // node name; non-empty; wins over any lockstep.WithNodeName
	Epoch      uint32     // uid epoch (ACT-040); non-zero
	Blocking   bool       // a goroutine of an external caller may block (BlockSender, MBX)
	Scheduler  Scheduler  // required
	Clock      Clock      // required
	Deadlines  Deadlines  // nil: ask timeouts use Clock.AfterFunc (every engine but production)
	Transport  Transport  // nil: the system has no other nodes
	Effects    Effects    // required
	Entropy    Entropy    // required
	Tracer     Tracer     // required
	Admission  Admission  // required
	Failer     Failer     // required
	Waiter     Waiter     // required
	Interposer Interposer // nil: no interposition (every engine but sim)
	Stop       func()     // nil: nothing to stop; called once by System.Shutdown (ENG-140)
}

// Validate returns the first problem of e in field order, or nil (ENG-030).
func (e Engine) Validate() error {
	switch {
	case e.Node == "":
		return errors.New("lockstep: engine: Node is empty")
	case e.Epoch == 0:
		return errors.New("lockstep: engine: Epoch is 0")
	case e.Scheduler == nil:
		return errors.New("lockstep: engine: Scheduler is nil")
	case e.Clock == nil:
		return errors.New("lockstep: engine: Clock is nil")
	case e.Effects == nil:
		return errors.New("lockstep: engine: Effects is nil")
	case e.Entropy == nil:
		return errors.New("lockstep: engine: Entropy is nil")
	case e.Tracer == nil:
		return errors.New("lockstep: engine: Tracer is nil")
	case e.Admission == nil:
		return errors.New("lockstep: engine: Admission is nil")
	case e.Failer == nil:
		return errors.New("lockstep: engine: Failer is nil")
	case e.Waiter == nil:
		return errors.New("lockstep: engine: Waiter is nil")
	}
	return nil
}
