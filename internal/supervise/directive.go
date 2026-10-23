// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package supervise

import "strconv"

// Directive has the values of lockstep.Directive.
type Directive uint8

const (
	Restart Directive = iota + 1
	Stop
	Escalate
	Resume
)

// Scope is OneForOne or AllForOne.
type Scope uint8

const (
	OneForOne Scope = iota + 1
	AllForOne
)

// Source tells where a failure happened.
type Source uint8

const (
	InFactory Source = iota + 1
	InStarted
	InReceive
	InSignal
	InStopped
	InDecider
	InEscalate
	InInjected // a failure the simulation injected with Sim.InjectFail (SFT-040)
)

// String returns "factory", "started", "receive", "signal", "stopped", "decider", "escalate", "injected", or
// "Source(<n>)" for a value outside InFactory..InInjected.
func (s Source) String() string {
	switch s {
	case InFactory:
		return "factory"
	case InStarted:
		return "started"
	case InReceive:
		return "receive"
	case InSignal:
		return "signal"
	case InStopped:
		return "stopped"
	case InDecider:
		return "decider"
	case InEscalate:
		return "escalate"
	case InInjected:
		return "injected"
	}
	return "Source(" + strconv.Itoa(int(s)) + ")"
}

// Normalize turns a decider's result d for a failure at src into the directive to apply
// (SUP-180). Resume for InFactory or InStarted becomes Restart, because an actor that never
// finished starting has no initialized state to resume. ok is false, and eff is zero, for a value
// outside Restart..Resume.
func Normalize(d Directive, src Source) (eff Directive, ok bool) {
	if d < Restart || d > Resume {
		return 0, false
	}
	if d == Resume && (src == InFactory || src == InStarted) {
		return Restart, true
	}
	return d, true
}
