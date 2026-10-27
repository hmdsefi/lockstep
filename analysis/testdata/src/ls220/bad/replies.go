// Package bad holds the LS220 findings: replies that are never awaited or piped (ANL-290,
// AT-ANL-19) and discarded Send and Spawn errors (ANL-300, AT-ANL-20).
package bad

import (
	"github.com/hmdsefi/lockstep"
)

// Msg is the message type of the asked actors.
type Msg struct{ To lockstep.ReplyTo[int] }

func build(rt lockstep.ReplyTo[int]) Msg { return Msg{To: rt} }

// statements ignore the reply in every statement form.
func statements(sys *lockstep.System, pid lockstep.PID[Msg]) {
	lockstep.Ask(sys, pid, build)             // want `^LS220: the reply of lockstep\.Ask is never awaited or piped; call Await outside actors or lockstep\.Pipe inside them$`
	_ = lockstep.Ask(sys, pid, build)         // want `^LS220: the reply of lockstep\.Ask is never awaited or piped; call Await outside actors or lockstep\.Pipe inside them$`
	go lockstep.Ask(sys, pid, build)          // want `^LS220: the reply of lockstep\.Ask is never awaited or piped; call Await outside actors or lockstep\.Pipe inside them$`
	defer lockstep.Ask(sys, pid, build)       // want `^LS220: the reply of lockstep\.Ask is never awaited or piped; call Await outside actors or lockstep\.Pipe inside them$`
	var _ = lockstep.Ask(sys, pid, build)     // want `^LS220: the reply of lockstep\.Ask is never awaited or piped; call Await outside actors or lockstep\.Pipe inside them$`
	if lockstep.Ask(sys, pid, build) == nil { // want `^LS220: the reply of lockstep\.Ask is never awaited or piped; call Await outside actors or lockstep\.Pipe inside them$`
		return
	}
	lockstep.Ask[Msg, int](sys, pid, build) // want `^LS220: the reply of lockstep\.Ask is never awaited or piped; call Await outside actors or lockstep\.Pipe inside them$`
}

// variables are assigned the reply and never use it.
func variables(sys *lockstep.System, pid lockstep.PID[Msg]) {
	r := lockstep.Ask(sys, pid, build) // want `^LS220: the reply of lockstep\.Ask is never awaited or piped; call Await outside actors or lockstep\.Pipe inside them$`
	if r == nil {
		return
	}

	blank := lockstep.Ask(sys, pid, build) // want `^LS220: the reply of lockstep\.Ask is never awaited or piped; call Await outside actors or lockstep\.Pipe inside them$`
	_ = blank

	// The copy of a variable that is never used does not use the reply either.
	first := lockstep.Ask(sys, pid, build) // want `^LS220: the reply of lockstep\.Ask is never awaited or piped; call Await outside actors or lockstep\.Pipe inside them$`
	second := first
	_ = second

	// A variable that is only assigned again is not used.
	var again *lockstep.Reply[int]
	again = lockstep.Ask(sys, pid, build) // want `^LS220: the reply of lockstep\.Ask is never awaited or piped; call Await outside actors or lockstep\.Pipe inside them$`
	_ = again == nil

	switch lockstep.Ask(sys, pid, build) { // want `^LS220: the reply of lockstep\.Ask is never awaited or piped; call Await outside actors or lockstep\.Pipe inside them$`
	case nil:
	}

	// Of two replies in one assignment, only the one that is used is used.
	used, unused := lockstep.Ask(sys, pid, build), lockstep.Ask(sys, pid, build) // want `^LS220: the reply of lockstep\.Ask is never awaited or piped; call Await outside actors or lockstep\.Pipe inside them$`
	_, _ = used.Await(nil)
	_ = unused
}

// shadowed declares a second variable of the same name: the reply of the outer one stays unused.
func shadowed(sys *lockstep.System, pid lockstep.PID[Msg]) {
	r := lockstep.Ask(sys, pid, build) // want `^LS220: the reply of lockstep\.Ask is never awaited or piped; call Await outside actors or lockstep\.Pipe inside them$`
	func() {
		r := lockstep.Ask(sys, pid, build)
		_, _ = r.Await(nil)
	}()
	_ = r
}

// inLiteral asks inside a literal and keeps the reply in a variable of the literal.
func inLiteral(sys *lockstep.System, pid lockstep.PID[Msg]) func() {
	return func() {
		r := lockstep.Ask(sys, pid, build) // want `^LS220: the reply of lockstep\.Ask is never awaited or piped; call Await outside actors or lockstep\.Pipe inside them$`
		_ = r
	}
}

// packageVar initializes a package variable with the call itself; the variable holds it, so it is a
// use, and a blank package variable is not.
var _ = lockstep.Ask(nil, lockstep.PID[Msg]{}, build) // want `^LS220: the reply of lockstep\.Ask is never awaited or piped; call Await outside actors or lockstep\.Pipe inside them$`
