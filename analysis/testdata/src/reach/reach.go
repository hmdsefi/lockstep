// Package reach has two actors that share helpers. A probe reports "reach <root>: <functions>" at
// every root, the functions reachable from it alone, and "actor code: <functions>" at every actor
// type, the functions reachable from all its roots.
package reach

import "github.com/hmdsefi/lockstep"

type Msg int

type A struct{} // want `^actor code: \(\*A\)\.Receive, \(\*A\)\.Stopped, onlyA, shared, closeA, leaf$`

func (a *A) Receive(ctx *lockstep.Context[Msg], m Msg) { // want `^reach \(\*A\)\.Receive: \(\*A\)\.Receive, onlyA, shared, leaf$`
	shared()
	onlyA()
}

func (a *A) Stopped(ctx *lockstep.Context[Msg], r lockstep.StopReason) { // want `^reach \(\*A\)\.Stopped: \(\*A\)\.Stopped, closeA, leaf$`
	closeA()
}

// B reaches shared after A did: its chain goes through A, but B's actor code holds it too.
type B struct{} // want `^actor code: \(\*B\)\.Receive, \(\*B\)\.Receive\$1, shared, onlyB, leaf$`

func (b *B) Receive(ctx *lockstep.Context[Msg], m Msg) { // want `^reach \(\*B\)\.Receive: \(\*B\)\.Receive, \(\*B\)\.Receive\$1, shared, onlyB, leaf$`
	f := func() { onlyB() }
	f()
	shared()
}

func onlyA() {}

func onlyB() {}

func shared() { leaf() }

func closeA() { leaf() }

func leaf() {}

func unreached() { leaf() }
