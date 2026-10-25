// Package bad holds the LS110 findings: Reply.Await as a call, a deferred call and a method value
// in actor code (ANL-170, AT-ANL-12).
package bad

import (
	"context"

	"github.com/hmdsefi/lockstep"
)

// Msg is Account's message type.
type Msg int

// Account is an actor type with all four roots.
type Account struct {
	n   *lockstep.Reply[int]
	s   *lockstep.Reply[string]
	pid lockstep.PID[Msg]
}

// Started binds the method value of Await.
func (a *Account) Started(ctx *lockstep.Context[Msg]) {
	f := a.n.Await // want `^LS110: Reply\.Await blocks the actor; use lockstep\.Pipe to receive the reply as a message \(actor code: \(\*Account\)\.Started\)$`
	_ = f
}

// Receive holds the call forms. The literal comes first so that its chain is Receive$1.
func (a *Account) Receive(ctx *lockstep.Context[Msg], msg Msg) {
	later := func() {
		_, _ = a.n.Await(context.Background()) // want `^LS110: Reply\.Await blocks the actor; use lockstep\.Pipe to receive the reply as a message \(actor code: \(\*Account\)\.Receive -> \(\*Account\)\.Receive\$1\)$`
	}
	later()
	_, _ = a.n.Await(context.Background())                                                                     // want `^LS110: Reply\.Await blocks the actor; use lockstep\.Pipe to receive the reply as a message \(actor code: \(\*Account\)\.Receive\)$`
	_, _ = a.s.Await(context.Background())                                                                     // want `^LS110: Reply\.Await blocks the actor; use lockstep\.Pipe to receive the reply as a message \(actor code: \(\*Account\)\.Receive\)$`
	_, _ = lockstep.Ask(ctx, a.pid, func(lockstep.ReplyTo[bool]) Msg { return 0 }).Await(context.Background()) // want `^LS110: Reply\.Await blocks the actor; use lockstep\.Pipe to receive the reply as a message \(actor code: \(\*Account\)\.Receive\)$`
	g := (*lockstep.Reply[int]).Await                                                                          // want `^LS110: Reply\.Await blocks the actor; use lockstep\.Pipe to receive the reply as a message \(actor code: \(\*Account\)\.Receive\)$`
	_ = g
	go a.n.Await(context.Background()) // want `^LS110: Reply\.Await blocks the actor; use lockstep\.Pipe to receive the reply as a message \(actor code: \(\*Account\)\.Receive\)$`
}

// Stopped defers the call.
func (a *Account) Stopped(ctx *lockstep.Context[Msg], r lockstep.StopReason) {
	ctx2 := context.Background()
	defer a.n.Await(ctx2) // want `^LS110: Reply\.Await blocks the actor; use lockstep\.Pipe to receive the reply as a message \(actor code: \(\*Account\)\.Stopped\)$`
}

// Signal reaches Await through the helper wait.
func (a *Account) Signal(ctx *lockstep.Context[Msg], s lockstep.Signal) {
	wait(a.n)
}

// wait is reached from Signal.
func wait(r *lockstep.Reply[int]) {
	_, _ = r.Await(context.Background()) // want `^LS110: Reply\.Await blocks the actor; use lockstep\.Pipe to receive the reply as a message \(actor code: \(\*Account\)\.Signal -> wait\)$`
}
