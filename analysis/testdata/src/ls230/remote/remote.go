// Package remote holds actors whose message types are sealed interfaces of another package
// (ANL-320, AT-ANL-22): only the exported members are expected, through the package's fact.
package remote

import (
	"github.com/hmdsefi/lockstep"

	"ls230/msgs"
)

// Remote handles msgs.A only: msgs.B is reported, the unexported member is not.
type Remote struct{}

func (r *Remote) Receive(ctx *lockstep.Context[msgs.Msg], msg msgs.Msg) {
	switch msg.(type) { // want `^LS230: \(\*Remote\)\.Receive has no case for message type msgs\.B of msgs\.Msg$`
	case msgs.A:
	}
}

// Runner handles msgs.Start only: the pointer member *msgs.Stop is reported.
type Runner struct{}

func (r Runner) Receive(ctx *lockstep.Context[msgs.Cmd], msg msgs.Cmd) {
	switch msg.(type) { // want `^LS230: Runner\.Receive has no case for message type \*msgs\.Stop of msgs\.Cmd$`
	case msgs.Start:
	}
}

// Full covers every exported member.
type Full struct{}

func (f *Full) Receive(ctx *lockstep.Context[msgs.Msg], msg msgs.Msg) {
	switch msg.(type) {
	case msgs.A, msgs.B:
	}
}

// Any handles an interface that is not sealed: nothing is expected but self-messages.
type Any struct{}

func (a *Any) Receive(ctx *lockstep.Context[msgs.Open], msg msgs.Open) {
	switch msg.(type) {
	case nil:
	}
}
