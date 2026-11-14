// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package remote

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/remoting"
)

// Lookup returns the PID of the actor registered at path on node, if it was spawned with exactly
// message type M. On sys's own node it is lockstep.Lookup. On another node it asks that node over
// sys's remoting and waits for the answer as Reply.Await does: ctx's deadline, if it has one, is
// the ask's timeout, else the default ask timeout applies, and inside a turn it returns
// lockstep.ErrAwaitInActor. The PID carries the actor's uid.
//
// In simulation, Lookup of another node from a test body fails the seed: it waits as
// Reply.Await does, and the simulation never waits on a pending reply (inside a turn it returns
// lockstep.ErrAwaitInActor, as everywhere). Address the actor with a path PID from
// lockstep.PIDAt instead, which needs no lookup, or call lockstep.Lookup on the system of the
// node that holds it.
//
// Errors: lockstep.ErrInvalidName for a node or path that is not well-formed; an error wrapping
// ErrUnreachable, which says so, when remoting is not enabled on sys; a *Error from the other
// node, whose code is CodeNotFound (it unwraps to lockstep.ErrNotFound) when no actor is
// registered at path and CodeWrongType (lockstep.ErrWrongType) when its message type is not M; a
// failure matching ErrPeerRestarted when the node restarts first; an error wrapping
// lockstep.ErrStopped when sys, and so its remoting, has stopped; and the errors of Reply.Await,
// lockstep.ErrTimeout among them. Lookup panics if sys is nil.
func Lookup[M any](ctx context.Context, sys *lockstep.System, node, path string) (lockstep.PID[M], error) {
	if sys == nil {
		panic("remote: Lookup with nil System")
	}
	if node == sys.Node() {
		return lockstep.Lookup[M](sys, path)
	}
	if !engine.ValidNodeName(node) {
		return lockstep.PID[M]{}, fmt.Errorf("%w: node %q", lockstep.ErrInvalidName, node)
	}
	var target lockstep.Address
	if err := target.UnmarshalText([]byte("lockstep://" + node + path + "#0")); err != nil || target.Path() != path {
		return lockstep.PID[M]{}, fmt.Errorf("%w: path %q", lockstep.ErrInvalidName, path)
	}
	core := remoting.CoreOf(sys)
	if core == nil {
		return lockstep.PID[M]{}, fmt.Errorf("%w: %s: remoting is not enabled on node %q", ErrUnreachable, node, sys.Node())
	}
	var opts []lockstep.AskOption
	if dl, ok := ctx.Deadline(); ok {
		d := time.Until(dl)
		if d <= 0 {
			return lockstep.PID[M]{}, context.DeadlineExceeded
		}
		opts = append(opts, lockstep.WithTimeout(d))
	}
	rt, r := lockstep.NewReply[lockstep.Address](sys, opts...)
	reply, ok := replyAddress(rt)
	if !ok {
		// NewReply resolved the reply at once: the system has stopped.
		_, err := r.Await(ctx)
		return lockstep.PID[M]{}, err
	}
	if err := core.Lookup(reply, node, path, typeKey(reflect.TypeFor[M]())); err != nil {
		rt.Fail(sys, err)
		return lockstep.PID[M]{}, err
	}
	a, err := r.Await(ctx)
	if err != nil {
		return lockstep.PID[M]{}, err
	}
	var pid lockstep.PID[M]
	text, _ := a.MarshalText()
	if err := pid.UnmarshalText(text); err != nil {
		return lockstep.PID[M]{}, err
	}
	return pid, nil
}

// replyAddress returns the address of rt's ask, from its text form, and false for the zero
// ReplyTo.
func replyAddress[R any](rt lockstep.ReplyTo[R]) (engine.Address, bool) {
	text, err := rt.MarshalText()
	rest, ok := strings.CutPrefix(string(text), "lockstep-reply://")
	if err != nil || !ok {
		return engine.Address{}, false
	}
	var a lockstep.Address
	if err := a.UnmarshalText([]byte("lockstep://" + rest)); err != nil {
		return engine.Address{}, false
	}
	return engine.Address{Node: a.Node(), Path: a.Path(), UID: a.UID()}, true
}

// typeKey returns the type key that another node compares with the message type of the actor it
// finds: PkgPath + "." + Name for a type declared in a package, else the type's spelling.
func typeKey(t reflect.Type) string {
	if t.PkgPath() != "" {
		return t.PkgPath() + "." + t.Name()
	}
	return t.String()
}
