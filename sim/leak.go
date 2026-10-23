// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim

import (
	"errors"
	"fmt"

	"github.com/hmdsefi/lockstep/internal/engine"
)

// heldEffect is an effect the engine started for an actor and still holds: its result event has
// not run and its Cancel has not succeeded (SIM-290 b).
type heldEffect struct {
	id   uint64
	name string
}

// hold adds ef to the effects held for the actor with the address text actor. Effects are
// started in ID order, so each actor's slice is in ID order.
func (e *simEngine) hold(actor string, ef *heldEffect) {
	if e.held == nil {
		e.held = make(map[string][]*heldEffect)
	}
	e.held[actor] = append(e.held[actor], ef)
}

// release removes ef from the effects held for actor: its result event ran or its Cancel
// succeeded.
func (e *simEngine) release(actor string, ef *heldEffect) {
	list := e.held[actor]
	for i, h := range list {
		if h == ef {
			e.held[actor] = append(list[:i:i], list[i+1:]...)
			return
		}
	}
}

// checkLeak is the leak check of SIM-290: Record calls it with every record after emitting it, so
// the lockstep.leak records follow the record they are about. A dead engine never gets here
// (SIM-120), so the actors of a discarded system are never checked.
//
// A lockstep.close record with a non-empty err is a resource that failed to close. At a
// lockstep.restart or lockstep.stop record of an actor, every effect the engine still holds for it
// is a pending effect; they are reported in ID order and forgotten.
func (e *simEngine) checkLeak(kind string, attrs []engine.Attr) {
	switch kind {
	case "lockstep.close":
		if msg := attrValue(attrs, "err"); msg != "" {
			e.leak(attrs, "resource", fmt.Sprintf("resource %s (%s) failed to close: %s",
				attrValue(attrs, "index"), attrValue(attrs, "type"), msg))
		}
	case "lockstep.restart", "lockstep.stop":
		actor := attrValue(attrs, "actor")
		list := e.held[actor]
		delete(e.held, actor)
		for _, ef := range list {
			e.leak(attrs, "effect", fmt.Sprintf("effect %d %q still pending", ef.id, ef.name))
		}
	}
}

// leak stores the failure "lockstep: leak: <actor> (incarnation <inc>): <what>" in the
// lockstep/leak slot and emits lockstep.leak for the record with attributes attrs (SIM-290).
func (e *simEngine) leak(attrs []engine.Attr, what, detail string) {
	actor, inc := attrValue(attrs, "actor"), attrValue(attrs, "inc")
	msg := fmt.Sprintf("lockstep: leak: %s (incarnation %s): %s", actor, inc, detail)
	e.emit("lockstep.leak", "leak "+actor, []engine.Attr{
		{Key: "actor", Value: actor},
		{Key: "inc", Value: inc},
		{Key: "what", Value: what},
		{Key: "error", Value: msg},
	})
	e.s.store(checkLeak, errors.New(msg))
}
