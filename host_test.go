// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"slices"
	"testing"

	"github.com/hmdsefi/lockstep/internal/engine"
)

// remoteSender is the sender of the test envelopes: an actor on node n2.
var remoteSender = Address{node: "n2", path: "/user/x", uid: 7}

// envelopeTo returns an envelope from remoteSender to to, carrying msg as a user message.
func envelopeTo(to Address, msg any) engine.Envelope {
	return engine.Envelope{From: remoteSender.engine(), To: to.engine(), Payload: userPayload{msg: msg}}
}

// MBX-120, ENG-080: envelopes handed to Host.Inbound reach Receive in call order; one for an
// actor that is not alive, or for a path with no actor, delivers nothing.
func TestInboundUserMessages(t *testing.T) {
	sys, eng := newManualSystem(t)
	eng.SetHost(host{sys})
	a, log := logActor(t, sys, eng)

	eng.Inbound(envelopeTo(a.Address(), 1))
	eng.Inbound(envelopeTo(a.Address(), 2))
	eng.Inbound(envelopeTo(Address{node: "local", path: a.Address().Path(), uid: a.Address().UID() + 100}, 3))
	eng.Inbound(envelopeTo(Address{node: "local", path: "/user/zz"}, 4))
	eng.Inbound(envelopeTo(Address{node: "local", path: "/user/zz", uid: 5}, 5))
	eng.Inbound(envelopeTo(a.Address(), 6))
	eng.RunUntilIdle()
	if want := []int{1, 2, 6}; !slices.Equal(*log, want) {
		t.Fatalf("log %v, want %v", *log, want)
	}

	// A path address resolves on arrival, and only to an actor of the sender's message type.
	path := Address{node: "local", path: a.Address().Path()}
	right := envelopeTo(path, 7)
	right.Payload = userPayload{msg: 7, typ: typeOf[int]()}
	wrong := envelopeTo(path, 8)
	wrong.Payload = userPayload{msg: 8, typ: typeOf[string]()}
	eng.Inbound(right)
	eng.Inbound(wrong)
	eng.RunUntilIdle()
	if want := []int{1, 2, 6, 7}; !slices.Equal(*log, want) {
		t.Errorf("log %v, want %v", *log, want)
	}
}

// ENG-150, MBX-120: an inbound user message goes through the interposer first, and a held one is
// offered later without a second call.
func TestInboundInterposed(t *testing.T) {
	eng, sys, fake := interposedSystem(t, func(msg any) bool { return msg == 1 })
	eng.SetHost(host{sys})
	a, log := logActor(t, sys, eng)
	eng.Inbound(envelopeTo(a.Address(), 1))
	eng.Inbound(envelopeTo(a.Address(), 2))
	eng.RunUntilIdle()
	if !slices.Equal(*log, []int{2}) {
		t.Fatalf("log %v, want [2]", *log)
	}
	calls := fake.callsTo(a.Address().engine())
	if len(calls) != 2 || calls[0].from != remoteSender.engine() {
		t.Errorf("interposer calls %+v, want two from the envelopes' sender", calls)
	}
	fake.kept()
	eng.RunUntilIdle()
	if n := len(fake.callsTo(a.Address().engine())); !slices.Equal(*log, []int{2, 1}) || n != 2 {
		t.Errorf("after the offer: log %v, %d calls; want [2 1], 2", *log, n)
	}
}
