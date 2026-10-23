// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim

import (
	"errors"
	"strconv"

	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/lockstep/internal/engine"
)

// sendLabel is the label of the node event that does SIM-160 step 4 for an envelope a paused node
// delivered: it sends the envelope once the node resumes (SIM-140).
const sendLabel = "lockstep/send"

// frame is the simnet payload that carries one envelope between lockstep nodes (SIM-160). inc is
// the incarnation of the sending node; with the sending node it forms the stream key whose
// numbers the receiver checks (SIM-170). The envelope travels by reference: sim never copies or
// encodes its payload (R30).
type frame struct {
	inc uint32
	env engine.Envelope
}

// deferredFrame is a frame that waits in the engine's queue of deferred sends, with its
// destination node.
type deferredFrame struct {
	to kernel.NodeID
	f  frame
}

// stream is a stream key: the sending node and its incarnation (SIM-170).
type stream struct {
	from kernel.NodeID
	inc  uint32
}

// Deliver sends env to the lockstep node named env.To.Node over simnet (SIM-160). It numbers env
// with the engine's counter for that node, and from a paused node it sends env once the node
// resumes (SIM-140). Envelopes leave in Deliver order: while deferred sends wait, a later envelope
// queues behind them even if the node is up again. An envelope for a node that is not a lockstep
// node of this Sim is dropped with a lockstep.drop record. Deliver always returns nil: loss on
// the network is never reported (ENG-080).
func (e *simEngine) Deliver(env engine.Envelope) error {
	if !e.canSchedule() {
		return nil
	}
	b, ok := e.s.byName[env.To.Node]
	if !ok {
		e.drop(env, "unknown-node")
		return nil
	}
	to := b.n.ID()
	if e.seq == nil {
		e.seq = make(map[kernel.NodeID]uint64)
	}
	e.seq[to]++
	env.Seq = e.seq[to]
	f := frame{inc: e.inc, env: env}
	if e.n.State() == kernel.NodePaused || len(e.deferred) > 0 {
		e.deferred = append(e.deferred, deferredFrame{to: to, f: f})
		e.n.Post(sendLabel, e.sendDeferred)
		return nil
	}
	e.s.w.Net.Send(e.n, to, f)
	return nil
}

// sendDeferred is the lockstep/send event: it sends the oldest deferred frame (SIM-160 step 4).
// Each deferred frame has one event, and every event sends the queue's head, so the frames leave
// in Deliver order whatever order the events run in.
func (e *simEngine) sendDeferred() {
	d := e.deferred[0]
	e.deferred[0] = deferredFrame{}
	e.deferred = e.deferred[1:]
	e.s.w.Net.Send(e.n, d.to, d.f)
}

// drop emits the lockstep.drop record of an envelope sim discards (SIM §8): reason unknown-node
// (SIM-160) or stale-seq (SIM-170), which adds the envelope's number.
func (e *simEngine) drop(env engine.Envelope, reason string) {
	attrs := []engine.Attr{{Key: "to", Value: env.To.String()}, {Key: "reason", Value: reason}, {Key: "from", Value: env.From.String()}}
	if reason == "stale-seq" {
		attrs = append(attrs, engine.Attr{Key: "seq", Value: strconv.FormatUint(env.Seq, 10)})
	}
	attrs = append(attrs, engine.Attr{Key: "msg", Value: kernel.Describe(env.Payload)})
	e.emit("lockstep.drop", "drop to "+env.To.String(), attrs)
}

// arrive is the simnet handler of the engine's node for the incarnation of e (SIM-170). It hands
// the envelope of each frame to the system's Host in the order of its stream: an envelope
// numbered below the next expected number of its stream is a duplicate or a late copy and is
// dropped with a lockstep.drop record; a gap is a loss, and nothing waits for it. A payload that
// is not a frame fails the seed with a misuse failure. A dead engine ignores every arrival
// (SIM-120).
func (e *simEngine) arrive(from kernel.NodeID, payload any) {
	if e.dead {
		return
	}
	f, ok := payload.(frame)
	if !ok {
		e.misuse(errors.New("lockstep: node " + e.n.Name() + " received a non-lockstep payload " + kernel.Describe(payload)))
		return
	}
	key := stream{from: from, inc: f.inc}
	next := e.next[key]
	if next == 0 {
		next = 1
	}
	env := f.env
	if env.Seq < next {
		e.drop(env, "stale-seq")
		return
	}
	if e.next == nil {
		e.next = make(map[stream]uint64)
	}
	e.next[key] = env.Seq + 1
	e.host.Inbound(env)
}
