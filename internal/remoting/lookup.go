// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package remoting

import (
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/hooks"
)

// errShutDown is the error of the calls that need a core that has not shut down.
var errShutDown = errors.New("remoting: shut down")

// lookup is one lookup sent to a peer and not answered yet (REM-290).
type lookup struct {
	ask    engine.Address // the reply address the answer goes to
	target engine.Address // the looked-up path on the peer, uid 0
}

// Lookup sends node a LOOKUP of the actor registered at path whose message type has the type key
// typeKey, or of any type when typeKey is empty. The answer goes to reply, the address of a pending
// ask of this node that takes a lockstep.Address (lockstep.NewReply): node answers with the actor's
// address, with its uid, or with a failure whose code is not-found or wrong-type. The frame is
// queued and sent as Deliver sends frames; a LOOKUP that becomes a dead letter fails reply with
// Errors.Unreachable, and a restart of node fails it with Errors.PeerRestarted. The core forgets
// a lookup once it is answered, failed, or no longer pending at a heartbeat tick.
//
// Lookup returns an error, and sends nothing, when node is this node or not a valid node name,
// when reply is not an address of this node with a uid, when the frame exceeds MaxFrame (the
// error of Errors.FrameTooLarge), and after Shutdown. Call it in engine context or from any
// goroutine that may call Deliver.
func (c *Core) Lookup(reply engine.Address, node, path, typeKey string) error {
	if node == c.node || !engine.ValidNodeName(node) {
		return fmt.Errorf("remoting: cannot look up on node %q", node)
	}
	if reply.Node != c.node || reply.UID == 0 {
		return fmt.Errorf("remoting: lookup reply address %s is not an ask address of node %q", reply, c.node)
	}
	target := engine.Address{Node: node, Path: path}
	f := Frame{Kind: KindLookup, Sender: addrText(reply), Recipient: target.String(), Type: typeKey}
	out := &outFrame{b: AppendFrame(nil, &f), kind: KindLookup, from: reply, to: target, typ: typeKey, msg: path, ask: reply}
	if n := out.size(); n > c.set.MaxFrame {
		return c.errs.FrameTooLarge(n, c.set.MaxFrame)
	}
	c.mu.Lock()
	defer c.unlock()
	if c.shut {
		return errShutDown
	}
	p := c.peer(node)
	p.lookups = append(p.lookups, lookup{ask: reply, target: target})
	c.enqueue(node, out)
	return nil
}

// forgetLookup forgets the lookup of p, which may be nil, whose answer goes to ask, if there is
// one.
func (c *Core) forgetLookup(p *peer, ask engine.Address) {
	if p == nil {
		return
	}
	i := slices.IndexFunc(p.lookups, func(l lookup) bool { return l.ask == ask })
	if i < 0 {
		return
	}
	p.lookups = slices.Delete(p.lookups, i, i+1)
	if len(p.lookups) == 0 {
		p.lookups = nil
	}
}

// failLookups fails every lookup sent to p with err, in the order they were sent, through the
// host once the lock is released, and forgets them.
func (c *Core) failLookups(p *peer, err error) {
	for _, l := range p.lookups {
		env := engine.Envelope{From: l.target, To: l.ask, Payload: hooks.NewPayload(hooks.RemotePayload{Kind: hooks.RemoteFail, Err: err})}
		c.deferred = append(c.deferred, func() { c.host.Inbound(env) })
	}
	p.lookups = nil
}

// peerLookup is a lookup and the peer it was sent to.
type peerLookup struct {
	p   *peer
	ask engine.Address
}

// pruneLookups forgets, once the lock is released, the lookups whose asks are no longer pending:
// they timed out, or the system stopped (REM-290). hooks.Pending runs without the lock; the
// lookups are forgotten under it.
func (c *Core) pruneLookups() {
	var ls []peerLookup
	for _, name := range slices.Sorted(maps.Keys(c.peers)) {
		p := c.peers[name]
		for _, l := range p.lookups {
			ls = append(ls, peerLookup{p: p, ask: l.ask})
		}
	}
	if len(ls) == 0 {
		return
	}
	c.deferred = append(c.deferred, func() {
		var gone []peerLookup
		for _, l := range ls {
			if !hooks.Pending(c.sys, l.ask) {
				gone = append(gone, l)
			}
		}
		if len(gone) == 0 {
			return
		}
		c.mu.Lock()
		defer c.unlock()
		for _, l := range gone {
			c.forgetLookup(l.p, l.ask)
		}
	})
}

// receiveLookup applies a LOOKUP from the peer p of target's path with typeKey, whose answer goes
// to reply (REM-290): hooks.LookupPath runs once the lock is released, and its answer is queued
// for p (answerLookup).
func (c *Core) receiveLookup(p *peer, reply, target engine.Address, typeKey string) {
	c.deferred = append(c.deferred, func() { c.answerLookup(p.name, reply, target, typeKey) })
}

// answerLookup looks target's path up with typeKey, without the lock, and queues the answer for
// node: a REPLY of the actor's lockstep.Address from the actor, or a FAIL from target with the
// lookup's error, whose code is not-found or wrong-type (other for an invalid path). An address
// that does not encode is answered as a FAIL of code other with the encode error. After Shutdown
// nothing is sent.
func (c *Core) answerLookup(node string, reply, target engine.Address, typeKey string) {
	addr, err := hooks.LookupPath(c.sys, target.Path, typeKey)
	var out *outFrame
	if err == nil {
		out, err = c.lookupReply(reply, addr)
	}
	if err != nil {
		out = c.lookupFail(reply, target, err)
	}
	c.mu.Lock()
	defer c.unlock()
	if c.shut {
		return
	}
	c.enqueue(node, out)
}

// lookupReply returns the REPLY from addr to reply that answers a lookup with addr.
func (c *Core) lookupReply(reply, addr engine.Address) (*outFrame, error) {
	var a lockstep.Address
	if err := a.UnmarshalText([]byte(addrText(addr))); err != nil {
		return nil, err
	}
	name, data, err := c.set.Registry.Encode(c.set.Codec, a)
	if err != nil {
		return nil, err
	}
	f := Frame{Kind: KindReply, Sender: addrText(addr), Recipient: addrText(reply), Type: name, Payload: data}
	return &outFrame{b: AppendFrame(nil, &f), kind: KindReply, from: addr, to: reply, typ: name, msg: a}, nil
}

// lookupFail returns the FAIL from target to reply that answers a lookup with err, its message cut
// to fit MaxFrame.
func (c *Core) lookupFail(reply, target engine.Address, err error) *outFrame {
	f := Frame{Kind: KindFail, Sender: addrText(target), Recipient: addrText(reply)}
	b := c.failFrame(&f, Failure{Code: c.errs.Code(err), Message: err.Error()})
	return &outFrame{b: b, kind: KindFail, from: target, to: reply, msg: err}
}
