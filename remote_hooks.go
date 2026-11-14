// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/hooks"
)

// This file is the message side of the remoting hooks (REM §4.3): the payloads of messages, ask
// requests, replies and failures, and the ask table's answers to the remoting core. The runtime
// already hands these payloads to the transport (sendRemote, replyRemote) and applies them on
// arrival (host.Inbound); the hooks only translate between them and hooks.RemotePayload, so a
// payload built by NewPayload takes exactly the path of one that crossed by reference.

// remotePayloadHook is hooks.Payload for messages, ask requests and reply attempts: a userPayload
// is RemoteMsg, or RemoteAsk when it carries an ask address; a replyPayload is RemoteReply when its
// error is nil and RemoteFail otherwise (ReplyTo.Fail never has a nil error, and neither has a
// failure the runtime issues: MSG-340, MSG-345, MSG-350). It reports false for anything else.
func remotePayloadHook(p any) (hooks.RemotePayload, bool) {
	switch p := p.(type) {
	case userPayload:
		if p.ask.IsZero() {
			return hooks.RemotePayload{Kind: hooks.RemoteMsg, Value: p.msg}, true
		}
		return hooks.RemotePayload{Kind: hooks.RemoteAsk, Value: p.msg, Ask: p.ask.engine()}, true
	case replyPayload:
		if p.err != nil {
			return hooks.RemotePayload{Kind: hooks.RemoteFail, Err: p.err}, true
		}
		return hooks.RemotePayload{Kind: hooks.RemoteReply, Value: p.v}, true
	}
	return hooks.RemotePayload{}, false
}

// newRemotePayloadHook is hooks.NewPayload for messages, ask requests and reply attempts (REM-160).
// A message or ask request gets no message type, so a path recipient resolves to the actor
// registered at the path whatever its M (resolve): the core has checked Assignable. host.Inbound
// then delivers it as a local send from an actor sender (MBX-120), and offer tracks an ask request
// (MSG-340) or deadRequest fails it with ErrTerminated (MSG-350); a reply attempt is applied by
// settle (MSG-420). It panics for a RemoteAsk without an ask address and a RemoteFail without an
// error, which the core never builds, and returns nil, which host.Inbound drops, for a kind it does
// not cover.
func newRemotePayloadHook(p hooks.RemotePayload) any {
	switch p.Kind {
	case hooks.RemoteMsg:
		return userPayload{msg: p.Value}
	case hooks.RemoteAsk:
		if p.Ask == (engine.Address{}) {
			panic("lockstep: hooks.NewPayload: RemoteAsk without an ask address")
		}
		return userPayload{msg: p.Value, ask: addressOf(p.Ask)}
	case hooks.RemoteReply:
		return replyPayload{v: p.Value}
	case hooks.RemoteFail:
		if p.Err == nil {
			panic("lockstep: hooks.NewPayload: RemoteFail with a nil Err")
		}
		return replyPayload{err: p.Err}
	}
	return nil
}

// assignableHook is hooks.Assignable. Each half answers for the addresses it owns, and every
// other address is false: the ask table owns the pending asks of sys (askAssignable); the actor
// half, for the live actors of sys, joins it with remote death watch.
func assignableHook(sys any, to engine.Address, v any) bool {
	s := hookSystem("Assignable", sys)
	a := addressOf(to)
	if ok, owned := s.askAssignable(a, v); owned {
		return ok
	}
	return false
}

// askAssignable is the ask table's half of hooks.Assignable. owned reports whether ask is a
// pending ask of s; ok then reports whether v is assignable to the ask's R, where a nil v needs an
// interface R. The core fails an ask whose answer is not assignable with ErrWrongType (REM-264).
func (s *System) askAssignable(ask Address, v any) (ok, owned bool) {
	r := s.asks.pendingReply(ask)
	if r == nil {
		return false, false
	}
	return r.assignable(v), true
}

// failAsksHook is hooks.FailAsks (REM-294): it resolves with err, in ask-counter order, every
// pending ask of sys whose target is on node. An ask without a target (NewReply) is left alone. A
// stopped or discarded system has no pending asks left. Each resolution goes through settle, so an
// ask that a reply, a timeout or the stop resolves meanwhile resolves once (MSG-430); it records
// lockstep.reply from the target with outcome resolved.
func failAsksHook(sys any, node string, err error) {
	s := hookSystem("FailAsks", sys)
	if err == nil {
		panic("lockstep: hooks.FailAsks: nil error")
	}
	for _, p := range s.pendingAsks() {
		if p.target.IsZero() || p.target.node != node {
			continue
		}
		s.settle(p.addr, resolution{how: byRouting, from: p.target, err: err})
	}
}

// pendingHook is hooks.Pending (MSG-370): whether ask is a pending ask of sys.
func pendingHook(sys any, ask engine.Address) bool {
	return hookSystem("Pending", sys).asks.pendingReply(addressOf(ask)) != nil
}

// pendingReply returns the resolver of the pending ask with exactly the address ask (node, path
// and uid), or nil if there is none: the ask resolved, or is not this system's.
func (t *askTable) pendingReply(ask Address) resolver {
	sh := t.shard(ask.uid)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if p := sh.lookup(ask); p != nil && p.pending {
		return p.reply
	}
	return nil
}
