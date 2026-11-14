// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"fmt"
	"reflect"

	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/hooks"
)

// This file is the remoting hooks (REM §4.3). The message side: the payloads of messages, ask
// requests, replies and failures, and the ask table's answers to the remoting core. The runtime
// already hands these payloads to the transport (sendRemote, replyRemote) and applies them on
// arrival (host.Inbound); the hooks only translate between them and hooks.RemotePayload, so a
// payload built by NewPayload takes exactly the path of one that crossed by reference. The actor
// side, below the message side: the payloads of remote death watch (Watch, Unwatch and
// remoteTerminated in watch.go), the transport's attachment, path lookups without a type, the
// watches of other nodes' actors on local ones, and the end of the watches of a node's actors.

// remotePayloadHook is hooks.Payload for messages, ask requests and reply attempts: a userPayload
// is RemoteMsg, or RemoteAsk when it carries an ask address; a replyPayload is RemoteReply when its
// error is nil and RemoteFail otherwise (ReplyTo.Fail never has a nil error, and neither has a
// failure the runtime issues: MSG-340, MSG-345, MSG-350). A watchPayload is the RemoteWatch or
// RemoteUnwatch it carries, and a terminatedPayload RemoteTerminated with its StopReason
// (watchPayloadOf). It reports false for anything else.
func remotePayloadHook(p any) (hooks.RemotePayload, bool) {
	if rp, ok := watchPayloadOf(p); ok {
		return rp, true
	}
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
// not cover. The watch kinds are newWatchPayload's.
func newRemotePayloadHook(p hooks.RemotePayload) any {
	switch p.Kind {
	case hooks.RemoteWatch, hooks.RemoteUnwatch, hooks.RemoteTerminated:
		return newWatchPayload(p)
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
// other address is false: the ask table owns the pending asks of sys (askAssignable), the
// registry the live actors of sys (actorAssignable).
func assignableHook(sys any, to engine.Address, v any) bool {
	s := hookSystem("Assignable", sys)
	a := addressOf(to)
	if ok, owned := s.askAssignable(a, v); owned {
		return ok
	}
	return s.actorAssignable(a, v)
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

// watchPayload is the payload of a RemoteWatch or RemoteUnwatch envelope, which Watch, Unwatch and
// the end of an incarnation hand the transport (ACT-602, ACT-630, ACT-640): From watches, or stops
// watching, To. seq is the watcher's number for the watch (ACT-630), which a RemoteTerminated
// echoes back.
type watchPayload struct {
	kind hooks.RemoteKind
	seq  uint64
}

// terminatedPayload is the payload of a RemoteTerminated envelope: From ended with reason, To is
// the watcher, and seq the watcher's number for the watch that ended (host.Inbound,
// remoteTerminated).
type terminatedPayload struct {
	reason StopReason
	seq    uint64
}

// watchPayloadOf is hooks.Payload for the watch kinds: a watchPayload is its RemoteWatch or
// RemoteUnwatch, a terminatedPayload RemoteTerminated with its StopReason; each carries its watch
// number. It reports false for anything else.
func watchPayloadOf(p any) (hooks.RemotePayload, bool) {
	switch p := p.(type) {
	case watchPayload:
		return hooks.RemotePayload{Kind: p.kind, Seq: p.seq}, true
	case terminatedPayload:
		return hooks.RemotePayload{Kind: hooks.RemoteTerminated, Reason: p.reason, Seq: p.seq}, true
	}
	return hooks.RemotePayload{}, false
}

// newWatchPayload is hooks.NewPayload for the watch kinds, each with its watch number. A
// RemoteTerminated becomes the payload host.Inbound pushes to the watcher as a Terminated
// (remoteTerminated); it panics if its Reason is not a StopReason, which the core never builds. A
// RemoteWatch or RemoteUnwatch becomes a watchPayload, which host.Inbound applies itself
// (inboundWatch); the core never hands one over, since it registers other nodes' watches with
// WatchFor.
func newWatchPayload(p hooks.RemotePayload) any {
	if p.Kind != hooks.RemoteTerminated {
		return watchPayload{kind: p.Kind, seq: p.Seq}
	}
	r, ok := p.Reason.(StopReason)
	if !ok {
		panic(fmt.Sprintf("lockstep: hooks.NewPayload: RemoteTerminated with a reason of type %T, want lockstep.StopReason", p.Reason))
	}
	return terminatedPayload{reason: r, seq: p.Seq}
}

// actorAssignable is the registry's half of hooks.Assignable: whether to names a live actor of s,
// by uid, or by path when the uid is 0 (resolve), whose message type M takes v. A live actor is one
// that is registered: its stop may have begun, but not completed.
func (s *System) actorAssignable(to Address, v any) bool {
	if to.node != s.node {
		return false
	}
	c, _, _ := s.resolve(to, nil)
	if c == nil {
		return false
	}
	return assignableTo(v, c.msgType())
}

// assignableTo reports whether v may be delivered as a t: v.(t) holds for an interface t that v
// implements or a t that is v's dynamic type, and a nil v needs an interface t, whose zero value
// is nil. It is the type assertion the actor's mailbox makes, so a value of another type with the
// same underlying type is not assignable.
func assignableTo(v any, t reflect.Type) bool {
	if v == nil {
		return t.Kind() == reflect.Interface
	}
	vt := reflect.TypeOf(v)
	if t.Kind() == reflect.Interface {
		return vt.Implements(t)
	}
	return vt == t
}

// attachTransportHook is hooks.AttachTransport (ACT-050): it attaches t to sys (attachTransport)
// and returns what the transport needs of the system. The attachment's DeadLetter is
// remoteDeadLetter. The send paths read t as soon as attachTransport stores it, so t's Deliver may
// run, on any sending goroutine, before this returns. It panics if t is nil.
func attachTransportHook(sys any, t engine.Transport, stop func()) (hooks.Attachment, error) {
	s := hookSystem("AttachTransport", sys)
	if t == nil {
		panic("lockstep: hooks.AttachTransport: nil transport")
	}
	if err := s.attachTransport(t, stop); err != nil {
		return hooks.Attachment{}, err
	}
	return hooks.Attachment{
		Host:        host{s},
		Node:        s.node,
		Incarnation: s.incarnation,
		Clock:       s.eng.Clock,
		Entropy:     s.eng.Entropy,
		Tracer:      s.eng.Tracer,
		Failer:      s.eng.Failer,
		Log:         s.log,
		DeadLetter:  s.remoteDeadLetter,
	}, nil
}

// remoteDeadLetter is hooks.Attachment.DeadLetter: msg becomes a dead letter from from to to with
// the reason spelled reason, through the runtime's own path (deadLetter: record, log event and
// counter). It panics if reason is not a dead-letter reason (deadReasons).
func (s *System) remoteDeadLetter(from, to engine.Address, reason string, msg any) {
	r, ok := deadReasonOf(reason)
	if !ok {
		panic(fmt.Sprintf("lockstep: hooks.Attachment.DeadLetter: unknown reason %q", reason))
	}
	s.deadLetter(addressOf(from), addressOf(to), r, msg)
}

// lookupPathHook is hooks.LookupPath (REM-284): Lookup without a type parameter. The actor's
// message type is compared by its type key (typeKeyOf), and an empty typeKey takes any type. Its
// errors are Lookup's, with the type key in place of the looked-up type's name.
func lookupPathHook(sys any, path, typeKey string) (engine.Address, error) {
	s := hookSystem("LookupPath", sys)
	if !validPath(path) {
		return engine.Address{}, fmt.Errorf("%w: path %q", ErrInvalidName, path)
	}
	uid, a, ok := s.reg.Lookup(path)
	if !ok {
		return engine.Address{}, fmt.Errorf("%w: path %q", ErrNotFound, path)
	}
	addr := Address{node: s.node, path: path, uid: uid}
	if typeKey == "" {
		return addr.engine(), nil
	}
	spawned, key := "unknown", ""
	if c, ok := a.(actorCell); ok {
		t := c.msgType()
		spawned, key = t.String(), typeKeyOf(t)
	}
	if key != typeKey {
		return engine.Address{}, fmt.Errorf("%w: %s: spawned as %s, looked up as %s", ErrWrongType, addr, spawned, typeKey)
	}
	return addr.engine(), nil
}

// typeKeyOf returns the type key of t (hooks.LookupPath): PkgPath + "." + Name for a type declared
// in a package, else String(), so a predeclared type such as int or error is its name and an
// unnamed type such as []byte its spelling.
func typeKeyOf(t reflect.Type) string {
	if t.PkgPath() != "" {
		return t.PkgPath() + "." + t.Name()
	}
	return t.String()
}

// watchForHook is hooks.WatchFor (ACT-605, REM-280): watcher, an address on another node, watches
// the local actor at watched (watchFor). A watched address on another node, the zero address or a
// guardian's is not alive. It panics if fn is nil or watcher is the zero address or on this node.
func watchForHook(sys any, watcher, watched engine.Address, fn func(reason any)) (bool, func()) {
	s := hookSystem("WatchFor", sys)
	if fn == nil {
		panic("lockstep: hooks.WatchFor: nil fn")
	}
	w := addressOf(watcher)
	if w.IsZero() || w.node == s.node {
		panic(fmt.Sprintf("lockstep: hooks.WatchFor: watcher %s is not on another node", w))
	}
	a := addressOf(watched)
	if a.node != s.node {
		return false, func() {}
	}
	return s.watchFor(w, a, fn)
}

// watchesDownHook is hooks.WatchesDown (REM-294): every watch a local actor of sys holds on an
// actor on node ends with Failed(err) (watchesDown). It panics if err is nil.
func watchesDownHook(sys any, node string, err error) {
	s := hookSystem("WatchesDown", sys)
	if err == nil {
		panic("lockstep: hooks.WatchesDown: nil error")
	}
	s.watchesDown(node, err)
}
