// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package hooks

import (
	"log/slog"

	"github.com/hmdsefi/lockstep/internal/engine"
)

// The declarations below are REM §4.3: they let the remoting core (internal/remoting) read what
// the runtime hands its transport, and build the inbound payloads the runtime understands, without
// importing package lockstep's internals. Package lockstep sets every hook in its init function
// (ENG-210); each is nil until package lockstep is initialized. Called by internal/remoting and
// remote; implemented by MSG (messages, asks, replies, failures and the ask table) and ACT (the
// transport, actors, lookups and remote watches). Every sys is a *lockstep.System, and a hook
// panics if it has another type.

// RemoteKind says what a remote envelope carries.
type RemoteKind uint8

// The kinds of remote envelope (REM-120 maps them one to one to frame kinds).
const (
	RemoteMsg        RemoteKind = iota + 1 // a Send: From sender, To target
	RemoteAsk                              // an Ask request: From asker, To target, Ask the ask address
	RemoteReply                            // an answer: From replier, To the ask address
	RemoteFail                             // a failure of an ask: From replier, To the ask address
	RemoteWatch                            // From (watcher) watches To (watched)
	RemoteUnwatch                          // From (watcher) stops watching To (watched)
	RemoteTerminated                       // From (watched) ended; To is the watcher
)

// RemotePayload is the transport's view of an engine.Envelope's Payload.
type RemotePayload struct {
	Kind   RemoteKind
	Value  any            // RemoteMsg, RemoteAsk: the message; RemoteReply: the answer; else nil
	Ask    engine.Address // RemoteAsk: the ask address the answer goes to; else zero
	Err    error          // RemoteFail: the failure; else nil
	Reason any            // RemoteTerminated: the lockstep.StopReason; else nil
	// Seq is, for RemoteWatch, RemoteUnwatch and RemoteTerminated, the watcher's watch number
	// (ACT-630's sequence), echoed back in RemoteTerminated; 0 elsewhere.
	Seq uint64
}

// Attachment is what a transport gets from the system it is attached to.
type Attachment struct {
	Host        engine.Host
	Node        string
	Incarnation uint32
	Clock       engine.Clock
	Entropy     engine.Entropy
	Tracer      engine.Tracer
	Failer      engine.Failer
	Log         *slog.Logger
	// DeadLetter makes msg a dead letter from from to to with reason, as the runtime's own dead
	// letters are (record lockstep.deadletter, log event, lockstep_deadletters_total).
	DeadLetter func(from, to engine.Address, reason string, msg any)
}

// Payload reads p, an engine.Envelope's Payload that the runtime handed its transport, and reports
// false when p is not a runtime payload (REM-110). A Send is RemoteMsg, an Ask request RemoteAsk,
// a reply attempt with an answer RemoteReply and one with an error (ReplyTo.Fail, or the
// runtime's own failure of an ask: MSG-340, MSG-345, MSG-350) RemoteFail. Implemented by MSG and,
// for the watch kinds, ACT.
var Payload func(p any) (RemotePayload, bool) // false: not a runtime payload

// NewPayload returns the runtime payload of p for an engine.Envelope handed to the system's
// Host.Inbound (REM-160). A RemoteMsg or RemoteAsk whose recipient is a path address resolves to
// the actor registered at the path whatever its message type: the core has checked Assignable
// already. Inbound delivers it as a local send from an actor sender (MBX-120); an ask request is
// tracked by its target (MSG-340), or fails back with ErrTerminated when no live actor takes it
// (MSG-350); a RemoteReply or RemoteFail is applied to the asker's table (MSG-420), where one for
// an ask that resolved already is recorded late. NewPayload panics for a RemoteAsk with a zero Ask
// and for a RemoteFail with a nil Err, and returns nil, which Inbound drops, for a kind it does
// not cover. Implemented by MSG and, for the watch kinds, ACT.
var NewPayload func(p RemotePayload) any // the runtime payload for Host.Inbound

// Assignable reports whether v may go to to on sys (REM-230, REM-264). For a pending ask address
// of sys it reports whether v is assignable to the ask's R, where a nil v needs an interface R;
// for an actor address, whether a live actor at to (by uid, or by path when the uid is 0) has an M
// that v is assignable to. It reports false for any other address, a resolved ask address
// included. For REPLY and FAIL the core therefore checks Pending first: an answer for an ask that
// is no longer pending goes to Inbound unchecked and stays a late reply (lockstep.reply outcome
// late), not a wrong-type discard. Implemented by MSG (asks) and ACT (actors).
var Assignable func(sys any, to engine.Address, v any) bool

// FailAsks resolves with err, in ask-counter order, every pending ask of sys whose target is on
// node (REM-294); asks without a target (NewReply) are left alone. It panics if err is nil.
// Implemented by MSG.
var FailAsks func(sys any, node string, err error)

// Pending reports whether ask is a pending ask of sys (MSG-370). The core calls it for REPLY and
// FAIL before Assignable, which is false for an ask that resolved already, so a late reply stays a
// late reply. Implemented by MSG.
var Pending func(sys any, ask engine.Address) bool

// AttachTransport makes t the transport of sys and runs stop once from Shutdown, after the last
// actor stopped and before Engine.Stop. It returns an error if sys has a transport already, the
// engine's own Transport included, and an error wrapping lockstep.ErrStopped on a stopped system.
// The send paths see t as soon as it is stored, so t's Deliver may be called, from any goroutine
// that sends, before AttachTransport returns. Implemented by ACT.
var AttachTransport func(sys any, t engine.Transport, stop func()) (Attachment, error)

// LookupPath returns the address, with its uid, of the actor registered at path on sys whose
// message type has the type key typeKey (PkgPath + "." + Name for a named type, else String());
// an empty typeKey matches any type. Its errors wrap lockstep.ErrInvalidName, ErrNotFound or
// ErrWrongType, as lockstep.Lookup's do (REM-284). Implemented by ACT.
var LookupPath func(sys any, path, typeKey string) (engine.Address, error)

// WatchFor registers watcher, an address on another node, as a watcher of the local actor watched
// (ACT-605, REM-280). alive is false when watched is not alive, and fn then never runs; otherwise
// fn runs once, in engine context, with the lockstep.StopReason when watched ends, unless cancel
// ran first. Implemented by ACT.
var WatchFor func(sys any, watcher, watched engine.Address, fn func(reason any)) (alive bool, cancel func())

// WatchesDown ends every watch a local actor of sys holds on an actor on node (REM-294): each
// watcher gets Terminated{watched, Failed(err)}, in ascending (watched, watcher) text order.
// Implemented by ACT.
var WatchesDown func(sys any, node string, err error)

// ShuttingDown reports whether Shutdown has begun on sys, so that sys is shutting down or
// stopped, or whether sys was discarded (REM-040: remote.Enable on a simulated system then
// returns an error wrapping lockstep.ErrStopped). Implemented by ACT.
var ShuttingDown func(sys any) bool
