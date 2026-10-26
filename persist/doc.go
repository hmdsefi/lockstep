// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package persist provides event-sourced actors. A command handler returns events; after the
// journal commits them, Apply folds them into the state and the handler's Then callbacks run.
// Start and restart recover the state from the newest snapshot plus the events after it. Every
// storage call is an effect, so the actor never blocks.
//
// An EventSourced value defines the actor: Initial gives the state before the first event,
// Handle turns a command into events, and Apply folds one event into the state. Spawn starts it
// under a persistence ID, a string of 1 to MaxIDLen bytes that names its stream of events:
//
//	pid, err := persist.Spawn(sys, "account", "acct-1", newAccount, persist.Config[Deposited, Balance]{
//		Journal: store,
//		Events:  persist.JSON[Deposited](),
//	})
//
// The actor's message type is the command type, so lockstep.Send and lockstep.Ask reach it like
// any actor. A reply that must wait until the events are durable goes in a Then callback:
//
//	func (a *account) Handle(ctx *persist.Context[Cmd, Deposited, Balance], b Balance, cmd Cmd) []Deposited {
//		ctx.Then(func(ctx *persist.Context[Cmd, Deposited, Balance], b Balance) {
//			cmd.ReplyTo.Send(ctx, b.Total)
//		})
//		return []Deposited{{Amount: cmd.Amount}}
//	}
//
// While the actor recovers, and while one batch of events is being written and committed, it
// handles no command: commands wait in the mailbox and run in arrival order afterwards. Signals
// that arrive during recovery reach Signal after Recovered.
//
// Events in a stream are numbered 1, 2, 3 and so on, without gaps. A snapshot records the encoded
// state after one event, so recovery can start from the newest snapshot and replay only the
// events after it.
//
// A Journal separates writing from committing. Write accepts a batch of events, Commit makes
// every written event durable, and only committed events count as stored. If the process stops
// between the two, Recover on the next start reports which events survived.
//
// A journal error, a snapshot that does not load or decode, an event that does not encode or
// decode, or a journal that does not fit the snapshot fails the actor, and so does a panic in
// Handle, Apply or a Then callback. Its
// supervisor decides as for any failure: a restart recovers the state again from the store. When
// the supervisor resumes the actor after a failure of the storage or the codecs, or after a panic
// in Apply, in Recovered or in a Then callback of committed events, the actor recovers again from
// the store before it handles its next message, so it never runs on a state the store does not
// hold. Then callbacks run only for events that the journal committed.
//
// With a SnapshotStore in the Config, the actor saves a snapshot of its state after every
// SnapshotEvery committed events and whenever Context.Snapshot was called, once the events up to
// that point are committed. The save runs beside the commands, which are not held back, one
// save at a time. A state that does not encode, a save that fails and a deletion that fails do not
// fail the actor: the failure is traced and the next snapshot that falls due tries again. After a
// save, the snapshots before the previous one are deleted, and with PruneEvents also the events up
// to the previous one, so that the newest two snapshots and the events after the older of them
// stay: if the newest snapshot is damaged, recovery falls back to the older one.
//
// JSON returns a Codec for concrete types that uses encoding/json. A type that needs another
// format implements Codec itself.
//
// The package persist/filestore provides a Journal and a SnapshotStore that keep their data in
// files.
package persist
