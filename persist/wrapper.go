// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package persist

import (
	"context"
	"fmt"
	"strconv"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/internal/hooks"
)

// The names of persist's internal effects and records (PER §8). An internal effect and the record
// of its success share the name.
const (
	effectLoad    = "lockstep.persist.load"
	effectRecover = "lockstep.persist.recover"
	effectRead    = "lockstep.persist.read"
	effectWrite   = "lockstep.persist.write"
	effectCommit  = "lockstep.persist.commit"
	recordFail    = "lockstep.persist.fail"

	effectSave            = "lockstep.persist.save"
	effectDeleteSnapshots = "lockstep.persist.delete-snapshots"
	effectDeleteEvents    = "lockstep.persist.delete-events"
	recordSnapshot        = "lockstep.persist.snapshot"
	recordDelete          = "lockstep.persist.delete"
)

// phase is where a wrapper's incarnation stands (PER §11).
type phase uint8

const (
	recovering phase = iota // Started ran; recovery has not finished
	idle                    // handling commands; no batch in flight
	writing                 // a batch's Write is in flight
	committing              // a batch's Commit is in flight
)

// The wrapper is the lockstep actor of a persistent actor, with every optional hook.
var (
	_ lockstep.Starter[int]        = (*wrapper[int, int, int])(nil)
	_ lockstep.Stopper[int]        = (*wrapper[int, int, int])(nil)
	_ lockstep.SignalReceiver[int] = (*wrapper[int, int, int])(nil)
)

// wrapper is the lockstep actor behind a persistent actor: one value per incarnation, built by
// the factory that Spawn gives lockstep.Spawn. It recovers the state in Started (PER-040,
// PER-050), turns each command into one Handle call (PER-090), and writes and commits the
// returned events before it applies them and runs the Then callbacks (PER-100). User delivery is
// paused while recovery runs and while a batch is in flight (PER-110), so the wrapper's state is
// only ever touched by one turn at a time. Store calls run only in its internal effects, whose
// Run functions capture copies of every value they need and touch no wrapper state (PER-220).
//
// A persist failure (a store, codec or consistency error, PER-070, PER-100 step 1, PER-140, or a
// panic in a turn of an internal effect's Done, PER-130) resumes user delivery and marks the
// wrapper broken before the incarnation fails, so that a supervisor's DirectiveResume does not
// leave delivery paused for good. The next turn of a resumed incarnation that reaches the wrapper
// recovers again from the store (PER §5.4): a command is kept and handled after that recovery,
// a signal is kept as during any recovery, and an effect completion is dropped.
//
// A snapshot (PER §5.5) is saved by internal effects that do not pause user delivery. One snapshot
// cycle, the save and then the deletions that follow it, is in flight at a time; a snapshot that
// falls due meanwhile is evaluated again when the cycle ends. A failure in a cycle is recorded and
// otherwise ignored.
type wrapper[C, E, S any] struct {
	id  string
	cfg Config[E, S]

	user      EventSourced[C, E, S]
	recoverer Recoverer[C, E, S]      // nil if user does not implement it
	stopper   Stopper[C, E, S]        // likewise
	receiver  SignalReceiver[C, E, S] // likewise

	ctx     *Context[C, E, S] // the incarnation's context, built by Started
	phase   phase
	state   S
	lastSeq uint64 // the last committed and applied sequence number

	// broken is set by a persist failure and cleared when recovery starts again; gen counts the
	// recoveries of the incarnation, so that a completion of an effect started before the latest
	// one is dropped; pending holds the commands that reached a broken wrapper, to be handled after
	// the recovery, oldest first; halted records that the running turn called lockstep's Stop or
	// Fail, after which no further command is handled in it.
	broken  bool
	gen     uint64
	pending []C
	halted  bool

	// Recovery (PER-050): snapSeq is the snapshot's sequence number (0 for none), bounds the
	// journal's Recover result, next the next sequence number to replay; kept holds the signals
	// that arrived during recovery, in arrival order (PER-040).
	snapSeq uint64
	bounds  Bounds
	next    uint64
	kept    []lockstep.Signal

	// Snapshots (PER-170, PER-190): lastSnap is the sequence number of the newest snapshot saved or
	// recovered; saving is set while a snapshot cycle is in flight, with saveSeq the sequence
	// number being saved, cancelSave the cancel function of the cycle's current effect, and again
	// set if a snapshot fell due during the cycle.
	lastSnap   uint64
	saving     bool
	again      bool
	saveSeq    uint64
	cancelSave func()

	// The batch in flight (PER-100): its original events, sequence numbers first..last, the Then
	// callbacks of the Handle call that returned it, and whether that call asked to stop (PER-120).
	events      []E
	first, last uint64
	thens       []func(ctx *Context[C, E, S], state S)
	stopAfter   bool
}

// newWrapper returns the wrapper of a new incarnation; it calls factory once (PER-020).
func newWrapper[C, E, S any](id string, cfg Config[E, S], factory func() EventSourced[C, E, S]) *wrapper[C, E, S] {
	user := factory()
	w := &wrapper[C, E, S]{id: id, cfg: cfg, user: user}
	w.recoverer, _ = user.(Recoverer[C, E, S])
	w.stopper, _ = user.(Stopper[C, E, S])
	w.receiver, _ = user.(SignalReceiver[C, E, S])
	return w
}

// use makes ctx, the context of the turn that runs, the embedded context of w.ctx, so that user
// code always sees the current turn's context (PER §11), and returns w.ctx. Every entry point of
// a turn calls it first.
func (w *wrapper[C, E, S]) use(ctx *lockstep.Context[C]) *Context[C, E, S] {
	if w.ctx == nil {
		w.ctx = &Context[C, E, S]{w: w}
	}
	w.ctx.Context = ctx
	w.halted = false
	return w.ctx
}

// Started starts recovery (PER-040).
func (w *wrapper[C, E, S]) Started(ctx *lockstep.Context[C]) {
	w.use(ctx)
	w.guarded(ctx, func() { w.startRecovery(ctx) })
}

// startRecovery pauses user delivery and starts recovery from Initial: with the snapshot load if
// Snapshots is set, otherwise with the journal's Recover (PER-040, PER-050). Started calls it, and
// so does the first turn that reaches a broken wrapper. Whatever an earlier recovery or batch
// left is reset; LastSeq keeps its value until the recovery finishes.
func (w *wrapper[C, E, S]) startRecovery(ctx *lockstep.Context[C]) {
	hooks.Control(ctx).PauseUser(true)
	w.broken = false
	w.gen++
	w.dropSnapshot()
	w.phase = recovering
	w.snapSeq, w.bounds, w.next = 0, Bounds{}, 0
	w.events, w.thens, w.stopAfter = nil, nil, false
	w.state = w.user.Initial()
	if w.cfg.Snapshots != nil {
		w.load(ctx)
		return
	}
	w.recover(ctx)
}

// dropSnapshot forgets the snapshot cycle in flight and the pending snapshot request, when a
// recovery starts over: the cycle's effect is cancelled, and a completion that is already queued is
// dropped as it is of an earlier recovery.
func (w *wrapper[C, E, S]) dropSnapshot() {
	if w.cancelSave != nil {
		w.cancelSave()
	}
	w.cancelSave, w.saving, w.again = nil, false, false
	w.ctx.snapshot = false
}

// loaded is the result of Snapshots.Load.
type loaded struct {
	snap Snapshot
	ok   bool
}

// load starts recovery step 1 (PER-050): Snapshots.Load.
func (w *wrapper[C, E, S]) load(ctx *lockstep.Context[C]) {
	snaps, id := w.cfg.Snapshots, w.id
	w.start(ctx, effectLoad, func(c context.Context) (any, error) {
		snap, ok, err := snaps.Load(c, id)
		return loaded{snap: snap, ok: ok}, err
	}, w.loaded)
}

// loaded takes the newest snapshot, if any, as the recovery's starting state and starts the
// journal's Recover.
func (w *wrapper[C, E, S]) loaded(ctx *lockstep.Context[C], v any, err error) {
	if err != nil {
		w.fail(ctx, "load", fmt.Errorf("persist: %s: load snapshot: %w", w.id, err))
		return
	}
	if l, _ := v.(loaded); l.ok {
		state, err := w.cfg.State.Decode(l.snap.Data)
		if err != nil {
			w.fail(ctx, "decode-state", fmt.Errorf("persist: %s: decode snapshot seq %d: %w", w.id, l.snap.Seq, err))
			return
		}
		w.state, w.snapSeq = state, l.snap.Seq
	}
	w.recover(ctx)
}

// recover starts recovery step 2 (PER-050): Journal.Recover, once per recovery and before any
// Write of it (PER-220).
func (w *wrapper[C, E, S]) recover(ctx *lockstep.Context[C]) {
	j, id := w.cfg.Journal, w.id
	w.start(ctx, effectRecover, func(c context.Context) (any, error) {
		return j.Recover(c, id)
	}, w.recovered)
}

// recovered checks that the journal's bounds fit the snapshot (PER-060) and starts the replay of
// the events after it.
func (w *wrapper[C, E, S]) recovered(ctx *lockstep.Context[C], v any, err error) {
	if err != nil {
		w.fail(ctx, "recover", fmt.Errorf("persist: %s: recover: %w", w.id, err))
		return
	}
	b, _ := v.(Bounds)
	s := w.snapSeq
	switch {
	case s > b.Last:
		w.fail(ctx, "inconsistent", fmt.Errorf("persist: %s: snapshot at seq %d is ahead of the journal (last seq %d): %w",
			w.id, s, b.Last, ErrInconsistent))
		return
	case b.First > s+1:
		w.fail(ctx, "inconsistent", fmt.Errorf("persist: %s: journal starts at seq %d but recovery needs seq %d: %w",
			w.id, b.First, s+1, ErrInconsistent))
		return
	}
	w.bounds, w.next = b, s+1
	w.replay(ctx)
}

// replay reads the next batch of events, or finishes recovery once every event up to the
// journal's last sequence number is applied (PER-050 step 4).
func (w *wrapper[C, E, S]) replay(ctx *lockstep.Context[C]) {
	if w.next > w.bounds.Last {
		w.finishRecovery(ctx)
		return
	}
	j, id, from, max := w.cfg.Journal, w.id, w.next, w.cfg.ReplayBatch
	if max == 0 {
		max = DefaultReplayBatch
	}
	w.start(ctx, effectRead, func(c context.Context) (any, error) {
		return j.Read(c, id, from, max)
	}, w.replayed)
}

// replayed decodes and applies a batch of events read from the journal. Their sequence numbers
// must continue the recovered ones without a gap (PER-060); events past the journal's last
// sequence number at Recover are ignored.
func (w *wrapper[C, E, S]) replayed(ctx *lockstep.Context[C], v any, err error) {
	if err != nil {
		w.fail(ctx, "read", fmt.Errorf("persist: %s: replay from seq %d: %w", w.id, w.next, err))
		return
	}
	events, _ := v.([]Event)
	if len(events) == 0 {
		w.fail(ctx, "inconsistent", fmt.Errorf("persist: %s: journal returned seq %s, expected %d: %w",
			w.id, "none", w.next, ErrInconsistent))
		return
	}
	for _, ev := range events {
		if w.next > w.bounds.Last {
			break
		}
		if ev.Seq != w.next {
			w.fail(ctx, "inconsistent", fmt.Errorf("persist: %s: journal returned seq %d, expected %d: %w",
				w.id, ev.Seq, w.next, ErrInconsistent))
			return
		}
		e, err := w.cfg.Events.Decode(ev.Data)
		if err != nil {
			w.fail(ctx, "decode", fmt.Errorf("persist: %s: decode event seq %d: %w", w.id, ev.Seq, err))
			return
		}
		w.state = w.user.Apply(w.state, e)
		w.next++
	}
	w.replay(ctx)
}

// finishRecovery ends recovery (PER-040, PER-050 step 5): it emits lockstep.persist.recover,
// calls Recovered, passes the signals kept during recovery, and goes on with the kept commands
// and user delivery.
func (w *wrapper[C, E, S]) finishRecovery(ctx *lockstep.Context[C]) {
	b := w.bounds
	w.lastSeq, w.lastSnap = b.Last, w.snapSeq
	r := Recovery{SnapshotSeq: w.snapSeq, Events: b.Last - w.snapSeq, LastSeq: b.Last, Repaired: b.Repaired}
	if ctl := hooks.Control(ctx); ctl.TraceEnabled() {
		w.record(ctl, effectRecover,
			"snapshot_seq", strconv.FormatUint(r.SnapshotSeq, 10),
			"events", strconv.FormatUint(r.Events, 10),
			"last_seq", strconv.FormatUint(r.LastSeq, 10),
			"repaired", strconv.FormatInt(r.Repaired, 10))
	}
	w.phase = idle
	if w.recoverer != nil {
		w.recoverer.Recovered(w.ctx, w.state, r)
	}
	w.snapshotCheck(ctx, false)
	kept := w.kept
	w.kept = nil
	for _, sig := range kept {
		w.receiver.Signal(w.ctx, w.state, sig)
	}
	w.proceed(ctx)
}

// proceed runs when no batch is in flight: it handles the commands kept while the wrapper was
// broken, oldest first, until one starts a batch, fails the incarnation or stops it, and then
// resumes user delivery unless a batch is in flight or a persist failure has resumed it already.
func (w *wrapper[C, E, S]) proceed(ctx *lockstep.Context[C]) {
	for len(w.pending) > 0 && !w.halted {
		cmd := w.pending[0]
		w.pending = w.pending[1:]
		w.receive(ctx, cmd)
		if w.phase != idle || w.broken {
			return
		}
	}
	if len(w.pending) == 0 {
		w.pending = nil
	}
	hooks.Control(ctx).PauseUser(false)
}

// Receive handles cmd. A broken wrapper keeps cmd and recovers again first. Otherwise no
// recovery or batch is in flight, since user delivery is paused during both (PER-110); a command
// that arrives anyway is a defect of the runtime, and Receive panics. Kept commands that a
// failure or stop left unhandled go first.
func (w *wrapper[C, E, S]) Receive(ctx *lockstep.Context[C], cmd C) {
	w.use(ctx)
	if w.broken {
		w.pending = append(w.pending, cmd)
		w.guarded(ctx, func() { w.startRecovery(ctx) })
		return
	}
	if w.phase != idle {
		panic(fmt.Sprintf("persist: %s: internal error: command delivered during recovery or a batch", w.id))
	}
	if len(w.pending) > 0 {
		w.pending = append(w.pending, cmd)
		w.proceed(ctx)
		return
	}
	w.receive(ctx, cmd)
}

// receive makes one Handle call for cmd (PER-090). Without events, the call's Then callbacks run
// at once with the unchanged state. With events, it encodes them, pauses user delivery and
// starts the batch's Write (PER-100 steps 1 to 3). A Fail during Handle discards the events and
// the Then callbacks (PER-140). A Stop during Handle waits for the batch's commit (PER-120); it
// takes effect at once if Handle returns no events or calls Fail, and is dropped if the events
// do not encode.
func (w *wrapper[C, E, S]) receive(ctx *lockstep.Context[C], cmd C) {
	pc := w.ctx
	events, thens, failed, stop := w.handle(pc, cmd)
	if failed || len(events) == 0 {
		if !failed {
			for _, fn := range thens {
				fn(pc, w.state)
			}
			w.snapshotCheck(ctx, false)
		}
		if stop {
			w.stop(ctx)
		}
		return
	}
	batch := make([]Event, len(events))
	bytes := 0
	for i, e := range events {
		data, err := w.cfg.Events.Encode(e)
		if err != nil {
			w.fail(ctx, "encode", fmt.Errorf("persist: %s: encode event %d: %w", w.id, i, err))
			return
		}
		batch[i] = Event{Seq: w.lastSeq + 1 + uint64(i), Data: data}
		bytes += len(data)
	}
	hooks.Control(ctx).PauseUser(true)
	w.phase = writing
	w.events, w.thens, w.stopAfter = events, thens, stop
	w.first, w.last = batch[0].Seq, batch[len(batch)-1].Seq
	j, id := w.cfg.Journal, w.id
	w.start(ctx, effectWrite, func(c context.Context) (any, error) {
		return bytes, j.Write(c, id, batch)
	}, w.written)
}

// handle calls Handle with the current state and returns its events, its Then callbacks, and
// whether it called Fail and Stop. The context reports Handle as running only during the call,
// also when it panics.
func (w *wrapper[C, E, S]) handle(pc *Context[C, E, S], cmd C) (events []E, thens []func(*Context[C, E, S], S), failed, stop bool) {
	pc.thens, pc.failed, pc.stop, pc.handling = nil, false, false, true
	defer func() {
		thens, failed, stop = pc.thens, pc.failed, pc.stop
		pc.thens, pc.failed, pc.stop, pc.handling = nil, false, false, false
	}()
	return w.user.Handle(pc, w.state, cmd), nil, false, false
}

// written emits lockstep.persist.write and starts the batch's Commit (PER-100 step 3).
func (w *wrapper[C, E, S]) written(ctx *lockstep.Context[C], v any, err error) {
	if err != nil {
		w.fail(ctx, "write", fmt.Errorf("persist: %s: write seq %d..%d: %w", w.id, w.first, w.last, err))
		return
	}
	if ctl := hooks.Control(ctx); ctl.TraceEnabled() {
		bytes, _ := v.(int)
		w.record(ctl, effectWrite,
			"first", strconv.FormatUint(w.first, 10),
			"count", strconv.FormatUint(w.last-w.first+1, 10),
			"bytes", strconv.Itoa(bytes))
	}
	w.phase = committing
	j, id := w.cfg.Journal, w.id
	w.start(ctx, effectCommit, func(c context.Context) (any, error) {
		return nil, j.Commit(c, id)
	}, w.committed)
}

// committed acknowledges the committed batch (PER-100 step 4): it emits lockstep.persist.commit,
// applies the batch's original events, advances LastSeq, runs the Then callbacks with the new
// state, then stops the actor if Handle asked to (PER-120) and otherwise goes on with the kept
// commands and user delivery.
func (w *wrapper[C, E, S]) committed(ctx *lockstep.Context[C], _ any, err error) {
	if err != nil {
		w.fail(ctx, "commit", fmt.Errorf("persist: %s: commit seq %d..%d: %w", w.id, w.first, w.last, err))
		return
	}
	if ctl := hooks.Control(ctx); ctl.TraceEnabled() {
		w.record(ctl, effectCommit, "last", strconv.FormatUint(w.last, 10))
	}
	events, thens, stop := w.events, w.thens, w.stopAfter
	w.events, w.thens, w.stopAfter = nil, nil, false
	for _, e := range events {
		w.state = w.user.Apply(w.state, e)
	}
	w.lastSeq = w.last
	w.phase = idle
	for _, fn := range thens {
		fn(w.ctx, w.state)
	}
	w.snapshotCheck(ctx, true)
	if stop {
		w.stop(ctx)
		return
	}
	w.proceed(ctx)
}

// snapshotCheck starts a snapshot if one is due (PER-170): after a commit, when SnapshotEvery
// events were committed since the last snapshot, and whenever Snapshot was called since the last
// snapshot started and an event came after it. A snapshot that falls due while a cycle is in
// flight waits for the cycle's end. A state that does not encode is recorded and retried at the
// next check (PER-180); the state is encoded in the turn, so the save sees a value that later
// commands cannot change.
func (w *wrapper[C, E, S]) snapshotCheck(ctx *lockstep.Context[C], afterCommit bool) {
	if w.cfg.Snapshots == nil || w.halted {
		return
	}
	seq := w.lastSeq
	due := w.ctx.snapshot && seq > w.lastSnap
	if afterCommit && w.cfg.SnapshotEvery > 0 && seq-w.lastSnap >= w.cfg.SnapshotEvery {
		due = true
	}
	switch {
	case !due:
		return
	case w.saving:
		w.again = true
		return
	}
	data, err := w.cfg.State.Encode(w.state)
	if err != nil {
		w.note(ctx, "encode-state", fmt.Errorf("persist: %s: encode state at seq %d: %w", w.id, seq, err))
		return
	}
	w.ctx.snapshot = false
	w.saving, w.saveSeq = true, seq
	snaps, id := w.cfg.Snapshots, w.id
	w.cancelSave = w.start(ctx, effectSave, func(c context.Context) (any, error) {
		return len(data), snaps.Save(c, id, Snapshot{Seq: seq, Data: data})
	}, w.saved)
}

// saved ends the save (PER-190): on success it advances lastSnap, emits lockstep.persist.snapshot
// and starts the deletions that the previous snapshot P allows; on failure it records and ends the
// cycle, leaving lastSnap for the next check.
func (w *wrapper[C, E, S]) saved(ctx *lockstep.Context[C], v any, err error) {
	seq, prev := w.saveSeq, w.lastSnap
	if err != nil {
		w.note(ctx, "save", fmt.Errorf("persist: %s: save snapshot seq %d: %w", w.id, seq, err))
		w.endSnapshot(ctx)
		return
	}
	w.lastSnap = seq
	if ctl := hooks.Control(ctx); ctl.TraceEnabled() {
		bytes, _ := v.(int)
		w.record(ctl, recordSnapshot, "seq", strconv.FormatUint(seq, 10), "bytes", strconv.Itoa(bytes))
	}
	if prev == 0 {
		w.endSnapshot(ctx)
		return
	}
	w.deleteSnapshots(ctx, prev)
}

// deleteSnapshots deletes the snapshots below prev, the snapshot before the one just saved: the
// newest two stay (PER-190). The deletion of events follows, also if this one fails.
func (w *wrapper[C, E, S]) deleteSnapshots(ctx *lockstep.Context[C], prev uint64) {
	snaps, id, to := w.cfg.Snapshots, w.id, prev-1
	w.cancelSave = w.start(ctx, effectDeleteSnapshots, func(c context.Context) (any, error) {
		return nil, snaps.DeleteSnapshots(c, id, to)
	}, func(ctx *lockstep.Context[C], _ any, err error) {
		w.deleted(ctx, "snapshots", "delete-snapshots", to, err)
		w.deleteEvents(ctx, prev)
	})
}

// deleteEvents deletes the events up to prev if PruneEvents is set: the events after the older of
// the two newest snapshots stay, so a damaged newest snapshot falls back to the older one. prev is
// the sequence number of a snapshot that an earlier save made durable or recovery loaded.
func (w *wrapper[C, E, S]) deleteEvents(ctx *lockstep.Context[C], prev uint64) {
	if !w.cfg.PruneEvents {
		w.endSnapshot(ctx)
		return
	}
	j, id := w.cfg.Journal, w.id
	w.cancelSave = w.start(ctx, effectDeleteEvents, func(c context.Context) (any, error) {
		return nil, j.DeleteEvents(c, id, prev)
	}, func(ctx *lockstep.Context[C], _ any, err error) {
		w.deleted(ctx, "events", "delete-events", prev, err)
		w.endSnapshot(ctx)
	})
}

// deleted emits lockstep.persist.delete for a deletion that succeeded and records the failure of
// one that did not, with the fail op op.
func (w *wrapper[C, E, S]) deleted(ctx *lockstep.Context[C], store, op string, to uint64, err error) {
	if err != nil {
		w.note(ctx, op, fmt.Errorf("persist: %s: delete %s up to seq %d: %w", w.id, store, to, err))
		return
	}
	if ctl := hooks.Control(ctx); ctl.TraceEnabled() {
		w.record(ctl, recordDelete, "store", store, "to", strconv.FormatUint(to, 10))
	}
}

// endSnapshot ends the snapshot cycle and evaluates again a snapshot that fell due during it.
func (w *wrapper[C, E, S]) endSnapshot(ctx *lockstep.Context[C]) {
	w.saving, w.cancelSave = false, nil
	if w.again {
		w.again = false
		w.snapshotCheck(ctx, true)
	}
}

// Signal passes sig to the user's Signal; during recovery it keeps sig until recovery finishes
// (PER-040). Without a user Signal, sig is ignored. A broken wrapper keeps sig and recovers
// again.
func (w *wrapper[C, E, S]) Signal(ctx *lockstep.Context[C], sig lockstep.Signal) {
	pc := w.use(ctx)
	switch {
	case w.broken:
		if w.receiver != nil {
			w.kept = append(w.kept, sig)
		}
		w.guarded(ctx, func() { w.startRecovery(ctx) })
	case w.receiver == nil:
	case w.phase == recovering:
		w.kept = append(w.kept, sig)
	default:
		w.receiver.Signal(pc, w.state, sig)
	}
}

// Stopped passes the stop to the user's Stopped, with the current state.
func (w *wrapper[C, E, S]) Stopped(ctx *lockstep.Context[C], reason lockstep.StopReason) {
	pc := w.use(ctx)
	if w.stopper != nil {
		w.stopper.Stopped(pc, w.state, reason)
	}
}

// start starts the inline internal effect name for the incarnation of ctx (PER-240, PER-520):
// run calls the store; done runs in a later turn of the incarnation with that turn's context,
// after w.ctx has been switched to it. The incarnation's end cancels the effect, so done never
// runs for an ended incarnation (PER-160). In a broken wrapper the completion starts recovery
// instead; a completion of an effect started before the latest recovery began is dropped. A
// panic in done breaks the wrapper (guarded).
func (w *wrapper[C, E, S]) start(ctx *lockstep.Context[C], name string, run func(context.Context) (any, error),
	done func(ctx *lockstep.Context[C], v any, err error)) (cancel func()) {
	gen := w.gen
	return hooks.Control(ctx).StartInternal(hooks.InternalEffect{
		Name:   name,
		Inline: true,
		Run:    run,
		Done: func(c any, v any, err error) {
			lc, ok := c.(*lockstep.Context[C])
			if !ok {
				panic(fmt.Sprintf("persist: internal error: effect %q completed with a context of type %T", name, c))
			}
			w.use(lc)
			switch {
			case w.broken:
				w.guarded(lc, func() { w.startRecovery(lc) })
			case gen == w.gen:
				w.guarded(lc, func() { done(lc, v, err) })
			}
		},
	})
}

// guarded calls fn. If fn panics, guarded breaks the wrapper before the panic goes on to fail
// the turn (PER-130): user code that runs while delivery is paused, such as Apply, a Then callback
// or Recovered in an effect's Done, must not leave it paused for good.
func (w *wrapper[C, E, S]) guarded(ctx *lockstep.Context[C], fn func()) {
	ok := false
	defer func() {
		if !ok {
			w.breakDown(ctx)
		}
	}()
	fn()
	ok = true
}

// breakDown resumes user delivery and marks the wrapper broken, so that the next turn of the
// incarnation, if a supervisor resumes it, recovers again from the store.
func (w *wrapper[C, E, S]) breakDown(ctx *lockstep.Context[C]) {
	hooks.Control(ctx).PauseUser(false)
	w.broken = true
}

// fail emits lockstep.persist.fail with op, breaks the wrapper, and fails the incarnation with
// err through the lockstep context's Fail (PER-070, PER-100 step 1, PER-140). A deferred stop is
// dropped with the batch.
func (w *wrapper[C, E, S]) fail(ctx *lockstep.Context[C], op string, err error) {
	w.note(ctx, op, err)
	w.breakDown(ctx)
	w.halted = true
	ctx.Fail(err)
}

// note emits lockstep.persist.fail with op and err's text, for a failure that does not fail the
// actor.
func (w *wrapper[C, E, S]) note(ctx *lockstep.Context[C], op string, err error) {
	if ctl := hooks.Control(ctx); ctl.TraceEnabled() {
		w.record(ctl, recordFail, "op", op, "err", err.Error())
	}
}

// stop calls the lockstep context's Stop: the incarnation handles no further command.
func (w *wrapper[C, E, S]) stop(ctx *lockstep.Context[C]) {
	w.halted = true
	ctx.Stop()
}

// record emits kind with the attributes actor and id, then kv (PER §8). Callers check
// TraceEnabled first, so the attribute values are built only when a tracer records them.
func (w *wrapper[C, E, S]) record(ctl hooks.ActorControl, kind string, kv ...string) {
	ctl.Record(kind, append([]string{"actor", ctl.Address(), "id", w.id}, kv...)...)
}
