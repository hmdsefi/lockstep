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
)

// phase is where a wrapper's incarnation stands (PER §11).
type phase uint8

const (
	recovering phase = iota // Started ran; recovery has not finished
	idle                    // handling commands; no batch in flight
	writing                 // a batch's Write is in flight
	committing              // a batch's Commit is in flight
)

// wrapper is the lockstep actor behind a persistent actor: one value per incarnation, built by
// the factory that Spawn gives lockstep.Spawn. It recovers the state in Started (PER-040,
// PER-050), turns each command into one Handle call (PER-090), and writes and commits the
// returned events before it applies them and runs the Then callbacks (PER-100). User delivery is
// paused while recovery runs and while a batch is in flight (PER-110), so the wrapper's state is
// only ever touched by one turn at a time. Store calls run only in its internal effects, whose
// Run functions get every value they need as arguments (PER-220).
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

	// Recovery (PER-050): snapSeq is the snapshot's sequence number (0 for none), bounds the
	// journal's Recover result, next the next sequence number to replay; kept holds the signals
	// that arrived during recovery, in arrival order (PER-040).
	snapSeq uint64
	bounds  Bounds
	next    uint64
	kept    []lockstep.Signal

	// The batch in flight (PER-100): its original events, sequence numbers first..last, and the
	// Then callbacks of the Handle call that returned it.
	events      []E
	first, last uint64
	thens       []func(ctx *Context[C, E, S], state S)
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
// code always sees the current turn's context (PER §11), and returns w.ctx.
func (w *wrapper[C, E, S]) use(ctx *lockstep.Context[C]) *Context[C, E, S] {
	if w.ctx == nil {
		w.ctx = &Context[C, E, S]{w: w}
	}
	w.ctx.Context = ctx
	return w.ctx
}

// Started pauses user delivery and starts recovery from Initial: with the snapshot load if
// Snapshots is set, otherwise with the journal's Recover (PER-040, PER-050).
func (w *wrapper[C, E, S]) Started(ctx *lockstep.Context[C]) {
	w.use(ctx)
	hooks.Control(ctx).PauseUser(true)
	w.phase = recovering
	w.state = w.user.Initial()
	if w.cfg.Snapshots != nil {
		w.load(ctx)
		return
	}
	w.recover(ctx)
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

// recover starts recovery step 2 (PER-050): Journal.Recover, once per incarnation and before any
// Write (PER-220).
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
// calls Recovered, passes the signals kept during recovery, and resumes user delivery.
func (w *wrapper[C, E, S]) finishRecovery(ctx *lockstep.Context[C]) {
	b := w.bounds
	w.lastSeq = b.Last
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
	kept := w.kept
	w.kept = nil
	for _, sig := range kept {
		w.receiver.Signal(w.ctx, w.state, sig)
	}
	hooks.Control(ctx).PauseUser(false)
}

// Receive makes one Handle call for cmd (PER-090). Without events, the call's Then callbacks run
// at once with the unchanged state. With events, it encodes them, pauses user delivery and
// starts the batch's Write (PER-100 steps 1 to 3). A Fail during Handle discards the events and
// the Then callbacks (PER-140).
func (w *wrapper[C, E, S]) Receive(ctx *lockstep.Context[C], cmd C) {
	pc := w.use(ctx)
	events, thens, failed := w.handle(pc, cmd)
	if failed {
		return
	}
	if len(events) == 0 {
		for _, fn := range thens {
			fn(pc, w.state)
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
	w.events, w.thens = events, thens
	w.first, w.last = batch[0].Seq, batch[len(batch)-1].Seq
	j, id := w.cfg.Journal, w.id
	w.start(ctx, effectWrite, func(c context.Context) (any, error) {
		return bytes, j.Write(c, id, batch)
	}, w.written)
}

// handle calls Handle with the current state and returns its events, its Then callbacks and
// whether it called Fail. The context reports Handle as running only during the call, also when
// it panics.
func (w *wrapper[C, E, S]) handle(pc *Context[C, E, S], cmd C) (events []E, thens []func(*Context[C, E, S], S), failed bool) {
	pc.thens, pc.failed, pc.handling = nil, false, true
	defer func() {
		thens, failed = pc.thens, pc.failed
		pc.thens, pc.failed, pc.handling = nil, false, false
	}()
	return w.user.Handle(pc, w.state, cmd), nil, false
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
// state, and resumes user delivery.
func (w *wrapper[C, E, S]) committed(ctx *lockstep.Context[C], _ any, err error) {
	if err != nil {
		w.fail(ctx, "commit", fmt.Errorf("persist: %s: commit seq %d..%d: %w", w.id, w.first, w.last, err))
		return
	}
	if ctl := hooks.Control(ctx); ctl.TraceEnabled() {
		w.record(ctl, effectCommit, "last", strconv.FormatUint(w.last, 10))
	}
	events, thens := w.events, w.thens
	w.events, w.thens = nil, nil
	for _, e := range events {
		w.state = w.user.Apply(w.state, e)
	}
	w.lastSeq = w.last
	w.phase = idle
	for _, fn := range thens {
		fn(w.ctx, w.state)
	}
	hooks.Control(ctx).PauseUser(false)
}

// Signal passes sig to the user's Signal; during recovery it keeps sig until recovery finishes
// (PER-040). Without a user Signal, sig is ignored.
func (w *wrapper[C, E, S]) Signal(ctx *lockstep.Context[C], sig lockstep.Signal) {
	pc := w.use(ctx)
	switch {
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
// runs for an ended incarnation (PER-160).
func (w *wrapper[C, E, S]) start(ctx *lockstep.Context[C], name string, run func(context.Context) (any, error),
	done func(ctx *lockstep.Context[C], v any, err error)) {
	hooks.Control(ctx).StartInternal(hooks.InternalEffect{
		Name:   name,
		Inline: true,
		Run:    run,
		Done: func(c any, v any, err error) {
			lc, _ := c.(*lockstep.Context[C])
			w.use(lc)
			done(lc, v, err)
		},
	})
}

// fail emits lockstep.persist.fail with op and fails the incarnation with err through the
// lockstep context's Fail (PER-070, PER-100 step 1, PER-140).
func (w *wrapper[C, E, S]) fail(ctx *lockstep.Context[C], op string, err error) {
	if ctl := hooks.Control(ctx); ctl.TraceEnabled() {
		w.record(ctl, recordFail, "op", op, "err", err.Error())
	}
	ctx.Fail(err)
}

// record emits kind with the attributes actor and id, then kv (PER §8). Callers check
// TraceEnabled first, so the attribute values are built only when a tracer records them.
func (w *wrapper[C, E, S]) record(ctl hooks.ActorControl, kind string, kv ...string) {
	ctl.Record(kind, append([]string{"actor", ctl.Address(), "id", w.id}, kv...)...)
}
