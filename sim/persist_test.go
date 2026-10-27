// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/internal/hooks"
	"github.com/hmdsefi/lockstep/persist"
	"github.com/hmdsefi/lockstep/persist/filestore"
	"github.com/hmdsefi/lockstep/sim"
)

// AT-PER-01, PER-010: each defect of the table fails Spawn with the table's error, whose
// errors.Is matches the sentinel, and spawns nothing; an 81-byte id fails and an 80-byte id
// spawns a counter that recovers.
func TestATPER01SpawnValidation(t *testing.T) {
	type row struct {
		name    string
		id      string
		factory bool
		edit    func(cfg *persist.Config[Added, CounterState])
		want    error
	}
	long := strings.Repeat("a", persist.MaxIDLen+1)
	rows := []row{
		{"empty id", "", true, nil, fmt.Errorf("persist: Spawn: id %q: %w", "", persist.ErrInvalidID)},
		{"81-byte id", long, true, nil, fmt.Errorf("persist: Spawn: id %q: %w", long, persist.ErrInvalidID)},
		{"nil factory", "c-1", false, nil, fmt.Errorf("persist: Spawn %q: nil factory: %w", "c-1", persist.ErrInvalidConfig)},
		{"nil Journal", "c-1", true, func(c *persist.Config[Added, CounterState]) { c.Journal = nil },
			fmt.Errorf("persist: Spawn %q: nil Journal: %w", "c-1", persist.ErrInvalidConfig)},
		{"nil Events", "c-1", true, func(c *persist.Config[Added, CounterState]) { c.Events = nil },
			fmt.Errorf("persist: Spawn %q: nil Events codec: %w", "c-1", persist.ErrInvalidConfig)},
		{"Snapshots without State", "c-1", true, func(c *persist.Config[Added, CounterState]) { c.State = nil },
			fmt.Errorf("persist: Spawn %q: Snapshots without State codec: %w", "c-1", persist.ErrInvalidConfig)},
		{"SnapshotEvery without Snapshots", "c-1", true, func(c *persist.Config[Added, CounterState]) {
			c.Snapshots, c.SnapshotEvery = nil, 3
		}, fmt.Errorf("persist: Spawn %q: SnapshotEvery or PruneEvents without Snapshots: %w", "c-1", persist.ErrInvalidConfig)},
		{"PruneEvents without Snapshots", "c-1", true, func(c *persist.Config[Added, CounterState]) {
			c.Snapshots, c.State, c.PruneEvents = nil, nil, true
		}, fmt.Errorf("persist: Spawn %q: SnapshotEvery or PruneEvents without Snapshots: %w", "c-1", persist.ErrInvalidConfig)},
		{"negative ReplayBatch", "c-1", true, func(c *persist.Config[Added, CounterState]) { c.ReplayBatch = -1 },
			fmt.Errorf("persist: Spawn %q: negative ReplayBatch %d: %w", "c-1", -1, persist.ErrInvalidConfig)},
	}
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		r := newCounterRun(s)
		errs := make([]error, len(rows))
		var okPID lockstep.PID[CounterCmd]
		var okErr error
		s.Node("n1", func(sys *lockstep.System) {
			store := filestore.New(s.Volume("n1"))
			spawn(sys, "parent", &actor[PMsg]{started: func(ctx *lockstep.Context[PMsg]) {
				for i, row := range rows {
					cfg := persist.Config[Added, CounterState]{
						Journal:   store,
						Events:    persist.JSON[Added](),
						Snapshots: store,
						State:     persist.JSON[CounterState](),
					}
					if row.edit != nil {
						row.edit(&cfg)
					}
					factory := r.newCounter
					if !row.factory {
						factory = nil
					}
					_, errs[i] = persist.Spawn(ctx, fmt.Sprintf("bad%d", i), row.id, factory, cfg)
				}
				okPID, okErr = persist.Spawn(ctx, "ok", strings.Repeat("a", persist.MaxIDLen), r.newCounter,
					persist.Config[Added, CounterState]{Journal: store, Events: persist.JSON[Added]()})
			}})
		})
		w.RunFor(10 * time.Millisecond)

		sys := s.System("n1")
		for i, row := range rows {
			err := errs[i]
			if err == nil || err.Error() != row.want.Error() {
				tt.Errorf("%s: Spawn error %v, want %q", row.name, err, row.want)
			}
			if !errors.Is(err, errors.Unwrap(row.want)) {
				tt.Errorf("%s: errors.Is(%v, %v) is false", row.name, err, errors.Unwrap(row.want))
			}
			if _, lerr := lockstep.Lookup[CounterCmd](sys, fmt.Sprintf("/user/parent/bad%d", i)); !errors.Is(lerr, lockstep.ErrNotFound) {
				tt.Errorf("%s: Lookup of the child: %v, want ErrNotFound", row.name, lerr)
			}
		}
		if okErr != nil {
			tt.Fatalf("Spawn with an 80-byte id: %v", okErr)
		}
		if got, err := lockstep.Lookup[CounterCmd](sys, "/user/parent/ok"); err != nil || got != okPID {
			tt.Errorf("Lookup of the 80-byte-id counter: %v, %v; want %v", got, err, okPID)
		}
		if r.factories != 1 || len(r.recs) != 1 {
			tt.Errorf("factory calls %d, Recovered calls %d; want 1 and 1 (only the valid counter)", r.factories, len(r.recs))
		}
	})
}

// AT-PER-02, PER-010, PER-090: Add{5} then Add{2} reply 5 and 7, and Lookup of the counter's path
// returns the PID that Spawn returned.
func TestATPER02AddReplies(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		tt := s.World().T()
		r := newCounterRun(s)
		r.node()
		s.World().RunFor(time.Millisecond)
		if got := r.ask(tt, addCmd(5)); got != 5 {
			tt.Errorf("Add{5} replied %d, want 5", got)
		}
		if got := r.ask(tt, addCmd(2)); got != 7 {
			tt.Errorf("Add{2} replied %d, want 7", got)
		}
		pid, err := lockstep.Lookup[CounterCmd](s.System("n1"), "/user/parent/counter")
		if err != nil || pid != r.pid {
			tt.Errorf("Lookup: %v, %v; want %v", pid, err, r.pid)
		}
	})
}

// AT-PER-03, PER-020, PER-050: after Add{5} and Add{2}, a crash and restart of n1 recovers both
// events: the new incarnation's Recovery is {0, 2, 2, 0}, Get replies 7, and the factory ran
// once per boot.
func TestATPER03RecoverAfterRestart(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		r := newCounterRun(s)
		n1 := r.node()
		w.RunFor(time.Millisecond)
		r.ask(tt, addCmd(5))
		r.ask(tt, addCmd(2))
		now := w.Sim.Now()
		w.Sim.At(now.Add(time.Millisecond), "test/crash", n1.Crash)
		w.Sim.At(now.Add(2*time.Millisecond), "test/restart", n1.Restart)
		w.RunFor(5 * time.Millisecond)

		if got := r.ask(tt, getCmd); got != 7 {
			tt.Errorf("Get after the restart replied %d, want 7", got)
		}
		want := []persist.Recovery{{}, {SnapshotSeq: 0, Events: 2, LastSeq: 2, Repaired: 0}}
		if !slices.Equal(r.recs, want) {
			tt.Errorf("recs %+v, want %+v", r.recs, want)
		}
		if r.boots != 2 || r.factories != 2 {
			tt.Errorf("%d boots, %d factory calls; want 2 and 2", r.boots, r.factories)
		}
	})
}

// AT-PER-04, PER-040: while recovery reads 300 events slowly, an Add, a Get and a signal arrive.
// No Handle call happens before the lockstep.persist.recover record; the kept signal runs after
// Recovered, with the recovered sum; then Add and Get reply 301, in that order.
func TestATPER04PausedRecovery(t *testing.T) {
	opts := fullTrace(2)
	opts.Disk.Latency.Read.Base = 5 * time.Millisecond
	sim.Simulate(t, opts, func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		r := newCounterRun(s)
		r.node()
		w.RunFor(time.Millisecond)
		for i := range 300 {
			if got := r.ask(tt, addCmd(1)); got != int64(i+1) {
				tt.Fatalf("Add{1} number %d replied %d", i+1, got)
			}
		}
		if n := len(effectStarts(w.Sim.Records(), "lockstep.persist.read")); n != 0 {
			tt.Fatalf("%d effect starts of lockstep.persist.read before the failure, want 0", n)
		}

		var addR, getR *lockstep.Reply[int64]
		sentAt, scanned, signalled := -1, 0, false
		w.Sim.OnEvent(func() {
			if sentAt >= 0 {
				return
			}
			recs := w.Sim.Records()
			for ; scanned < len(recs); scanned++ {
				if recs[scanned].Kind == "lockstep.effect.start" && attr(recs[scanned], "name") == "lockstep.persist.read" {
					break
				}
			}
			if scanned == len(recs) {
				return
			}
			sentAt = len(recs)
			sys := s.System("n1")
			addR = lockstep.Ask(sys, r.pid, addCmd(1))
			getR = lockstep.Ask(sys, r.pid, getCmd)
			signalled = hooks.Signal(sys, engineAddr(r.pid.Address()), wake())
		})
		logged := len(r.log)
		if err := s.InjectFail(r.pid.Address(), errors.New("injected")); err != nil {
			tt.Fatalf("InjectFail: %v", err)
		}
		w.RunFor(time.Second)

		if sentAt < 0 || !signalled {
			tt.Fatalf("the sends at the first read ran: %v, signal queued: %v", sentAt >= 0, signalled)
		}
		recs := w.Sim.Records()
		recovers := ofKind(recs, "lockstep.persist.recover")
		if len(recovers) != 2 {
			tt.Fatalf("%d lockstep.persist.recover records, want 2", len(recovers))
		}
		recoverAt := slices.IndexFunc(recs, func(rec kernel.Record) bool { return rec.Seq == recovers[1].Seq })
		if sentAt > recoverAt {
			tt.Errorf("the sends ran after the recover record (%d > %d), want during recovery", sentAt, recoverAt)
		}
		for _, at := range r.handleAt[300:] {
			if at <= recoverAt {
				tt.Errorf("a Handle call at record %d, before the recover record at %d", at, recoverAt)
			}
		}
		want := []string{"recovered 300", "signal 300", "handle sim_test.Add", "reply 301", "handle sim_test.Get", "get 301"}
		if got := r.log[logged:]; !slices.Equal(got, want) {
			tt.Errorf("counter calls after the failure %q, want %q", got, want)
		}
		for _, rep := range []*lockstep.Reply[int64]{addR, getR} {
			if !hooks.AskResolved(rep) {
				tt.Fatal("an ask sent during recovery is unresolved")
			}
			if v, err := rep.Await(context.Background()); v != 301 || err != nil {
				tt.Errorf("reply %d, %v; want 301", v, err)
			}
		}
	})
}

// AT-PER-05, PER-050: with ReplayBatch 2, recovering five events takes exactly three reads, and
// Get replies 5.
func TestATPER05ReplayBatches(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		r := newCounterRun(s)
		r.config = func(cfg *persist.Config[Added, CounterState]) { cfg.ReplayBatch = 2 }
		n1 := r.node()
		w.RunFor(time.Millisecond)
		for range 5 {
			r.ask(tt, addCmd(1))
		}
		now := w.Sim.Now()
		w.Sim.At(now.Add(time.Millisecond), "test/crash", n1.Crash)
		w.Sim.At(now.Add(2*time.Millisecond), "test/restart", n1.Restart)
		w.RunFor(5 * time.Millisecond)

		if got := r.ask(tt, getCmd); got != 5 {
			tt.Errorf("Get after the restart replied %d, want 5", got)
		}
		if n := len(effectStarts(w.Sim.Records(), "lockstep.persist.read")); n != 3 {
			tt.Errorf("%d effect starts of lockstep.persist.read, want 3", n)
		}
	})
}

// AT-PER-08, PER-090: Get on a new counter replies 0, and no effect starts between its
// lockstep.deliver record and its lockstep.reply record.
func TestATPER08NoEventsNoEffect(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		r := newCounterRun(s)
		r.node()
		w.RunFor(time.Millisecond)
		if len(ofKind(w.Sim.Records(), "lockstep.persist.recover")) != 1 {
			tt.Fatal("the counter has not recovered after 1 ms")
		}
		if got := r.ask(tt, getCmd); got != 0 {
			tt.Errorf("Get replied %d, want 0", got)
		}
		recs := w.Sim.Records()
		deliverAt := slices.IndexFunc(recs, func(rec kernel.Record) bool {
			return rec.Kind == "lockstep.deliver" && attr(rec, "to") == r.pid.String()
		})
		if deliverAt < 0 {
			tt.Fatal("no lockstep.deliver record for the Get")
		}
		replyAt := deliverAt + indexOf(recs[deliverAt:], isKind("lockstep.reply"))
		if replyAt < deliverAt {
			tt.Fatal("no lockstep.reply record after the Get's delivery")
		}
		if starts := ofKind(recs[deliverAt:replyAt], "lockstep.effect.start"); len(starts) != 0 {
			tt.Errorf("effect starts between the Get's delivery and its reply: %v", starts)
		}
	})
}

// AT-PER-09, PER-100: AddMany{3} writes one batch (first 1, count 3, 21 bytes), commits it (last
// 3), and only then replies 3; LastSeq in the Then callback is 3.
func TestATPER09WriteCommitRecords(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		r := newCounterRun(s)
		r.node()
		w.RunFor(time.Millisecond)
		if got := r.ask(tt, addManyCmd(3)); got != 3 {
			tt.Errorf("AddMany{3} replied %d, want 3", got)
		}
		recs := w.Sim.Records()
		writes, commits := ofKind(recs, "lockstep.persist.write"), ofKind(recs, "lockstep.persist.commit")
		if len(writes) != 1 || len(commits) != 1 {
			tt.Fatalf("%d write and %d commit records, want one each", len(writes), len(commits))
		}
		wantWrite := []string{"actor", r.pid.String(), "id", "c-1", "first", "1", "count", "3", "bytes", "21"}
		if got := kvs(writes[0]); !slices.Equal(got, wantWrite) {
			tt.Errorf("write record %q, want %q", got, wantWrite)
		}
		wantCommit := []string{"actor", r.pid.String(), "id", "c-1", "last", "3"}
		if got := kvs(commits[0]); !slices.Equal(got, wantCommit) {
			tt.Errorf("commit record %q, want %q", got, wantCommit)
		}
		writeAt, commitAt := seqIndex(recs, writes[0]), seqIndex(recs, commits[0])
		replyAt := indexOf(recs, isKind("lockstep.reply"))
		if writeAt >= commitAt || commitAt >= replyAt {
			tt.Errorf("record order write %d, commit %d, reply %d; want increasing", writeAt, commitAt, replyAt)
		}
		if !slices.Equal(r.thenSeqs, []uint64{3}) {
			tt.Errorf("LastSeq in Then callbacks %v, want [3]", r.thenSeqs)
		}
	})
}

// AT-PER-11, PER-110: three Adds sent at once while every sync takes 10 ms are handled one batch
// at a time: replies 1, 3 and 6 in that order, and each write record follows the previous
// batch's commit record.
func TestATPER11OneBatchInFlight(t *testing.T) {
	opts := fullTrace(3)
	opts.Disk.Latency.Sync.Base = 10 * time.Millisecond
	sim.Simulate(t, opts, func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		r := newCounterRun(s)
		r.node()
		// The store's first operation syncs its directories, at 10 ms each.
		w.RunFor(100 * time.Millisecond)
		if len(ofKind(w.Sim.Records(), "lockstep.persist.recover")) != 1 {
			tt.Fatal("the counter has not recovered after 100 ms")
		}
		sys := s.System("n1")
		var replies []*lockstep.Reply[int64]
		for n := range int64(3) {
			replies = append(replies, lockstep.Ask(sys, r.pid, addCmd(n+1)))
		}
		w.RunFor(time.Second)

		for i, want := range []int64{1, 3, 6} {
			if !hooks.AskResolved(replies[i]) {
				tt.Fatalf("ask %d is unresolved", i+1)
			}
			if v, err := replies[i].Await(context.Background()); v != want || err != nil {
				tt.Errorf("ask %d replied %d, %v; want %d", i+1, v, err, want)
			}
		}
		if want := []string{"reply 1", "reply 3", "reply 6"}; !slices.Equal(replyLog(r.log), want) {
			tt.Errorf("replies in order %q, want %q", replyLog(r.log), want)
		}
		recs := w.Sim.Records()
		writes, commits := ofKind(recs, "lockstep.persist.write"), ofKind(recs, "lockstep.persist.commit")
		if len(writes) != 3 || len(commits) != 3 {
			tt.Fatalf("%d write and %d commit records, want 3 each", len(writes), len(commits))
		}
		for i, wr := range writes {
			if got := attr(wr, "first"); got != fmt.Sprint(i+1) {
				tt.Errorf("write record %d has first=%s, want %d", i+1, got, i+1)
			}
			if i > 0 && seqIndex(recs, wr) <= seqIndex(recs, commits[i-1]) {
				tt.Errorf("write record %d precedes commit record %d", i+1, i)
			}
		}
		deliveries := slices.DeleteFunc(ofKind(recs, "lockstep.deliver"), func(rec kernel.Record) bool {
			return attr(rec, "to") != r.pid.String()
		})
		if len(deliveries) != 3 || seqIndex(recs, deliveries[2]) > seqIndex(recs, commits[0]) {
			tt.Errorf("the three Adds were not all queued before the first commit: %d deliveries", len(deliveries))
		}
	})
}

// AT-PER-24, PER-230: JSON round-trips a concrete type and panics for an interface type.
func TestATPER24JSONCodec(t *testing.T) {
	c := persist.JSON[CounterState]()
	data, err := c.Encode(CounterState{Sum: 3, Count: 2})
	if err != nil || string(data) != `{"Sum":3,"Count":2}` {
		t.Errorf("Encode: %q, %v; want {\"Sum\":3,\"Count\":2}", data, err)
	}
	if v, err := c.Decode(data); err != nil || v != (CounterState{Sum: 3, Count: 2}) {
		t.Errorf("Decode: %+v, %v; want {3 2}", v, err)
	}
	expectPanic(t, "persist: JSON: sim_test.CounterCmd is an interface type; use a codec that records the concrete type",
		func() { persist.JSON[CounterCmd]() })
}

// kvs returns r's attributes as keys and values, alternating.
func kvs(r kernel.Record) []string {
	out := make([]string, 0, 2*len(r.Attrs))
	for _, a := range r.Attrs {
		out = append(out, a.Key, a.Value)
	}
	return out
}

// seqIndex returns the index in recs of the record with rec's Seq, or -1.
func seqIndex(recs []kernel.Record, rec kernel.Record) int {
	return slices.IndexFunc(recs, func(x kernel.Record) bool { return x.Seq == rec.Seq })
}

// replyLog returns the "reply" entries of a counter log, in order.
func replyLog(log []string) []string {
	var out []string
	for _, e := range log {
		if strings.HasPrefix(e, "reply ") {
			out = append(out, e)
		}
	}
	return out
}
