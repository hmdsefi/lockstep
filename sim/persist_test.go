// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim_test

import (
	"context"
	"errors"
	"fmt"
	"path"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/faultline/kernel/simdisk"
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

// AT-PER-06, PER-060: a journal that does not fit the snapshot, or returns the wrong events, fails
// recovery with PER-060's error, which wraps ErrInconsistent, after a lockstep.persist.fail record
// with op=inconsistent.
func TestATPER06Inconsistent(t *testing.T) {
	added1, _ := persist.JSON[Added]().Encode(Added{N: 1})
	state0, _ := persist.JSON[CounterState]().Encode(CounterState{})
	cases := []struct {
		name string
		edit func(cfg *persist.Config[Added, CounterState])
		want error
	}{
		{"snapshot ahead of the journal", func(cfg *persist.Config[Added, CounterState]) {
			cfg.Journal = &fakeJournal{Journal: cfg.Journal, recover: bounds(1, 3)}
			cfg.Snapshots = &fakeSnapshots{SnapshotStore: cfg.Snapshots, load: func() (persist.Snapshot, bool, error) {
				return persist.Snapshot{Seq: 5, Data: state0}, true, nil
			}}
		}, fmt.Errorf("persist: %s: snapshot at seq %d is ahead of the journal (last seq %d): %w", "c-1", 5, 3, persist.ErrInconsistent)},
		{"journal starts too late", func(cfg *persist.Config[Added, CounterState]) {
			cfg.Journal = &fakeJournal{Journal: cfg.Journal, recover: bounds(4, 9)}
			cfg.Snapshots, cfg.State = nil, nil
		}, fmt.Errorf("persist: %s: journal starts at seq %d but recovery needs seq %d: %w", "c-1", 4, 1, persist.ErrInconsistent)},
		{"read returns the wrong seq", func(cfg *persist.Config[Added, CounterState]) {
			cfg.Journal = &fakeJournal{Journal: cfg.Journal, recover: bounds(1, 2), read: func(from uint64, _ int) ([]persist.Event, error) {
				if from == 1 {
					return []persist.Event{{Seq: 2, Data: added1}}, nil
				}
				return nil, nil
			}}
			cfg.Snapshots, cfg.State = nil, nil
		}, fmt.Errorf("persist: %s: journal returned seq %s, expected %d: %w", "c-1", "2", 1, persist.ErrInconsistent)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sim.Simulate(t, fullTrace(2), func(s *sim.Sim) {
				w, tt := s.World(), s.World().T()
				r := newCounterRun(s)
				r.config = c.edit
				r.directive = func(error) lockstep.Directive { return lockstep.DirectiveStop }
				r.node()
				w.RunFor(50 * time.Millisecond)
				if len(r.errs) != 1 {
					tt.Fatalf("errs %v, want one error", r.errs)
				}
				err := r.errs[0]
				if err.Error() != c.want.Error() || !errors.Is(err, persist.ErrInconsistent) {
					tt.Errorf("error %q (errors.Is ErrInconsistent: %v), want %q", err, errors.Is(err, persist.ErrInconsistent), c.want)
				}
				r.checkFailRecord(tt, "inconsistent", err)
				if len(r.recs) != 0 {
					tt.Errorf("recs %+v, want none", r.recs)
				}
			})
		})
	}
}

// AT-PER-07, PER-070: a failing Load, snapshot decode, Recover, Read or event decode fails the
// counter with PER-070's error after a lockstep.persist.fail record with the step's op, and every
// restart runs recovery again and fails again.
func TestATPER07RecoveryErrors(t *testing.T) {
	errX := errors.New("store failed")
	_, stateErr := persist.JSON[CounterState]().Decode([]byte("{"))
	_, eventErr := persist.JSON[Added]().Decode([]byte("{"))
	loadFails := func(cfg *persist.Config[Added, CounterState]) {
		cfg.Snapshots = &fakeSnapshots{SnapshotStore: cfg.Snapshots, load: func() (persist.Snapshot, bool, error) {
			return persist.Snapshot{}, false, errX
		}}
	}
	badSnapshot := func(cfg *persist.Config[Added, CounterState]) {
		cfg.Snapshots = &fakeSnapshots{SnapshotStore: cfg.Snapshots, load: func() (persist.Snapshot, bool, error) {
			return persist.Snapshot{Seq: 1, Data: []byte("{")}, true, nil
		}}
	}
	recoverFails := func(cfg *persist.Config[Added, CounterState]) {
		cfg.Journal = &fakeJournal{Journal: cfg.Journal, recover: func() (persist.Bounds, error) { return persist.Bounds{}, errX }}
	}
	readFails := func(cfg *persist.Config[Added, CounterState]) {
		cfg.Journal = &fakeJournal{Journal: cfg.Journal, recover: bounds(1, 2), read: func(uint64, int) ([]persist.Event, error) {
			return nil, errX
		}}
		cfg.Snapshots, cfg.State = nil, nil
	}
	badEvent := func(cfg *persist.Config[Added, CounterState]) {
		cfg.Journal = &fakeJournal{Journal: cfg.Journal, recover: bounds(1, 1), read: func(uint64, int) ([]persist.Event, error) {
			return []persist.Event{{Seq: 1, Data: []byte("{")}}, nil
		}}
		cfg.Snapshots, cfg.State = nil, nil
	}
	cases := []struct {
		op, effect string
		edit       func(cfg *persist.Config[Added, CounterState])
		want       error
		wrapsX     bool
	}{
		{"load", "lockstep.persist.load", loadFails, fmt.Errorf("persist: %s: load snapshot: %w", "c-1", errX), true},
		{"decode-state", "lockstep.persist.load", badSnapshot, fmt.Errorf("persist: %s: decode snapshot seq %d: %w", "c-1", 1, stateErr), false},
		{"recover", "lockstep.persist.recover", recoverFails, fmt.Errorf("persist: %s: recover: %w", "c-1", errX), true},
		{"read", "lockstep.persist.read", readFails, fmt.Errorf("persist: %s: replay from seq %d: %w", "c-1", 1, errX), true},
		{"decode", "lockstep.persist.read", badEvent, fmt.Errorf("persist: %s: decode event seq %d: %w", "c-1", 1, eventErr), false},
	}
	for _, c := range cases {
		t.Run(c.op, func(t *testing.T) {
			sim.Simulate(t, fullTrace(2), func(s *sim.Sim) {
				w, tt := s.World(), s.World().T()
				r := newCounterRun(s)
				r.config = c.edit
				r.directive = r.restartUpTo(3)
				r.node()
				w.RunFor(200 * time.Millisecond)
				if len(r.errs) != 3 {
					tt.Fatalf("errs %v, want 3 errors", r.errs)
				}
				for i, err := range r.errs {
					if err.Error() != c.want.Error() {
						tt.Errorf("errs[%d] %q, want %q", i, err, c.want)
					}
					if c.wrapsX && !errors.Is(err, errX) {
						tt.Errorf("errs[%d] %q does not wrap the store's error", i, err)
					}
				}
				r.checkFailRecord(tt, c.op, r.errs[0])
				if n := len(effectStarts(w.Sim.Records(), c.effect)); n < 2 {
					tt.Errorf("%d effect starts of %s, want at least 2", n, c.effect)
				}
				if len(r.recs) != 0 {
					tt.Errorf("recs %+v, want none", r.recs)
				}
			})
		})
	}
}

// AT-PER-10, PER-100 step 1: an event that does not encode fails the counter with the encode
// error and writes nothing; after the restart, the next Add commits as seq 1.
func TestATPER10EncodeFailure(t *testing.T) {
	errX := errors.New("encode failed")
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		r := newCounterRun(s)
		r.config = func(cfg *persist.Config[Added, CounterState]) {
			cfg.Events = failCodec{Codec: cfg.Events, n: 13, err: errX}
		}
		r.node()
		w.RunFor(time.Millisecond)
		//lockstep:allow LS220 encoding the event fails on purpose; the test reads the actor's error
		lockstep.Ask(s.System("n1"), r.pid, addCmd(13))
		w.RunFor(50 * time.Millisecond)

		want := fmt.Errorf("persist: %s: encode event %d: %w", "c-1", 0, errX)
		if len(r.errs) != 1 || r.errs[0].Error() != want.Error() || !errors.Is(r.errs[0], errX) {
			tt.Fatalf("errs %v, want [%q]", r.errs, want)
		}
		r.checkFailRecord(tt, "encode", r.errs[0])
		for _, rec := range ofKind(w.Sim.Records(), "disk.write") {
			if attr(rec, "path") == vseg(1) {
				tt.Errorf("a disk.write record for %s after the encode failure: %v", vseg(1), rec)
			}
		}
		if got := r.ask(tt, addCmd(1)); got != 1 {
			tt.Errorf("Add{1} after the restart replied %d, want 1", got)
		}
		recs := w.Sim.Records()
		writes, commits := ofKind(recs, "lockstep.persist.write"), ofKind(recs, "lockstep.persist.commit")
		if len(writes) != 1 || attr(writes[0], "first") != "1" || len(commits) != 1 || attr(commits[0], "last") != "1" {
			tt.Errorf("write records %v and commit records %v, want one each for seq 1", writes, commits)
		}
	})
}

// fullMailbox is the outcome of AT-PER-12's setup (runFullMailbox).
type fullMailbox struct {
	r          *counterRun
	rep1, rep2 *lockstep.Reply[int64]
	err2, err3 error
	sentAt     int // len(Records()) when Add{2} and Add{3} were sent; -1 if never
}

// runFullMailbox runs AT-PER-12's setup: a counter with a mailbox of capacity 1 that fails sends
// when full, every sync taking 10 ms. It asks Add{1}; once its write record exists, it sends Add{2}
// and Add{3}, then runs one second.
func runFullMailbox(s *sim.Sim) *fullMailbox {
	w := s.World()
	f := &fullMailbox{r: newCounterRun(s), sentAt: -1}
	f.r.opts = []lockstep.SpawnOption{lockstep.WithMailbox(1, lockstep.FailSend)}
	f.r.node()
	// The store's first operation syncs its directories, at 10 ms each.
	w.RunFor(100 * time.Millisecond)
	sys := s.System("n1")
	scanned := 0
	w.Sim.OnEvent(func() {
		if f.sentAt >= 0 {
			return
		}
		recs := w.Sim.Records()
		for ; scanned < len(recs) && recs[scanned].Kind != "lockstep.persist.write"; scanned++ {
		}
		if scanned == len(recs) {
			return
		}
		f.sentAt = len(recs)
		rt2, rep2 := lockstep.NewReply[int64](sys)
		f.rep2, f.err2 = rep2, lockstep.Send(sys, f.r.pid, CounterCmd(Add{N: 2, ReplyTo: rt2}))
		rt3, _ := lockstep.NewReply[int64](sys)
		f.err3 = lockstep.Send(sys, f.r.pid, CounterCmd(Add{N: 3, ReplyTo: rt3}))
	})
	f.rep1 = lockstep.Ask(sys, f.r.pid, addCmd(1))
	w.RunFor(time.Second)
	return f
}

// AT-PER-12, PER-110: while Add{1}'s batch is in flight, Add{2} waits in the full mailbox and
// Add{3} is refused with ErrMailboxFull; the replies are 1, then 3.
func TestATPER12MailboxFullWhileWriting(t *testing.T) {
	opts := fullTrace(3)
	opts.Disk.Latency.Sync.Base = 10 * time.Millisecond
	sim.Simulate(t, opts, func(s *sim.Sim) {
		tt := s.World().T()
		f := runFullMailbox(s)
		if f.sentAt < 0 {
			tt.Fatal("no lockstep.persist.write record for Add{1}")
		}
		if f.err2 != nil || !errors.Is(f.err3, lockstep.ErrMailboxFull) {
			tt.Errorf("Send(Add{2}) %v, Send(Add{3}) %v; want nil and ErrMailboxFull", f.err2, f.err3)
		}
		for i, c := range []struct {
			rep  *lockstep.Reply[int64]
			want int64
		}{{f.rep1, 1}, {f.rep2, 3}} {
			if !hooks.AskResolved(c.rep) {
				tt.Fatalf("reply %d is unresolved", i+1)
			}
			if v, err := c.rep.Await(context.Background()); v != c.want || err != nil {
				tt.Errorf("reply %d: %d, %v; want %d", i+1, v, err, c.want)
			}
		}
		if want := []string{"reply 1", "reply 3"}; !slices.Equal(replyLog(f.r.log), want) {
			tt.Errorf("replies in order %q, want %q", replyLog(f.r.log), want)
		}
	})
}

// AT-PER-25, PER-240: in AT-PER-12's setup, the commit completion of Add{1} runs while Add{2} fills
// the mailbox and waits, paused: the mailbox was full before the commit record, and Add{2} is
// handled after it.
func TestATPER25CommitWhileMailboxFull(t *testing.T) {
	opts := fullTrace(3)
	opts.Disk.Latency.Sync.Base = 10 * time.Millisecond
	sim.Simulate(t, opts, func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		f := runFullMailbox(s)
		if f.sentAt < 0 || f.err2 != nil || !errors.Is(f.err3, lockstep.ErrMailboxFull) {
			tt.Fatalf("the mailbox was not full during the batch: sent %v, errors %v and %v", f.sentAt >= 0, f.err2, f.err3)
		}
		recs := w.Sim.Records()
		commits := ofKind(recs, "lockstep.persist.commit")
		if len(commits) == 0 || attr(commits[0], "last") != "1" {
			tt.Fatalf("commit records %v, want the first for seq 1", commits)
		}
		commitAt := seqIndex(recs, commits[0])
		if f.sentAt > commitAt {
			tt.Errorf("the mailbox filled at record %d, after the commit record %d", f.sentAt, commitAt)
		}
		if len(f.r.handleAt) < 2 || f.r.handleAt[1] <= commitAt {
			tt.Errorf("Handle calls at records %v; want the second after the commit record %d", f.r.handleAt, commitAt)
		}
	})
}

// AT-PER-13, PER-120: Stop inside a Handle call that returns events stops the counter only after the
// batch is committed; the stopped incarnation sees the new state, and a later boot recovers the
// event. If the commit fails, the counter fails and does not stop.
func TestATPER13DeferredStop(t *testing.T) {
	t.Run("commit", func(t *testing.T) {
		sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
			w, tt := s.World(), s.World().T()
			r := newCounterRun(s)
			n1 := r.node()
			w.RunFor(time.Millisecond)
			if err := sim.Send(s, "n1", r.pid, CounterCmd(AddThenStop{N: 4})); err != nil {
				tt.Fatalf("Send: %v", err)
			}
			w.RunFor(50 * time.Millisecond)
			recs := w.Sim.Records()
			commits, stops := r.ofActor(recs, "lockstep.persist.commit"), r.ofActor(recs, "lockstep.stop")
			if len(commits) != 1 || len(stops) != 1 {
				tt.Fatalf("%d commit and %d stop records of the counter, want one each", len(commits), len(stops))
			}
			if seqIndex(recs, commits[0]) > seqIndex(recs, stops[0]) {
				tt.Error("the counter's stop record precedes its commit record")
			}
			if want := []string{fmt.Sprintf("4 %s", lockstep.Normal)}; !slices.Equal(r.stops, want) {
				tt.Errorf("Stopped calls %q, want %q", r.stops, want)
			}
			now := w.Sim.Now()
			w.Sim.At(now.Add(time.Millisecond), "test/crash", n1.Crash)
			w.Sim.At(now.Add(2*time.Millisecond), "test/restart", n1.Restart)
			w.RunFor(5 * time.Millisecond)
			want := []persist.Recovery{{}, {Events: 1, LastSeq: 1}}
			if !slices.Equal(r.recs, want) {
				tt.Errorf("recs %+v, want %+v", r.recs, want)
			}
		})
	})
	t.Run("commit fails", func(t *testing.T) {
		sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
			w, tt := s.World(), s.World().T()
			r := newCounterRun(s)
			r.node()
			w.RunFor(time.Millisecond)
			r.mustHaveRecovered(tt)
			vol(s).FailSyncs(1)
			if err := sim.Send(s, "n1", r.pid, CounterCmd(AddThenStop{N: 4})); err != nil {
				tt.Fatalf("Send: %v", err)
			}
			w.RunFor(50 * time.Millisecond)
			if len(r.errs) != 1 {
				tt.Fatalf("errs %v, want one error", r.errs)
			}
			r.checkCommitError(tt, r.errs[0])
			if stops := r.ofActor(w.Sim.Records(), "lockstep.stop"); len(stops) != 0 {
				tt.Errorf("stop records of the counter %v, want none", stops)
			}
		})
	})
}

// AT-PER-14, PER-130: a panic in Handle fails the counter and the restarted counter keeps the
// committed sum; a panic in Apply after a commit fails the counter, and every recovery replays the
// committed event and fails again.
func TestATPER14Panics(t *testing.T) {
	t.Run("Handle", func(t *testing.T) {
		sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
			w, tt := s.World(), s.World().T()
			r := newCounterRun(s)
			r.node()
			w.RunFor(time.Millisecond)
			r.ask(tt, addCmd(5))
			if err := sim.Send(s, "n1", r.pid, CounterCmd(Boom{})); err != nil {
				tt.Fatalf("Send: %v", err)
			}
			w.RunFor(50 * time.Millisecond)
			if len(r.errs) != 1 || !isPanic(r.errs[0], "boom") {
				tt.Fatalf("errs %v, want one panic error for boom", r.errs)
			}
			if len(r.stops) != 1 || !strings.HasPrefix(r.stops[0], "5 ") {
				tt.Errorf("Stopped calls %q, want one with the sum 5", r.stops)
			}
			if got := r.ask(tt, getCmd); got != 5 {
				tt.Errorf("Get after the restart replied %d, want 5", got)
			}
		})
	})
	t.Run("Apply", func(t *testing.T) {
		sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
			w, tt := s.World(), s.World().T()
			r := newCounterRun(s)
			r.apply = func(e Added) {
				if e.N == 99 {
					panic("apply 99")
				}
			}
			r.directive = r.restartUpTo(3)
			r.node()
			w.RunFor(time.Millisecond)
			//lockstep:allow LS220 applying the command panics on purpose; the test reads the commits
			lockstep.Ask(s.System("n1"), r.pid, addCmd(99))
			w.RunFor(500 * time.Millisecond)
			commits := r.ofActor(w.Sim.Records(), "lockstep.persist.commit")
			if len(commits) != 1 || attr(commits[0], "last") != "1" {
				tt.Errorf("commit records %v, want one for seq 1", commits)
			}
			if len(r.errs) != 3 {
				tt.Fatalf("errs %v, want 3 errors", r.errs)
			}
			for i, err := range r.errs {
				if !isPanic(err, "apply 99") {
					tt.Errorf("errs[%d] %v, want a panic error for apply 99", i, err)
				}
			}
			if want := []persist.Recovery{{}}; !slices.Equal(r.recs, want) {
				tt.Errorf("recs %+v, want %+v", r.recs, want)
			}
		})
	})
}

// AT-PER-15, PER-140: a failed commit or write fails the counter with PER-140's error and
// acknowledges nothing; Fail during Handle discards the events and the Then callbacks and fails
// the counter with the given error, so the ask gets no reply.
func TestATPER15WriteCommitFailure(t *testing.T) {
	errX := errors.New("write failed")
	t.Run("commit", func(t *testing.T) {
		sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
			w, tt := s.World(), s.World().T()
			r := newCounterRun(s)
			r.node()
			w.RunFor(time.Millisecond)
			r.mustHaveRecovered(tt)
			vol(s).FailSyncs(1)
			if _, err := sim.Ask(s, "n1", r.pid, addCmd(7)); !errors.Is(err, lockstep.ErrTimeout) {
				tt.Errorf("Ask(Add{7}): %v, want ErrTimeout", err)
			}
			if len(r.errs) == 0 {
				tt.Fatal("no error after the failed commit")
			}
			r.checkCommitError(tt, r.errs[0])
			if replies := replyLog(r.log); len(replies) != 0 {
				tt.Errorf("Then callbacks ran: %q", replies)
			}
		})
	})
	t.Run("write", func(t *testing.T) {
		sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
			w, tt := s.World(), s.World().T()
			r := newCounterRun(s)
			r.config = func(cfg *persist.Config[Added, CounterState]) {
				cfg.Journal = &fakeJournal{Journal: cfg.Journal, write: func([]persist.Event) error { return errX }}
			}
			r.directive = func(error) lockstep.Directive { return lockstep.DirectiveStop }
			r.node()
			w.RunFor(time.Millisecond)
			//lockstep:allow LS220 the journal write fails on purpose; the test reads the actor's error
			lockstep.Ask(s.System("n1"), r.pid, addCmd(7))
			w.RunFor(50 * time.Millisecond)
			want := fmt.Errorf("persist: %s: write seq %d..%d: %w", "c-1", 1, 1, errX)
			if len(r.errs) != 1 || r.errs[0].Error() != want.Error() || !errors.Is(r.errs[0], errX) {
				tt.Fatalf("errs %v, want [%q]", r.errs, want)
			}
			r.checkFailRecord(tt, "write", r.errs[0])
			recs := w.Sim.Records()
			if n := len(ofKind(recs, "lockstep.persist.write")) + len(effectStarts(recs, "lockstep.persist.commit")); n != 0 {
				tt.Errorf("%d write records and commit starts after the failed write, want none", n)
			}
			if replies := replyLog(r.log); len(replies) != 0 {
				tt.Errorf("Then callbacks ran: %q", replies)
			}
		})
	})
	t.Run("Fail in Handle", func(t *testing.T) {
		sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
			w, tt := s.World(), s.World().T()
			r := newCounterRun(s)
			r.node()
			w.RunFor(time.Millisecond)
			_, err := sim.Ask(s, "n1", r.pid, func(rt lockstep.ReplyTo[int64]) CounterCmd {
				return FailWith{N: 7, Err: errX, ReplyTo: rt}
			})
			if !errors.Is(err, lockstep.ErrTimeout) {
				tt.Errorf("Ask(FailWith{7, errX}): %v, want ErrTimeout", err)
			}
			if replies := replyLog(r.log); len(replies) != 0 {
				tt.Errorf("Then callbacks ran: %q", replies)
			}
			if len(r.errs) != 1 || r.errs[0] != errX {
				tt.Errorf("errs %v, want [%v]", r.errs, errX)
			}
			recs := w.Sim.Records()
			if n := len(effectStarts(recs, "lockstep.persist.write")) + len(ofKind(recs, "lockstep.persist.write")); n != 0 {
				tt.Errorf("%d write starts and records after Fail, want none", n)
			}
			if pf := ofKind(recs, "lockstep.persist.fail"); len(pf) != 0 {
				tt.Errorf("lockstep.persist.fail records %v, want none", pf)
			}
		})
	})
}

// PER-120: Stop inside a Handle call that returns no events stops the counter at once, after
// the Then callbacks ran; no write starts.
func TestPersistStopWithoutEvents(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		r := newCounterRun(s)
		r.directive = func(error) lockstep.Directive { return lockstep.DirectiveStop }
		r.node()
		w.RunFor(time.Millisecond)
		r.ask(tt, addCmd(5))
		writes := len(effectStarts(w.Sim.Records(), "lockstep.persist.write"))
		if got := r.ask(tt, func(rt lockstep.ReplyTo[int64]) CounterCmd { return StopNow{ReplyTo: rt} }); got != 5 {
			tt.Errorf("StopNow replied %d, want 5", got)
		}
		recs := w.Sim.Records()
		if n := len(effectStarts(recs, "lockstep.persist.write")) - writes; n != 0 {
			tt.Errorf("%d write starts after StopNow, want none", n)
		}
		if stops := r.ofActor(recs, "lockstep.stop"); len(stops) != 1 {
			tt.Errorf("stop records of the counter %v, want one", stops)
		}
		if want := []string{fmt.Sprintf("5 %s", lockstep.Normal)}; !slices.Equal(r.stops, want) {
			tt.Errorf("Stopped calls %q, want %q", r.stops, want)
		}
		if want := []string{"reply 5", "reply 5"}; !slices.Equal(replyLog(r.log), want) || len(r.errs) != 0 {
			tt.Errorf("replies %q and errs %v, want %q and none", replyLog(r.log), r.errs, want)
		}
	})
}

// AT-PER-23, PER-220: the store's calls come only from internal effects, never while Handle or
// Apply runs, and each incarnation calls Recover, then Read, then Write and Commit in pairs.
func TestATPER23StoreCallsFromEffects(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		r := newCounterRun(s)
		var rj *recJournal
		r.config = func(cfg *persist.Config[Added, CounterState]) {
			rj = &recJournal{Journal: cfg.Journal, r: r}
			cfg.Journal = rj
		}
		r.node()
		w.RunFor(time.Millisecond)
		r.ask(tt, addCmd(1))
		r.ask(tt, addCmd(1))
		if err := s.InjectFail(r.pid.Address(), errors.New("injected")); err != nil {
			tt.Fatalf("InjectFail: %v", err)
		}
		w.RunFor(50 * time.Millisecond)
		r.ask(tt, addCmd(1))
		if got := r.ask(tt, getCmd); got != 3 {
			tt.Errorf("Get replied %d, want 3", got)
		}
		want := []string{"Recover", "Write", "Commit", "Write", "Commit", "Recover", "Read", "Write", "Commit"}
		if !slices.Equal(rj.calls, want) {
			tt.Errorf("journal calls %q, want %q", rj.calls, want)
		}
		if len(rj.bad) != 0 {
			tt.Errorf("journal calls while Handle or Apply ran: %q", rj.bad)
		}
	})
}

// PER-140, PER-130: after a persist failure, a supervisor's DirectiveResume keeps the incarnation,
// and its next turn recovers the state from the store again before the command, signal or effect
// completion that reached it is handled. The factory runs once.
func TestPersistResumeRecoversAgain(t *testing.T) {
	resume := func(error) lockstep.Directive { return lockstep.DirectiveResume }
	// commitFails runs the counter with a parent that resumes it, and fails the commit of Add{7}.
	commitFails := func(s *sim.Sim, r *counterRun) {
		w, tt := s.World(), s.World().T()
		r.directive = resume
		r.node()
		w.RunFor(time.Millisecond)
		r.mustHaveRecovered(tt)
		vol(s).FailSyncs(1)
		//lockstep:allow LS220 the sync fails on purpose; the test reads the actor's error
		lockstep.Ask(s.System("n1"), r.pid, addCmd(7))
		w.RunFor(50 * time.Millisecond)
		if len(r.errs) != 1 {
			tt.Fatalf("errs %v, want one error", r.errs)
		}
		r.checkCommitError(tt, r.errs[0])
	}
	// recovered returns the sum of the second Recovered call: 7 if it found Add{7}'s batch.
	recovered := func(tt testing.TB, r *counterRun) int64 {
		tt.Helper()
		if len(r.recs) != 2 {
			tt.Fatalf("recs %+v, want two recoveries", r.recs)
		}
		if r.recs[1].LastSeq == 1 {
			return 7
		}
		return 0
	}
	t.Run("commit failure, then a command", func(t *testing.T) {
		sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
			tt := s.World().T()
			r := newCounterRun(s)
			commitFails(s, r)
			logged := len(r.log)
			got := r.ask(tt, addCmd(1))
			sum := recovered(tt, r)
			want := []string{fmt.Sprintf("recovered %d", sum), "handle sim_test.Add", fmt.Sprintf("reply %d", sum+1)}
			if !slices.Equal(r.log[logged:], want) || got != sum+1 {
				tt.Errorf("counter calls %q and reply %d, want %q and %d", r.log[logged:], got, want, sum+1)
			}
			if got := r.ask(tt, getCmd); got != sum+1 {
				tt.Errorf("Get replied %d, want %d", got, sum+1)
			}
			if r.factories != 1 || len(r.recs) != 2 {
				tt.Errorf("%d factory calls and %d recoveries, want 1 and 2", r.factories, len(r.recs))
			}
		})
	})
	t.Run("commit failure, then a signal", func(t *testing.T) {
		sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
			w, tt := s.World(), s.World().T()
			r := newCounterRun(s)
			commitFails(s, r)
			logged := len(r.log)
			if !hooks.Signal(s.System("n1"), engineAddr(r.pid.Address()), wake()) {
				tt.Fatal("the signal was not queued")
			}
			w.RunFor(10 * time.Millisecond)
			sum := recovered(tt, r)
			want := []string{fmt.Sprintf("recovered %d", sum), fmt.Sprintf("signal %d", sum)}
			if !slices.Equal(r.log[logged:], want) {
				tt.Errorf("counter calls %q, want %q", r.log[logged:], want)
			}
			if got := r.ask(tt, addCmd(1)); got != sum+1 {
				tt.Errorf("Add{1} replied %d, want %d", got, sum+1)
			}
			if r.factories != 1 || len(r.recs) != 2 {
				tt.Errorf("%d factory calls and %d recoveries, want 1 and 2", r.factories, len(r.recs))
			}
		})
	})
	t.Run("commit failure, then a failed recovery and two commands", func(t *testing.T) {
		// The second Recover call, the re-run after the commit failure, fails; Add{1} that started
		// it stays kept, Add{2} joins it, and the next re-run handles both in order.
		sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
			w, tt := s.World(), s.World().T()
			r := newCounterRun(s)
			errX := errors.New("recover failed")
			recovers := 0
			r.config = func(cfg *persist.Config[Added, CounterState]) {
				inner := cfg.Journal
				cfg.Journal = &fakeJournal{Journal: inner, recover: func() (persist.Bounds, error) {
					if recovers++; recovers == 2 {
						return persist.Bounds{}, errX
					}
					return inner.Recover(context.Background(), "c-1")
				}}
			}
			commitFails(s, r)
			logged := len(r.log)
			//lockstep:allow LS220 the commit fails on purpose; the test reads the actor's error
			lockstep.Ask(s.System("n1"), r.pid, addCmd(1))
			w.RunFor(10 * time.Millisecond)
			if len(r.errs) != 2 || !errors.Is(r.errs[1], errX) {
				tt.Fatalf("errs %v, want a second error wrapping %v", r.errs, errX)
			}
			//lockstep:allow LS220 the test reads the recovered state, not the reply
			lockstep.Ask(s.System("n1"), r.pid, addCmd(2))
			w.RunFor(50 * time.Millisecond)
			sum := recovered(tt, r)
			want := []string{
				fmt.Sprintf("recovered %d", sum),
				"handle sim_test.Add", fmt.Sprintf("reply %d", sum+1),
				"handle sim_test.Add", fmt.Sprintf("reply %d", sum+3),
			}
			if !slices.Equal(r.log[logged:], want) {
				tt.Errorf("counter calls %q, want %q", r.log[logged:], want)
			}
			if got := r.ask(tt, getCmd); got != sum+3 {
				tt.Errorf("Get replied %d, want %d", got, sum+3)
			}
			if r.factories != 1 || recovers != 3 {
				tt.Errorf("%d factory calls and %d Recover calls, want 1 and 3", r.factories, recovers)
			}
		})
	})
	t.Run("Apply panic", func(t *testing.T) {
		sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
			w, tt := s.World(), s.World().T()
			r := newCounterRun(s)
			armed := true
			r.apply = func(e Added) {
				if e.N == 99 && armed {
					armed = false
					panic("apply 99")
				}
			}
			r.directive = resume
			r.node()
			w.RunFor(time.Millisecond)
			//lockstep:allow LS220 applying the command panics on purpose; the test reads the actor's error
			lockstep.Ask(s.System("n1"), r.pid, addCmd(99))
			w.RunFor(50 * time.Millisecond)
			if len(r.errs) != 1 || !isPanic(r.errs[0], "apply 99") {
				tt.Fatalf("errs %v, want one panic error for apply 99", r.errs)
			}
			logged := len(r.log)
			if got := r.ask(tt, getCmd); got != 99 {
				tt.Errorf("Get after the resume replied %d, want 99", got)
			}
			if want := []string{"recovered 99", "handle sim_test.Get", "get 99"}; !slices.Equal(r.log[logged:], want) {
				tt.Errorf("counter calls %q, want %q", r.log[logged:], want)
			}
			if want := []persist.Recovery{{}, {Events: 1, LastSeq: 1}}; !slices.Equal(r.recs, want) {
				tt.Errorf("recs %+v, want %+v", r.recs, want)
			}
			if r.factories != 1 {
				tt.Errorf("%d factory calls, want 1", r.factories)
			}
		})
	})
}

// bounds returns a Recover replacement that returns Bounds{first, last, 0}.
func bounds(first, last uint64) func() (persist.Bounds, error) {
	return func() (persist.Bounds, error) { return persist.Bounds{First: first, Last: last}, nil }
}

// isPanic reports whether err is a *lockstep.PanicError for the value v.
func isPanic(err error, v any) bool {
	var pe *lockstep.PanicError
	return errors.As(err, &pe) && pe.Value == v
}

// mustHaveRecovered stops the test unless the counter has emitted lockstep.persist.recover. After
// that record the store's first operation, which syncs its directories, is done, so a FailSyncs
// hits the next commit.
func (r *counterRun) mustHaveRecovered(tt testing.TB) {
	tt.Helper()
	if len(ofKind(r.s.World().Sim.Records(), "lockstep.persist.recover")) == 0 {
		tt.Fatal("the counter has not recovered yet")
	}
}

// checkCommitError checks that err is the commit error of a batch of seq 1 whose sync failed, and
// that a lockstep.persist.fail record with op=commit preceded the failure.
func (r *counterRun) checkCommitError(tt testing.TB, err error) {
	tt.Helper()
	if !strings.HasPrefix(err.Error(), "persist: c-1: commit seq 1..1: ") || !errors.Is(err, simdisk.ErrIO) {
		tt.Errorf("error %q (errors.Is ErrIO: %v), want the commit error of seq 1..1 wrapping ErrIO", err, errors.Is(err, simdisk.ErrIO))
	}
	r.checkFailRecord(tt, "commit", err)
}

// checkFailRecord checks that the counter's first lockstep.persist.fail record has op and err's
// text and precedes its first lockstep.fail record.
func (r *counterRun) checkFailRecord(tt testing.TB, op string, err error) {
	tt.Helper()
	recs := r.s.World().Sim.Records()
	pf, lf := r.ofActor(recs, "lockstep.persist.fail"), r.ofActor(recs, "lockstep.fail")
	if len(pf) == 0 || len(lf) == 0 {
		tt.Errorf("%d lockstep.persist.fail and %d lockstep.fail records of the counter, want both", len(pf), len(lf))
		return
	}
	if got, want := kvs(pf[0]), r.failRecord(op, err); !slices.Equal(got, want) {
		tt.Errorf("lockstep.persist.fail record %q, want %q", got, want)
	}
	if seqIndex(recs, pf[0]) > seqIndex(recs, lf[0]) {
		tt.Error("the lockstep.persist.fail record follows the lockstep.fail record")
	}
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

// snapshotEffects are the names of the internal effects of one snapshot cycle.
var snapshotEffects = []string{"lockstep.persist.save", "lockstep.persist.delete-snapshots", "lockstep.persist.delete-events"}

// AT-PER-19, PER-170: with SnapshotEvery 3, five Add{1} give one snapshot of seq 3; SnapNow gives
// seq 5; the next Add{1} gives none; after a crash and restart the recovery starts from seq 5 and
// replays one event. With SnapshotEvery 1 and slow syncs, three Add{1} sent at once give strictly
// increasing snapshot seqs, and no save starts while another cycle is in flight.
func TestATPER19SnapshotPolicy(t *testing.T) {
	t.Run("policy", func(t *testing.T) {
		sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
			w, tt := s.World(), s.World().T()
			r := newCounterRun(s)
			r.config = func(cfg *persist.Config[Added, CounterState]) { cfg.SnapshotEvery = 3 }
			n1 := r.node()
			w.RunFor(time.Millisecond)
			for range 5 {
				r.ask(tt, addCmd(1))
			}
			w.RunFor(50 * time.Millisecond)
			if got := snapSeqs(s); !slices.Equal(got, []string{"3"}) {
				tt.Fatalf("snapshot seqs after five Add{1}: %v, want [3]", got)
			}
			rec := ofKind(w.Sim.Records(), "lockstep.persist.snapshot")[0]
			want := []string{"actor", r.pid.String(), "id", "c-1", "seq", "3", "bytes", fmt.Sprint(len(`{"Sum":3,"Count":3}`))}
			if got := kvs(rec); !slices.Equal(got, want) {
				tt.Errorf("snapshot record %q, want %q", got, want)
			}

			if err := sim.Send(s, "n1", r.pid, CounterCmd(SnapNow{})); err != nil {
				tt.Fatalf("Send: %v", err)
			}
			w.RunFor(50 * time.Millisecond)
			if got := snapSeqs(s); !slices.Equal(got, []string{"3", "5"}) {
				tt.Fatalf("snapshot seqs after SnapNow: %v, want [3 5]", got)
			}
			r.ask(tt, addCmd(1))
			w.RunFor(50 * time.Millisecond)
			if got := snapSeqs(s); !slices.Equal(got, []string{"3", "5"}) {
				tt.Errorf("snapshot seqs after the sixth Add{1}: %v, want [3 5]", got)
			}

			now := w.Sim.Now()
			w.Sim.At(now.Add(time.Millisecond), "test/crash", n1.Crash)
			w.Sim.At(now.Add(2*time.Millisecond), "test/restart", n1.Restart)
			w.RunFor(5 * time.Millisecond)
			if got, want := r.recs[len(r.recs)-1], (persist.Recovery{SnapshotSeq: 5, Events: 1, LastSeq: 6}); got != want {
				tt.Errorf("recovery after the restart %+v, want %+v", got, want)
			}
			if got := r.ask(tt, getCmd); got != 6 {
				tt.Errorf("Get after the restart replied %d, want 6", got)
			}
		})
	})
	t.Run("one save at a time", func(t *testing.T) {
		opts := fullTrace(3)
		opts.Disk.Latency.Sync.Base = 20 * time.Millisecond
		sim.Simulate(t, opts, func(s *sim.Sim) {
			w, tt := s.World(), s.World().T()
			r := newCounterRun(s)
			r.config = func(cfg *persist.Config[Added, CounterState]) { cfg.SnapshotEvery = 1 }
			r.node()
			w.RunFor(500 * time.Millisecond)
			r.mustHaveRecovered(tt)
			sys := s.System("n1")
			var replies []*lockstep.Reply[int64]
			for range 3 {
				replies = append(replies, lockstep.Ask(sys, r.pid, addCmd(1)))
			}
			w.RunFor(5 * time.Second)
			for i, rp := range replies {
				if !hooks.AskResolved(rp) {
					tt.Fatalf("ask %d is unresolved", i+1)
				}
			}

			seqs := snapSeqs(s)
			if len(seqs) < 2 || seqs[len(seqs)-1] != "3" {
				tt.Errorf("snapshot seqs %v, want at least two, the last one 3", seqs)
			}
			prev := uint64(0)
			for _, q := range seqs {
				n, _ := strconv.ParseUint(q, 10, 64)
				if n <= prev {
					tt.Errorf("snapshot seqs %v are not strictly increasing", seqs)
				}
				prev = n
			}
			r.checkOneCycleAtATime(tt)
		})
	})
}

// AT-PER-20, PER-180: a state that does not encode at Sum 2 is recorded once as op=encode-state;
// the counter does not fail, and snapshots are saved for seq 1 and 3.
func TestATPER20SnapshotEncodeFailure(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		r := newCounterRun(s)
		errX := errors.New("x")
		r.config = func(cfg *persist.Config[Added, CounterState]) {
			cfg.SnapshotEvery = 1
			cfg.State = stateFailCodec{Codec: persist.JSON[CounterState](), sum: 2, err: errX}
		}
		r.node()
		w.RunFor(time.Millisecond)
		for range 3 {
			r.ask(tt, addCmd(1))
		}
		w.RunFor(50 * time.Millisecond)

		if len(r.errs) != 0 {
			tt.Errorf("errs %v, want none", r.errs)
		}
		if got := snapSeqs(s); !slices.Equal(got, []string{"1", "3"}) {
			tt.Errorf("snapshot seqs %v, want [1 3]", got)
		}
		fails := ofKind(w.Sim.Records(), "lockstep.persist.fail")
		if len(fails) != 1 {
			tt.Fatalf("%d lockstep.persist.fail records, want one", len(fails))
		}
		wantErr := fmt.Errorf("persist: c-1: encode state at seq 2: %w", errX)
		if got, want := kvs(fails[0]), r.failRecord("encode-state", wantErr); !slices.Equal(got, want) {
			tt.Errorf("fail record %q, want %q", got, want)
		}
		if got := r.ask(tt, getCmd); got != 3 {
			tt.Errorf("Get replied %d, want 3", got)
		}
	})
}

// AT-PER-21, PER-190: with a snapshot every 2 events, pruning and one event per segment, eight
// Add{1} give snapshots 2, 4, 6 and 8 and leave the snapshots 6 and 8 and the segments 7 and 8.
// With the payload of snapshot 8 damaged, the restart recovers from snapshot 6 and replays two
// events.
func TestATPER21Retention(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		r := newCounterRun(s)
		r.config = func(cfg *persist.Config[Added, CounterState]) {
			store := filestore.New(s.Volume("n1"), filestore.WithSegmentSize(1))
			cfg.Journal, cfg.Snapshots = store, store
			cfg.SnapshotEvery, cfg.PruneEvents = 2, true
		}
		n1 := r.node()
		w.RunFor(time.Millisecond)
		for range 8 {
			r.ask(tt, addCmd(1))
		}
		w.RunFor(100 * time.Millisecond)

		if got := snapSeqs(s); !slices.Equal(got, []string{"2", "4", "6", "8"}) {
			tt.Fatalf("snapshot seqs %v, want [2 4 6 8]", got)
		}
		v := vol(s)
		if got, want := names(tt, v, "/snapshots/id-c-1"), []string{path.Base(vsnap(6)), path.Base(vsnap(8))}; !slices.Equal(got, want) {
			tt.Errorf("snapshot files %v, want %v", got, want)
		}
		if got, want := names(tt, v, "/journal/id-c-1"), []string{path.Base(vseg(7)), path.Base(vseg(8))}; !slices.Equal(got, want) {
			tt.Errorf("journal segments %v, want %v", got, want)
		}
		if ops := failOps(s); len(ops) != 0 {
			tt.Errorf("lockstep.persist.fail records %v, want none", ops)
		}

		now := w.Sim.Now()
		w.Sim.At(now.Add(time.Millisecond), "test/crash", n1.Crash)
		w.RunFor(2 * time.Millisecond)
		// Offset 16 is the first byte of the snapshot's data: the checksum no longer matches.
		if err := v.Corrupt(vsnap(8), 16, 1); err != nil {
			tt.Fatalf("Corrupt: %v", err)
		}
		w.Sim.At(w.Sim.Now().Add(time.Millisecond), "test/restart", n1.Restart)
		w.RunFor(5 * time.Millisecond)
		if got, want := r.recs[len(r.recs)-1], (persist.Recovery{SnapshotSeq: 6, Events: 2, LastSeq: 8}); got != want {
			tt.Errorf("recovery after the restart %+v, want %+v", got, want)
		}
		if got := r.ask(tt, getCmd); got != 8 {
			tt.Errorf("Get after the restart replied %d, want 8", got)
		}
	})
}

// AT-PER-39, PER-180, PER-400 (simulation part): a sync failure during a save is recorded as
// op=save, the counter keeps running and the snapshot directory holds no .tmp and no .snap file; a
// .tmp file put there while n1 is down is gone after the next successful save.
func TestATPER39FailedSave(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		r := newCounterRun(s)
		n1 := r.node()
		w.RunFor(time.Millisecond)
		r.ask(tt, addCmd(1))
		r.mustHaveRecovered(tt)
		v := vol(s)
		v.FailSyncs(1)
		if err := sim.Send(s, "n1", r.pid, CounterCmd(SnapNow{})); err != nil {
			tt.Fatalf("Send: %v", err)
		}
		w.RunFor(50 * time.Millisecond)

		if ops := failOps(s); !slices.Equal(ops, []string{"save"}) {
			tt.Fatalf("lockstep.persist.fail ops %v, want [save]", ops)
		}
		fail := ofKind(w.Sim.Records(), "lockstep.persist.fail")[0]
		if err := attr(fail, "err"); !strings.HasPrefix(err, "persist: c-1: save snapshot seq 1: filestore: save snapshot c-1 seq 1: ") ||
			!strings.Contains(err, simdisk.ErrIO.Error()) {
			tt.Errorf("save failure text %q, want the save error of seq 1 wrapping the I/O error", err)
		}
		if got := names(tt, v, "/snapshots/id-c-1"); len(got) != 0 {
			tt.Errorf("snapshot directory holds %v after the failed save, want nothing", got)
		}
		if len(r.errs) != 0 || len(snapSeqs(s)) != 0 {
			tt.Errorf("errs %v, snapshot seqs %v; want none", r.errs, snapSeqs(s))
		}

		now := w.Sim.Now()
		w.Sim.At(now.Add(time.Millisecond), "test/crash", n1.Crash)
		w.RunFor(2 * time.Millisecond)
		leftover := path.Dir(vsnap(1)) + "/" + fmt.Sprintf("%020d.tmp", 9)
		if err := v.WriteFileDurable(leftover, []byte("junk")); err != nil {
			tt.Fatalf("WriteFileDurable: %v", err)
		}
		w.Sim.At(w.Sim.Now().Add(time.Millisecond), "test/restart", n1.Restart)
		w.RunFor(5 * time.Millisecond)
		if err := sim.Send(s, "n1", r.pid, CounterCmd(SnapNow{})); err != nil {
			tt.Fatalf("Send: %v", err)
		}
		w.RunFor(50 * time.Millisecond)
		if got, want := names(tt, v, "/snapshots/id-c-1"), []string{path.Base(vsnap(1))}; !slices.Equal(got, want) {
			tt.Errorf("snapshot directory holds %v after the next save, want %v", got, want)
		}
		if got := snapSeqs(s); !slices.Equal(got, []string{"1"}) {
			tt.Errorf("snapshot seqs %v, want [1]", got)
		}
	})
}

// PER-190: the deletions of a cycle follow its save, in order: snapshots up to P-1, then events up
// to P, with P the previous snapshot. A failed deletion of snapshots is recorded and the deletion
// of events still runs; a failed deletion of events is recorded; the counter does not fail.
func TestPER190DeleteOrder(t *testing.T) {
	errX := errors.New("x")
	run := func(t *testing.T, failSnaps, failEvents bool, wantCalls, wantFails, wantDeleted []string) {
		sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
			w, tt := s.World(), s.World().T()
			r := newCounterRun(s)
			var log *storeLog
			r.config = func(cfg *persist.Config[Added, CounterState]) {
				log = &storeLog{Store: filestore.New(s.Volume("n1"))}
				if failSnaps {
					log.failSnapshots = errX
				}
				if failEvents {
					log.failEvents = errX
				}
				cfg.Journal, cfg.Snapshots = log, log
				cfg.SnapshotEvery, cfg.PruneEvents = 2, true
			}
			r.node()
			w.RunFor(time.Millisecond)
			for range 6 {
				r.ask(tt, addCmd(1))
			}
			w.RunFor(50 * time.Millisecond)

			var calls []string
			for _, c := range log.calls {
				if !strings.HasPrefix(c, "Write ") {
					calls = append(calls, c)
				}
			}
			if !slices.Equal(calls, wantCalls) {
				tt.Errorf("store calls %v, want %v", calls, wantCalls)
			}
			if got := failOps(s); !slices.Equal(got, wantFails) {
				tt.Errorf("fail ops %v, want %v", got, wantFails)
			}
			var deleted []string
			for _, rec := range ofKind(w.Sim.Records(), "lockstep.persist.delete") {
				deleted = append(deleted, attr(rec, "store")+" "+attr(rec, "to"))
			}
			if !slices.Equal(deleted, wantDeleted) {
				tt.Errorf("delete records %v, want %v", deleted, wantDeleted)
			}
			if len(r.errs) != 0 {
				tt.Errorf("errs %v, want none", r.errs)
			}
			if got := r.ask(tt, getCmd); got != 6 {
				tt.Errorf("Get replied %d, want 6", got)
			}
		})
	}
	calls := []string{"Save 2", "Save 4", "DeleteSnapshots 1", "DeleteEvents 2", "Save 6", "DeleteSnapshots 3", "DeleteEvents 4"}
	t.Run("ok", func(t *testing.T) {
		run(t, false, false, calls, nil, []string{"snapshots 1", "events 2", "snapshots 3", "events 4"})
	})
	t.Run("snapshots deletion fails", func(t *testing.T) {
		run(t, true, false, calls, []string{"delete-snapshots", "delete-snapshots"}, []string{"events 2", "events 4"})
	})
	t.Run("events deletion fails", func(t *testing.T) {
		run(t, false, true, calls, []string{"delete-events", "delete-events"}, []string{"snapshots 1", "snapshots 3"})
	})
}

// PER-190: a save that fails starts no deletion, even with an older snapshot to keep, and without
// PruneEvents a cycle deletes snapshots only.
func TestPER190NoDeletion(t *testing.T) {
	run := func(t *testing.T, prune bool, failSave uint64, wantCalls, wantFails []string) {
		sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
			w, tt := s.World(), s.World().T()
			r := newCounterRun(s)
			var log *storeLog
			r.config = func(cfg *persist.Config[Added, CounterState]) {
				log = &storeLog{Store: filestore.New(s.Volume("n1")), failSave: failSave}
				cfg.Journal, cfg.Snapshots = log, log
				cfg.SnapshotEvery, cfg.PruneEvents = 2, prune
			}
			r.node()
			w.RunFor(time.Millisecond)
			for range 6 {
				r.ask(tt, addCmd(1))
			}
			w.RunFor(50 * time.Millisecond)

			var calls []string
			for _, c := range log.calls {
				if !strings.HasPrefix(c, "Write ") {
					calls = append(calls, c)
				}
			}
			if !slices.Equal(calls, wantCalls) {
				tt.Errorf("store calls %v, want %v", calls, wantCalls)
			}
			if got := failOps(s); !slices.Equal(got, wantFails) {
				tt.Errorf("fail ops %v, want %v", got, wantFails)
			}
			if len(r.errs) != 0 {
				tt.Errorf("errs %v, want none", r.errs)
			}
		})
	}
	t.Run("failed save after a snapshot", func(t *testing.T) {
		// The failed save of seq 4 deletes nothing; seq 5 is due at once, and its save deletes.
		run(t, true, 4, []string{"Save 2", "Save 4", "Save 5", "DeleteSnapshots 1", "DeleteEvents 2"}, []string{"save"})
	})
	t.Run("PruneEvents unset", func(t *testing.T) {
		run(t, false, 0, []string{"Save 2", "Save 4", "DeleteSnapshots 1", "Save 6", "DeleteSnapshots 3"}, nil)
	})
}

// PER-170: after recovery SnapshotEvery is evaluated only at the next commit, while a Snapshot
// call in Recovered takes a snapshot at once.
func TestPER170SnapshotAfterRecovery(t *testing.T) {
	// run adds 1 three times with no snapshot due, restarts n1 with SnapshotEvery 2, waits, and
	// adds 1 once more; inRecovered makes the second Recovered call Snapshot.
	run := func(t *testing.T, inRecovered bool, wantAfterRecovery, wantEnd []string) {
		sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
			w, tt := s.World(), s.World().T()
			r := newCounterRun(s)
			r.config = func(cfg *persist.Config[Added, CounterState]) {
				if r.boots > 1 {
					cfg.SnapshotEvery = 2
				}
			}
			r.recovered = func(ctx *counterCtx) {
				if inRecovered && r.boots > 1 {
					ctx.Snapshot()
				}
			}
			n1 := r.node()
			w.RunFor(time.Millisecond)
			for range 3 {
				r.ask(tt, addCmd(1))
			}
			now := w.Sim.Now()
			w.Sim.At(now.Add(time.Millisecond), "test/crash", n1.Crash)
			w.Sim.At(now.Add(2*time.Millisecond), "test/restart", n1.Restart)
			w.RunFor(50 * time.Millisecond)
			if got, want := r.recs[len(r.recs)-1], (persist.Recovery{Events: 3, LastSeq: 3}); got != want {
				tt.Fatalf("recovery after the restart %+v, want %+v", got, want)
			}
			if got := snapSeqs(s); !slices.Equal(got, wantAfterRecovery) {
				tt.Errorf("snapshot seqs after the recovery %v, want %v", got, wantAfterRecovery)
			}
			r.ask(tt, addCmd(1))
			w.RunFor(50 * time.Millisecond)
			if got := snapSeqs(s); !slices.Equal(got, wantEnd) {
				tt.Errorf("snapshot seqs after the next Add{1} %v, want %v", got, wantEnd)
			}
		})
	}
	t.Run("SnapshotEvery", func(t *testing.T) { run(t, false, nil, []string{"4"}) })
	t.Run("Snapshot in Recovered", func(t *testing.T) { run(t, true, []string{"3"}, []string{"3"}) })
}

// PER-170: a Snapshot call while a snapshot cycle is in flight is evaluated again when the cycle
// ends, so its snapshot follows without another command.
func TestPER170RequestDuringCycle(t *testing.T) {
	opts := fullTrace(3)
	opts.Disk.Latency.Sync.Base = 50 * time.Millisecond
	sim.Simulate(t, opts, func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		r := newCounterRun(s)
		r.node()
		w.RunFor(time.Second)
		r.mustHaveRecovered(tt)
		r.ask(tt, addCmd(1))
		if err := sim.Send(s, "n1", r.pid, CounterCmd(SnapNow{})); err != nil {
			tt.Fatalf("Send: %v", err)
		}
		r.ask(tt, addCmd(1)) // one sync; the save of seq 1 takes more
		if got := snapSeqs(s); len(got) != 0 || len(effectStarts(w.Sim.Records(), "lockstep.persist.save")) != 1 {
			tt.Fatalf("snapshot seqs %v before the second SnapNow, want the save of seq 1 in flight", got)
		}
		if err := sim.Send(s, "n1", r.pid, CounterCmd(SnapNow{})); err != nil {
			tt.Fatalf("Send: %v", err)
		}
		w.RunFor(2 * time.Second)
		if got := snapSeqs(s); !slices.Equal(got, []string{"1", "2"}) {
			tt.Errorf("snapshot seqs %v, want [1 2]", got)
		}
		r.checkOneCycleAtATime(tt)
	})
}

// PER-160, PER-170: a snapshot save in flight when the incarnation fails is cancelled: its snapshot
// record never appears, and the next incarnation takes snapshots as before.
func TestPersistSnapshotCancelledAtRestart(t *testing.T) {
	opts := fullTrace(3)
	opts.Disk.Latency.Sync.Base = 10 * time.Millisecond
	sim.Simulate(t, opts, func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		r := newCounterRun(s)
		r.config = func(cfg *persist.Config[Added, CounterState]) { cfg.SnapshotEvery = 1 }
		r.node()
		w.RunFor(500 * time.Millisecond)
		r.mustHaveRecovered(tt)
		old := r.pid
		r.ask(tt, addCmd(1))
		// The reply comes with the commit; the save that follows takes several 10 ms syncs.
		if err := s.InjectFail(old.Address(), errors.New("injected")); err != nil {
			tt.Fatalf("InjectFail: %v", err)
		}
		w.RunFor(time.Second)
		if len(r.errs) != 1 {
			tt.Fatalf("errs %v, want the injected failure", r.errs)
		}
		if got := snapSeqs(s); len(got) != 0 {
			tt.Errorf("snapshot seqs %v after the failure, want none: the save was cancelled", got)
		}
		if got := r.ask(tt, addCmd(1)); got != 2 {
			tt.Errorf("Add{1} after the restart replied %d, want 2", got)
		}
		w.RunFor(time.Second)
		// The cancelled save ran before the failure, so its snapshot is on disk and the recovery
		// starts from it; only its completion was dropped. The next incarnation's snapshot is of
		// seq 2 once the commit is done, and nothing else.
		if want := []persist.Recovery{{}, {SnapshotSeq: 1, LastSeq: 1}}; !slices.Equal(r.recs, want) {
			tt.Errorf("recs %+v, want %+v: the second recovery starts from the snapshot of seq 1", r.recs, want)
		}
		if got := snapSeqs(s); !slices.Equal(got, []string{"2"}) {
			tt.Errorf("snapshot seqs %v, want [2]", got)
		}
	})
}

// checkOneCycleAtATime checks over the whole trace that no save of the counter started while an
// effect of a snapshot cycle (a save or a deletion) was in flight, and that no cycle effect is left
// in flight at the end. A cancelled effect ends with its lockstep.effect.done record.
func (r *counterRun) checkOneCycleAtATime(tt testing.TB) {
	tt.Helper()
	inFlight := map[string]bool{}
	for _, rec := range r.s.World().Sim.Records() {
		if attr(rec, "actor") != r.pid.String() || !slices.Contains(snapshotEffects, attr(rec, "name")) {
			continue
		}
		switch rec.Kind {
		case "lockstep.effect.start":
			if attr(rec, "name") == "lockstep.persist.save" && len(inFlight) > 0 {
				tt.Errorf("a save started while %v are in flight", inFlight)
			}
			inFlight[attr(rec, "name")] = true
		case "lockstep.effect.done":
			delete(inFlight, attr(rec, "name"))
		}
	}
	if len(inFlight) != 0 {
		tt.Errorf("snapshot effects still in flight at the end: %v", inFlight)
	}
}

// PER-145, PER-160, PER-170: a snapshot save in flight when a failure breaks the counter, which
// its parent resumes, does not count. A completion that reaches the broken counter only starts
// the recovery; a command that reaches it first cancels the save. Either way the save's snapshot
// record never appears, no deletion of the cycle starts, no second save runs beside it, and the
// next save, after the recovery, is of the recovered counter. The save itself ran before the
// failure, so its snapshot is on disk and the recovery uses it.
func TestPersistSnapshotInFlightAtBreak(t *testing.T) {
	opts := fullTrace(3)
	opts.Disk.Latency.Sync.Base = 20 * time.Millisecond
	// run adds 1 (its save starts), then 99, whose Apply panics once 1 ms later, so the counter
	// breaks while the save runs; afterwards, if get is set, it asks Get while the save runs.
	run := func(t *testing.T, get bool, wantOutcome string) {
		sim.Simulate(t, opts, func(s *sim.Sim) {
			w, tt := s.World(), s.World().T()
			r := newCounterRun(s)
			r.config = func(cfg *persist.Config[Added, CounterState]) { cfg.SnapshotEvery = 1 }
			r.directive = func(error) lockstep.Directive { return lockstep.DirectiveResume }
			panicked := false
			r.apply = func(e Added) {
				if e.N == 99 && !panicked {
					panicked = true
					panic("boom")
				}
			}
			r.node()
			w.RunFor(500 * time.Millisecond)
			r.mustHaveRecovered(tt)
			r.ask(tt, addCmd(1))
			if err := sim.Send(s, "n1", r.pid, CounterCmd(Add{N: 99})); err != nil {
				tt.Fatalf("Send: %v", err)
			}
			w.RunFor(30 * time.Millisecond)
			if len(r.errs) != 1 || !isPanic(r.errs[0], "boom") {
				tt.Fatalf("errs %v, want the panic of Apply", r.errs)
			}
			recs := w.Sim.Records()
			saveAt := indexOf(recs, func(x kernel.Record) bool {
				return x.Kind == "lockstep.effect.start" && attr(x, "name") == "lockstep.persist.save"
			})
			if saveAt < 0 || len(r.ofActor(recs[saveAt:], "lockstep.fail")) == 0 || len(slices.DeleteFunc(
				ofKind(recs[saveAt:], "lockstep.effect.done"), func(x kernel.Record) bool { return attr(x, "name") != "lockstep.persist.save" })) != 0 {
				tt.Fatal("the save is not in flight when the counter fails")
			}
			var reply *lockstep.Reply[int64]
			if get {
				reply = lockstep.Ask(s.System("n1"), r.pid, getCmd)
			}
			w.RunFor(2 * time.Second)

			recs = w.Sim.Records()
			var saves []kernel.Record
			for _, rec := range ofKind(recs, "lockstep.effect.done") {
				if attr(rec, "name") == "lockstep.persist.save" && attr(rec, "actor") == r.pid.String() {
					saves = append(saves, rec)
				}
			}
			if len(saves) != 1 || attr(saves[0], "outcome") != wantOutcome {
				tt.Errorf("outcomes of the saves %v, want one %q", saves, wantOutcome)
			}
			if got := snapSeqs(s); len(got) != 0 {
				tt.Errorf("snapshot seqs %v, want none: the save of the broken counter does not count", got)
			}
			if got := effectStarts(recs, "lockstep.persist.delete-snapshots"); len(got) != 0 {
				tt.Errorf("%d deletions of snapshots started, want none", len(got))
			}
			if len(r.recs) != 2 || r.recs[1] != (persist.Recovery{SnapshotSeq: 1, Events: 1, LastSeq: 2}) {
				tt.Fatalf("recs %+v, want a second recovery from the snapshot of seq 1 with one event", r.recs)
			}
			if get && (!hooks.AskResolved(reply) || !slices.Contains(r.log, "get 100")) {
				tt.Errorf("Get was not answered with 100 after the recovery: %v", r.log)
			}
			if got := r.ask(tt, addCmd(1)); got != 101 {
				tt.Errorf("Add{1} after the recovery replied %d, want 101", got)
			}
			w.RunFor(time.Second)
			if got := snapSeqs(s); !slices.Equal(got, []string{"3"}) {
				tt.Errorf("snapshot seqs %v, want [3]: the next save runs after the recovery", got)
			}
			r.checkOneCycleAtATime(tt)
		})
	}
	t.Run("a completion reaches the broken counter", func(t *testing.T) { run(t, false, "ok") })
	t.Run("a command reaches the broken counter", func(t *testing.T) { run(t, true, "cancelled") })
}
