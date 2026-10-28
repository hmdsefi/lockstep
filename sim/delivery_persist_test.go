// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim_test

import (
	"slices"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/dedup"
	"github.com/hmdsefi/lockstep/persist"
	"github.com/hmdsefi/lockstep/persist/filestore"
	"github.com/hmdsefi/lockstep/sim"
)

// AT-DLV-29 under simulation: a persistent ledger keeps its reply cache in its event-sourced
// state, so a retry that arrives after its server crashed and restarted gets the first reply. As
// in delivery_test.go, the body creates what the fixtures observe and checks it before the run
// ends, so the second run of a seed under LOCKSTEP_CHECK_DETERMINISM starts fresh.

// DlvDeposited is the ledger's event: the deposit of Amount with request key Key, handled at At.
type DlvDeposited struct {
	Key    string
	Amount int64
	At     time.Time
}

// DlvLedger is the ledger's state: the sum of the deposits applied, and the reply to each deposit
// key, nil for a ledger without a cache. Replies is a pointer field, so persist.JSON encodes the
// cache with the state.
type DlvLedger struct {
	Sum     int64
	Replies *dedup.Replies[string, int64]
}

// dlvLedgerCtx is the persist context of the ledger.
type dlvLedgerCtx = persist.Context[DlvDeposit, DlvDeposited, DlvLedger]

// dlvRecovered is what one Recovered call of the ledger saw: the Recovery, the state's sum and the
// encoding of its cache ("" without a cache).
type dlvRecovered struct {
	rec persist.Recovery
	sum int64
	enc string
}

// dlvLedgerRun is one run of AT-DLV-29: the ledger's cache bounds (nil: no cache), the results of
// the two asks, and what the ledger observed.
type dlvLedgerRun struct {
	cache      *dedup.Config
	v1, v2     int64
	err1, err2 error

	firstThen string         // the cache's encoding in the first Then callback
	boots     []dlvRecovered // the Recovered call of each node boot, in order
	hits      []uint64       // ctx.LastSeq() at each deposit answered from the cache
	errs      []error        // the errors of MarshalJSON
}

// encode returns the encoding of c, and records the error if c does not encode.
func (r *dlvLedgerRun) encode(c *dedup.Replies[string, int64]) string {
	b, err := c.MarshalJSON()
	if err != nil {
		r.errs = append(r.errs, err)
	}
	return string(b)
}

// newLedger is the ledger's factory.
func (r *dlvLedgerRun) newLedger() persist.EventSourced[DlvDeposit, DlvDeposited, DlvLedger] {
	return &dlvLedgerActor{r: r}
}

// dlvLedgerActor is the ledger of AT-DLV-29: an EventSourced[DlvDeposit, DlvDeposited, DlvLedger]
// that also implements Recoverer and reports to r.
type dlvLedgerActor struct{ r *dlvLedgerRun }

// Initial returns an empty ledger, with a new cache for each incarnation.
func (l *dlvLedgerActor) Initial() DlvLedger {
	if l.r.cache == nil {
		return DlvLedger{}
	}
	return DlvLedger{Replies: dedup.NewReplies[string, int64](*l.r.cache)}
}

// Handle answers a deposit whose key has a cached reply with that reply and persists nothing.
// Otherwise it persists the deposit and, once the deposit is applied, replies with the reply Apply
// cached, or with the sum in a ledger without a cache.
func (l *dlvLedgerActor) Handle(ctx *dlvLedgerCtx, st DlvLedger, m DlvDeposit) []DlvDeposited {
	if st.Replies != nil {
		if v, ok := st.Replies.Get(ctx.Now(), m.Key); ok {
			l.r.hits = append(l.r.hits, ctx.LastSeq())
			m.ReplyTo.Send(ctx, v)
			return nil
		}
	}
	ctx.Then(func(ctx *dlvLedgerCtx, st DlvLedger) {
		if st.Replies == nil {
			m.ReplyTo.Send(ctx, st.Sum)
			return
		}
		if l.r.firstThen == "" {
			l.r.firstThen = l.r.encode(st.Replies)
		}
		v, _ := st.Replies.Get(ctx.Now(), m.Key)
		m.ReplyTo.Send(ctx, v)
	})
	return []DlvDeposited{{Key: m.Key, Amount: m.Amount, At: ctx.Now()}}
}

// Apply adds the deposit to the sum and caches the new sum as the deposit's reply, at the time the
// event carries. Handle only reads the cache, so recovery rebuilds it exactly (DLV-230).
func (l *dlvLedgerActor) Apply(st DlvLedger, e DlvDeposited) DlvLedger {
	st.Sum += e.Amount
	if st.Replies != nil {
		st.Replies.Put(e.At, e.Key, st.Sum)
	}
	return st
}

// Recovered records what the recovery rebuilt.
func (l *dlvLedgerActor) Recovered(_ *dlvLedgerCtx, st DlvLedger, rec persist.Recovery) {
	b := dlvRecovered{rec: rec, sum: st.Sum}
	if st.Replies != nil {
		b.enc = l.r.encode(st.Replies)
	}
	l.r.boots = append(l.r.boots, b)
}

// runDlvLedger runs the world of AT-DLV-29 in s: server srv spawns the ledger /user/ledger with
// persistence ID ledger-1 on a filestore of its volume, with a reply cache bounded by cache (nil:
// none), and with a snapshot after every event if snapshots is set. From client cli it asks to
// deposit 10 with key k1, crashes and restarts srv, and asks the same again.
func runDlvLedger(s *sim.Sim, cache *dedup.Config, snapshots bool) *dlvLedgerRun {
	w := s.World()
	r := &dlvLedgerRun{cache: cache}
	srv := s.Node("srv", func(sys *lockstep.System) {
		store := filestore.New(s.Volume("srv"))
		cfg := persist.Config[DlvDeposited, DlvLedger]{Journal: store, Events: persist.JSON[DlvDeposited]()}
		if snapshots {
			cfg.Snapshots, cfg.State, cfg.SnapshotEvery = store, persist.JSON[DlvLedger](), 1
		}
		if _, err := persist.Spawn(sys, "ledger", "ledger-1", r.newLedger, cfg); err != nil {
			panic(err)
		}
	})
	s.Client("cli", noSetup)
	w.RunFor(10 * time.Millisecond)

	to, deposit := lockstep.PIDAt[DlvDeposit]("srv", "/user/ledger"), dlvDeposit("k1", 10)
	r.v1, r.err1 = sim.Ask(s, "cli", to, deposit)
	w.RunFor(100 * time.Millisecond) // lets a snapshot save finish
	now := w.Sim.Now()
	w.Sim.At(now.Add(time.Millisecond), "test/crash", srv.Crash)
	w.Sim.At(now.Add(2*time.Millisecond), "test/restart", srv.Restart)
	w.RunFor(10 * time.Millisecond)
	r.v2, r.err2 = sim.Ask(s, "cli", to, deposit)
	return r
}

// checkDlvAsk fails the seed unless an ask returned want and no error.
func checkDlvAsk(tt testing.TB, what string, v int64, err error, want int64) {
	tt.Helper()
	if v != want || err != nil {
		tt.Errorf("%s: %d, %v; want %d, nil", what, v, err, want)
	}
}

// AT-DLV-29, DLV-230, DLV-430: a ledger that writes its reply cache only in Apply, with the time
// carried by the event, rebuilds the cache after a crash, from its events alone or from a
// snapshot, to the bytes it had before. The retry of a deposit after the restart then gets the
// first reply, 10. Without a cache, the second boot recovers the sum 10 and the retry is applied
// again and gets 20.
func TestATDLV29CachedReplySurvivesCrash(t *testing.T) {
	cache := dedup.Config{Capacity: 1024, TTL: time.Hour}
	empty, err := dedup.NewReplies[string, int64](cache).MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON of an empty cache: %v", err)
	}
	for _, tc := range []struct {
		name      string
		snapshots bool
		rec       persist.Recovery // of the second boot
	}{
		{name: "events", rec: persist.Recovery{Events: 1, LastSeq: 1}},
		{name: "snapshot", snapshots: true, rec: persist.Recovery{SnapshotSeq: 1, LastSeq: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sim.SimulateSeeds(t, dlvOptions(0), func(s *sim.Sim) {
				tt := s.World().T()
				r := runDlvLedger(s, &cache, tc.snapshots)
				checkDlvAsk(tt, "first ask", r.v1, r.err1, 10)
				checkDlvAsk(tt, "retry after the restart", r.v2, r.err2, 10)
				if len(r.errs) != 0 {
					tt.Errorf("MarshalJSON failed: %v", r.errs)
				}
				if r.firstThen == "" {
					tt.Error("no Then callback encoded the cache")
				}
				// The first boot recovers nothing; the second rebuilds the cache of the first
				// Then callback.
				want := []dlvRecovered{{enc: string(empty)}, {rec: tc.rec, sum: 10, enc: r.firstThen}}
				if !slices.Equal(r.boots, want) {
					tt.Errorf("Recovered calls by boot:\n%+v\nwant:\n%+v", r.boots, want)
				}
				if !slices.Equal(r.hits, []uint64{1}) {
					tt.Errorf("LastSeq at the cache hits: %v, want [1]", r.hits)
				}
			}, 1)
		})
	}
	t.Run("no-cache", func(t *testing.T) {
		sim.SimulateSeeds(t, dlvOptions(0), func(s *sim.Sim) {
			tt := s.World().T()
			r := runDlvLedger(s, nil, false)
			checkDlvAsk(tt, "first ask", r.v1, r.err1, 10)
			checkDlvAsk(tt, "retry after the restart", r.v2, r.err2, 20)
			// 20 is the recovered 10 plus the retry applied again, not a run without a crash.
			if len(r.boots) != 2 || r.boots[1].sum != 10 {
				tt.Errorf("Recovered calls by boot: %+v; want 2, the second with sum 10", r.boots)
			}
		}, 1)
	})
}
