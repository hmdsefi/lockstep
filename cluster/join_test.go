// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package cluster

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/sim"
)

// newSystem returns a production System named n1 that logs nothing and shuts down when t ends.
func newSystem(t *testing.T, opts ...lockstep.Option) *lockstep.System {
	t.Helper()
	sys := lockstep.New(append([]lockstep.Option{lockstep.WithNodeName("n1"), quiet}, opts...)...)
	t.Cleanup(func() { shutdown(t, sys) })
	return sys
}

// quiet makes a System log nothing.
var quiet = lockstep.WithLogger(slog.New(slog.DiscardHandler))

// shutdown shuts sys down within 5s.
func shutdown(t *testing.T, sys *lockstep.System) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := sys.Shutdown(ctx); err != nil {
		t.Errorf("Shutdown: %v", err)
	}
}

// expectPanic fails t unless fn panics with want.
func expectPanic(t testing.TB, want string, fn func()) {
	t.Helper()
	defer func() {
		t.Helper()
		if got := recover(); got != want {
			t.Errorf("panic %v, want %q", got, want)
		}
	}()
	fn()
}

// AT-CLU-01, CLU-010, CLU-020, CLU-030: Join panics on a nil System; it validates a copy of the
// Config, so an invalid one returns the validation error and marks nothing; it fills in the
// defaults without changing the caller's Config; a spawn error is wrapped and marks nothing. The
// actor runs at /system/cluster, records its start, and has joined at start when the seeds name
// no other node. View 1 is published before Join returns and holds only self, Alive with
// incarnation 0.
func TestATCLU01Join(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		expectPanic(t, "cluster: Join with nil System", func() { _, _ = Join(nil, Config{}) })
		if v := new(Membership).View(); v != nil {
			t.Errorf("the zero Membership's View = %v, want nil", v)
		}
	})

	t.Run("invalid", func(t *testing.T) {
		sys := newSystem(t)
		for _, c := range []struct {
			cfg  Config
			want string
		}{
			{Config{Seeds: []string{"n1", "bad name"}}, `cluster: invalid config: Seeds: seed "bad name" is not a valid node name`},
			{Config{Seeds: []string{"n2", "n1", "n2"}}, `cluster: invalid config: Seeds: duplicate seed "n2"`},
			{Config{ProbeInterval: -time.Second}, "cluster: invalid config: ProbeInterval: -1s is not positive"},
			{Config{ProbeTimeout: 2 * time.Second}, "cluster: invalid config: ProbeTimeout: 2s is not less than ProbeInterval 1s"},
			{Config{MaxPiggyback: -1}, "cluster: invalid config: MaxPiggyback: -1 is less than 1"},
		} {
			m, err := Join(sys, c.cfg)
			if m != nil || !errors.Is(err, ErrInvalidConfig) || err.Error() != c.want {
				t.Errorf("Join(%+v) = %v, %v, want nil, %q", c.cfg, m, err, c.want)
			}
		}
		if _, err := Join(sys, Config{}); err != nil {
			t.Fatalf("Join after invalid configs: %v", err)
		}
		// Validation comes before the joined check.
		if _, err := Join(sys, Config{IndirectProbes: -1}); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("an invalid Join of a joined system returned %v, want ErrInvalidConfig", err)
		}
	})

	t.Run("defaults", func(t *testing.T) {
		sys := newSystem(t)
		seeds := []string{"n2", "n1"}
		cfg := Config{Seeds: seeds, SyncInterval: time.Minute}
		m, err := Join(sys, cfg)
		if err != nil {
			t.Fatal(err)
		}
		seeds[0] = "changed"
		want := DefaultConfig()
		want.Seeds, want.SyncInterval = []string{"n2", "n1"}, time.Minute
		if !reflect.DeepEqual(m.cfg, want) {
			t.Errorf("Join's config %+v, want %+v (the seeds copied)", m.cfg, want)
		}
		if cfg.ProbeInterval != 0 || cfg.MaxPiggyback != 0 {
			t.Errorf("Join changed the caller's Config: %+v", cfg)
		}
	})

	t.Run("spawn error", func(t *testing.T) {
		sys := lockstep.New(lockstep.WithNodeName("n1"), quiet)
		shutdown(t, sys)
		// The second Join fails the same way, not with ErrAlreadyJoined: the first left no mark.
		for range 2 {
			m, err := Join(sys, Config{})
			if m != nil || !errors.Is(err, lockstep.ErrStopped) || !strings.HasPrefix(err.Error(), "cluster: join: ") {
				t.Errorf("Join of a stopped system = %v, %v, want nil and a cluster: join: error wrapping ErrStopped", m, err)
			}
		}
	})

	t.Run("spawn", func(t *testing.T) {
		sim.Simulate(t, simOpts(2, 2*time.Second), func(s *sim.Sim) {
			w, tt := s.World(), s.World().T()
			c := newSimCluster(s)
			var first *View
			c.n["n1"] = s.Node("n1", func(sys *lockstep.System) {
				m, err := Join(sys, Config{Seeds: []string{"n1"}})
				if err != nil {
					panic(err)
				}
				first = m.View() // before the actor started
				c.ms["n1"] = m
			})
			w.RunFor(time.Second)

			sys := s.System("n1")
			pid, err := lockstep.Lookup[memberMsg](sys, "/system/cluster")
			if err != nil || pid.Address().Path() != "/system/cluster" {
				tt.Errorf("Lookup of /system/cluster: %v, %v", pid, err)
			}
			self := Member{Node: "n1", ID: sys.Incarnation(), Incarnation: 0, State: Alive}
			for _, v := range []*View{first, c.view("n1")} {
				if v.Version != 1 || !reflect.DeepEqual(v.Self, self) || !reflect.DeepEqual(v.Members, []Member{self}) || !reflect.DeepEqual(v.Up(), []Member{self}) {
					tt.Errorf("view %+v, want version 1 with only %v", v, self)
				}
			}
			if c.view("n1") != first {
				tt.Error("a node alone published a second view")
			}
			start, joinedRecs := c.recs("n1", "start"), c.recs("n1", "joined")
			if len(start) != 1 || attrs(start[0]) != "node=n1 id="+uitoa(sys.Incarnation())+" inc=0 seeds=1 restart=false" {
				tt.Errorf("start records: %s", describe(start))
			}
			if len(joinedRecs) != 1 || attrs(joinedRecs[0]) != "members=1" || joinedRecs[0].At != start[0].At {
				tt.Errorf("joined records: %s, want members=1 at the start", describe(joinedRecs))
			}
		})
	})
}

// AT-CLU-02, CLU-020: a second Join of a System returns ErrAlreadyJoined, also after the membership
// actor stopped and after Leave; of concurrent Joins of one System exactly one succeeds.
func TestATCLU02AlreadyJoined(t *testing.T) {
	t.Run("again", func(t *testing.T) {
		sys := newSystem(t)
		m, err := Join(sys, Config{})
		if err != nil {
			t.Fatal(err)
		}
		if m2, err := Join(sys, Config{}); m2 != nil || !errors.Is(err, ErrAlreadyJoined) {
			t.Errorf("second Join = %v, %v, want nil, ErrAlreadyJoined", m2, err)
		}
		if m.View().Version != 1 {
			t.Errorf("a refused Join published view %d", m.View().Version)
		}
	})

	t.Run("after stop", func(t *testing.T) {
		sim.Simulate(t, simOpts(2, 2*time.Second), func(s *sim.Sim) {
			w, tt := s.World(), s.World().T()
			s.Node("n1", func(sys *lockstep.System) {
				if _, err := Join(sys, Config{}); err != nil {
					panic(err)
				}
			})
			w.RunFor(100 * time.Millisecond)
			sys := s.System("n1")
			if err := s.StopActor(lockstep.PIDAt[memberMsg]("n1", actorPath).Address()); err != nil {
				tt.Fatal(err)
			}
			w.RunFor(100 * time.Millisecond)
			if _, err := lockstep.Lookup[memberMsg](sys, actorPath); !errors.Is(err, lockstep.ErrNotFound) {
				tt.Fatalf("Lookup of the stopped actor: %v, want ErrNotFound", err)
			}
			if m, err := Join(sys, Config{}); m != nil || !errors.Is(err, ErrAlreadyJoined) {
				tt.Errorf("Join after the actor stopped = %v, %v, want nil, ErrAlreadyJoined", m, err)
			}
		})
	})

	// After Leave the actor stops, and the System cannot join again.
	t.Run("after leave", func(t *testing.T) {
		sys := newSystem(t)
		m, err := Join(sys, Config{})
		if err != nil {
			t.Fatal(err)
		}
		if err := m.Leave(); err != nil {
			t.Fatalf("Leave: %v", err)
		}
		for i := 0; i < 1000 && m.View().Self.State != Left; i++ {
			time.Sleep(time.Millisecond)
		}
		if self := m.View().Self; self.State != Left {
			t.Fatalf("own entry %v after Leave, want Left", self)
		}
		if err := m.Leave(); !errors.Is(err, ErrLeft) {
			t.Errorf("second Leave = %v, want ErrLeft", err)
		}
		if m2, err := Join(sys, Config{}); m2 != nil || !errors.Is(err, ErrAlreadyJoined) {
			t.Errorf("Join after Leave = %v, %v, want nil, ErrAlreadyJoined", m2, err)
		}
	})

	t.Run("concurrent", func(t *testing.T) {
		sys := newSystem(t)
		var wg sync.WaitGroup
		var mu sync.Mutex
		var ok, already int
		for range 16 {
			wg.Go(func() {
				_, err := Join(sys, Config{})
				mu.Lock()
				defer mu.Unlock()
				switch {
				case err == nil:
					ok++
				case errors.Is(err, ErrAlreadyJoined):
					already++
				default:
					t.Errorf("concurrent Join: %v", err)
				}
			})
		}
		wg.Wait()
		if ok != 1 || already != 15 {
			t.Errorf("16 concurrent Joins: %d succeeded and %d returned ErrAlreadyJoined, want 1 and 15", ok, already)
		}
	})
}

// countingMetrics counts the series registered with it.
type countingMetrics struct {
	mu sync.Mutex
	n  int
}

type nopCounter struct{}

func (nopCounter) Add(uint64) {}

func (m *countingMetrics) Counter(string, string, ...lockstep.MetricLabel) lockstep.Counter {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.n++
	return nopCounter{}
}

func (m *countingMetrics) GaugeFunc(string, string, func() int64, ...lockstep.MetricLabel) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.n++
}

func (m *countingMetrics) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.n
}

// AT-CLU-29, CLU-050, CLU-500, CLU-710. "views": every turn that changes the table publishes one
// new view with the next version and records it once; a turn without a change publishes nothing;
// a published view never changes. Invalid updates are dropped alone, and an invalid From drops
// its whole message, each with an invalid record. Reaping is a change. "metrics": Join and the
// actor register no metric.
func TestATCLU29Views(t *testing.T) {
	t.Run("views", func(t *testing.T) {
		sim.Simulate(t, simOpts(2, 4*time.Second), func(s *sim.Sim) {
			w, tt := s.World(), s.World().T()
			c := newSimCluster(s)
			c.node("n1", Config{DeadRetention: time.Second})
			w.RunFor(10 * time.Millisecond)
			self := c.view("n1").Self
			to := lockstep.PIDAt[memberMsg]("n1", actorPath)

			type snapshot struct {
				v       *View
				version uint64
				self    Member
				members []Member
			}
			var views []snapshot
			seen := 0 // records of n1 checked so far
			// step sends msg, runs 10ms and checks that n1 recorded exactly want (kind and
			// attributes) in that time, and whether it published a new view.
			step := func(name string, msg memberMsg, want ...string) {
				tt.Helper()
				if msg != nil {
					if err := sim.Send(s, "n1", to, msg); err != nil {
						tt.Fatal(err)
					}
				}
				w.RunFor(10 * time.Millisecond)
				var got []string
				var recs []kernel.Record
				for _, r := range w.Sim.Records() {
					if r.Node == c.n["n1"].ID() && strings.HasPrefix(r.Kind, recordPrefix) {
						recs = append(recs, r)
					}
				}
				for _, r := range recs[seen:] {
					got = append(got, strings.TrimPrefix(r.Kind, recordPrefix)+" "+attrs(r))
				}
				seen = len(recs)
				if !slices.Equal(got, want) {
					tt.Errorf("%s: n1 recorded %q, want %q", name, got, want)
				}
				v := c.view("n1")
				if n := len(views); n == 0 || views[n-1].v != v {
					views = append(views, snapshot{v, v.Version, v.Self, slices.Clone(v.Members)})
				}
			}

			step("start", nil, "start node=n1 id="+uitoa(self.ID)+" inc=0 seeds=0 restart=false", "joined members=1")
			a, b := update{Node: "a", ID: 11, State: Alive}, update{Node: "b", ID: 12, State: Alive}
			step("syncreq", syncReq{From: a, Members: []update{b, {ID: 1, State: Alive}, {Node: "c", ID: 13, State: 9}, toUpdate(self)}},
				"member node=a id=11 inc=0 from=none to=alive source=syncreq",
				"member node=b id=12 inc=0 from=none to=alive source=syncreq",
				"invalid peer=a type=lockstep.cluster.syncreq reason=empty-node",
				"invalid peer=a type=lockstep.cluster.syncreq reason=bad-state",
				"view version=2 members=3 up=3",
				"node.up node=a id=11",
				"node.up node=b id=12")
			step("no change", syncReq{From: a, Members: []update{b}})
			step("bare update", update{Node: "d", ID: 14, State: Alive})
			step("invalid from", syncResp{From: update{ID: 15, State: Alive}, Members: []update{{Node: "d", ID: 14, State: Alive}}},
				"invalid peer= type=lockstep.cluster.syncresp reason=empty-node")
			step("bad from state", ack{From: update{Node: "x", ID: 16}, Updates: []update{{Node: "d", ID: 14, State: Alive}}},
				"invalid peer=x type=lockstep.cluster.ack reason=bad-state")
			a.Inc = 1
			step("ping", ping{From: a, Updates: []update{{Node: "b", ID: 12, State: Dead}}},
				"member node=a id=11 inc=1 from=alive to=alive source=ping",
				"member node=b id=12 inc=0 from=alive to=dead source=ping",
				"view version=3 members=3 up=2",
				"node.down node=b id=12 left=false")
			a.State = Left
			step("leave", leave{From: a},
				"member node=a id=11 inc=1 from=alive to=left source=leave",
				"view version=4 members=3 up=1",
				"node.down node=a id=11 left=true")
			// b went Dead at 70ms and a Left at 80ms; their reap timers fire DeadRetention later,
			// in the next two steps. Nothing is recorded meanwhile.
			w.RunFor(975 * time.Millisecond)
			step("reap b", nil, "reap node=b id=12 inc=0", "view version=5 members=2 up=1")
			step("reap a", nil, "reap node=a id=11 inc=1", "view version=6 members=1 up=1")

			for i, sn := range views {
				if sn.version != uint64(i+1) {
					tt.Errorf("view %d has version %d, want %d", i, sn.version, i+1)
				}
				if sn.v.Version != sn.version || !reflect.DeepEqual(sn.v.Self, sn.self) || !reflect.DeepEqual(sn.v.Members, sn.members) {
					tt.Errorf("view %d changed after it was published: %+v, was %v %v", sn.version, sn.v, sn.self, sn.members)
				}
			}
			if last := views[len(views)-1].v; !reflect.DeepEqual(last.Members, []Member{self}) {
				tt.Errorf("the last view holds %v, want only %v", last.Members, self)
			}
			v := views[1].v
			up := v.Up()
			up[0] = Member{}
			if v.Members[0].Node != "a" {
				tt.Error("changing the slice Up returned changed the view")
			}
		})
	})

	t.Run("metrics", func(t *testing.T) {
		rec := &countingMetrics{}
		sys := newSystem(t, lockstep.WithMetrics(rec))
		before := rec.count()
		m, err := Join(sys, Config{})
		if err != nil {
			t.Fatal(err)
		}
		to := lockstep.PIDAt[memberMsg]("n1", actorPath)
		if err := lockstep.Send(sys, to, memberMsg(syncResp{From: update{Node: "n2", ID: 2, State: Alive}})); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 1000 && m.View().Version < 2; i++ {
			time.Sleep(time.Millisecond)
		}
		if v := m.View(); v.Version != 2 {
			t.Fatalf("view %+v, want version 2 with n2", v)
		}
		if after := rec.count(); after != before {
			t.Errorf("%d series registered after New, want none", after-before)
		}
	})
}
