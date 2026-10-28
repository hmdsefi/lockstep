// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/faultline/kernel/simnet"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/dedup"
	"github.com/hmdsefi/lockstep/sim"
)

// The delivery protocols of DLV §10.2 under simulation: each fails without its dedup state (a
// scenario run in a child process) and passes 20 seeds with it. The fixtures keep what they
// observe in values the body creates, so every run, including the second run of a seed under
// LOCKSTEP_CHECK_DETERMINISM, starts fresh; a test that counts across seeds adds the counts of
// each run after it ends.

// dlvOptions returns the options of DLV §10.2: seeds seeds of 2 s each. A failing variant passes
// 0 and names its one seed to sim.SimulateSeeds.
func dlvOptions(seeds int) sim.Options {
	return sim.Options{Options: faultline.Options{Seeds: seeds, Duration: 2 * time.Second}}
}

// dlvForward returns an actor that forwards every message it receives to to. It appends each
// error of lockstep.Send to *errs, which the body checks at the end.
func dlvForward[M any](to lockstep.PID[M], errs *[]error) *actor[M] {
	return &actor[M]{receive: func(ctx *lockstep.Context[M], m M) {
		if err := lockstep.Send(ctx, to, m); err != nil {
			*errs = append(*errs, err)
		}
	}}
}

// checkForwarded fails the seed if a forwarding actor of the run could not send.
func checkForwarded(s *sim.Sim, errs []error) {
	if len(errs) != 0 {
		s.World().T().Errorf("forwarding failed: %v", errs)
	}
}

// DlvDeposit asks an account to add Amount and to answer with its sum. Key identifies the request:
// a retry carries the Key of the first attempt.
type DlvDeposit struct {
	Key     string
	Amount  int64
	ReplyTo lockstep.ReplyTo[int64]
}

// dlvDeposit returns the build function of an ask that deposits amount with key.
func dlvDeposit(key string, amount int64) func(lockstep.ReplyTo[int64]) DlvDeposit {
	return func(r lockstep.ReplyTo[int64]) DlvDeposit { return DlvDeposit{Key: key, Amount: amount, ReplyTo: r} }
}

// dlvAccount is the state of the account actor of AT-DLV-22, owned by the test: the sum, the
// number of times each key was applied, and the reply cache, nil for an account without one.
type dlvAccount struct {
	sum     int64
	applied map[string]int
	replies *dedup.Replies[string, int64]
}

// receive handles a deposit. With a cache, a key that has a reply gets that reply and nothing
// else (DLV-130); otherwise the deposit is applied and the new sum is the reply, cached first.
func (a *dlvAccount) receive(ctx *lockstep.Context[DlvDeposit], m DlvDeposit) {
	if a.replies != nil {
		if r, ok := a.replies.Get(ctx.Now(), m.Key); ok {
			m.ReplyTo.Send(ctx, r)
			return
		}
	}
	a.sum += m.Amount
	a.applied[m.Key]++
	if a.replies != nil {
		a.replies.Put(ctx.Now(), m.Key, a.sum)
	}
	m.ReplyTo.Send(ctx, a.sum)
}

// dlvRetry is what the body of AT-DLV-22 observed: the error of the first ask, the result of the
// second (asked only after the first timed out) and the account's sum at the end.
type dlvRetry struct {
	err1   error
	asked2 bool
	v2     int64
	err2   error
	sum    int64
}

// dlvRetryOptions returns dlvOptions with every link 60 to 70 ms one way, so that a round trip
// takes 120 to 140 ms and the first ask of AT-DLV-22, with its 100 ms timeout, always times out.
func dlvRetryOptions(seeds int) sim.Options {
	o := dlvOptions(seeds)
	o.Net.Default = simnet.Link{Latency: 60 * time.Millisecond, Jitter: 10 * time.Millisecond}
	return o
}

// runDlvRetry runs the world of AT-DLV-22 in s: the account /user/acct on server srv, with a reply
// cache bounded by cache (nil: no cache), and the invariant dlv/applied-once. From client cli it
// asks to deposit 10 with key k1 with a 100 ms timeout and, after a timeout, once more with a
// 200 ms timeout; then it runs to the end.
func runDlvRetry(s *sim.Sim, cache *dedup.Config) dlvRetry {
	w := s.World()
	acct := &dlvAccount{applied: map[string]int{}}
	if cache != nil {
		acct.replies = dedup.NewReplies[string, int64](*cache)
	}
	s.Node("srv", func(sys *lockstep.System) {
		spawn(sys, "acct", &actor[DlvDeposit]{receive: acct.receive})
	})
	s.Client("cli", noSetup)
	w.Invariant("dlv/applied-once", func() error {
		if n := acct.applied["k1"]; n > 1 {
			return fmt.Errorf("key k1 applied %d times", n)
		}
		return nil
	})
	w.RunFor(10 * time.Millisecond)

	to := lockstep.PIDAt[DlvDeposit]("srv", "/user/acct")
	var r dlvRetry
	_, r.err1 = sim.Ask(s, "cli", to, dlvDeposit("k1", 10), lockstep.WithTimeout(100*time.Millisecond))
	if errors.Is(r.err1, lockstep.ErrTimeout) {
		r.asked2 = true
		r.v2, r.err2 = sim.Ask(s, "cli", to, dlvDeposit("k1", 10), lockstep.WithTimeout(200*time.Millisecond))
	}
	runToEnd(s)
	r.sum = acct.sum
	return r
}

// dlvRetryScenario returns a scenario that runs the world of AT-DLV-22 in one seed, with a reply
// cache bounded by cache (nil: no cache).
func dlvRetryScenario(cache *dedup.Config) func(t *testing.T) {
	return func(t *testing.T) {
		sim.SimulateSeeds(t, dlvRetryOptions(0), func(s *sim.Sim) { runDlvRetry(s, cache) }, 1)
	}
}

// DlvOrder creates or cancels the order ID: Op is "create" or "cancel".
type DlvOrder struct {
	Op string
	ID string
}

// dlvOrders is the state of the orders actor of AT-DLV-25, owned by the test: the live order IDs,
// the tombstones (nil: none), and how many cancels added a tombstone or removed a live order.
type dlvOrders struct {
	live       map[string]bool
	tombs      *dedup.Window[string]
	tombstoned int
	removed    int
}

// receive handles an order message. A create adds the order unless a tombstone holds its ID
// (DLV-120). A cancel removes a live order; with tombstones, a cancel of an order that is not live
// adds a tombstone (DLV-110), and without them it is ignored.
func (o *dlvOrders) receive(ctx *lockstep.Context[DlvOrder], m DlvOrder) {
	switch m.Op {
	case "create":
		if o.tombs != nil && o.tombs.Contains(ctx.Now(), m.ID) {
			return
		}
		o.live[m.ID] = true
	case "cancel":
		switch {
		case o.live[m.ID]:
			delete(o.live, m.ID)
			o.removed++
		case o.tombs != nil:
			o.tombs.Add(ctx.Now(), m.ID)
			o.tombstoned++
		}
	}
}

// dlvOrdersRun is the world of AT-DLV-25: node n1 with /user/router, which forwards each order
// message to /user/orders, and the IDs the body cancelled, in send order.
type dlvOrdersRun struct {
	s         *sim.Sim
	router    lockstep.PID[DlvOrder]
	orders    lockstep.PID[DlvOrder]
	state     dlvOrders
	cancelled []string
	errs      []error
}

// newDlvOrdersRun builds the world of AT-DLV-25 in s, with tombstones bounded by tombs (nil:
// none) and the final check dlv/cancelled-orders. The caller adds the delay and boots it.
func newDlvOrdersRun(s *sim.Sim, tombs *dedup.Config) *dlvOrdersRun {
	r := &dlvOrdersRun{
		s:      s,
		router: lockstep.PIDAt[DlvOrder]("n1", "/user/router"),
		orders: lockstep.PIDAt[DlvOrder]("n1", "/user/orders"),
		state:  dlvOrders{live: map[string]bool{}},
	}
	if tombs != nil {
		r.state.tombs = dedup.NewWindow[string](*tombs)
	}
	s.Node("n1", func(sys *lockstep.System) {
		spawn(sys, "orders", &actor[DlvOrder]{receive: r.state.receive})
		spawn(sys, "router", dlvForward(r.orders, &r.errs))
	})
	s.World().Final("dlv/cancelled-orders", func() error {
		for _, id := range r.cancelled {
			if r.state.live[id] {
				return fmt.Errorf("order %s is live after its cancel", id)
			}
		}
		return nil
	})
	return r
}

// boot runs the first 10 ms, in which n1 boots.
func (r *dlvOrdersRun) boot() { r.s.World().RunFor(10 * time.Millisecond) }

// send sends m from n1 to to, and fails the seed if it cannot.
func (r *dlvOrdersRun) send(to lockstep.PID[DlvOrder], m DlvOrder) {
	if err := sim.Send(r.s, "n1", to, m); err != nil {
		r.s.World().T().Fatalf("send %+v to %s: %v", m, to, err)
	}
}

// create sends the create of id to the router.
func (r *dlvOrdersRun) create(id string) { r.send(r.router, DlvOrder{Op: "create", ID: id}) }

// cancel sends the cancel of id straight to the orders actor and records id as cancelled.
func (r *dlvOrdersRun) cancel(id string) {
	r.send(r.orders, DlvOrder{Op: "cancel", ID: id})
	r.cancelled = append(r.cancelled, id)
}

// end runs to the end of the run and checks that the router forwarded every message.
func (r *dlvOrdersRun) end() {
	runToEnd(r.s)
	checkForwarded(r.s, r.errs)
}

// DlvUpdate sets Entity's value to Value as of Version. The sender numbers each entity's updates
// from 1.
type DlvUpdate struct {
	Entity  string
	Version uint64
	Value   int64
}

// dlvProjection is the state of the projection actor of AT-DLV-27, owned by the test: the last
// update applied to each entity, the version gate (nil: none), and how many updates the gate
// dropped.
type dlvProjection struct {
	applied  map[string]DlvUpdate
	versions *dedup.Versions[string]
	rejected int
}

// receive applies an update, unless the gate holds a version of its entity at least as new
// (DLV-140).
func (p *dlvProjection) receive(ctx *lockstep.Context[DlvUpdate], m DlvUpdate) {
	if p.versions != nil && !p.versions.Accept(ctx.Now(), m.Entity, m.Version) {
		p.rejected++
		return
	}
	p.applied[m.Entity] = m
}

// dlvSent is an entity the body sent updates of, with the highest version sent.
type dlvSent struct {
	entity  string
	highest uint64
}

// dlvUpdatesRun is the world of AT-DLV-27: node n1 with the relays /user/ra and /user/rb, which
// forward each update to /user/proj, and the entities the body sent updates of, in first-send
// order.
type dlvUpdatesRun struct {
	s      *sim.Sim
	ra, rb lockstep.PID[DlvUpdate]
	proj   lockstep.PID[DlvUpdate]
	state  dlvProjection
	sent   []dlvSent
	errs   []error
}

// newDlvUpdatesRun builds the world of AT-DLV-27 in s, with a version gate bounded by gate (nil:
// none) and the final check dlv/latest-version. The caller adds the delays and boots it.
func newDlvUpdatesRun(s *sim.Sim, gate *dedup.Config) *dlvUpdatesRun {
	r := &dlvUpdatesRun{
		s:     s,
		ra:    lockstep.PIDAt[DlvUpdate]("n1", "/user/ra"),
		rb:    lockstep.PIDAt[DlvUpdate]("n1", "/user/rb"),
		proj:  lockstep.PIDAt[DlvUpdate]("n1", "/user/proj"),
		state: dlvProjection{applied: map[string]DlvUpdate{}},
	}
	if gate != nil {
		r.state.versions = dedup.NewVersions[string](*gate)
	}
	s.Node("n1", func(sys *lockstep.System) {
		spawn(sys, "proj", &actor[DlvUpdate]{receive: r.state.receive})
		spawn(sys, "ra", dlvForward(r.proj, &r.errs))
		spawn(sys, "rb", dlvForward(r.proj, &r.errs))
	})
	s.World().Final("dlv/latest-version", func() error {
		for _, e := range r.sent {
			if got := r.state.applied[e.entity].Version; got != e.highest {
				return fmt.Errorf("%s: version %d, want %d", e.entity, got, e.highest)
			}
		}
		return nil
	})
	return r
}

// boot runs the first 10 ms, in which n1 boots.
func (r *dlvUpdatesRun) boot() { r.s.World().RunFor(10 * time.Millisecond) }

// send sends version v of entity e with value from n1, to relay ra for an odd v and to rb for an
// even one, records it, and runs 1 ms. The callers pass 10*v as value (AT-DLV-27): computing it
// here would convert v to int64, which gosec reports.
func (r *dlvUpdatesRun) send(e string, v uint64, value int64) {
	relay := r.rb
	if v%2 == 1 {
		relay = r.ra
	}
	m := DlvUpdate{Entity: e, Version: v, Value: value}
	if err := sim.Send(r.s, "n1", relay, m); err != nil {
		r.s.World().T().Fatalf("send %+v to %s: %v", m, relay, err)
	}
	r.record(e, v)
	r.s.World().RunFor(time.Millisecond)
}

// record notes that version v of e was sent.
func (r *dlvUpdatesRun) record(e string, v uint64) {
	for i := range r.sent {
		if r.sent[i].entity == e {
			r.sent[i].highest = max(r.sent[i].highest, v)
			return
		}
	}
	r.sent = append(r.sent, dlvSent{entity: e, highest: v})
}

// end runs to the end of the run and checks that the relays forwarded every update.
func (r *dlvUpdatesRun) end() {
	runToEnd(r.s)
	checkForwarded(r.s, r.errs)
}

// The failing variants of AT-DLV-22, 24, 25 and 27: each runs one seed and fails it.
var (
	_ = scenario("dlv-retry-no-cache", dlvRetryScenario(nil))
	_ = scenario("dlv-retry-short-ttl", dlvRetryScenario(&dedup.Config{Capacity: 16, TTL: 50 * time.Millisecond}))
	_ = scenario("dlv-cancel-no-tombstone", func(t *testing.T) {
		sim.SimulateSeeds(t, dlvOptions(0), func(s *sim.Sim) {
			r := newDlvOrdersRun(s, nil)
			s.DelayBetween(r.router.Address(), r.orders.Address(), 10*time.Millisecond, 10*time.Millisecond)
			r.boot()
			r.create("o1")
			r.cancel("o1")
			r.end()
		}, 1)
	})
	_ = scenario("dlv-stale-no-gate", func(t *testing.T) {
		sim.SimulateSeeds(t, dlvOptions(0), func(s *sim.Sim) {
			r := newDlvUpdatesRun(s, nil)
			s.DelayBetween(r.ra.Address(), r.proj.Address(), 10*time.Millisecond, 10*time.Millisecond)
			r.boot()
			r.send("e1", 1, 10)
			r.send("e1", 2, 20)
			r.end()
		}, 1)
	})
)

// dlvFailure is the failure of check, of kind kind, with message msg.
func dlvFailure(kind, check, msg string) failure {
	return failure{Kind: kind, Check: check, Signature: kind + ":" + check, Message: msg}
}

// AT-DLV-22, DLV-130, DLV-400: without a reply cache, the retry of an ask that timed out after
// its deposit was applied applies the deposit a second time.
func TestATDLV22RetriedAskAppliedTwice(t *testing.T) {
	want := dlvFailure("invariant", "dlv/applied-once", "key k1 applied 2 times")
	if got := failingSeed(t, "dlv-retry-no-cache"); got != want {
		t.Errorf("failure %+v, want %+v", got, want)
	}
}

// AT-DLV-23, DLV-130, DLV-400: with a reply cache whose TTL exceeds the retry horizon (100 ms
// timeout plus at most 70 ms to the handling), the retry gets the first reply and the deposit is
// applied once.
func TestATDLV23RetriedAskAnsweredFromCache(t *testing.T) {
	cache := dedup.Config{Capacity: 16, TTL: time.Second}
	sim.Simulate(t, dlvRetryOptions(20), func(s *sim.Sim) {
		tt := s.World().T()
		r := runDlvRetry(s, &cache)
		if !errors.Is(r.err1, lockstep.ErrTimeout) {
			tt.Errorf("first ask: %v, want an error wrapping ErrTimeout", r.err1)
		}
		if !r.asked2 || r.v2 != 10 || r.err2 != nil {
			tt.Errorf("second ask: asked %t, %d, %v; want asked, 10, nil", r.asked2, r.v2, r.err2)
		}
		if r.sum != 10 {
			tt.Errorf("sum %d at the end, want 10", r.sum)
		}
	})
}

// AT-DLV-24, DLV-400: a reply cache whose TTL (50 ms) is shorter than the retry horizon forgets
// the first reply before the retry arrives, and the retry is applied as a new deposit.
func TestATDLV24UndersizedWindow(t *testing.T) {
	want := dlvFailure("invariant", "dlv/applied-once", "key k1 applied 2 times")
	if got := failingSeed(t, "dlv-retry-short-ttl"); got != want {
		t.Errorf("failure %+v, want %+v", got, want)
	}
}

// AT-DLV-25, DLV-110, DLV-120: without tombstones, a cancel that overtakes its create is ignored
// and the create then makes the cancelled order live.
func TestATDLV25CancelBeforeCreate(t *testing.T) {
	want := dlvFailure("final", "dlv/cancelled-orders", "order o1 is live after its cancel")
	if got := failingSeed(t, "dlv-cancel-no-tombstone"); got != want {
		t.Errorf("failure %+v, want %+v", got, want)
	}
}

// AT-DLV-26, DLV-110, DLV-120: with tombstones, a cancel that overtakes its create leaves a
// tombstone that drops the create, so no cancelled order is live. Across the seeds, both a
// tombstone and a plain cancel of a live order occur.
func TestATDLV26Tombstones(t *testing.T) {
	tombs := dedup.Config{Capacity: 64, TTL: time.Second}
	var seeds, tombstoned, removed int
	sim.Simulate(t, dlvOptions(20), func(s *sim.Sim) {
		seeds = s.World().Options().Seeds
		r := newDlvOrdersRun(s, &tombs)
		s.DelayBetween(r.router.Address(), r.orders.Address(), 0, 10*time.Millisecond)
		r.boot()
		for i := 1; i <= 10; i++ {
			id := fmt.Sprintf("o%d", i)
			r.create(id)
			s.World().RunFor(5 * time.Millisecond)
			r.cancel(id)
			s.World().RunFor(15 * time.Millisecond)
		}
		r.end()
		tombstoned += r.state.tombstoned
		removed += r.state.removed
	})
	if seeds != 20 {
		t.Logf("skipped the cross-seed checks, which need 20 seeds: this run had %d", seeds)
		return
	}
	t.Logf("across the runs, %d cancels added a tombstone and %d removed a live order", tombstoned, removed)
	if tombstoned == 0 || removed == 0 {
		t.Errorf("cancels: %d added a tombstone, %d removed a live order; want both above 0", tombstoned, removed)
	}
}

// AT-DLV-27, DLV-140: without a version gate, an update delayed on its route is applied after a
// newer one and the entity ends at the older version.
func TestATDLV27StaleUpdate(t *testing.T) {
	want := dlvFailure("final", "dlv/latest-version", "e1: version 1, want 2")
	if got := failingSeed(t, "dlv-stale-no-gate"); got != want {
		t.Errorf("failure %+v, want %+v", got, want)
	}
}

// AT-DLV-28, DLV-140, DLV-440: with a version gate, updates that took different routes are
// applied only in version order, so every entity ends at its highest version. Across the seeds,
// the gate rejects at least one update.
func TestATDLV28VersionGate(t *testing.T) {
	gate := dedup.Config{Capacity: 16, TTL: time.Second}
	var seeds, rejected int
	sim.Simulate(t, dlvOptions(20), func(s *sim.Sim) {
		seeds = s.World().Options().Seeds
		r := newDlvUpdatesRun(s, &gate)
		s.DelayBetween(r.ra.Address(), r.proj.Address(), 0, 10*time.Millisecond)
		s.DelayBetween(r.rb.Address(), r.proj.Address(), 0, 10*time.Millisecond)
		r.boot()
		for v, value := uint64(1), int64(10); v <= 10; v, value = v+1, value+10 {
			r.send("e1", v, value)
			r.send("e2", v, value)
		}
		r.end()
		rejected += r.state.rejected
	})
	if seeds != 20 {
		t.Logf("skipped the cross-seed checks, which need 20 seeds: this run had %d", seeds)
		return
	}
	t.Logf("across the runs, the gate rejected %d updates", rejected)
	if rejected == 0 {
		t.Error("the gate rejected no update; want at least one")
	}
}
