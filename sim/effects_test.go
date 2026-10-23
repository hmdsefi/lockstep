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
	"github.com/hmdsefi/faultline/kernel/fault"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/sim"
)

// testFetch is the effect of EFF §10's tests. Simulation never calls its Run.
type testFetch struct{ Sym string }

func (testFetch) Name() string                       { return "prices.fetch" }
func (testFetch) Run(context.Context) (int64, error) { panic("Run called") }

// fetchName is testFetch's name.
const fetchName = "prices.fetch"

// QMsg is the message type of the Quoter.
type QMsg interface{ qmsg() }

// Quote asks the Quoter to fetch the price of Sym.
type Quote struct{ Sym string }

// Fetched is the outcome of one fetch.
type Fetched struct {
	P   int64
	Err error
}

// Filler is a message that only takes mailbox space.
type Filler struct{}

// failNow makes the Quoter fail.
type failNow struct{}

func (Quote) qmsg()   {}
func (Fetched) qmsg() {}
func (Filler) qmsg()  {}
func (failNow) qmsg() {}

// errX is the failure of the Quoter that fails on purpose.
var errX = errors.New("x failed")

// fetchedAt is one Fetched that the Quoter received, with ctx.Now() of that turn.
type fetchedAt struct {
	Fetched
	At time.Time
}

// fetchQuote starts testFetch{sym} as EFF §10's Quoter does.
func fetchQuote(ctx *lockstep.Context[QMsg], sym string, opts ...lockstep.EffectOption) lockstep.EffectID {
	return lockstep.Do(ctx, testFetch{sym}, func(p int64, err error) QMsg { return Fetched{p, err} }, opts...)
}

// quoter is EFF §10's Quoter: on Quote it starts testFetch{m.Sym} with opts, and it appends
// every Fetched with ctx.Now() to got. The optional functions change or extend that.
type quoter struct {
	got     *[]fetchedAt
	opts    []lockstep.EffectOption
	started func(ctx *lockstep.Context[QMsg])
	onQuote func(ctx *lockstep.Context[QMsg], m Quote) // replaces the default Do
	onOther func(ctx *lockstep.Context[QMsg], m QMsg)  // every message but Quote, after got
	stopped func(ctx *lockstep.Context[QMsg])
}

func (q *quoter) Started(ctx *lockstep.Context[QMsg]) {
	if q.started != nil {
		q.started(ctx)
	}
}

func (q *quoter) Receive(ctx *lockstep.Context[QMsg], m QMsg) {
	if m, ok := m.(Quote); ok {
		if q.onQuote != nil {
			q.onQuote(ctx, m)
			return
		}
		fetchQuote(ctx, m.Sym, q.opts...)
		return
	}
	if f, ok := m.(Fetched); ok && q.got != nil {
		*q.got = append(*q.got, fetchedAt{f, ctx.Now()})
	}
	if q.onOther != nil {
		q.onOther(ctx, m)
	}
}

func (q *quoter) Stopped(ctx *lockstep.Context[QMsg], _ lockstep.StopReason) {
	if q.stopped != nil {
		q.stopped(ctx)
	}
}

// addQuoter adds node n1, whose setup spawns q at /user/q, and returns the node and the PID of
// the Quoter of its latest boot.
func addQuoter(s *sim.Sim, q *quoter, opts ...lockstep.SpawnOption) (*kernel.Node, *lockstep.PID[QMsg]) {
	pid := new(lockstep.PID[QMsg])
	n1 := s.Node("n1", func(sys *lockstep.System) { *pid = spawnQuoter(sys, "q", q, opts...) })
	return n1, pid
}

// spawnQuoter spawns q at /user/<name> and returns its PID. It panics on an error.
func spawnQuoter(sys *lockstep.System, name string, q *quoter, opts ...lockstep.SpawnOption) lockstep.PID[QMsg] {
	pid, err := lockstep.Spawn(sys, name, func() lockstep.Actor[QMsg] { return q }, opts...)
	if err != nil {
		panic(err)
	}
	return pid
}

// send sends m to the Quoter pid from n1's System and fails the test on an error.
func send(tt testing.TB, s *sim.Sim, pid lockstep.PID[QMsg], m QMsg) {
	tt.Helper()
	if err := sim.Send(s, "n1", pid, m); err != nil {
		tt.Fatalf("Send(%T): %v", m, err)
	}
}

// at returns the local time of a node without clock drift at virtual time d.
func at(d time.Duration) time.Time { return kernel.Time(0).Add(d).Std() }

// ran returns the number of kernel events labeled label that ran on n.
func ran(recs []kernel.Record, n *kernel.Node, label string) int {
	c := 0
	for _, r := range recs {
		if r.Kind == "kernel.event" && r.Node == n.ID() && r.Text == label {
			c++
		}
	}
	return c
}

// attrsOf returns the attribute key of every record in recs, in order.
func attrsOf(recs []kernel.Record, key string) []string {
	out := make([]string, len(recs))
	for i, r := range recs {
		out[i] = attr(r, key)
	}
	return out
}

// constLatency returns a handler that returns v after d.
func constLatency(v any, d time.Duration) func(sim.EffectCall) sim.EffectResult {
	return func(sim.EffectCall) sim.EffectResult { return sim.EffectResult{Value: v, Latency: d} }
}

// AT-EFF-01, EFF-020, EFF-060, EFF-400, EFF-420, EFF-430: the handler runs inside the turn that
// called Do with the call's fields, and its result arrives as one lockstep/effect event after its
// latency; the timeout timer never runs.
func TestATEFF01HandlerResult(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var got []fetchedAt
		var calls []sim.EffectCall
		var callNow []time.Time
		var callRand []bool
		var n1 *kernel.Node
		s.HandleEffect(fetchName, func(c sim.EffectCall) sim.EffectResult {
			calls = append(calls, c)
			callNow = append(callNow, c.Now())
			callRand = append(callRand, c.Rand() == w.Sim.Rand("lockstep/effect/n1/1/prices.fetch"))
			if !s.Engine("n1").Scheduler.InTurn() {
				tt.Error("the handler runs outside a kernel callback")
			}
			return sim.EffectResult{Value: int64(100), Latency: 10 * time.Millisecond}
		})
		n1, q := addQuoter(s, &quoter{got: &got})
		w.RunFor(time.Second)
		send(tt, s, *q, Quote{"X"})
		w.RunFor(35 * time.Second) // past the 30s timeout

		if want := []fetchedAt{{Fetched{100, nil}, at(1010 * time.Millisecond)}}; !slices.Equal(got, want) {
			tt.Errorf("Fetched %v, want %v", got, want)
		}
		if len(calls) != 1 {
			tt.Fatalf("handler called %d times, want 1", len(calls))
		}
		c := calls[0]
		if c.Effect() != (testFetch{"X"}) || c.Name != fetchName || c.Node != "n1" || c.ID != 1 || c.Actor != q.Address() {
			tt.Errorf("EffectCall{Effect: %v, Name: %q, Node: %q, ID: %d, Actor: %v}, want {%v, %q, n1, 1, %v}",
				c.Effect(), c.Name, c.Node, c.ID, c.Actor, testFetch{"X"}, fetchName, q.Address())
		}
		if !callNow[0].Equal(at(time.Second)) || !callRand[0] {
			tt.Errorf("c.Now() = %v (want %v); c.Rand() is the effect stream: %t", callNow[0], at(time.Second), callRand[0])
		}

		recs := w.Sim.Records()
		starts, dones := ofKind(recs, "lockstep.effect.start"), ofKind(recs, "lockstep.effect.done")
		if len(starts) != 1 || len(dones) != 1 {
			tt.Fatalf("%d effect.start and %d effect.done records, want 1 each", len(starts), len(dones))
		}
		if attr(starts[0], "effect") != "1" || attr(starts[0], "timeout") != "30s" {
			tt.Errorf("effect.start %v, want effect=1 timeout=30s", starts[0].Attrs)
		}
		if attr(dones[0], "outcome") != "ok" || attr(dones[0], "err") != "" || dones[0].Seq < starts[0].Seq {
			tt.Errorf("effect.done %v (seq %d), want outcome=ok err=\"\" after the start (seq %d)", dones[0].Attrs, dones[0].Seq, starts[0].Seq)
		}
		if dones[0].At != kernel.Time(0).Add(1010*time.Millisecond) {
			tt.Errorf("effect.done at %v, want 1.01s", dones[0].At)
		}
		if n, m := ran(recs, n1, "lockstep/effect"), ran(recs, n1, "lockstep/effect-timeout"); n != 1 || m != 0 {
			tt.Errorf("%d lockstep/effect and %d lockstep/effect-timeout events ran, want 1 and 0", n, m)
		}
	})
}

// AT-EFF-02, EFF-420: a handler draws from the stream lockstep/effect/<node>/<epoch>/<name>, and a
// restarted node draws from a fresh one; two runs of a seed have the same trace.
func TestATEFF02HandlerRand(t *testing.T) {
	opts := fullTrace(3)
	opts.CheckDeterminism = true
	sim.Simulate(t, opts, func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var got []fetchedAt
		var lat []time.Duration
		s.HandleEffect(fetchName, func(c sim.EffectCall) sim.EffectResult {
			d := kernel.Uniform(c.Rand(), time.Millisecond, 50*time.Millisecond)
			lat = append(lat, d)
			return sim.EffectResult{Value: int64(1), Latency: d}
		})
		_, q := addQuoter(s, &quoter{got: &got})
		ref := kernel.New(kernel.Config{Seed: w.Seed()})
		for epoch := 1; epoch <= 2; epoch++ {
			if epoch == 2 {
				w.Faults.Inject(fault.Event{Kind: fault.KindCrash, Node: "n1"})
				w.Faults.Inject(fault.Event{Kind: fault.KindRestart, Node: "n1"})
			}
			got, lat = nil, nil
			w.RunFor(time.Millisecond)
			for range 20 {
				send(tt, s, *q, Quote{"X"})
			}
			w.RunFor(time.Second)
			r := ref.Rand(fmt.Sprintf("lockstep/effect/n1/%d/prices.fetch", epoch))
			want := make([]time.Duration, 20)
			for i := range want {
				want[i] = kernel.Uniform(r, time.Millisecond, 50*time.Millisecond)
			}
			if !slices.Equal(lat, want) {
				tt.Errorf("epoch %d: latencies %v, want the stream's draws %v", epoch, lat, want)
			}
			if len(got) != 20 {
				tt.Errorf("epoch %d: %d Fetched, want 20", epoch, len(got))
			}
		}
	})
}

// AT-EFF-03, EFF-090: a result later than the default timeout is never delivered; the Quoter gets
// ErrTimeout 30s after Do.
func TestATEFF03Timeout(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var got []fetchedAt
		s.HandleEffect(fetchName, constLatency(int64(100), 40*time.Second))
		n1, q := addQuoter(s, &quoter{got: &got})
		w.RunFor(time.Second)
		send(tt, s, *q, Quote{"X"})
		w.RunFor(45 * time.Second)

		if len(got) != 1 || got[0].P != 0 || !errors.Is(got[0].Err, lockstep.ErrTimeout) || !got[0].At.Equal(at(31*time.Second)) {
			tt.Fatalf("Fetched %v, want one Fetched{0, ErrTimeout} at 31s", got)
		}
		recs := w.Sim.Records()
		if got := attrsOf(ofKind(recs, "lockstep.effect.done"), "outcome"); !slices.Equal(got, []string{"timeout"}) {
			tt.Errorf("effect.done outcomes %q, want [timeout]", got)
		}
		if n := ran(recs, n1, "lockstep/effect"); n != 0 {
			tt.Errorf("%d lockstep/effect events ran, want 0", n)
		}
	})
}

// AT-EFF-04, EFF-040, EFF-010: the last WithEffectTimeout wins; a timeout of 0 fails the actor.
func TestATEFF04TimeoutOptions(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var got, badGot []fetchedAt
		var bad lockstep.PID[QMsg]
		s.HandleEffect(fetchName, constLatency(int64(100), 3*time.Second))
		_, q := addQuoter(s, &quoter{got: &got, opts: []lockstep.EffectOption{
			lockstep.WithEffectTimeout(time.Hour), lockstep.WithEffectTimeout(2 * time.Second),
		}})
		w.RunFor(time.Second)
		bad = spawnQuoter(s.System("n1"), "bad", &quoter{got: &badGot, opts: []lockstep.EffectOption{lockstep.WithEffectTimeout(0)}})
		send(tt, s, *q, Quote{"X"})
		send(tt, s, bad, Quote{"Y"})
		w.RunFor(5 * time.Second)

		if len(got) != 1 || !errors.Is(got[0].Err, lockstep.ErrTimeout) || !got[0].At.Equal(at(3*time.Second)) {
			tt.Errorf("Fetched %v, want one ErrTimeout at 3s", got)
		}
		if len(badGot) != 0 {
			tt.Errorf("the bad Quoter got %v, want nothing", badGot)
		}
		var fails []kernel.Record
		for _, r := range ofKind(w.Sim.Records(), "lockstep.fail") {
			if attr(r, "actor") == bad.String() {
				fails = append(fails, r)
			}
		}
		want := "lockstep: panic: lockstep: Do: effect timeout must be positive, got 0s"
		if len(fails) != 1 || attr(fails[0], "source") != "receive" || attr(fails[0], "err") != want {
			tt.Errorf("lockstep.fail records of the bad Quoter %v, want one with source=receive err=%q", fails, want)
		}
	})
}

// Markers that scenario children print, for the parent to check in their output.
const (
	fetchedMarker  = "QUOTER RECEIVED Fetched"
	doReturnMarker = "DO RETURNED"
)

// printFetched is an onOther function that prints fetchedMarker for every Fetched.
func printFetched(_ *lockstep.Context[QMsg], m QMsg) {
	if _, ok := m.(Fetched); ok {
		fmt.Println(fetchedMarker)
	}
}

// failingQuote is the body of the failing-seed scenarios of this file: it runs one seed on which
// the Quoter of n1, made by q, gets Quote{"X"} at t=1s.
func failingQuote(t *testing.T, install func(s *sim.Sim), q func(s *sim.Sim) *quoter) {
	sim.Simulate(t, fullTrace(1), func(s *sim.Sim) {
		w := s.World()
		install(s)
		_, pid := addQuoter(s, q(s))
		w.RunFor(time.Second)
		send(w.T(), s, *pid, Quote{"X"})
		w.RunFor(10 * time.Second)
	})
}

// plainQuoter returns a Quoter that prints fetchedMarker for every Fetched.
func plainQuoter(*sim.Sim) *quoter { return &quoter{onOther: printFetched} }

// wantSeedFailure checks that scenario name fails its seed with check and message, and that its
// Quoter never received a Fetched. It returns the child's output.
func wantSeedFailure(t *testing.T, name, check, message string) string {
	t.Helper()
	got, out := failingSeedOutput(t, name)
	want := failure{Kind: "invariant", Check: check, Signature: "invariant:" + check, Message: message}
	if got != want {
		t.Errorf("failure %+v, want %+v", got, want)
	}
	if strings.Contains(out, fetchedMarker) {
		t.Errorf("the Quoter received a Fetched:\n%s", out)
	}
	return out
}

// noHandlerMessage is EFF-450's failure text for the Quoter of failingQuote. Its uid 0x100000003
// is epoch 1 (the node's first incarnation) in the high 32 bits and seq 3 in the low ones: the
// user and system guardians take seqs 1 and 2 (ACT-030, ACT-040), and the Quoter is the first
// actor that setup spawns.
const noHandlerMessage = `lockstep: no effect handler: effect "prices.fetch" called by lockstep://n1/user/q#100000003`

// With no handler, the Quoter's Do stores EFF-450's failure; a failed in-process check stores
// a misuse failure, which the report then shows instead.
var _ = scenario("eff-no-handler", func(t *testing.T) {
	failingQuote(t, func(*sim.Sim) {}, func(s *sim.Sim) *quoter {
		return &quoter{onOther: printFetched, onQuote: func(ctx *lockstep.Context[QMsg], m Quote) {
			fetchQuote(ctx, m.Sym)
			if err := s.Stored(sim.CheckNoEffectHandler); !errors.Is(err, lockstep.ErrNoEffectHandler) {
				s.Store(sim.CheckMisuse, fmt.Errorf("stored %v, want an error wrapping ErrNoEffectHandler", err))
			}
		}}
	})
})

// AT-EFF-05, EFF-450: with no handler the seed fails through lockstep/no-effect-handler with an
// error wrapping ErrNoEffectHandler.
func TestATEFF05NoHandler(t *testing.T) {
	wantSeedFailure(t, "eff-no-handler", sim.CheckNoEffectHandler, noHandlerMessage)
}

var _ = scenario("eff-wrong-type", func(t *testing.T) {
	failingQuote(t, func(s *sim.Sim) { s.HandleEffect(fetchName, constLatency("100", time.Millisecond)) }, plainQuoter)
})

// AT-EFF-06, EFF-070: a value that is not of the effect's type fails the seed.
func TestATEFF06WrongType(t *testing.T) {
	wantSeedFailure(t, "eff-wrong-type", sim.CheckMisuse, `lockstep: effect "prices.fetch": engine returned string, want int64`)
}

var _ = scenario("eff-negative-latency", func(t *testing.T) {
	failingQuote(t, func(s *sim.Sim) { s.HandleEffect(fetchName, constLatency(int64(100), -1)) }, plainQuoter)
})

// AT-EFF-07, EFF-440: a negative latency fails the seed.
func TestATEFF07NegativeLatency(t *testing.T) {
	wantSeedFailure(t, "eff-negative-latency", sim.CheckMisuse, `lockstep: effect "prices.fetch": handler returned negative latency -1ns`)
}

// The handler panics; the Quoter prints doReturnMarker when Do returns, with the number of
// lockstep.fail records so far.
var _ = scenario("eff-handler-panics", func(t *testing.T) {
	failingQuote(t,
		func(s *sim.Sim) {
			s.HandleEffect(fetchName, func(sim.EffectCall) sim.EffectResult { panic("boom") })
		},
		func(s *sim.Sim) *quoter {
			return &quoter{onOther: printFetched, onQuote: func(ctx *lockstep.Context[QMsg], m Quote) {
				fetchQuote(ctx, m.Sym)
				fmt.Printf("%s fails=%d\n", doReturnMarker, len(ofKind(s.World().Sim.Records(), "lockstep.fail")))
			}}
		})
})

// AT-EFF-08, EFF-460: the engine recovers a handler's panic and fails the seed; the actor does
// not fail.
func TestATEFF08HandlerPanics(t *testing.T) {
	out := wantSeedFailure(t, "eff-handler-panics", sim.CheckMisuse, `lockstep: effect handler "prices.fetch" panicked: "boom"`)
	if !strings.Contains(out, doReturnMarker+" fails=0") {
		t.Errorf("Do did not return to the Quoter, or it has a lockstep.fail record:\n%s", out)
	}
}

// AT-EFF-10, EFF-100: an effect pending when its actor fails is cancelled with the incarnation;
// the restarted Quoter never gets its result.
func TestATEFF10CancelOnRestart(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var got []fetchedAt
		starts := 0
		s.HandleEffect(fetchName, constLatency(int64(100), 5*time.Second))
		n1, q := addQuoter(s, &quoter{
			got:     &got,
			started: func(*lockstep.Context[QMsg]) { starts++ },
			onQuote: func(ctx *lockstep.Context[QMsg], m Quote) {
				fetchQuote(ctx, m.Sym)
				ctx.After(time.Second, failNow{})
			},
			onOther: func(ctx *lockstep.Context[QMsg], m QMsg) {
				if _, ok := m.(failNow); ok {
					ctx.Fail(errX)
				}
			},
		})
		w.RunFor(time.Second)
		send(tt, s, *q, Quote{"X"})
		w.RunFor(40 * time.Second)

		recs := w.Sim.Records()
		if len(got) != 0 {
			tt.Errorf("Fetched %v, want none", got)
		}
		if starts != 2 {
			tt.Errorf("the Quoter started %d times, want 2", starts)
		}
		if got := attrsOf(ofKind(recs, "lockstep.effect.done"), "outcome"); !slices.Equal(got, []string{"cancelled"}) {
			tt.Errorf("effect.done outcomes %q, want [cancelled]", got)
		}
		if n := ran(recs, n1, "lockstep/effect") + ran(recs, n1, "lockstep/effect-timeout"); n != 0 {
			tt.Errorf("%d lockstep/effect and lockstep/effect-timeout events ran, want 0", n)
		}
	})
}

// AT-EFF-11, EFF-050, EFF-110: CancelEffect cancels a pending effect once.
func TestATEFF11CancelEffect(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var got []fetchedAt
		var results []bool
		s.HandleEffect(fetchName, constLatency(int64(100), 10*time.Millisecond))
		n1, q := addQuoter(s, &quoter{got: &got, onQuote: func(ctx *lockstep.Context[QMsg], m Quote) {
			id := fetchQuote(ctx, m.Sym)
			results = append(results, ctx.CancelEffect(id), ctx.CancelEffect(id), ctx.CancelEffect(0))
		}})
		w.RunFor(time.Second)
		send(tt, s, *q, Quote{"X"})
		w.RunFor(35 * time.Second)

		recs := w.Sim.Records()
		if !slices.Equal(results, []bool{true, false, false}) {
			tt.Errorf("CancelEffect returned %v, want [true false false]", results)
		}
		if len(got) != 0 {
			tt.Errorf("Fetched %v, want none", got)
		}
		if got := attrsOf(ofKind(recs, "lockstep.effect.done"), "outcome"); !slices.Equal(got, []string{"cancelled"}) {
			tt.Errorf("effect.done outcomes %q, want [cancelled]", got)
		}
		if n := ran(recs, n1, "lockstep/effect") + ran(recs, n1, "lockstep/effect-timeout"); n != 0 {
			tt.Errorf("%d lockstep/effect and lockstep/effect-timeout events ran, want 0", n)
		}
	})
}

// AT-EFF-12, EFF-030: Do from Stopped starts nothing and calls no handler.
func TestATEFF12DoInStopped(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		calls := 0
		var ids []lockstep.EffectID
		s.HandleEffect(fetchName, func(sim.EffectCall) sim.EffectResult { calls++; return sim.EffectResult{} })
		_, q := addQuoter(s, &quoter{stopped: func(ctx *lockstep.Context[QMsg]) { ids = append(ids, fetchQuote(ctx, "X")) }})
		w.RunFor(time.Second)
		if err := lockstep.Stop(s.System("n1"), q.Address()); err != nil {
			tt.Fatalf("Stop: %v", err)
		}
		w.RunFor(time.Second)

		if !slices.Equal(ids, []lockstep.EffectID{0}) || calls != 0 {
			tt.Errorf("Do in Stopped returned %v and the handler ran %d times; want [0] and 0", ids, calls)
		}
		dones := ofKind(w.Sim.Records(), "lockstep.effect.done")
		if len(dones) != 1 || attr(dones[0], "effect") != "0" || attr(dones[0], "outcome") != "refused" {
			tt.Errorf("effect.done records %v, want one with effect=0 outcome=refused", dones)
		}
		if n := len(ofKind(w.Sim.Records(), "lockstep.effect.start")); n != 0 {
			tt.Errorf("%d effect.start records, want 0", n)
		}
	})
}

// AT-EFF-13, EFF-080: a result that arrives at a full mailbox is still delivered, without a
// mailbox.full record, after the message already queued.
func TestATEFF13ResultEntryWhenFull(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var got []fetchedAt
		var order []string
		s.HandleEffect(fetchName, constLatency(int64(100), time.Millisecond))
		_, q := addQuoter(s, &quoter{got: &got, onOther: func(_ *lockstep.Context[QMsg], m QMsg) {
			order = append(order, fmt.Sprintf("%T", m))
		}}, lockstep.WithMailbox(1, lockstep.FailSend))
		w.RunFor(time.Second)
		t0 := w.Sim.Now()
		send(tt, s, *q, Quote{"X"})
		w.RunFor(0) // the Quoter calls Do at t0
		if n := len(ofKind(w.Sim.Records(), "lockstep.effect.start")); n != 1 {
			tt.Fatalf("%d effect.start records at t0, want 1", n)
		}
		w.Faults.Inject(fault.Event{Kind: fault.KindPause, Node: "n1"})
		w.RunFor(1500 * time.Microsecond)
		send(tt, s, *q, Filler{}) // the mailbox is now full
		w.RunFor(500 * time.Microsecond)
		w.Faults.Inject(fault.Event{Kind: fault.KindResume, Node: "n1"})
		w.RunFor(10 * time.Millisecond)

		recs := w.Sim.Records()
		dones := ofKind(recs, "lockstep.effect.done")
		if len(dones) != 1 || attr(dones[0], "outcome") != "ok" || dones[0].At != t0.Add(2*time.Millisecond) {
			tt.Errorf("effect.done records %v, want one outcome=ok at t0+2ms (%v)", dones, t0.Add(2*time.Millisecond))
		}
		if n := len(ofKind(recs, "lockstep.mailbox.full")); n != 0 {
			tt.Errorf("%d lockstep.mailbox.full records, want 0", n)
		}
		if want := []string{"sim_test.Filler", "sim_test.Fetched"}; !slices.Equal(order, want) || len(got) != 1 {
			tt.Errorf("the Quoter handled %q and got %d Fetched, want %q and 1", order, len(got), want)
		}
	})
}

// AT-EFF-14, EFF-080: results reach the actor in the order they arrive, not the order of Do.
func TestATEFF14ResultOrder(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var got []fetchedAt
		latency := map[string]time.Duration{"slow": 5 * time.Millisecond, "fast": 2 * time.Millisecond}
		s.HandleEffect(fetchName, func(c sim.EffectCall) sim.EffectResult {
			f, _ := c.Effect().(testFetch)
			d := latency[f.Sym]
			return sim.EffectResult{Value: int64(d / time.Millisecond), Latency: d}
		})
		_, q := addQuoter(s, &quoter{got: &got, onQuote: func(ctx *lockstep.Context[QMsg], _ Quote) {
			fetchQuote(ctx, "slow")
			fetchQuote(ctx, "fast")
		}})
		w.RunFor(time.Second)
		send(tt, s, *q, Quote{})
		w.RunFor(time.Second)

		want := []fetchedAt{{Fetched{2, nil}, at(1002 * time.Millisecond)}, {Fetched{5, nil}, at(1005 * time.Millisecond)}}
		if !slices.Equal(got, want) {
			tt.Errorf("Fetched %v, want %v", got, want)
		}
	})
}

// AT-EFF-15, EFF-080: a panic in done fails the actor, which is restarted; the seed passes.
func TestATEFF15DonePanics(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		starts := 0
		s.HandleEffect(fetchName, constLatency(int64(100), time.Millisecond))
		_, q := addQuoter(s, &quoter{
			started: func(*lockstep.Context[QMsg]) { starts++ },
			onQuote: func(ctx *lockstep.Context[QMsg], _ Quote) {
				lockstep.Do(ctx, testFetch{}, func(int64, error) QMsg { panic("done panics") })
			},
		})
		w.RunFor(time.Second)
		send(tt, s, *q, Quote{})
		w.RunFor(time.Second)

		recs := w.Sim.Records()
		fails := ofKind(recs, "lockstep.fail")
		if len(fails) != 1 || attr(fails[0], "err") != "lockstep: panic: done panics" {
			tt.Errorf("lockstep.fail records %v, want one for the panic in done", fails)
		}
		if n := len(ofKind(recs, "lockstep.restart")); n != 1 || starts != 2 {
			tt.Errorf("%d lockstep.restart records and %d starts, want 1 and 2", n, starts)
		}
	})
}

// EFF-430, ENG §4.1: with latency 0 the result still arrives in a later lockstep/effect event at
// the same virtual time, so done never runs inside Start or the turn that called Do.
func TestEFF430ZeroLatency(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var got []fetchedAt
		var doneInTurn []int
		s.HandleEffect(fetchName, constLatency(int64(7), 0))
		n1, q := addQuoter(s, &quoter{got: &got, onQuote: func(ctx *lockstep.Context[QMsg], m Quote) {
			fetchQuote(ctx, m.Sym)
			doneInTurn = append(doneInTurn, len(ofKind(w.Sim.Records(), "lockstep.effect.done"))+len(got))
		}})
		w.RunFor(time.Second)
		send(tt, s, *q, Quote{"X"})
		w.RunFor(time.Second)

		if !slices.Equal(doneInTurn, []int{0}) {
			tt.Errorf("effect.done records plus Fetched in the turn of Do: %v, want [0]", doneInTurn)
		}
		if want := []fetchedAt{{Fetched{7, nil}, at(time.Second)}}; !slices.Equal(got, want) {
			tt.Errorf("Fetched %v, want %v", got, want)
		}
		recs := w.Sim.Records()
		i := slices.IndexFunc(recs, func(r kernel.Record) bool { return r.Kind == "lockstep.effect.done" })
		if i < 0 {
			tt.Fatal("no effect.done record")
		}
		var event kernel.Record
		for _, r := range recs[:i] {
			if r.Kind == "kernel.event" {
				event = r
			}
		}
		if event.Text != "lockstep/effect" || event.Node != n1.ID() || ran(recs, n1, "lockstep/effect") != 1 {
			tt.Errorf("effect.done follows event %q on node %d, want the one lockstep/effect event of n1", event.Text, event.Node)
		}
	})
}

// EFF-470, EFF-410: a handler may install and replace handlers; the change applies to later Do
// calls.
func TestEFF470HandleEffectInHandler(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var got []fetchedAt
		s.HandleEffect(fetchName, func(sim.EffectCall) sim.EffectResult {
			s.HandleEffect(fetchName, constLatency(int64(200), 2*time.Millisecond)) // later than this result: equal times tie-break by seed
			return sim.EffectResult{Value: int64(100), Latency: time.Millisecond}
		})
		_, q := addQuoter(s, &quoter{got: &got})
		w.RunFor(time.Second)
		send(tt, s, *q, Quote{"X"})
		send(tt, s, *q, Quote{"X"})
		w.RunFor(time.Second)

		if len(got) != 2 || got[0].P != 100 || got[1].P != 200 {
			tt.Errorf("Fetched %v, want 100 then 200", got)
		}
	})
}

// A removed handler is the same as none.
var _ = scenario("eff-handler-removed", func(t *testing.T) {
	failingQuote(t, func(s *sim.Sim) {
		s.HandleEffect(fetchName, constLatency(int64(100), time.Millisecond))
		s.HandleEffect(fetchName, nil)
	}, plainQuoter)
})

// AT-EFF-16, EFF-410: HandleEffect panics for an invalid or reserved name, and a nil handler
// removes the handler.
func TestATEFF16HandleEffectNames(t *testing.T) {
	t.Run("names", func(t *testing.T) {
		sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
			tt := s.World().T()
			h := constLatency(nil, 0)
			expectPanic(tt, `lockstep: sim: HandleEffect: name "lockstep.journal.append" is reserved for lockstep's built-in effects`,
				func() { s.HandleEffect("lockstep.journal.append", h) })
			for _, name := range []string{"", "a b", ".x", strings.Repeat("x", 65)} {
				expectPanic(tt, fmt.Sprintf("lockstep: sim: HandleEffect: invalid effect name %q", name), func() { s.HandleEffect(name, h) })
			}
			s.HandleEffect(strings.Repeat("x", 64), h)
			s.HandleEffect("0-a_b.c", h)
			s.HandleEffect("never.installed", nil)
		})
	})
	t.Run("removed", func(t *testing.T) {
		wantSeedFailure(t, "eff-handler-removed", sim.CheckNoEffectHandler, noHandlerMessage)
	})
}

// errDown is the error of the replacement handler of AT-EFF-17.
var errDown = errors.New("prices down")

// AT-EFF-17, EFF-410, EFF-470: a handler replaced from a kernel callback applies to later Do
// calls; a handler may send to the actor that called Do.
func TestATEFF17ReplaceHandler(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		var got []fetchedAt
		var n1 *kernel.Node
		var q *lockstep.PID[QMsg]
		var sendErrs []error
		var handled []string
		s.HandleEffect(fetchName, constLatency(int64(100), 10*time.Millisecond))
		n1, q = addQuoter(s, &quoter{got: &got, onOther: func(_ *lockstep.Context[QMsg], m QMsg) {
			handled = append(handled, fmt.Sprintf("%T", m))
		}, started: func(*lockstep.Context[QMsg]) {
			n1.After(2*time.Second, "test/replace", func() {
				s.HandleEffect(fetchName, func(sim.EffectCall) sim.EffectResult {
					sendErrs = append(sendErrs, sim.Send(s, "n1", *q, QMsg(Filler{})))
					return sim.EffectResult{Err: errDown, Latency: 10 * time.Millisecond}
				})
			})
		}})
		w.RunFor(time.Second)
		send(tt, s, *q, Quote{"X"})
		w.RunFor(2 * time.Second)
		send(tt, s, *q, Quote{"X"})
		w.RunFor(time.Second)

		want := []fetchedAt{{Fetched{100, nil}, at(1010 * time.Millisecond)}, {Fetched{0, errDown}, at(3010 * time.Millisecond)}}
		if !slices.Equal(got, want) {
			tt.Errorf("Fetched %v, want %v", got, want)
		}
		if want := []string{"sim_test.Fetched", "sim_test.Filler", "sim_test.Fetched"}; !slices.Equal(sendErrs, []error{nil}) || !slices.Equal(handled, want) {
			tt.Errorf("the handler's Send returned %v and the Quoter handled %q; want [<nil>] and %q", sendErrs, handled, want)
		}
		dones := ofKind(w.Sim.Records(), "lockstep.effect.done")
		if o, e := attrsOf(dones, "outcome"), attrsOf(dones, "err"); !slices.Equal(o, []string{"ok", "error"}) || !slices.Equal(e, []string{"", errDown.Error()}) {
			tt.Errorf("effect.done outcomes %q and errs %q, want [ok error] and [\"\" %q]", o, e, errDown.Error())
		}
	})
}

// The handler calls sim.Ask, which panics inside the simulation.
var _ = scenario("eff-ask-in-handler", func(t *testing.T) {
	var s *sim.Sim
	failingQuote(t,
		func(sm *sim.Sim) {
			s = sm
			s.HandleEffect(fetchName, func(sim.EffectCall) sim.EffectResult {
				_, _ = sim.Ask(s, "n1", lockstep.PIDAt[Ping]("n1", "/user/echo"), buildPing(1))
				return sim.EffectResult{}
			})
		}, plainQuoter)
})

// AT-EFF-18, EFF-470, EFF-460: sim.Ask in a handler panics with SIM-200's message, which fails the
// seed through lockstep/misuse.
func TestATEFF18AskInHandler(t *testing.T) {
	askPanic := "lockstep: sim: Ask called inside the simulation; use lockstep.Ask with lockstep.Pipe in actors"
	wantSeedFailure(t, "eff-ask-in-handler", sim.CheckMisuse, `lockstep: effect handler "prices.fetch" panicked: `+kernel.Describe(askPanic))
}

// badName is an effect whose name EFF-010 rejects.
type badName struct{}

func (badName) Name() string                       { return "a b" }
func (badName) Run(context.Context) (int64, error) { return 0, nil }

// AT-EFF-19, EFF-010: Do with an invalid name, a nil effect or a nil done fails the actor.
func TestATEFF19InvalidDo(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		calls := 0
		s.HandleEffect(fetchName, func(sim.EffectCall) sim.EffectResult { calls++; return sim.EffectResult{} })
		done := func(p int64, err error) QMsg { return Fetched{p, err} }
		cases := []struct {
			name  string
			do    func(ctx *lockstep.Context[QMsg])
			panic string
		}{
			{"name", func(ctx *lockstep.Context[QMsg]) { lockstep.Do(ctx, badName{}, done) }, `lockstep: Do: invalid effect name "a b"`},
			{"effect", func(ctx *lockstep.Context[QMsg]) { lockstep.Do[int64](ctx, nil, done) }, "lockstep: Do: nil effect"},
			{"done", func(ctx *lockstep.Context[QMsg]) { lockstep.Do[int64, QMsg](ctx, testFetch{"X"}, nil) }, "lockstep: Do: nil done"},
		}
		pids := make([]lockstep.PID[QMsg], len(cases))
		s.Node("n1", func(sys *lockstep.System) {
			for i, c := range cases {
				pids[i] = spawnQuoter(sys, c.name, &quoter{onQuote: func(ctx *lockstep.Context[QMsg], _ Quote) { c.do(ctx) }})
			}
		})
		w.RunFor(time.Second)
		for _, pid := range pids {
			send(tt, s, pid, Quote{})
		}
		w.RunFor(time.Second)

		fails := ofKind(w.Sim.Records(), "lockstep.fail")
		for i, c := range cases {
			var errs []string
			for _, r := range fails {
				if attr(r, "actor") == pids[i].String() {
					errs = append(errs, attr(r, "source")+": "+attr(r, "err"))
				}
			}
			if want := []string{"receive: lockstep: panic: " + c.panic}; !slices.Equal(errs, want) {
				tt.Errorf("%s: lockstep.fail records %q, want %q", c.name, errs, want)
			}
		}
		if calls != 0 {
			tt.Errorf("the handler ran %d times, want 0", calls)
		}
	})
}
