// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package client_test

import (
	"maps"
	"math/rand/v2"
	"reflect"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/lockstep/examples/raft/client"
	"github.com/hmdsefi/lockstep/examples/raft/kv"
	"github.com/hmdsefi/lockstep/examples/raft/raft"
	"github.com/hmdsefi/lockstep/sim"
)

// testSeed is the seed of every simulation test here.
const testSeed = 20260101

// streamLabel is the random stream of the actor at /user/client on node name (TMR-110): the
// node's first boot and the actor's first incarnation are both numbered 1.
func streamLabel(name string) string { return "lockstep/" + name + "/1/user/client/1" }

// model runs fn on the client's random stream of node name, as the client sees it. The kernel hands
// out one stream per label and run, so the model draws it in a run of its own, with the same seed.
func model(t *testing.T, name string, fn func(rng *rand.Rand)) {
	t.Helper()
	t.Run("model-"+name, func(t *testing.T) {
		opts := sim.Options{Options: faultline.Options{Seeds: 1}}
		sim.SimulateSeeds(t, opts, func(s *sim.Sim) { fn(s.World().Sim.Rand(streamLabel(name))) }, testSeed)
	})
}

// firstServerIndex returns the index of the first server the client on node name draws: after the
// kind and the key of its first operation, or right away for a probe.
func firstServerIndex(t *testing.T, name string, servers, keys int, probe bool) int {
	t.Helper()
	var first int
	model(t, name, func(rng *rand.Rand) {
		if !probe {
			rng.IntN(100)
			rng.IntN(keys)
		}
		first = rng.IntN(servers)
	})
	return first
}

// serversStartingAt returns the two servers a and b in an order that makes the client on node name
// start at a.
func serversStartingAt(t *testing.T, name, a, b string) []string {
	t.Helper()
	if firstServerIndex(t, name, 2, 4, false) == 0 {
		return []string{a, b}
	}
	return []string{b, a}
}

// run runs body on a one-seed simulation of d virtual time.
// faultline allows one Run per test, so each call is a subtest named name.
func run(t *testing.T, name string, d time.Duration, body func(s *sim.Sim)) {
	t.Helper()
	t.Run(name, func(t *testing.T) {
		opts := sim.Options{Options: faultline.Options{Seeds: 1, Duration: d}}
		sim.SimulateSeeds(t, opts, body, testSeed)
	})
}

// newRecorder returns a recorder on s's virtual clock.
func newRecorder(s *sim.Sim) *recorder {
	return &recorder{now: s.World().Sim.Now}
}

// okScript answers every Propose with OK.
func okScript() *script {
	return &script{answers: []raft.Result{{Status: raft.OK}}}
}

// valid returns a Config that passes New.
func valid() client.Config {
	return client.Config{Name: "c1", ID: 1, Servers: []string{"n1"}, Recorder: &recorder{}}
}

// AT-RFT-26, RFT-340: New returns the first error of the rules, in order. The first config breaks
// every rule, and each step repairs the rule the previous step reported.
func TestATRFT26NewErrors(t *testing.T) {
	cfg := client.Config{
		ReadPercent: 101,
		ThinkMin:    30 * time.Millisecond, // above the default ThinkMax of 20ms
		Timeout:     -time.Millisecond,
		MaxRetries:  -1,
		Keys:        []string{},
	}
	steps := []struct {
		repair func(c *client.Config)
		want   string
	}{
		{func(c *client.Config) {}, "client: Name is empty"},
		{func(c *client.Config) { c.Name = "c1" }, "client: Servers is empty"},
		{func(c *client.Config) { c.Servers = []string{"n1", "n2"} }, "client: Keys is empty"},
		{func(c *client.Config) { c.Keys = []string{"a"} }, "client: ReadPercent (101) must be in [0, 100]"},
		{func(c *client.Config) { c.ReadPercent = 100 }, "client: ThinkMin (30ms) must be in [0, ThinkMax (20ms)]"},
		{func(c *client.Config) { c.ThinkMin = 0 }, "client: Timeout (-1ms) must be positive"},
		{func(c *client.Config) { c.Timeout = time.Second }, "client: MaxRetries (-1) must be >= 0"},
		{func(c *client.Config) { c.MaxRetries = 1 }, "client: Recorder is nil"},
		{func(c *client.Config) { c.Recorder = &recorder{} }, ""},
	}
	for i, st := range steps {
		st.repair(&cfg)
		factory, err := client.New(cfg)
		switch {
		case st.want == "" && (err != nil || factory == nil):
			t.Errorf("step %d: New = (%v, %v), want a factory", i, factory != nil, err)
		case st.want != "" && (err == nil || err.Error() != st.want):
			t.Errorf("step %d: New error = %v, want %q", i, err, st.want)
		case st.want != "" && factory != nil:
			t.Errorf("step %d: New returned a factory with an error", i)
		}
	}
}

// RFT-340: a zero Config field takes its default, a nil Keys the default keys, and an empty
// non-nil Keys is an error. A negative ReadPercent is not a zero field.
func TestNewDefaults(t *testing.T) {
	cfg := valid()
	if _, err := client.New(cfg); err != nil {
		t.Fatalf("New(defaults) error = %v", err)
	}
	cfg.ReadPercent = -1
	if _, err := client.New(cfg); err == nil || err.Error() != "client: ReadPercent (-1) must be in [0, 100]" {
		t.Errorf("New(ReadPercent -1) error = %v", err)
	}
	cfg = valid()
	cfg.ThinkMin = 20 * time.Millisecond // equal to the default ThinkMax: allowed
	if _, err := client.New(cfg); err != nil {
		t.Errorf("New(ThinkMin = default ThinkMax) error = %v", err)
	}
}

// AT-RFT-27, RFT-350: the operations follow the draw order kind, key, first server (once, after the
// first key) and think time, on the client's own random stream.
func TestATRFT27Operations(t *testing.T) {
	const (
		maxOps   = 20
		thinkMin = 5 * time.Millisecond
		thinkMax = 15 * time.Millisecond
	)
	keys := []string{"k0", "k1", "k2", "k3"}
	servers := []string{"n1", "n2", "n3"}

	// The expected draws, in RFT-350's order; think[i] precedes operation i+1.
	reads := make([]bool, maxOps)
	picked := make([]string, maxOps)
	think := make([]time.Duration, maxOps)
	first := -1
	model(t, "c1", func(rng *rand.Rand) {
		for i := range maxOps {
			reads[i] = rng.IntN(100) < client.DefaultReadPercent
			picked[i] = keys[rng.IntN(len(keys))]
			if i == 0 {
				first = rng.IntN(len(servers))
			}
			if i < maxOps-1 {
				think[i+1] = thinkMin + time.Duration(rng.Int64N(int64(thinkMax-thinkMin)+1))
			}
		}
	})

	var runs [2][]*op
	for r := range runs {
		var log attempts
		var rec *recorder
		run(t, "run"+strconv.Itoa(r), 10*time.Second, func(s *sim.Sim) {
			log, rec = attempts{}, newRecorder(s)
			store := kv.New()
			addStubs(s, &log,
				stubSpec{node: "n1", store: store}, stubSpec{node: "n2", store: store}, stubSpec{node: "n3", store: store})
			addClient(s, "c1", client.Config{
				Name: "c1", ID: 1, Servers: servers, ThinkMin: thinkMin, ThinkMax: thinkMax,
				MaxOps: maxOps, Recorder: rec,
			})
			s.World().RunFor(10 * time.Second)
		})
		runs[r] = rec.ops
		if len(rec.ops) != maxOps || len(log.list) != maxOps {
			t.Fatalf("run %d: %d operations and %d attempts, want %d of each", r, len(rec.ops), len(log.list), maxOps)
		}
		if got := log.list[0].node; got != servers[first] {
			t.Errorf("run %d: first attempt went to %s, want %s", r, got, servers[first])
		}
		state := map[string]string{}
		for i, o := range rec.ops {
			seq := i + 1
			if i > 0 {
				if got := o.invokedAt.Sub(rec.ops[i-1].doneAt); got != think[i] {
					t.Errorf("run %d op %d: think time %v, want %v", r, seq, got, think[i])
				}
			}
			if o.process != "c1" || !o.completed || o.outcome != client.OK {
				t.Errorf("run %d op %d: %+v, want a completed OK op of c1", r, seq, *o)
			}
			if m := log.list[i].msg; m.Client != 1 || m.Seq != uint64(seq) || m.Read != reads[i] {
				t.Errorf("run %d op %d: proposed as client %d seq %d read %v", r, seq, m.Client, m.Seq, m.Read)
			}
			key := picked[i]
			if reads[i] {
				var want any
				if v, ok := state[key]; ok {
					want = v
				}
				if o.f != "read" || !equalAny(o.input, map[string]string{"key": key}) || o.output != want {
					t.Errorf("run %d op %d: %s %v -> %v, want read of %s -> %v", r, seq, o.f, o.input, o.output, key, want)
				}
				continue
			}
			value := "c1-" + strconv.Itoa(seq)
			state[key] = value
			if o.f != "write" || !equalAny(o.input, map[string]string{"key": key, "value": value}) || o.output != nil {
				t.Errorf("run %d op %d: %s %v -> %v, want write of %s=%s", r, seq, o.f, o.input, o.output, key, value)
			}
		}
	}
	if !reflect.DeepEqual(runs[0], runs[1]) {
		t.Errorf("two runs of one seed recorded different operations")
	}
}

// AT-RFT-28, RFT-360: n1 answers NotLeader with a hint, n2 answers Busy twice and then OK. With
// MaxRetries 3 the operation succeeds on the third retry; with 2 it fails.
func TestATRFT28Retries(t *testing.T) {
	const retryDelay = 20 * time.Millisecond
	cases := []struct {
		maxRetries  int
		wantNodes   []string
		wantOutcome client.Outcome
	}{
		{3, []string{"n1", "n2", "n2", "n2"}, client.OK},
		{2, []string{"n1", "n2", "n2"}, client.Fail},
	}
	servers := serversStartingAt(t, "c1", "n1", "n2")
	for _, c := range cases {
		var log attempts
		var rec *recorder
		run(t, "max-retries-"+strconv.Itoa(c.maxRetries), 2*time.Second, func(s *sim.Sim) {
			log, rec = attempts{}, newRecorder(s)
			addStubs(s, &log,
				stubSpec{node: "n1", script: &script{answers: []raft.Result{{Status: raft.NotLeader, Leader: "n2"}}}},
				stubSpec{node: "n2", script: &script{answers: []raft.Result{
					{Status: raft.Busy}, {Status: raft.Busy}, {Status: raft.OK},
				}}})
			addClient(s, "c1", client.Config{
				Name: "c1", ID: 1, Servers: servers, MaxRetries: c.maxRetries,
				RetryDelay: retryDelay, MaxOps: 1, Recorder: rec,
			})
			s.World().RunFor(2 * time.Second)
		})
		if got := log.nodes(); !slices.Equal(got, c.wantNodes) {
			t.Errorf("MaxRetries %d: attempts %v, want %v", c.maxRetries, got, c.wantNodes)
		}
		if len(rec.ops) != 1 {
			t.Errorf("MaxRetries %d: %d operations, want 1", c.maxRetries, len(rec.ops))
		} else if o := rec.ops[0]; !o.completed || o.outcome != c.wantOutcome {
			t.Errorf("MaxRetries %d: completed %v with outcome %v, want outcome %v", c.maxRetries, o.completed, o.outcome, c.wantOutcome)
		}
		if len(log.list) >= 2 && log.list[1].at.Sub(log.list[0].at) >= retryDelay {
			t.Errorf("MaxRetries %d: the hinted retry waited %v, want it at once", c.maxRetries, log.list[1].at.Sub(log.list[0].at))
		}
		for i := 2; i < len(log.list); i++ {
			if gap := log.list[i].at.Sub(log.list[i-1].at); gap < retryDelay {
				t.Errorf("MaxRetries %d: Busy retry %d after %v, want at least %v", c.maxRetries, i, gap, retryDelay)
			}
		}
	}
}

// RFT-360: NotLeader without a usable hint (empty, or a server outside Servers) retries at the next
// server in Servers order after RetryDelay.
func TestNotLeaderWithoutHint(t *testing.T) {
	const retryDelay = 30 * time.Millisecond
	servers := serversStartingAt(t, "c1", "n1", "n2")
	for _, hint := range []string{"", "n9"} {
		var log attempts
		var rec *recorder
		run(t, "hint-"+hint, time.Second, func(s *sim.Sim) {
			log, rec = attempts{}, newRecorder(s)
			addStubs(s, &log,
				stubSpec{node: "n1", script: &script{answers: []raft.Result{{Status: raft.NotLeader, Leader: hint}}}},
				stubSpec{node: "n2", script: okScript()})
			addClient(s, "c1", client.Config{
				Name: "c1", ID: 1, Servers: servers, RetryDelay: retryDelay,
				MaxOps: 1, Recorder: rec,
			})
			s.World().RunFor(time.Second)
		})
		if got := log.nodes(); !slices.Equal(got, []string{"n1", "n2"}) {
			t.Errorf("hint %q: attempts %v, want [n1 n2]", hint, got)
			continue
		}
		if gap := log.list[1].at.Sub(log.list[0].at); gap < retryDelay {
			t.Errorf("hint %q: retried after %v, want at least %v", hint, gap, retryDelay)
		}
		if !rec.ops[0].completed || rec.ops[0].outcome != client.OK {
			t.Errorf("hint %q: op = %+v, want OK", hint, *rec.ops[0])
		}
	}
}

// AT-RFT-29, RFT-360: a server that never answers makes the attempt time out. The operation
// completes Info exactly Timeout after its invoke, the next operation goes to the next server in
// Servers order, and the first attempt is never resent.
func TestATRFT29UnknownNeverResent(t *testing.T) {
	const timeout = 100 * time.Millisecond
	servers := []string{"n1", "n2", "n3"}
	first := firstServerIndex(t, "c1", len(servers), 4, false)
	var log attempts
	var rec *recorder
	run(t, "silent", 5*time.Second, func(s *sim.Sim) {
		log, rec = attempts{}, newRecorder(s)
		addStubs(s, &log,
			stubSpec{node: "n1", silent: true}, stubSpec{node: "n2", silent: true}, stubSpec{node: "n3", silent: true})
		addClient(s, "c1", client.Config{
			Name: "c1", ID: 1, Servers: servers, Timeout: timeout, MaxOps: 2, Recorder: rec,
		})
		s.World().RunFor(5 * time.Second)
	})
	want := []string{servers[first], servers[(first+1)%len(servers)]}
	if got := log.nodes(); !slices.Equal(got, want) {
		t.Errorf("attempts %v, want %v", got, want)
	}
	if len(rec.ops) != 2 {
		t.Fatalf("recorded %d operations, want 2", len(rec.ops))
	}
	for i, o := range rec.ops {
		if !o.completed || o.outcome != client.Info {
			t.Errorf("op %d = %+v, want a completed Info op", i+1, *o)
		}
		if got := o.doneAt.Sub(o.invokedAt); got != timeout {
			t.Errorf("op %d completed %v after its invoke, want %v", i+1, got, timeout)
		}
	}
	if len(log.list) == 2 && log.list[0].msg.Seq == log.list[1].msg.Seq {
		t.Errorf("both attempts carry seq %d, want one attempt per operation", log.list[0].msg.Seq)
	}
}

// AT-RFT-30, RFT-370: a probe writes only to key "probe", draws no kind and no key, and stops
// after its first OK. Its stubs answer Unknown, then OK.
func TestATRFT30Probe(t *testing.T) {
	servers := []string{"n1", "n2", "n3"}
	// The probe draws the first server first, then the think time after its first operation.
	var first int
	var think time.Duration
	model(t, "probe", func(rng *rand.Rand) {
		first = rng.IntN(len(servers))
		think = time.Duration(rng.Int64N(int64(client.DefaultThinkMax) + 1))
	})
	var log attempts
	var rec *recorder
	shared := &script{answers: []raft.Result{{Status: raft.Unknown}, {Status: raft.OK}}}
	run(t, "probe", 5*time.Second, func(s *sim.Sim) {
		log, rec = attempts{}, newRecorder(s)
		*shared = script{answers: shared.answers}
		addStubs(s, &log,
			stubSpec{node: "n1", script: shared}, stubSpec{node: "n2", script: shared}, stubSpec{node: "n3", script: shared})
		addClient(s, "probe", client.Config{
			Name: "probe", ID: 4, Servers: servers, Probe: true, ReadPercent: 100, Recorder: rec,
		})
		s.World().RunFor(5 * time.Second)
	})
	want := []string{servers[first], servers[(first+1)%len(servers)]}
	if got := log.nodes(); !slices.Equal(got, want) {
		t.Errorf("attempts %v, want %v", got, want)
	}
	if len(rec.ops) != 2 {
		t.Fatalf("recorded %d operations, want 2 (Unknown, then OK)", len(rec.ops))
	}
	if got := rec.ops[1].invokedAt.Sub(rec.ops[0].doneAt); got != think {
		t.Errorf("think time %v, want %v", got, think)
	}
	for i, want := range []client.Outcome{client.Info, client.OK} {
		o := rec.ops[i]
		wantInput := map[string]string{"key": "probe", "value": "probe-" + strconv.Itoa(i+1)}
		if o.f != "write" || !equalAny(o.input, wantInput) || o.outcome != want || o.output != nil {
			t.Errorf("op %d = %+v, want a write %v of key probe with outcome %v", i+1, *o, wantInput, want)
		}
	}
}

// RFT-350: MaxOps stops the client, and a client that has not reached MaxOps keeps going.
func TestMaxOpsStops(t *testing.T) {
	var log attempts
	var rec *recorder
	run(t, "max-ops", 5*time.Second, func(s *sim.Sim) {
		log, rec = attempts{}, newRecorder(s)
		addStubs(s, &log, stubSpec{node: "n1", script: okScript()})
		addClient(s, "c1", client.Config{Name: "c1", ID: 1, Servers: []string{"n1"}, MaxOps: 3, Recorder: rec})
		s.World().RunFor(5 * time.Second)
	})
	if len(rec.ops) != 3 || len(log.list) != 3 {
		t.Errorf("recorded %d operations and %d attempts, want 3 and 3", len(rec.ops), len(log.list))
	}
}

// RFT-350: StartAfter delays the first operation.
func TestStartAfter(t *testing.T) {
	const startAfter = 250 * time.Millisecond
	var rec *recorder
	var start kernel.Time
	run(t, "start-after", time.Second, func(s *sim.Sim) {
		rec = newRecorder(s)
		start = s.World().Sim.Now()
		var log attempts
		addStubs(s, &log, stubSpec{node: "n1", script: okScript()})
		addClient(s, "c1", client.Config{
			Name: "c1", ID: 1, Servers: []string{"n1"}, StartAfter: startAfter, MaxOps: 1, Recorder: rec,
		})
		s.World().RunFor(time.Second)
	})
	if len(rec.ops) != 1 {
		t.Fatalf("recorded %d operations, want 1", len(rec.ops))
	}
	if got := rec.ops[0].invokedAt.Sub(start); got < startAfter {
		t.Errorf("first operation invoked at %v, want at least StartAfter %v", got, startAfter)
	}
}

// equalAny reports whether v is a map[string]string equal to want.
func equalAny(v any, want map[string]string) bool {
	m, ok := v.(map[string]string)
	return ok && maps.Equal(m, want)
}
