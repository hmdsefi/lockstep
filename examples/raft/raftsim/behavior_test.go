// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package raftsim

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/faultline/kernel/fault"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/examples/raft/kv"
	"github.com/hmdsefi/lockstep/examples/raft/raft"
	"github.com/hmdsefi/lockstep/sim"
)

// obsCall is one Observer call a callLog saw.
type obsCall struct {
	node   string
	method string
	at     kernel.Time
	cause  uint64 // the Seq of the last record emitted before the call

	hs    raft.HardState // Recovered, PersistStarted, Durable
	last  uint64         // the same: the last log index
	chain uint64         // the same: Chain(last)
	term  uint64         // BecameLeader
}

// callLog wraps the oracle and records every Observer call the servers make, with the virtual time
// and the position in the trace of the call.
type callLog struct {
	raft.Observer
	s     *sim.Sim
	calls []obsCall

	// onPersist, if not nil, is called after each PersistStarted call. The persist's effect
	// handler runs right after it, in the same turn.
	onPersist func(node string)
}

// wrap is the setup hook that puts the callLog between the servers and the oracle.
func (l *callLog) wrap(o raft.Observer) raft.Observer {
	l.Observer = o
	return l
}

func (l *callLog) add(c obsCall) {
	c.at = l.s.World().Sim.Now()
	c.cause = l.s.World().Sim.Cause()
	l.calls = append(l.calls, c)
}

// of returns the calls of method by node, oldest first.
func (l *callLog) of(node, method string) []obsCall {
	var out []obsCall
	for _, c := range l.calls {
		if c.node == node && c.method == method {
			out = append(out, c)
		}
	}
	return out
}

func (l *callLog) Recovered(node string, hs raft.HardState, log raft.LogView) {
	last := log.LastIndex()
	l.add(obsCall{node: node, method: "Recovered", hs: hs, last: last, chain: log.Chain(last)})
	l.Observer.Recovered(node, hs, log)
}

func (l *callLog) PersistStarted(node string, hs raft.HardState, last, chain uint64) {
	l.add(obsCall{node: node, method: "PersistStarted", hs: hs, last: last, chain: chain})
	l.Observer.PersistStarted(node, hs, last, chain)
	if l.onPersist != nil {
		l.onPersist(node)
	}
}

func (l *callLog) Durable(node string, hs raft.HardState, last, chain uint64) {
	l.add(obsCall{node: node, method: "Durable", hs: hs, last: last, chain: chain})
	l.Observer.Durable(node, hs, last, chain)
}

func (l *callLog) LogWritten(node string, from uint64, log raft.LogView) {
	l.add(obsCall{node: node, method: "LogWritten"})
	l.Observer.LogWritten(node, from, log)
}

func (l *callLog) BecameLeader(node string, term uint64, log raft.LogView) {
	l.add(obsCall{node: node, method: "BecameLeader", term: term})
	l.Observer.BecameLeader(node, term, log)
}

func (l *callLog) Committed(node string, term, from, to uint64, log raft.LogView) {
	l.add(obsCall{node: node, method: "Committed"})
	l.Observer.Committed(node, term, from, to, log)
}

func (l *callLog) Applied(node string, index uint64, e raft.Entry, stateHash uint64) {
	l.add(obsCall{node: node, method: "Applied"})
	l.Observer.Applied(node, index, e, stateHash)
}

func (l *callLog) Replied(node string, client uint32, seq uint64, r raft.Result) {
	l.add(obsCall{node: node, method: "Replied"})
	l.Observer.Replied(node, client, seq, r)
}

// withLog runs setup for c in s with a callLog and a stable store the test can reach. It returns
// the callLog and the store.
func withLog(s *sim.Sim, c Cluster) (*callLog, *raft.MemStore) {
	l := &callLog{s: s}
	store := raft.NewMemStore()
	setup(s, c, hooks{reg: s.World(), wrap: l.wrap, store: store})
	return l, store
}

// noFaults returns the default cluster without faults and with one workload client.
func noFaults() Cluster {
	c := DefaultCluster()
	c.Faults = FaultsNone
	c.Clients = 1
	return c
}

// handlePersist replaces the persist handler of s with one whose results take latency. It applies
// each persist to store, as Setup's does, unless fail, which sees every persist first, returns an
// error: then the persist writes nothing and its result is that error. fail may be nil.
func handlePersist(t *testing.T, s *sim.Sim, store *raft.MemStore, latency time.Duration,
	fail func(p raft.Persist) error) {
	t.Helper()
	s.HandleEffect(raft.Persist{}.Name(), func(c sim.EffectCall) sim.EffectResult {
		p, ok := c.Effect().(raft.Persist)
		if !ok {
			t.Errorf("raft.persist handler got %T", c.Effect())
			return sim.EffectResult{Err: errors.New("wrong effect")}
		}
		var err error
		if fail != nil {
			err = fail(p)
		}
		if err == nil {
			err = store.Save(p.Node, p.HardState, p.From, p.Entries)
		}
		return sim.EffectResult{Value: struct{}{}, Err: err, Latency: latency}
	})
}

// handleLoad replaces the load handler of s with one that reads store and takes latency.
func handleLoad(t *testing.T, s *sim.Sim, store *raft.MemStore, latency time.Duration) {
	t.Helper()
	s.HandleEffect(raft.Load{}.Name(), func(c sim.EffectCall) sim.EffectResult {
		l, ok := c.Effect().(raft.Load)
		if !ok {
			t.Errorf("raft.load handler got %T", c.Effect())
			return sim.EffectResult{Err: errors.New("wrong effect")}
		}
		d, err := store.Load(l.Node)
		return sim.EffectResult{Value: d, Err: err, Latency: latency}
	})
}

// attr returns the value of the attribute key of r, or "".
func attr(r kernel.Record, key string) string {
	for _, a := range r.Attrs {
		if a.Key == key {
			return a.Value
		}
	}
	return ""
}

// raftActor returns the address prefix of the raft actor of server: its address without the
// incarnation's uid.
func raftActor(server string) string { return "lockstep://" + server + "/user/raft#" }

// sends returns the lockstep.send records of messages named msg ("raft.RequestVote", …) from an
// actor on node from to the raft actor of node to, in trace order.
func sends(recs []kernel.Record, msg, from, to string) []kernel.Record {
	var out []kernel.Record
	for _, r := range recs {
		if r.Kind == "lockstep.send" && attr(r, "msg") == msg &&
			strings.HasPrefix(attr(r, "from"), "lockstep://"+from+"/") && strings.HasPrefix(attr(r, "to"), raftActor(to)) {
			out = append(out, r)
		}
	}
	return out
}

// TestATRFT08BootBeforeLoad checks RFT-080: while the Load of a server takes 100 ms, a Propose gets
// NotLeader without a leader hint, an AppendEntries from a would-be leader gets no reply before
// the load ends, and Recovered is called when it ends.
func TestATRFT08BootBeforeLoad(t *testing.T) {
	const loadTime = 100 * time.Millisecond
	sim.Simulate(t, fullTraceOptions(2*time.Second), func(s *sim.Sim) {
		w := s.World()
		l, store := withLog(s, noFaults())
		handleLoad(t, s, store, loadTime)
		n1 := lockstep.PIDAt[raft.Msg]("n1", raft.DefaultPath)

		w.RunFor(10 * time.Millisecond)
		res, err := sim.Ask(s, "c1", n1, func(rt lockstep.ReplyTo[raft.Result]) raft.Msg {
			return raft.Propose{Client: 9, Seq: 1, Cmd: kv.EncodePut("k", "v"), ReplyTo: rt}
		})
		if err != nil || res.Status != raft.NotLeader || res.Leader != "" {
			t.Errorf("Propose before the load ended: %+v, %v; want NotLeader with Leader %q", res, err, "")
		}
		if err := sim.Send[raft.Msg](s, "n2", n1, raft.AppendEntries{Term: 1, Leader: "n2"}); err != nil {
			t.Fatalf("send AppendEntries: %v", err)
		}
		recs, ok := runToEnd(s)

		rec := l.of("n1", "Recovered")
		if len(rec) == 0 || rec[0].at != kernel.Time(loadTime) {
			t.Errorf("Recovered(n1) calls %+v; want the first at %v", rec, loadTime)
		}
		if !ok {
			return
		}
		if got := sends(recs, "raft.AppendEntries", "n2", "n1"); len(got) == 0 || got[0].At >= kernel.Time(loadTime) {
			t.Errorf("AppendEntries from n2 to n1: %d records, want one before %v", len(got), loadTime)
		}
		for _, r := range sends(recs, "raft.AppendReply", "n1", "n2") {
			if r.At < kernel.Time(loadTime) {
				t.Errorf("n1 sent an AppendReply at %s, before its load ended at %v", r.At, loadTime)
			}
		}
		for _, r := range sends(recs, "raft.AppendReply", "n1", "n3") {
			if r.At < kernel.Time(loadTime) {
				t.Errorf("n1 sent an AppendReply at %s, before its load ended at %v", r.At, loadTime)
			}
		}
	})
}

// TestATRFT10PersistErrorRestarts checks RFT-110: when the first persist of n1 fails, n1 restarts
// (a lockstep.restart record), loads again (a second Recovered call), and the run passes every
// check.
func TestATRFT10PersistErrorRestarts(t *testing.T) {
	sim.Simulate(t, fullTraceOptions(2*time.Second), func(s *sim.Sim) {
		l, store := withLog(s, noFaults())
		failed := false
		handlePersist(t, s, store, 2*time.Millisecond, func(p raft.Persist) error {
			if p.Node == "n1" && !failed {
				failed = true
				return errors.New("disk full")
			}
			return nil
		})
		recs, ok := runToEnd(s)

		if !failed {
			t.Fatal("n1 never persisted")
		}
		if got := len(l.of("n1", "Recovered")); got != 2 {
			t.Errorf("Recovered(n1) was called %d times, want 2", got)
		}
		if !ok {
			return
		}
		restarts := 0
		for _, r := range recs {
			if r.Kind == "lockstep.restart" && strings.HasPrefix(attr(r, "actor"), raftActor("n1")) {
				restarts++
			}
			if r.Kind == "lockstep.restart" && !strings.HasPrefix(attr(r, "actor"), raftActor("n1")) {
				t.Errorf("restart of %s, want only the raft actor of n1", attr(r, "actor"))
			}
		}
		if restarts != 1 {
			t.Errorf("%d lockstep.restart records for the raft actor of n1, want 1", restarts)
		}
	})
}

// TestATRFT14CandidateRetries checks RFT-160 and RFT-180: while n2 is cut off for a second, it
// campaigns again and again, and in every campaign that ended before the cut-off it sends at
// least two RequestVote messages to n1 (the first, then the retry on a tick); n1 and n3 elect a
// leader meanwhile and n2 is not one.
func TestATRFT14CandidateRetries(t *testing.T) {
	const cutOff = time.Second
	sim.Simulate(t, fullTraceOptions(2*time.Second), func(s *sim.Sim) {
		w := s.World()
		l, _ := withLog(s, noFaults())
		w.Plan(fault.Script(
			fault.Event{At: 0, Kind: fault.KindIsolate, Node: "n2"},
			fault.Event{At: kernel.Time(cutOff), Kind: fault.KindHeal},
		))
		recs, ok := runToEnd(s)

		// n2 starts a campaign when it persists a vote for itself in a new term.
		var starts []obsCall
		for _, c := range l.of("n2", "PersistStarted") {
			if c.hs.Vote == "n2" && (len(starts) == 0 || starts[len(starts)-1].hs.Term != c.hs.Term) {
				starts = append(starts, c)
			}
		}
		leaderByCutOff := false
		for _, c := range l.calls {
			if c.method == "BecameLeader" && c.at <= kernel.Time(cutOff) {
				if c.node == "n2" {
					t.Errorf("n2 became leader of term %d at %s while cut off", c.term, c.at)
				} else {
					leaderByCutOff = true
				}
			}
		}
		if !leaderByCutOff {
			t.Errorf("neither n1 nor n3 became leader by %v", cutOff)
		}
		if !ok {
			return
		}

		// A campaign ends when the next one starts, so only the campaigns that started before
		// the last one count: the last may be cut short by the cut-off ending.
		ended := 0
		for i := 0; i+1 < len(starts) && starts[i+1].at <= kernel.Time(cutOff); i++ {
			ended++
			from, to := starts[i].at, starts[i+1].at
			n := 0
			for _, r := range sends(recs, "raft.RequestVote", "n2", "n1") {
				if r.At >= from && r.At < to {
					n++
				}
			}
			if n < 2 {
				t.Errorf("n2 sent %d RequestVote messages to n1 in its campaign for term %d (%s to %s), want at least 2",
					n, starts[i].hs.Term, from, to)
			}
		}
		if ended < 2 {
			t.Errorf("%d campaigns of n2 ended before %v, want at least 2", ended, cutOff)
		}
	})
}

// TestATRFT22ObserverBeforeSends checks RFT-280: in every turn of a server, each Observer call
// comes before the turn's lockstep.send records, in a run of 2 s with each fault preset.
func TestATRFT22ObserverBeforeSends(t *testing.T) {
	for _, f := range []Faults{FaultsNone, FaultsPartition, FaultsCrash, FaultsDefault} {
		t.Run(f.String(), func(t *testing.T) {
			c := noFaults()
			c.Faults = f
			sim.Simulate(t, fullTraceOptions(2*time.Second), func(s *sim.Sim) {
				l, _ := withLog(s, c)
				recs, ok := runToEnd(s)
				if !ok {
					return
				}
				if len(l.calls) == 0 {
					t.Fatal("no Observer calls")
				}

				// Walk the trace in order. sent and called say, per server, whether its raft actor
				// has sent a message, or made an Observer call, since its last turn record, which
				// ends the turn. A turn record is the end of a turn, so a send in an earlier part of
				// the turn leaves sent true until it.
				type turn struct{ sent, called bool }
				state := map[string]*turn{}
				at := map[kernel.NodeID]string{} // the server of each node
				for _, name := range c.serverNames() {
					at[s.World().Sim.Lookup(name).ID()] = name
					state[name] = &turn{}
				}
				mixed := 0 // turns that made an Observer call and then sent
				i := 0
				for _, r := range recs {
					for i < len(l.calls) && l.calls[i].cause < r.Seq { // calls made before r
						call := l.calls[i]
						if state[call.node].sent {
							t.Errorf("%s: Observer call %s at %s came after a send of the same turn",
								call.node, call.method, call.at)
						}
						state[call.node].called = true
						i++
					}
					server := at[r.Node]
					if server == "" {
						continue
					}
					switch {
					case r.Kind == "lockstep.send" && strings.HasPrefix(attr(r, "from"), raftActor(server)):
						if state[server].called {
							mixed++
						}
						state[server].sent = true
					case r.Kind == "lockstep.turn" && strings.HasPrefix(attr(r, "actor"), raftActor(server)):
						*state[server] = turn{}
					}
				}
				if mixed == 0 {
					t.Error("no turn made an Observer call and then a send; the check saw nothing to order")
				}
			})
		})
	}
}

// TestATRFT33CrashKeepsHandledPersist checks RFT-400: n1 crashes 1 ms after the handler of a
// persist ran for it, before the persist's result arrives. The persist is not lost: after the
// restart n1 recovers exactly the state of that persist, never reported durable, and the durable
// state check passes. The persist latency is fixed at 20 ms, well above the 1 ms to the crash, so
// the result and the crash never coincide.
func TestATRFT33CrashKeepsHandledPersist(t *testing.T) {
	const latency = 20 * time.Millisecond
	c := noFaults()
	c.PersistMin, c.PersistMax = latency, latency
	sim.Simulate(t, fullTraceOptions(3*time.Second), func(s *sim.Sim) {
		w := s.World()
		l := &callLog{s: s}
		o := setup(s, c, hooks{reg: w, wrap: l.wrap})
		var crashAt kernel.Time // when n1 is crashed; 0 until its first persist
		l.onPersist = func(node string) {
			if node != "n1" || crashAt != 0 {
				return
			}
			now := w.Sim.Now()
			crashAt = now + kernel.Time(time.Millisecond)
			w.Plan(fault.Script(
				fault.Event{At: crashAt, Kind: fault.KindCrash, Node: "n1"},
				fault.Event{At: now + kernel.Time(100*time.Millisecond), Kind: fault.KindRestart, Node: "n1"},
			))
		}
		runToEnd(s)

		if crashAt == 0 {
			t.Fatal("n1 never persisted")
		}
		started := l.of("n1", "PersistStarted")
		recovered := l.of("n1", "Recovered")
		if len(started) == 0 || len(recovered) < 2 {
			t.Fatalf("n1 made %d PersistStarted and %d Recovered calls, want a persist and a restart", len(started), len(recovered))
		}
		p, first, second := started[0], recovered[0], recovered[1]
		if first.hs.Term != 0 || first.last != 0 {
			t.Errorf("first Recovered(n1) = term %d, last %d; want an empty state", first.hs.Term, first.last)
		}
		if p.hs.Term == 0 {
			t.Fatalf("first persist of n1 covers term 0; want a state that differs from the empty one")
		}
		if second.hs != p.hs || second.last != p.last || second.chain != p.chain {
			t.Errorf("second Recovered(n1) = %+v, last %d, chain %#x; want the state of the persist: %+v, last %d, chain %#x",
				second.hs, second.last, second.chain, p.hs, p.last, p.chain)
		}
		for _, d := range l.of("n1", "Durable") {
			if d.at > p.at && d.at <= second.at {
				t.Errorf("Durable(n1) at %s between the persist at %s and the recovery at %s; the crash lost its result",
					d.at, p.at, second.at)
			}
		}
		if second.at <= crashAt {
			t.Errorf("n1 recovered at %s, not after the crash at %s", second.at, crashAt)
		}
		if err := o.Err(CheckDurableState); err != nil {
			t.Errorf("durable state check: %v", err)
		}
	})
}

// rolePlanner is a fault planner that does nothing but keep the roles World.Role registered.
type rolePlanner struct{ roles fault.Roles }

func (*rolePlanner) Name() string { return "roles" }

func (p *rolePlanner) Start(ctx *fault.PlanContext) { p.roles = ctx.Roles }

// names returns the names of the servers that role selects now.
func (p *rolePlanner) names(s *sim.Sim, role string) []string {
	var out []string
	for _, id := range p.roles[role]() {
		out = append(out, s.World().Sim.Node(id).Name())
	}
	return out
}

// TestATRFT34LeaderRole checks RFT-410: role "leader" selects the first leader once it is elected,
// and, after a script isolates it and the other two servers elect a leader of a higher term,
// selects that one.
func TestATRFT34LeaderRole(t *testing.T) {
	sim.Simulate(t, fullTraceOptions(6*time.Second), func(s *sim.Sim) {
		w := s.World()
		l, _ := withLog(s, noFaults())
		rp := &rolePlanner{}
		w.Plan(rp)

		// leaders returns the BecameLeader calls so far.
		leaders := func() []obsCall {
			var out []obsCall
			for _, c := range l.calls {
				if c.method == "BecameLeader" {
					out = append(out, c)
				}
			}
			return out
		}
		runUntil := func(what string, done func() bool) {
			for deadline := w.Sim.Now() + kernel.Time(2*time.Second); !done(); {
				if w.Sim.Now() >= deadline {
					t.Fatalf("no %s within 2 s", what)
				}
				w.RunFor(5 * time.Millisecond)
			}
		}

		runUntil("first leader", func() bool { return len(leaders()) > 0 })
		first := leaders()[0]
		if got := rp.names(s, "leader"); len(got) != 1 || got[0] != first.node {
			t.Errorf("role leader = %v after %s became leader of term %d, want [%s]", got, first.node, first.term, first.node)
		}

		now := w.Sim.Now()
		w.Plan(fault.Script(
			fault.Event{At: now, Kind: fault.KindIsolate, Role: "leader"},
			fault.Event{At: now + kernel.Time(time.Second), Kind: fault.KindHeal},
		))
		runUntil("leader of a higher term", func() bool {
			ls := leaders()
			return ls[len(ls)-1].term > first.term
		})
		ls := leaders()
		second := ls[len(ls)-1]
		if second.node == first.node {
			t.Errorf("%s led term %d and again term %d; the isolation should have replaced it", first.node, first.term, second.term)
		}
		if got := rp.names(s, "leader"); len(got) != 1 || got[0] != second.node {
			t.Errorf("role leader = %v after %s became leader of term %d, want [%s]", got, second.node, second.term, second.node)
		}
		runToEnd(s)
	})
}
