// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim_test

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/faultline/kernel/fault"
	"github.com/hmdsefi/faultline/kernel/simnet"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/internal/remoting"
	"github.com/hmdsefi/lockstep/remote"
	"github.com/hmdsefi/lockstep/sim"
)

// The acceptance tests of remoting under faults (AT-REM-15 to AT-REM-21, AT-REM-26, AT-REM-32,
// AT-REM-38). Each runs a body of the form runATREMnn(t, opts, runs), so that AT-REM-38 can run
// it again under LOCKSTEP_CHECK_DETERMINISM=1 and count its runs.

// fastRemote are remoting settings whose timers fit runs of a few seconds.
var fastRemote = remote.Config{
	Heartbeat:   200 * time.Millisecond,
	IdleTimeout: time.Second,
	DialTimeout: 300 * time.Millisecond,
	RedialMin:   50 * time.Millisecond,
	RedialMax:   500 * time.Millisecond,
}

// remoteOpts returns fullTrace(seeds) with runs of d and the remoting settings rc.
func remoteOpts(seeds int, d time.Duration, rc remote.Config) sim.Options {
	o := fullTrace(seeds)
	o.Duration = d
	o.Remote = rc
	return o
}

// countRun adds one to *runs unless runs is nil: it counts the runs of a simulation body, so that
// AT-REM-38 can tell that the determinism check ran every seed twice.
func countRun(runs *int) {
	if runs != nil {
		*runs++
	}
}

// The layout of a traffic message's number: from*trafficFrom + inc*trafficInc + i, where from is
// the sending node's position in traffic.nodes plus one.
const (
	trafficFrom = 1_000_000_000
	trafficInc  = 1_000_000
)

// trafficKey names one stream of traffic messages: those that incarnation inc of node from sends
// to the sink of node to.
type trafficKey struct {
	from string
	inc  uint32
	to   string
}

func (k trafficKey) String() string {
	return k.from + "#" + strconv.FormatUint(uint64(k.inc), 10) + "->" + k.to
}

// delivery is one traffic message as a sink received it.
type delivery struct {
	key trafficKey
	i   int         // the message's position in its stream, from 0
	at  kernel.Time // when the sink received it
}

// traffic is a workload of numbered messages between lockstep nodes. At every boot each node
// spawns /user/sink, which logs every Num it receives, and each sender among them /user/src, which
// sends one Num to the sink of every other node every period, from one period after the boot until
// stop. A Num says which node and incarnation sent it and its position in that stream, so a sink
// can tell a duplicate, a reordering and a loss apart.
type traffic struct {
	nodes   []string
	senders []string // the nodes that run a source; every node of nodes when nil
	period  time.Duration
	stop    kernel.Time
	sent    map[trafficKey][]kernel.Time // the send time of each message of a stream; read by key
	got     []delivery                   // in arrival order
}

// newTraffic returns a traffic workload between nodes.
func newTraffic(period time.Duration, stop kernel.Time, nodes ...string) *traffic {
	return &traffic{nodes: nodes, period: period, stop: stop, sent: map[trafficKey][]kernel.Time{}}
}

// setup returns the setup of node name: the sink, the source if name is a sender, then extra
// when it is not nil.
func (tr *traffic) setup(s *sim.Sim, name string, extra func(sys *lockstep.System)) func(sys *lockstep.System) {
	from := 1 + slices.Index(tr.nodes, name)
	return func(sys *lockstep.System) {
		w := s.World()
		inc := sys.Incarnation()
		spawn(sys, "sink", &actor[Num]{receive: func(_ *lockstep.Context[Num], m Num) {
			key := trafficKey{from: tr.nodes[m.N/trafficFrom-1], inc: uint32(m.N % trafficFrom / trafficInc), to: name} //nolint:gosec // G115: below 1000
			tr.got = append(tr.got, delivery{key: key, i: m.N % trafficInc, at: w.Sim.Now()})
		}})
		if extra != nil {
			defer extra(sys)
		}
		if tr.senders != nil && !slices.Contains(tr.senders, name) {
			return
		}
		spawn(sys, "src", &actor[Tick]{
			started: func(ctx *lockstep.Context[Tick]) { ctx.After(tr.period, Tick{}) },
			receive: func(ctx *lockstep.Context[Tick], _ Tick) {
				if w.Sim.Now() >= tr.stop {
					return
				}
				for _, to := range tr.nodes {
					if to == name {
						continue
					}
					key := trafficKey{from: name, inc: inc, to: to}
					n := from*trafficFrom + int(inc)*trafficInc + len(tr.sent[key])
					if err := lockstep.Send(ctx, lockstep.PIDAt[Num](to, "/user/sink"), Num{n}); err != nil {
						panic(err)
					}
					tr.sent[key] = append(tr.sent[key], w.Sim.Now())
				}
				ctx.After(tr.period, Tick{})
			},
		})
	}
}

// check reports every delivery that breaks per-pair FIFO or at-most-once delivery: within each
// stream, the positions received must strictly increase (no duplicate, no reordering; a loss is
// allowed), and each must be one that was sent.
func (tr *traffic) check(tt testing.TB) {
	tt.Helper()
	last := map[trafficKey]int{} // read by key
	for _, d := range tr.got {
		prev, seen := last[d.key]
		switch {
		case d.i >= len(tr.sent[d.key]):
			tt.Errorf("%s #%d received at %s was never sent", d.key, d.i, d.at)
		case seen && d.i <= prev:
			tt.Errorf("%s #%d received at %s after #%d: a duplicate or a reordering", d.key, d.i, d.at, prev)
		}
		last[d.key] = d.i
	}
	if len(tr.got) == 0 {
		tt.Error("no traffic message was received")
	}
}

// received reports which messages of stream key were received, by position.
func (tr *traffic) received(key trafficKey) []bool {
	got := make([]bool, len(tr.sent[key]))
	for _, d := range tr.got {
		if d.key == key && d.i < len(got) {
			got[d.i] = true
		}
	}
	return got
}

// expectReceived reports every message of stream key whose send time t has want(t) false while it
// was received, or true while it was not. want returns ok false for a time it says nothing about.
func (tr *traffic) expectReceived(tt testing.TB, key trafficKey, want func(t kernel.Time) (received, ok bool)) {
	tt.Helper()
	got := tr.received(key)
	if len(got) == 0 {
		tt.Errorf("%s sent no message", key)
	}
	for i, at := range tr.sent[key] {
		if w, ok := want(at); ok && w != got[i] {
			tt.Errorf("%s #%d sent at %s: received %t, want %t", key, i, at, got[i], w)
		}
	}
}

// nodeRecords returns the records of kind on node n, in order.
func nodeRecords(recs []kernel.Record, n *kernel.Node, kind string) []kernel.Record {
	var out []kernel.Record
	for _, r := range recs {
		if r.Node == n.ID() && r.Kind == kind {
			out = append(out, r)
		}
	}
	return out
}

// sessionRecs returns n's records of kind (lockstep.remote.up or lockstep.remote.down) for its
// sessions of direction dir with peer, in order.
func sessionRecs(recs []kernel.Record, n *kernel.Node, kind, dir, peer string) []kernel.Record {
	return slices.DeleteFunc(nodeRecords(recs, n, kind), func(r kernel.Record) bool {
		return attr(r, "dir") != dir || attr(r, "peer") != peer
	})
}

// describe returns recs as lines "<at> <node> <kind> <attrs>" for failure messages.
func describe(s *sim.Sim, recs []kernel.Record) string {
	var b strings.Builder
	for _, r := range recs {
		name := "-"
		if r.Node != 0 {
			name = nodeName(s, r.Node)
		}
		fmt.Fprintf(&b, "\n  %s %s %s %v", r.At, name, r.Kind, r.Attrs)
	}
	return b.String()
}

// timesOf returns the times of recs.
func timesOf(recs []kernel.Record) []kernel.Time {
	out := make([]kernel.Time, len(recs))
	for i, r := range recs {
		out[i] = r.At
	}
	return out
}

// sentDatagrams returns the times at which node n sent datagrams whose frame is of kind (the
// datagram's Describe in net.send_raw names it).
func sentDatagrams(recs []kernel.Record, n *kernel.Node, kind string) []kernel.Time {
	var out []kernel.Time
	for _, r := range nodeRecords(recs, n, "net.send_raw") {
		if strings.Contains(attr(r, "payload"), " "+kind+" seq=") {
			out = append(out, r.At)
		}
	}
	return out
}

// peerRestarted reports whether err is the failure of a peer's restart: it matches both
// remote.ErrPeerRestarted and lockstep.ErrTerminated (REM-294).
func peerRestarted(err error) bool {
	return errors.Is(err, remote.ErrPeerRestarted) && errors.Is(err, lockstep.ErrTerminated)
}

// AT-REM-15 (defined here: the spec gives it one line), REM-090, REM-100, REM-130, REM-200: n1
// and n2 send each other a message every 50 ms, and a partition separates them from 2.025 s to
// 5.025 s. In each direction, the sender's session goes down idle about IdleTimeout after the
// partition, then the sender redials with backoff, every dial timing out, until a session comes up
// after the heal. The messages written on the session during the partition are lost; the ones sent
// before it, and the ones queued once the session was down, arrive, in order and once each.
func TestATREM15Partition(t *testing.T) {
	runATREM15(t, remoteOpts(5, 10*time.Second, fastRemote), nil)
}

func runATREM15(t *testing.T, opts sim.Options, runs *int) {
	const partitionAt, healAt = kernel.Time(2025 * time.Millisecond), kernel.Time(5025 * time.Millisecond)
	sim.Simulate(t, opts, func(s *sim.Sim) {
		countRun(runs)
		w, tt := s.World(), s.World().T()
		tr := newTraffic(50*time.Millisecond, vt(9*time.Second), "n1", "n2")
		nodes := map[string]*kernel.Node{"n1": s.Node("n1", tr.setup(s, "n1", nil)), "n2": s.Node("n2", tr.setup(s, "n2", nil))}
		w.Plan(fault.Script(
			fault.Event{At: partitionAt, Kind: fault.KindPartition, Groups: [][]string{{"n1"}, {"n2"}}},
			fault.Event{At: healAt, Kind: fault.KindHeal},
		))
		w.RunFor(10 * time.Second)
		tr.check(tt)

		recs := w.Sim.Records()
		hb, idle := opts.Remote.Heartbeat, opts.Remote.IdleTimeout
		for _, dir := range [][2]string{{"n1", "n2"}, {"n2", "n1"}} {
			a, b := nodes[dir[0]], dir[1]
			downs := sessionRecs(recs, a, "lockstep.remote.down", "out", b)
			if len(downs) == 0 || attr(downs[0], "reason") != "idle" {
				tt.Fatalf("%s's first session down to %s: %s, want reason idle", dir[0], b, describe(s, downs))
			}
			down := downs[0].At
			// The session last received a PONG at most a Heartbeat before the partition, and the
			// next tick after IdleTimeout without one closes it.
			if lo, hi := partitionAt.Add(idle-hb-10*time.Millisecond), partitionAt.Add(idle+hb); down < lo || down > hi {
				tt.Errorf("%s's session to %s went down idle at %s, want in [%s, %s]", dir[0], b, down, lo, hi)
			}
			ups := slices.DeleteFunc(sessionRecs(recs, a, "lockstep.remote.up", "out", b), func(r kernel.Record) bool { return r.At < down })
			if len(ups) == 0 || ups[0].At < healAt {
				tt.Errorf("%s's sessions to %s up after the idle close: %s, want the first after the heal at %s", dir[0], b, describe(s, ups), healAt)
			}
			var failures []string
			for _, r := range nodeRecords(recs, a, "lockstep.remote.redial") {
				if attr(r, "peer") == b && r.At > down && r.At < healAt {
					failures = append(failures, attr(r, "failures"))
				}
			}
			if len(failures) < 2 {
				tt.Errorf("%s redialed %s %d times during the partition, want at least 2", dir[0], b, len(failures))
			}
			for i, f := range failures {
				if f != strconv.Itoa(i+1) {
					tt.Errorf("%s's redials of %s during the partition count failures %q, want 1, 2, ...", dir[0], b, failures)
					break
				}
			}
			// Messages sent before the partition or after the idle close arrive; the ones written on
			// the session during the partition are lost.
			tr.expectReceived(tt, trafficKey{from: dir[0], inc: 1, to: b}, func(at kernel.Time) (bool, bool) {
				return at < partitionAt || at > down, at != down
			})
		}
		if n := len(ofKind(recs, "lockstep.remote.discard")); n != 0 {
			tt.Errorf("%d frames discarded, want none: %s", n, describe(s, ofKind(recs, "lockstep.remote.discard")))
		}
	})
}

// AT-REM-16 (defined here: the spec gives it one line), REM-200, REM-320: from 1.025 s to
// 4.025 s the link from n1 to n2 loses 30% of its datagrams, while n1 sends n2 a message every
// 50 ms. Each lost datagram has its lockstep.remote.loss record on n1; n2's session sees each lost
// data frame as part of a gap, recorded with lockstep.remote.gap, and accepts the frames after it.
// The lost data frames are exactly the ones the gaps skip, every other loss is a PING, and every
// message that was not lost arrives once, in order. IdleTimeout is long enough that the session
// stays up.
func TestATREM16LinkLoss(t *testing.T) {
	rc := fastRemote
	rc.IdleTimeout = 5 * time.Second
	runATREM16(t, remoteOpts(5, 7*time.Second, rc), nil)
}

func runATREM16(t *testing.T, opts sim.Options, runs *int) {
	const lossAt, resetAt = kernel.Time(1025 * time.Millisecond), kernel.Time(4025 * time.Millisecond)
	pingBytes := strconv.Itoa(len(remoting.AppendFrame(nil, &remoting.Frame{Kind: remoting.KindPing})))
	sim.Simulate(t, opts, func(s *sim.Sim) {
		countRun(runs)
		w, tt := s.World(), s.World().T()
		tr := newTraffic(50*time.Millisecond, vt(6*time.Second), "n1", "n2")
		tr.senders = []string{"n1"}
		n1 := s.Node("n1", tr.setup(s, "n1", nil))
		n2 := s.Node("n2", tr.setup(s, "n2", nil))
		w.Plan(fault.Script(
			fault.Event{At: lossAt, Kind: fault.KindLink, Node: "n1", Peer: "n2",
				Link: &simnet.Link{Latency: time.Millisecond, Jitter: 4 * time.Millisecond, DropPPM: 300_000}},
			fault.Event{At: resetAt, Kind: fault.KindLinkReset, Node: "n1", Peer: "n2"},
		))
		w.RunFor(7 * time.Second)
		tr.check(tt)

		recs := w.Sim.Records()
		losses := ofKind(recs, "lockstep.remote.loss")
		if len(losses) == 0 {
			tt.Fatal("no lockstep.remote.loss record")
		}
		for _, r := range losses {
			if keys := attrKeys(r); !slices.Equal(keys, []string{"from", "to", "conn", "bytes"}) {
				tt.Errorf("lockstep.remote.loss attributes %v, want from, to, conn, bytes", keys)
			}
			if r.Node != n1.ID() || attr(r, "from") != "n1" || attr(r, "to") != "n2" || r.At < lossAt || r.At >= resetAt {
				tt.Errorf("loss record %s, want n1's, to n2, during the loss", describe(s, []kernel.Record{r}))
			}
		}
		// The data frames each gap skips were lost, each with its loss record at its send time.
		sends := map[string]kernel.Record{} // n1's send records of MSG frames, by seq; read by key
		for _, r := range nodeRecords(recs, n1, "lockstep.remote.send") {
			if attr(r, "kind") == "MSG" {
				sends[attr(r, "seq")] = r
			}
		}
		unmatched := slices.Clone(losses)
		lostData := 0
		for _, g := range nodeRecords(recs, n2, "lockstep.remote.gap") {
			if keys := attrKeys(g); !slices.Equal(keys, []string{"peer", "conn", "want", "got"}) {
				tt.Errorf("lockstep.remote.gap attributes %v, want peer, conn, want, got", keys)
			}
			want, err1 := strconv.ParseUint(attr(g, "want"), 10, 64)
			got, err2 := strconv.ParseUint(attr(g, "got"), 10, 64)
			if err1 != nil || err2 != nil || got <= want || attr(g, "peer") != "n1" {
				tt.Fatalf("gap record %s", describe(s, []kernel.Record{g}))
			}
			for seq := want; seq < got; seq++ {
				lostData++
				send, ok := sends[strconv.FormatUint(seq, 10)]
				i := slices.IndexFunc(unmatched, func(l kernel.Record) bool {
					return ok && l.At == send.At && attr(l, "bytes") == attr(send, "bytes") && attr(l, "conn") == attr(g, "conn")
				})
				if i < 0 {
					tt.Errorf("frame %d, skipped by gap %s, has no loss record", seq, describe(s, []kernel.Record{g}))
					continue
				}
				unmatched = slices.Delete(unmatched, i, i+1)
			}
		}
		if lostData == 0 {
			tt.Error("no gap record: no data frame was lost")
		}
		for _, l := range unmatched {
			if attr(l, "bytes") != pingBytes {
				tt.Errorf("loss record %s is neither a skipped data frame nor a PING", describe(s, []kernel.Record{l}))
			}
		}
		key := trafficKey{from: "n1", inc: 1, to: "n2"}
		if got, sent := len(slices.DeleteFunc(tr.received(key), func(b bool) bool { return !b })), len(tr.sent[key]); got != sent-lostData {
			tt.Errorf("%d of %d messages received with %d data frames lost, want %d", got, sent, lostData, sent-lostData)
		}
		for _, kind := range []string{"lockstep.remote.down", "lockstep.remote.discard"} {
			if n := len(ofKind(recs, kind)); n != 0 {
				tt.Errorf("%d %s records, want none", n, kind)
			}
		}
	})
}

// askNote is what an asker receives: the order to start an ask, or the outcome of one.
type askNote struct {
	ask bool
	o   *askOutcome // the outcome's ask
	err error
}

// askOutcome is the outcome of one ask: when it was sent and resolved, and its error.
type askOutcome struct {
	sent, done kernel.Time
	err        error
}

// pingAsker returns an actor that asks target with a Ping and a timeout of an hour at each of times,
// relative to its start, and adds each ask to *log, which shows its outcome once the actor
// receives it (done is 0 until then).
func pingAsker(w interface{ Now() kernel.Time }, target lockstep.PID[Ping], log *[]*askOutcome, times ...time.Duration) *actor[askNote] {
	return &actor[askNote]{
		started: func(ctx *lockstep.Context[askNote]) {
			for _, d := range times {
				ctx.After(d, askNote{ask: true})
			}
		},
		receive: func(ctx *lockstep.Context[askNote], m askNote) {
			if !m.ask {
				m.o.done, m.o.err = w.Now(), m.err
				return
			}
			o := &askOutcome{sent: w.Now()}
			*log = append(*log, o)
			r := lockstep.Ask(ctx, target, buildPing(1), lockstep.WithTimeout(time.Hour))
			lockstep.Pipe(ctx, r, func(_ int, err error) askNote { return askNote{o: o, err: err} })
		},
	}
}

// timedWatcher watches target from its start and calls onTerm with every Terminated it gets.
type timedWatcher struct {
	target lockstep.Address
	onTerm func(lockstep.Terminated)
}

func (w *timedWatcher) Started(ctx *lockstep.Context[Tick]) {
	if err := lockstep.Watch(ctx, w.target); err != nil {
		panic(err)
	}
}

func (*timedWatcher) Receive(*lockstep.Context[Tick], Tick) {}

func (w *timedWatcher) Signal(_ *lockstep.Context[Tick], sig lockstep.Signal) {
	if t, ok := sig.(lockstep.Terminated); ok {
		w.onTerm(t)
	}
}

// simClock gives an actor the simulation's virtual time.
type simClock struct{ s *sim.Sim }

func (c simClock) Now() kernel.Time { return c.s.World().Sim.Now() }

// AT-REM-17 (defined here: the spec gives it one line), REM-130, REM-145, REM-294: n2 crashes at
// 1.025 s and restarts, while n1 sends its sink a message every 50 ms, asks its silent actor
// (which never answers) at 0.5 s and 2.5 s, and watches its actor b. n1 learns of the restart from
// the handshake of its next session, which shows incarnation 2, and in that one event records
// lockstep.remote.restart, fails every pending ask to n2 with an error that matches
// remote.ErrPeerRestarted and lockstep.ErrTerminated, ends the watch with Failed of that error,
// and makes the frames queued for incarnation 1 dead letters "peer-restarted". Messages sent after
// it reach the new incarnation.
//
// In "after-idle" n2 restarts at 3.025 s: n1's session went down idle meanwhile, and its frames
// waited in the queue while n1 redialed. In "before-idle" n2 restarts at 1.325 s: n1 still writes
// on its session to incarnation 1, n2 answers with CLOSE unknown-connection, and n1 dials again at
// once.
func TestATREM17PeerRestart(t *testing.T) {
	for _, restartAt := range []time.Duration{3025 * time.Millisecond, 1325 * time.Millisecond} {
		name := "after-idle"
		if restartAt < time.Second+fastRemote.IdleTimeout {
			name = "before-idle"
		}
		t.Run(name, func(t *testing.T) { runATREM17(t, remoteOpts(5, 6*time.Second, fastRemote), restartAt, nil) })
	}
}

func runATREM17(t *testing.T, opts sim.Options, restartAt time.Duration, runs *int) {
	const crashAt = kernel.Time(1025 * time.Millisecond)
	sim.Simulate(t, opts, func(s *sim.Sim) {
		countRun(runs)
		w, tt := s.World(), s.World().T()
		tr := newTraffic(50*time.Millisecond, vt(5*time.Second), "n1", "n2")
		tr.senders = []string{"n1"}
		watched := lockstep.PIDAt[Tick]("n2", "/user/b").Address()
		var asks []*askOutcome
		type term struct {
			at kernel.Time
			t  lockstep.Terminated
		}
		var terms []term
		n1 := s.Node("n1", tr.setup(s, "n1", func(sys *lockstep.System) {
			spawn(sys, "asker", pingAsker(simClock{s}, lockstep.PIDAt[Ping]("n2", "/user/silent"), &asks, 500*time.Millisecond, 2500*time.Millisecond))
			if _, err := lockstep.Spawn(sys, "watcher", func() lockstep.Actor[Tick] {
				return &timedWatcher{target: watched, onTerm: func(t lockstep.Terminated) { terms = append(terms, term{w.Sim.Now(), t}) }}
			}); err != nil {
				panic(err)
			}
		}))
		s.Node("n2", tr.setup(s, "n2", func(sys *lockstep.System) {
			spawn(sys, "silent", &actor[Ping]{})
			spawn(sys, "b", &actor[Tick]{})
		}))
		w.Plan(fault.Script(
			fault.Event{At: crashAt, Kind: fault.KindCrash, Node: "n2"},
			fault.Event{At: vt(restartAt), Kind: fault.KindRestart, Node: "n2"},
		))
		w.RunFor(6 * time.Second)
		tr.check(tt)

		recs := w.Sim.Records()
		restarts := nodeRecords(recs, n1, "lockstep.remote.restart")
		if len(restarts) != 1 || attr(restarts[0], "peer") != "n2" || attr(restarts[0], "old-inc") != "1" || attr(restarts[0], "new-inc") != "2" {
			tt.Fatalf("n1's restart records: %s, want one of n2 from incarnation 1 to 2", describe(s, restarts))
		}
		at := restarts[0].At
		ups := sessionRecs(recs, n1, "lockstep.remote.up", "out", "n2")
		if len(ups) != 2 || ups[1].At != at || attr(ups[1], "peer-inc") != "2" {
			tt.Errorf("n1's sessions to n2 up: %s, want the second at %s with peer-inc 2", describe(s, ups), at)
		}
		// The session to incarnation 1 went down before the restart showed: idle, or closed by the
		// new incarnation's CLOSE unknown-connection.
		downs := sessionRecs(recs, n1, "lockstep.remote.down", "out", "n2")
		wantReason := "idle"
		if vt(restartAt) < crashAt.Add(opts.Remote.IdleTimeout) {
			wantReason = "closed"
		}
		if len(downs) == 0 || attr(downs[0], "reason") != wantReason || downs[0].At > at {
			tt.Fatalf("n1's sessions to n2 down: %s, want the first with reason %s before %s", describe(s, downs), wantReason, at)
		}
		down := downs[0].At

		// Every ask sent before the restart showed fails at that moment; the later one waits.
		if len(asks) != 2 {
			tt.Fatalf("%d asks sent, want 2", len(asks))
		}
		for _, o := range asks {
			switch {
			case o.sent < at && (o.done != at || !peerRestarted(o.err)):
				tt.Errorf("ask sent at %s ended at %s with %v, want at %s with remote.ErrPeerRestarted and lockstep.ErrTerminated", o.sent, o.done, o.err, at)
			case o.sent > at && o.done != 0:
				tt.Errorf("ask sent at %s to incarnation 2 ended at %s with %v, want no outcome", o.sent, o.done, o.err)
			}
		}
		if len(terms) != 1 || terms[0].at != at || terms[0].t.Addr != watched ||
			terms[0].t.Reason.Kind != lockstep.StopFailed || !peerRestarted(terms[0].t.Reason.Err) {
			tt.Errorf("the watcher got %v, want one Terminated of %s at %s, Failed with remote.ErrPeerRestarted", terms, watched, at)
		}

		// The frames queued for incarnation 1 after its session went down are dead letters
		// "peer-restarted", recorded after the restart record: the messages, and the ask sent
		// meanwhile. The messages written on that session after the crash are lost.
		var queued, deadNums, deadPings int
		key := trafficKey{from: "n1", inc: 1, to: "n2"}
		for _, sent := range tr.sent[key] {
			if sent >= down && sent < at {
				queued++
			}
		}
		// A message sent at the very time of the down was written on the session if it was sent
		// before the down.
		for _, r := range nodeRecords(recs, n1, "lockstep.remote.send") {
			if r.At == down && attr(r, "kind") == "MSG" {
				queued--
			}
		}
		for _, r := range nodeRecords(recs, n1, "lockstep.deadletter") {
			if attr(r, "reason") != "peer-restarted" {
				tt.Errorf("dead letter %s, want reason peer-restarted", describe(s, []kernel.Record{r}))
			}
			if r.At != at || r.Seq < restarts[0].Seq {
				tt.Errorf("dead letter %s, want it after the restart record at %s", describe(s, []kernel.Record{r}), at)
			}
			switch attr(r, "msg") {
			case "sim_test.Num":
				deadNums++
			case "sim_test.Ping":
				deadPings++
			}
		}
		wantPings := 0
		if asks[1].sent > down && asks[1].sent < at {
			wantPings = 1
		}
		if deadNums != queued || deadPings != wantPings {
			tt.Errorf("%d messages and %d asks dead-lettered peer-restarted, want the %d and %d queued between %s and %s",
				deadNums, deadPings, queued, wantPings, down, at)
		}
		tr.expectReceived(tt, key, func(t kernel.Time) (bool, bool) {
			switch {
			case t < crashAt:
				return true, true
			case t > crashAt && t < down, t > down && t < at:
				return false, true
			case t > at:
				return true, true
			}
			return false, false
		})
	})
}

// AT-REM-18 (defined here: the spec gives it one line), REM-090, REM-330, SIM-140: n2 is paused
// from 1.025 s to 4.025 s, while n1 and n2 send each other a message every 50 ms. While n2 is
// paused, n1's session to it receives nothing and goes down idle, n1 then redials, and n2 runs
// nothing and sends nothing: a message the body sends from n2 at 2.5 s leaves only at the resume,
// on n2's old session, which n1 closed idle meanwhile, so n1 discards it (unknown-connection).
// After the resume both nodes have sessions up again, and every traffic message arrives, in order
// and once: the ones n1 wrote before its idle close wait parked at n2, and the later ones wait in
// n1's queue.
func TestATREM18Pause(t *testing.T) { runATREM18(t, remoteOpts(5, 10*time.Second, fastRemote), nil) }

func runATREM18(t *testing.T, opts sim.Options, runs *int) {
	const pauseAt, resumeAt = kernel.Time(1025 * time.Millisecond), kernel.Time(4025 * time.Millisecond)
	sim.Simulate(t, opts, func(s *sim.Sim) {
		countRun(runs)
		w, tt := s.World(), s.World().T()
		tr := newTraffic(50*time.Millisecond, vt(9*time.Second), "n1", "n2")
		var direct []int
		n1 := s.Node("n1", tr.setup(s, "n1", func(sys *lockstep.System) {
			spawn(sys, "direct", &actor[Num]{receive: func(_ *lockstep.Context[Num], m Num) { direct = append(direct, m.N) }})
		}))
		n2 := s.Node("n2", tr.setup(s, "n2", nil))
		w.Plan(fault.Script(
			fault.Event{At: pauseAt, Kind: fault.KindPause, Node: "n2"},
			fault.Event{At: resumeAt, Kind: fault.KindResume, Node: "n2"},
		))
		w.RunFor(2500 * time.Millisecond)
		if err := sim.Send(s, "n2", lockstep.PIDAt[Num]("n1", "/user/direct"), Num{7}); err != nil {
			tt.Fatalf("Send from the paused n2: %v", err)
		}
		w.RunFor(7500 * time.Millisecond)
		tr.check(tt)

		recs := w.Sim.Records()
		downs := sessionRecs(recs, n1, "lockstep.remote.down", "out", "n2")
		if len(downs) == 0 || attr(downs[0], "reason") != "idle" || downs[0].At <= pauseAt || downs[0].At >= resumeAt {
			tt.Fatalf("n1's sessions to n2 down: %s, want the first idle while n2 is paused", describe(s, downs))
		}
		if ups := sessionRecs(recs, n1, "lockstep.remote.up", "out", "n2"); len(ups) != 2 || ups[1].At < resumeAt {
			tt.Errorf("n1's sessions to n2 up: %s, want a second one after the resume at %s", describe(s, ups), resumeAt)
		}
		if ups := sessionRecs(recs, n2, "lockstep.remote.up", "out", "n1"); len(ups) < 2 || ups[len(ups)-1].At < resumeAt {
			tt.Errorf("n2's sessions to n1 up: %s, want one after the resume at %s", describe(s, ups), resumeAt)
		}
		redials := slices.DeleteFunc(nodeRecords(recs, n1, "lockstep.remote.redial"), func(r kernel.Record) bool {
			return r.At <= downs[0].At || r.At >= resumeAt
		})
		if len(redials) == 0 {
			tt.Error("n1 did not redial n2 while it was paused")
		}
		// The paused n2 runs nothing: it only defers its events and parks its arrivals, and sends
		// no datagram. The body's send at 2.5 s is recorded at once.
		for _, r := range w.Sim.Records() {
			body := r.At == vt(2500*time.Millisecond) && (r.Kind == "lockstep.send" || r.Kind == "lockstep.remote.send")
			if r.Node == n2.ID() && r.At > pauseAt && r.At < resumeAt && r.Kind != "net.defer" && r.Kind != "kernel.defer" && !body {
				tt.Errorf("record of the paused n2: %s", describe(s, []kernel.Record{r}))
			}
		}
		if msgs := sentDatagrams(recs, n2, "MSG"); !slices.Contains(msgs, resumeAt) {
			tt.Errorf("n2 sent MSG datagrams at %v, want one at the resume %s: the one sent while it was paused", msgs, resumeAt)
		}
		for _, k := range []trafficKey{{"n1", 1, "n2"}, {"n2", 1, "n1"}} {
			tr.expectReceived(tt, k, func(kernel.Time) (bool, bool) { return true, true })
		}
		// That message went on n2's session from before the pause, which n1 had closed idle
		// meanwhile: n1 discards it as on an unknown connection, so it is lost, not delayed.
		var seq string
		for _, r := range nodeRecords(recs, n2, "lockstep.remote.send") {
			if strings.HasSuffix(attr(r, "to"), "/user/direct#0") {
				seq = attr(r, "seq")
			}
		}
		lost := slices.ContainsFunc(nodeRecords(recs, n1, "lockstep.remote.discard"), func(r kernel.Record) bool {
			return attr(r, "kind") == "MSG" && attr(r, "seq") == seq && attr(r, "reason") == "unknown-connection" && r.At > resumeAt
		})
		if len(direct) != 0 || seq == "" || !lost {
			tt.Errorf("the message sent from the paused n2 (seq %q) arrived %v times, discarded after the resume %t; want it discarded unknown-connection", seq, len(direct), lost)
		}
	})
}

// AT-REM-19 (defined here: the spec gives it one line), REM-090: with Heartbeat 1 s,
// IdleTimeout 3 s and a fixed latency of 10 ms, n1 and n3 dial n2 at 1 ms, so their sessions are
// up at 21 ms and the cores tick at every whole second. A dialer writes PING at each tick at which
// its session has written or received nothing for a Heartbeat: from 2 s on, every second, and n2
// answers each with PONG 10 ms later. At 5.5 s the link from n2 to n1 is cut, so n1 receives
// nothing after the PONG of 5.020 s and closes its session idle at the 9 s tick, the first at
// least 3 s later, and n2 sees CLOSE idle at 9.010 s. The link from n3 to n2 is cut too, so n2
// receives nothing from n3 after the PING of 5.010 s and closes that incoming session idle at its
// own 9 s tick, while n3 closes its side then as well.
//
// n4 pins the boundary: the link from n2 to n4 has 990 ms of latency, so n4's session comes up at
// 1.001 s and each PONG arrives on a whole second. The PONG sent at 5.010 s is lost in flight by
// the cut at 5.5 s, so n4 last received at exactly 5 s and closes idle at the 8 s tick, where the
// silence equals IdleTimeout: the core closes at IdleTimeout, not after it.
func TestATREM19HeartbeatTiming(t *testing.T) {
	o := remoteOpts(3, 12*time.Second, remote.Config{Heartbeat: time.Second, IdleTimeout: 3 * time.Second})
	o.Net = simnet.Config{Default: simnet.Link{Latency: 10 * time.Millisecond}}
	runATREM19(t, o, nil)
}

func runATREM19(t *testing.T, opts sim.Options, runs *int) {
	sim.Simulate(t, opts, func(s *sim.Sim) {
		countRun(runs)
		w, tt := s.World(), s.World().T()
		n1 := s.Node("n1", noSetup)
		n2 := s.Node("n2", noSetup)
		n3 := s.Node("n3", noSetup)
		n4 := s.Node("n4", noSetup)
		slow := w.Net.Link(n2.ID(), n4.ID())
		slow.Latency = 990 * time.Millisecond
		w.Net.SetLink(n2.ID(), n4.ID(), slow)
		w.Plan(fault.Script(
			fault.Event{At: vt(5500 * time.Millisecond), Kind: fault.KindCut, Node: "n2", Peer: "n1"},
			fault.Event{At: vt(5500 * time.Millisecond), Kind: fault.KindCut, Node: "n3", Peer: "n2"},
			fault.Event{At: vt(5500 * time.Millisecond), Kind: fault.KindCut, Node: "n2", Peer: "n4"},
		))
		w.RunFor(time.Millisecond)
		for _, from := range []string{"n1", "n3", "n4"} {
			if err := s.Connect(from, "n2"); err != nil {
				tt.Fatalf("Connect(%s, n2): %v", from, err)
			}
		}
		w.RunFor(11 * time.Second)

		recs := w.Sim.Records()
		// fromTwo returns the whole seconds from 2 s to to s, each plus offset: the first tick
		// that pings is the 2 s one.
		fromTwo := func(to int, offset time.Duration) []kernel.Time {
			var out []kernel.Time
			for i := 2; i <= to; i++ {
				out = append(out, vt(time.Duration(i)*time.Second+offset))
			}
			return out
		}
		for _, n := range []*kernel.Node{n1, n3} {
			if got := timesOf(sessionRecs(recs, n, "lockstep.remote.up", "out", "n2")); !slices.Equal(got, []kernel.Time{vt(21 * time.Millisecond)}) {
				tt.Errorf("%s's sessions to n2 up at %v, want [21ms]", n.Name(), got)
			}
			if got, want := sentDatagrams(recs, n, "PING"), fromTwo(8, 0); !slices.Equal(got, want) {
				tt.Errorf("%s wrote PING at %v, want %v", n.Name(), got, want)
			}
			if got := sessionRecs(recs, n, "lockstep.remote.down", "out", "n2"); len(got) != 1 || got[0].At != vt(9*time.Second) || attr(got[0], "reason") != "idle" {
				tt.Errorf("%s's sessions to n2 down: %s, want one idle at 9s", n.Name(), describe(s, got))
			}
		}
		if got, want := sentDatagrams(recs, n1, "CLOSE"), []kernel.Time{vt(9 * time.Second)}; !slices.Equal(got, want) {
			tt.Errorf("n1 wrote CLOSE at %v, want %v", got, want)
		}
		if got := timesOf(sessionRecs(recs, n4, "lockstep.remote.up", "out", "n2")); !slices.Equal(got, []kernel.Time{vt(1001 * time.Millisecond)}) {
			tt.Errorf("n4's sessions to n2 up at %v, want [1.001s]", got)
		}
		if got, want := sentDatagrams(recs, n4, "PING"), fromTwo(7, 0); !slices.Equal(got, want) {
			tt.Errorf("n4 wrote PING at %v, want %v", got, want)
		}
		if got := sessionRecs(recs, n4, "lockstep.remote.down", "out", "n2"); len(got) != 1 || got[0].At != vt(8*time.Second) || attr(got[0], "reason") != "idle" {
			tt.Errorf("n4's sessions to n2 down: %s, want one idle at 8s, exactly IdleTimeout after its last receive", describe(s, got))
		}
		if got := sessionRecs(recs, n2, "lockstep.remote.down", "in", "n4"); len(got) != 1 || got[0].At != vt(8010*time.Millisecond) || attr(got[0], "reason") != "closed" {
			tt.Errorf("n2's sessions from n4 down: %s, want one closed at 8.01s", describe(s, got))
		}
		// n2 answers every PING that arrives: those of n1 at x.010 s from 2 s to 8 s (the ones
		// after 5.5 s are lost on the way back), those of n3 up to 5.010 s, and those of n4 from
		// 2 s to 7 s (the ones after 5.5 s are cut on the way back).
		want := append(fromTwo(8, 10*time.Millisecond), fromTwo(5, 10*time.Millisecond)...)
		want = append(want, fromTwo(7, 10*time.Millisecond)...)
		slices.Sort(want)
		if got := sentDatagrams(recs, n2, "PONG"); !slices.Equal(got, want) {
			tt.Errorf("n2 wrote PONG at %v, want %v", got, want)
		}
		if got := sessionRecs(recs, n2, "lockstep.remote.down", "in", "n1"); len(got) != 1 || got[0].At != vt(9010*time.Millisecond) || attr(got[0], "reason") != "closed" {
			tt.Errorf("n2's sessions from n1 down: %s, want one closed at 9.01s", describe(s, got))
		}
		if got := sessionRecs(recs, n2, "lockstep.remote.down", "in", "n3"); len(got) != 1 || got[0].At != vt(9*time.Second) || attr(got[0], "reason") != "idle" {
			tt.Errorf("n2's sessions from n3 down: %s, want one idle at 9s", describe(s, got))
		}
		if n := len(ofKind(recs, "lockstep.remote.dial")); n != 3 {
			tt.Errorf("%d dial records, want 3: nothing waits for a session after the idle closes", n)
		}
	})
}

// AT-REM-20 (defined here: the spec gives it one line), REM-130, REM-200, REM-294: three nodes
// under fault.Random crashes (with restarts) and partitions for 45 s, then 15 s of recovery. Each
// node sends each other one a message every 100 ms, asks its silent actor every second, and
// watches its actor b. Across seeds:
//
//   - per-pair FIFO and at-most-once delivery hold for every stream of messages;
//   - every restart a core records (peer, old and new incarnation) fails, in that event, every ask
//     its node's asker had pending to that peer, with remote.ErrPeerRestarted and
//     lockstep.ErrTerminated, and no ask ends any other way;
//   - the first restart of a peer a core records ends its node's watch of that peer's b with
//     Failed of the same error, at that moment, and no watch ends any other way;
//   - every dead letter is "peer-restarted" and recorded in the event of a restart record of its
//     node for its peer;
//   - every message sent from 5 s into the recovery arrives.
func TestATREM20RandomFaults(t *testing.T) {
	runATREM20(t, remoteOpts(20, 60*time.Second, fastRemote), nil)
}

func runATREM20(t *testing.T, opts sim.Options, runs *int) {
	sim.Simulate(t, opts, func(s *sim.Sim) {
		countRun(runs)
		w, tt := s.World(), s.World().T()
		nodes := []string{"n1", "n2", "n3"}
		tr := newTraffic(100*time.Millisecond, w.End()-kernel.Time(2*time.Second), nodes...)
		type ask struct {
			from string
			inc  uint32
			to   string
			o    *askOutcome
		}
		type term struct {
			node string
			inc  uint32
			at   kernel.Time
			t    lockstep.Terminated
		}
		var asks []ask
		var terms []term
		for _, name := range nodes {
			s.Node(name, tr.setup(s, name, func(sys *lockstep.System) {
				inc := sys.Incarnation()
				spawn(sys, "silent", &actor[Ping]{})
				spawn(sys, "b", &actor[Tick]{})
				for _, peer := range nodes {
					if peer == name {
						continue
					}
					var log []*askOutcome
					every := make([]time.Duration, 0, 60)
					for d := time.Second; d < w.End().Sub(w.Sim.Now()); d += time.Second {
						every = append(every, d)
					}
					a := pingAsker(simClock{s}, lockstep.PIDAt[Ping](peer, "/user/silent"), &log, every...)
					receive := a.receive
					a.receive = func(ctx *lockstep.Context[askNote], m askNote) {
						n := len(log)
						receive(ctx, m)
						if len(log) > n {
							asks = append(asks, ask{from: name, inc: inc, to: peer, o: log[n]})
						}
					}
					spawn(sys, "asker-"+peer, a)
					if _, err := lockstep.Spawn(sys, "watcher-"+peer, func() lockstep.Actor[Tick] {
						return &timedWatcher{target: lockstep.PIDAt[Tick](peer, "/user/b").Address(), onTerm: func(t lockstep.Terminated) {
							terms = append(terms, term{node: name, inc: inc, at: w.Sim.Now(), t: t})
						}}
					}); err != nil {
						panic(err)
					}
				}
			}))
		}
		w.Plan(&fault.Random{
			Rules: []fault.Rule{
				{Kind: fault.KindCrash, Every: 6 * time.Second, MinFor: time.Second, MaxFor: 4 * time.Second},
				{Kind: fault.KindCrash, Every: 6 * time.Second, MinFor: 10 * time.Millisecond, MaxFor: 300 * time.Millisecond},
				{Kind: fault.KindPartition, Every: 6 * time.Second, MinFor: 500 * time.Millisecond, MaxFor: 4 * time.Second},
			},
			MaxDown: 2,
			Quiet:   15 * time.Second,
		})
		w.RunFor(w.End().Sub(w.Sim.Now()))
		tr.check(tt)

		recs := w.Sim.Records()
		if len(ofKind(recs, "kernel.crash")) == 0 || len(ofKind(recs, "fault.partition")) == 0 {
			tt.Fatalf("%d crashes and %d partitions, want both", len(ofKind(recs, "kernel.crash")), len(ofKind(recs, "fault.partition")))
		}
		restarts := ofKind(recs, "lockstep.remote.restart")
		if len(restarts) == 0 {
			tt.Fatal("no lockstep.remote.restart record")
		}
		// A restart record at time t on incarnation inc of node a, for peer b.
		type restartKey struct {
			node string
			inc  uint32
			peer string
		}
		first := map[restartKey]kernel.Time{} // read by key
		for _, r := range restarts {
			k := restartKey{nodeName(s, r.Node), r.Inc, attr(r, "peer")}
			if _, ok := first[k]; !ok {
				first[k] = r.At
			}
			for _, a := range asks {
				if a.from == k.node && a.inc == k.inc && a.to == k.peer && a.o.sent < r.At && (a.o.done == 0 || a.o.done > r.At) {
					tt.Errorf("ask %s#%d -> %s sent at %s ended at %s, want it failed at the restart record at %s", a.from, a.inc, a.to, a.o.sent, a.o.done, r.At)
				}
			}
		}
		for _, a := range asks {
			if a.o.done == 0 {
				continue
			}
			atRestart := slices.ContainsFunc(restarts, func(r kernel.Record) bool {
				return r.At == a.o.done && nodeName(s, r.Node) == a.from && r.Inc == a.inc && attr(r, "peer") == a.to
			})
			if !peerRestarted(a.o.err) || !atRestart {
				tt.Errorf("ask %s#%d -> %s sent at %s ended at %s with %v, want remote.ErrPeerRestarted at a restart record of %s on %s#%d",
					a.from, a.inc, a.to, a.o.sent, a.o.done, a.o.err, a.to, a.from, a.inc)
			}
		}
		wantTerms := len(first)
		for _, tm := range terms {
			at, ok := first[restartKey{tm.node, tm.inc, tm.t.Addr.Node()}]
			if !ok || tm.at != at || tm.t.Reason.Kind != lockstep.StopFailed || !peerRestarted(tm.t.Reason.Err) {
				tt.Errorf("watcher on %s#%d got %v at %s, want Failed with remote.ErrPeerRestarted at the first restart record of its peer", tm.node, tm.inc, tm.t, tm.at)
			}
		}
		if len(terms) != wantTerms {
			tt.Errorf("%d watches ended, want %d: one per first restart record of a peer", len(terms), wantTerms)
		}
		for _, r := range ofKind(recs, "lockstep.deadletter") {
			to := strings.TrimPrefix(attr(r, "to"), "lockstep://")
			peer, _, _ := strings.Cut(to, "/")
			restarted := slices.ContainsFunc(restarts, func(x kernel.Record) bool {
				return x.Node == r.Node && x.Inc == r.Inc && x.At == r.At && attr(x, "peer") == peer && x.Seq < r.Seq
			})
			if attr(r, "reason") != "peer-restarted" || !restarted {
				tt.Errorf("dead letter %s, want reason peer-restarted in the event of a restart of its peer", describe(s, []kernel.Record{r}))
			}
		}
		settled := w.RecoveryStart().Add(5 * time.Second)
		for _, from := range nodes {
			inc := s.System(from).Incarnation()
			for _, to := range nodes {
				if to != from {
					key := trafficKey{from: from, inc: inc, to: to}
					tr.expectReceived(tt, key, func(at kernel.Time) (bool, bool) { return true, at >= settled })
				}
			}
		}
	})
}

// AT-REM-21 (defined here: the spec gives it one line), REM-320, SIM-060: from 0.5 s both links
// between n1 and n2 have up to 50 ms of jitter and a 10% chance of up to 200 ms more, with FIFO
// off, while each node sends the other a message every 5 ms. The delays drawn would have
// reordered datagrams, but remoting sends every datagram FIFO, so every message arrives once and
// in order, and no frame is late or skipped.
func TestATREM21DelaysKeepOrder(t *testing.T) {
	runATREM21(t, remoteOpts(5, 5*time.Second, fastRemote), nil)
}

func runATREM21(t *testing.T, opts sim.Options, runs *int) {
	sim.Simulate(t, opts, func(s *sim.Sim) {
		countRun(runs)
		w, tt := s.World(), s.World().T()
		tr := newTraffic(5*time.Millisecond, vt(4*time.Second), "n1", "n2")
		s.Node("n1", tr.setup(s, "n1", nil))
		s.Node("n2", tr.setup(s, "n2", nil))
		jitter := &simnet.Link{Latency: time.Millisecond, Jitter: 50 * time.Millisecond, TailPPM: 100_000, Tail: 200 * time.Millisecond}
		w.Plan(fault.Script(
			fault.Event{At: vt(500 * time.Millisecond), Kind: fault.KindLink, Node: "n1", Peer: "n2", Link: jitter},
			fault.Event{At: vt(500 * time.Millisecond), Kind: fault.KindLink, Node: "n2", Peer: "n1", Link: jitter},
		))
		w.RunFor(5 * time.Second)
		tr.check(tt)

		recs := w.Sim.Records()
		for _, dir := range [][2]string{{"n1", "n2"}, {"n2", "n1"}} {
			if n := heldBackByFIFO(recs, dir[0], dir[1]); n == 0 {
				tt.Errorf("no MSG datagram from %s to %s drew a delay that would have reordered it", dir[0], dir[1])
			}
			tr.expectReceived(tt, trafficKey{from: dir[0], inc: 1, to: dir[1]}, func(kernel.Time) (bool, bool) { return true, true })
		}
		for _, kind := range []string{"lockstep.remote.gap", "lockstep.remote.discard", "lockstep.remote.down"} {
			if n := len(ofKind(recs, kind)); n != 0 {
				tt.Errorf("%d %s records, want none: %s", n, kind, describe(s, ofKind(recs, kind)))
			}
		}
	})
}

// AT-REM-26 (defined here: the spec gives it one line), REM-140, REM-145: with OutboundQueue 4,
// n1 sends six messages to n2 while a partition keeps every dial from completing, then asks n2.
// The first four wait in the queue; the fifth, the sixth and the ask's request are dead letters
// "queue-full", and the ask fails at the same virtual time, in a lockstep/remote/fail callback,
// with an error wrapping remote.ErrUnreachable. After the heal the four queued messages arrive in
// order.
func TestATREM26QueueFull(t *testing.T) {
	rc := fastRemote
	rc.OutboundQueue = 4
	runATREM26(t, remoteOpts(5, 5*time.Second, rc), nil)
}

func runATREM26(t *testing.T, opts sim.Options, runs *int) {
	sim.Simulate(t, opts, func(s *sim.Sim) {
		countRun(runs)
		w, tt := s.World(), s.World().T()
		var got []int
		n1 := s.Node("n1", noSetup)
		s.Node("n2", func(sys *lockstep.System) {
			spawn(sys, "r", &actor[Num]{receive: func(_ *lockstep.Context[Num], m Num) { got = append(got, m.N) }})
			spawn(sys, "echo", echo())
		})
		w.Plan(fault.Script(
			fault.Event{At: 0, Kind: fault.KindPartition, Groups: [][]string{{"n1"}, {"n2"}}},
			fault.Event{At: vt(2 * time.Second), Kind: fault.KindHeal},
		))
		w.RunFor(time.Second)
		r := lockstep.PIDAt[Num]("n2", "/user/r")
		for i := range 6 {
			if err := sim.Send(s, "n1", r, Num{i}); err != nil {
				tt.Fatalf("Send of %d: %v, want nil: a full queue is no error of Send", i, err)
			}
		}
		echo := lockstep.PIDAt[Ping]("n2", "/user/echo")
		at := w.Sim.Now()
		v, err := sim.Ask(s, "n1", echo, buildPing(7))
		if !errors.Is(err, remote.ErrUnreachable) || err.Error() != "remote: node unreachable: n2" || v != 0 || w.Sim.Now() != at {
			tt.Errorf("Ask = %d, %v at %s; want 0, remote: node unreachable: n2 at %s", v, err, w.Sim.Now(), at)
		}
		w.RunFor(3 * time.Second)

		recs := w.Sim.Records()
		var dead []string
		for _, rec := range nodeRecords(recs, n1, "lockstep.deadletter") {
			dead = append(dead, rec.At.String()+" "+attr(rec, "to")+" "+attr(rec, "reason")+" "+attr(rec, "msg"))
		}
		want := []string{
			at.String() + " " + r.String() + " queue-full sim_test.Num",
			at.String() + " " + r.String() + " queue-full sim_test.Num",
			at.String() + " " + echo.String() + " queue-full sim_test.Ping",
		}
		if !slices.Equal(dead, want) {
			tt.Errorf("n1's dead letters %q, want %q", dead, want)
		}
		var fails []kernel.Time
		for _, rec := range nodeRecords(recs, n1, "kernel.event") {
			if rec.Text == "lockstep/remote/fail" {
				fails = append(fails, rec.At)
			}
		}
		if !slices.Equal(fails, []kernel.Time{at}) {
			tt.Errorf("lockstep/remote/fail events at %v, want one at %s", fails, at)
		}
		if !slices.Equal(got, []int{0, 1, 2, 3}) {
			tt.Errorf("n2 received %v, want [0 1 2 3]: the queued messages, in order", got)
		}
	})
}

// redialDelay replays REM-100: after f consecutive failures, b = min(max, min << (f-1)) without
// overflow, and the delay is b/2 + r.Int64N(b - b/2 + 1) nanoseconds.
func redialDelay(f int, lo, hi time.Duration, r *rand.Rand) time.Duration {
	b := int64(hi)
	if shift := f - 1; shift < 63 && int64(lo) <= int64(hi)>>shift {
		b = int64(lo) << shift
	}
	return time.Duration(b/2 + r.Int64N(b-b/2+1))
}

// AT-REM-32 (defined here: the spec gives it one line), REM-020, REM-100: node a sends b a message
// every second, and partitions separate them from the start to 15 s and from 18 s to 25 s. Every
// lockstep.remote.redial record of a has the delay that REM-100's formula gives for its failure
// count, drawn in order from kernel.New(kernel.Config{Seed: seed}).Rand("lockstep/a/1/$remote"):
// the core draws from no other stream and draws nothing else from it. Each redial's timer waits
// exactly that delay: a's next dial of b, with attempt failures+1, is at the record's time plus
// the delay. The failure count grows by
// one per failed dial, saturates the base at RedialMax, and starts again at 1 after a session was
// up.
func TestATREM32RedialBackoff(t *testing.T) {
	rc := remote.Config{
		Heartbeat: 200 * time.Millisecond, IdleTimeout: time.Second,
		DialTimeout: 100 * time.Millisecond, RedialMin: 10 * time.Millisecond, RedialMax: time.Second,
	}
	runATREM32(t, remoteOpts(5, 30*time.Second, rc), nil)
}

func runATREM32(t *testing.T, opts sim.Options, runs *int) {
	sim.Simulate(t, opts, func(s *sim.Sim) {
		countRun(runs)
		w, tt := s.World(), s.World().T()
		tr := newTraffic(time.Second, vt(29*time.Second), "a", "b")
		tr.senders = []string{"a"}
		a := s.Node("a", tr.setup(s, "a", nil))
		s.Node("b", tr.setup(s, "b", nil))
		w.Plan(fault.Script(
			fault.Event{At: 0, Kind: fault.KindPartition, Groups: [][]string{{"a"}, {"b"}}},
			fault.Event{At: vt(15 * time.Second), Kind: fault.KindHeal},
			fault.Event{At: vt(18 * time.Second), Kind: fault.KindPartition, Groups: [][]string{{"a"}, {"b"}}},
			fault.Event{At: vt(25 * time.Second), Kind: fault.KindHeal},
		))
		w.RunFor(30 * time.Second)
		tr.check(tt)

		recs := w.Sim.Records()
		redials := ofKind(recs, "lockstep.remote.redial")
		r := kernel.New(kernel.Config{Seed: w.Seed()}).Rand("lockstep/a/1/$remote")
		lo, hi := opts.Remote.RedialMin, opts.Remote.RedialMax
		dials := nodeRecords(recs, a, "lockstep.remote.dial")
		var failures []int
		saturated := false
		for _, rec := range redials {
			if keys := attrKeys(rec); !slices.Equal(keys, []string{"peer", "failures", "delay"}) {
				tt.Errorf("lockstep.remote.redial attributes %v, want peer, failures, delay", keys)
			}
			f, err := strconv.Atoi(attr(rec, "failures"))
			if err != nil || rec.Node != a.ID() || attr(rec, "peer") != "b" {
				tt.Fatalf("redial record %s, want a's of peer b", describe(s, []kernel.Record{rec}))
			}
			failures = append(failures, f)
			saturated = saturated || lo<<(f-1) > hi
			want := redialDelay(f, lo, hi, r)
			if got, err := time.ParseDuration(attr(rec, "delay")); err != nil || got != want {
				tt.Errorf("redial %d after %d failures: delay %s, want %s", len(failures), f, attr(rec, "delay"), want)
			}
			// The timer waits exactly the recorded delay: a's next dial of b, attempt f+1, is at
			// the record's time plus the delay. Only a redial due after the run's end has none.
			next := slices.IndexFunc(dials, func(d kernel.Record) bool { return d.Seq > rec.Seq && attr(d, "peer") == "b" })
			switch due := rec.At.Add(want); {
			case next < 0 && due <= w.End():
				tt.Errorf("redial %d after %d failures at %s: no dial after it, want one at %s", len(failures), f, rec.At, due)
			case next >= 0 && (dials[next].At != due || attr(dials[next], "attempt") != strconv.Itoa(f+1)):
				tt.Errorf("redial %d after %d failures at %s: next dial %s, want attempt %d at %s",
					len(failures), f, rec.At, describe(s, dials[next:next+1]), f+1, due)
			}
		}
		// 1, 2, ... in the first partition, then 1, 2, ... again after the session that came up
		// after the first heal went down idle in the second.
		if len(failures) == 0 {
			tt.Fatal("no lockstep.remote.redial record")
		}
		restart := slices.Index(failures[1:], 1) + 1
		if failures[0] != 1 || restart == 0 {
			tt.Fatalf("failure counts %v, want 1, 2, ... twice", failures)
		}
		for i, f := range failures {
			want := i + 1
			if i >= restart {
				want -= restart
			}
			if f != want {
				tt.Fatalf("failure counts %v, want 1, 2, ... twice", failures)
			}
		}
		if !saturated {
			tt.Errorf("failure counts %v never saturate the base at RedialMax %s", failures, hi)
		}
		ups := sessionRecs(recs, a, "lockstep.remote.up", "out", "b")
		if len(ups) != 2 || ups[0].At < vt(15*time.Second) || ups[1].At < vt(25*time.Second) {
			tt.Errorf("a's sessions to b up: %s, want one after each heal", describe(s, ups))
		}
	})
}

// AT-REM-38 (defined here: the spec gives it one line), REM-020, §6: the remoting scenarios above
// replay with equal trace hashes. Under LOCKSTEP_CHECK_DETERMINISM=1, which sim reads once per
// Simulate call, every seed runs twice and faultline compares the two traces, so each body runs
// twice per seed. Each runs without the Options switch, so only the environment turns the
// comparison on.
func TestATREM38Determinism(t *testing.T) {
	t.Setenv("LOCKSTEP_CHECK_DETERMINISM", "1")
	const seeds = 2
	opts := func(d time.Duration, rc remote.Config) sim.Options {
		o := remoteOpts(seeds, d, rc)
		o.CheckDeterminism = false
		return o
	}
	loss, queue := fastRemote, fastRemote
	loss.IdleTimeout = 5 * time.Second
	queue.OutboundQueue = 4
	timing := opts(12*time.Second, remote.Config{Heartbeat: time.Second, IdleTimeout: 3 * time.Second})
	timing.Net = simnet.Config{Default: simnet.Link{Latency: 10 * time.Millisecond}}
	for _, tc := range []struct {
		name string
		run  func(t *testing.T, runs *int)
	}{
		{"AT-REM-15", func(t *testing.T, runs *int) { runATREM15(t, opts(10*time.Second, fastRemote), runs) }},
		{"AT-REM-16", func(t *testing.T, runs *int) { runATREM16(t, opts(7*time.Second, loss), runs) }},
		{"AT-REM-17", func(t *testing.T, runs *int) {
			runATREM17(t, opts(6*time.Second, fastRemote), 3025*time.Millisecond, runs)
		}},
		{"AT-REM-18", func(t *testing.T, runs *int) { runATREM18(t, opts(10*time.Second, fastRemote), runs) }},
		{"AT-REM-19", func(t *testing.T, runs *int) { runATREM19(t, timing, runs) }},
		{"AT-REM-20", func(t *testing.T, runs *int) { runATREM20(t, opts(60*time.Second, fastRemote), runs) }},
		{"AT-REM-21", func(t *testing.T, runs *int) { runATREM21(t, opts(5*time.Second, fastRemote), runs) }},
		{"AT-REM-26", func(t *testing.T, runs *int) { runATREM26(t, opts(5*time.Second, queue), runs) }},
		{"AT-REM-32", func(t *testing.T, runs *int) {
			rc := remote.Config{
				Heartbeat: 200 * time.Millisecond, IdleTimeout: time.Second,
				DialTimeout: 100 * time.Millisecond, RedialMin: 10 * time.Millisecond, RedialMax: time.Second,
			}
			runATREM32(t, opts(30*time.Second, rc), runs)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runs := 0
			tc.run(t, &runs)
			if runs != 2*seeds {
				t.Errorf("the body ran %d times for %d seeds, want %d: the determinism check did not run every seed twice", runs, seeds, 2*seeds)
			}
		})
	}
}
