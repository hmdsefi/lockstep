// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package cluster

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/faultline/kernel/fault"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/sim"
)

// AT-CLU-17, CLU-510, CLU-520, CLU-530. n1, n2 and n3 join through n1; n4 joins through n1 too, but
// is cut off until 2s. "early" on n1 subscribes at n1's boot, when the view holds only n1, and
// gets NodeUp of n2 and n3 as they join; "late" on n1 subscribes at 1.5s and gets one NodeUp per
// up member other than n1, in name order, right away. When n4 joins after the heal, both get one
// NodeUp of n4, and so does n4's own subscriber for each of the three others. Every signal matches
// a node.up record of its node, nothing is about self, and nothing goes down.
func TestATCLU17SubscribeNodeUp(t *testing.T) {
	const healAt = 2 * time.Second
	sim.Simulate(t, simOpts(5, 6*time.Second), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		c := newSimCluster(s)
		early := c.subNode("n1", simConfig("n1"), true)
		c.node("n2", simConfig("n1"))
		c.node("n3", simConfig("n1"))
		n4 := c.subNode("n4", simConfig("n1"), true)
		w.Faults.Inject(fault.Event{Kind: fault.KindPartition, Groups: [][]string{{"n4"}, {"n1", "n2", "n3"}}})
		w.RunFor(1500 * time.Millisecond)
		late := c.spawnSub("late")

		ids := func(names ...string) []string {
			var out []string
			for _, n := range names {
				out = append(out, "up "+n+" "+uitoa(c.id(n)))
			}
			return out
		}
		if got := early.sigs(); len(got) != 2 || !slices.Contains(got, ids("n2")[0]) || !slices.Contains(got, ids("n3")[0]) {
			tt.Errorf("early's signals %q, want one NodeUp each of n2 and n3", got)
		}
		at := w.Sim.Now()
		if err := c.ms["n1"].Subscribe(late.pid.Address()); err != nil {
			tt.Fatalf("Subscribe(late): %v", err)
		}
		w.RunFor(10 * time.Millisecond)
		if got := late.sigs(); !slices.Equal(got, ids("n2", "n3")) {
			tt.Errorf("late's signals right after Subscribe %q, want %q", got, ids("n2", "n3"))
		}
		for _, e := range late.log {
			if e.at < at || e.at > at.Add(time.Millisecond) {
				tt.Errorf("late got a signal at %s, want all right after its Subscribe at %s", e.at, at)
			}
		}
		if got := n4.sigs(); len(got) != 0 {
			tt.Errorf("n4's subscriber got %q while n4 was cut off, want nothing", got)
		}

		w.RunFor(vt(healAt).Sub(w.Sim.Now()))
		w.Faults.Inject(fault.Event{Kind: fault.KindHeal})
		w.RunFor(4 * time.Second)
		all := []string{"n1", "n2", "n3", "n4"}
		for _, n := range all {
			if v := c.view(n); len(v.Up()) != 4 {
				tt.Fatalf("%s's view after the heal: %v, want the four up", n, v.Members)
			}
		}
		if got, want := early.sigs()[2:], ids("n4"); !slices.Equal(got, want) {
			tt.Errorf("early's signals after the heal %q, want %q", got, want)
		}
		if got, want := late.sigs()[2:], ids("n4"); !slices.Equal(got, want) {
			tt.Errorf("late's signals after the heal %q, want %q", got, want)
		}
		if got := n4.sigs(); len(got) != 3 || !slices.Contains(got, ids("n1")[0]) || !slices.Contains(got, ids("n2")[0]) || !slices.Contains(got, ids("n3")[0]) {
			tt.Errorf("n4's subscriber got %q, want one NodeUp each of n1, n2 and n3", got)
		}
		for _, n := range all {
			ups := c.recs(n, "node.up")
			if len(ups) != 3 || len(c.recs(n, "node.down")) != 0 {
				tt.Errorf("%s's node.up records %s and %d node.down, want three and none", n, describe(ups), len(c.recs(n, "node.down")))
			}
			for _, r := range ups {
				if attr(r, "node") == n || attr(r, "id") != uitoa(c.id(attr(r, "node"))) {
					tt.Errorf("%s recorded %s, want only other nodes at their IDs", n, describe([]kernel.Record{r}))
				}
			}
		}
		// Each of n1's signals to early follows its node.up record, in the same order.
		var recs []string
		for _, r := range c.recs("n1", "node.up") {
			recs = append(recs, "up "+attr(r, "node")+" "+attr(r, "id"))
		}
		if got := early.sigs(); !slices.Equal(got, recs) {
			tt.Errorf("early's signals %q, want n1's node.up records %q", got, recs)
		}
		for _, l := range []*subLog{early, late, n4} {
			expectAlternation(tt, l)
		}
	})
}

// AT-CLU-18, CLU-510, CLU-530. n4 of four nodes crashes at 3s. Before the crash, sub on n1 watches
// n4's actor target. When n1 declares n4 Dead it records node.down left=false; sub's watch ends
// with Failed(ErrNodeDown), naming n4, and then sub gets NodeDown{n4, ID, false}. A new Watch of
// an actor on n4 ends at once with the same reason, and hands nothing to the transport. The other
// live nodes record the same node.down.
func TestATCLU18CrashNodeDown(t *testing.T) {
	names := []string{"n1", "n2", "n3", "n4"}
	const crashAt = 3 * time.Second
	bound := 2*time.Duration(len(names)-1)*DefaultProbeInterval + DefaultProbeTimeout + DefaultSuspicionTimeout + DefaultProbeInterval
	sim.Simulate(t, simOpts(5, crashAt+bound+3*time.Second), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		c := newSimCluster(s)
		sub := c.subNode("n1", simConfig(names...), true)
		c.node("n2", simConfig(names...))
		c.node("n3", simConfig(names...))
		target := c.subNode("n4", simConfig(names...), false)
		w.RunFor(crashAt - 100*time.Millisecond)
		addr := target.pid.Address()
		if err := sim.Send(s, "n1", sub.pid, subMsg{watch: addr}); err != nil {
			tt.Fatal(err)
		}
		w.RunFor(100 * time.Millisecond)
		id4 := c.id("n4")
		w.Faults.Inject(fault.Event{Kind: fault.KindCrash, Node: "n4"})
		w.RunFor(bound)

		downs := c.recs("n1", "node.down")
		if len(downs) != 1 || attrs(downs[0]) != "node=n4 id="+uitoa(id4)+" left=false" {
			tt.Fatalf("n1's node.down records %s, want one of n4 with left=false", describe(downs))
		}
		dead := c.recs("n1", "member")
		dead = slices.DeleteFunc(dead, func(r kernel.Record) bool { return attr(r, "to") != "dead" })
		if len(dead) != 1 || dead[0].At != downs[0].At {
			tt.Errorf("n1's Dead records %s, want one at its node.down at %s", describe(dead), downs[0].At)
		}
		wantSigs := []string{"up n2 " + uitoa(c.id("n2")), "up n3 " + uitoa(c.id("n3")), "up n4 " + uitoa(id4)}
		got := sub.sigs()
		if len(got) != 5 || !sameSet(got[:3], wantSigs) {
			tt.Fatalf("sub's signals %q, want NodeUp of n2, n3 and n4, then a Terminated and a NodeDown", got)
		}
		reason := lockstep.Failed(fmt.Errorf("%w: n4", ErrNodeDown))
		if want := []string{"terminated " + addr.String() + " " + reason.String(), "down n4 " + uitoa(id4) + " left=false"}; !slices.Equal(got[3:], want) {
			tt.Errorf("sub's signals at the node.down %q, want %q", got[3:], want)
		}
		term, ok := sub.log[3].sig.(lockstep.Terminated)
		if !ok || term.Reason.Kind != lockstep.StopFailed || !errors.Is(term.Reason.Err, ErrNodeDown) || term.Reason.Err.Error() != "cluster: node down: n4" {
			tt.Errorf("the watch ended with %#v, want Failed wrapping ErrNodeDown", sub.log[3].sig)
		}
		for _, e := range sub.log[3:] {
			if e.at < downs[0].At || e.at > downs[0].At.Add(10*time.Millisecond) {
				tt.Errorf("sub got %v at %s, want it right after n1's node.down at %s", e.sig, e.at, downs[0].At)
			}
		}
		for _, n := range names[1:3] {
			if d := c.recs(n, "node.down"); len(d) != 1 || attrs(d[0]) != "node=n4 id="+uitoa(id4)+" left=false" {
				tt.Errorf("%s's node.down records %s, want one of n4 with left=false", n, describe(d))
			}
		}

		// A new Watch of an actor on n4 ends at once.
		other := lockstep.PIDAt[subMsg]("n4", "/user/other").Address()
		at := w.Sim.Now()
		if err := sim.Send(s, "n1", sub.pid, subMsg{watch: other}); err != nil {
			tt.Fatal(err)
		}
		w.RunFor(10 * time.Millisecond)
		if got := sub.sigs()[5:]; len(got) != 1 || got[0] != "terminated "+other.String()+" "+reason.String() || sub.log[5].at != at {
			tt.Errorf("a new Watch on n4 gave %q, want one Terminated with %s at once", got, reason)
		}
		expectAlternation(tt, sub)
	})
}

// AT-CLU-21, CLU-620, CLU-520. n1, n2 and n3 join through n1, with SyncIntervals longer than the
// run, so that n1's table goes to the others only through the broadcast queue. At 2.3s n1 is told
// of x, Suspect, and y, which comes up and goes Dead, and sub on n1 gets NodeUp x, NodeUp y,
// NodeDown y. 1ms later, before n1 sent anyone a message, its membership actor fails and its
// supervisor restarts it, with an empty broadcast queue, so no other node ever hears of x or y.
// The restarted actor records start with restart=true, publishes no view and signals nothing; its
// table is the last view's. It arms x's suspicion timer and y's reap timer again in full: y is
// reaped DeadRetention after the restart, x is Dead SuspicionTimeout after it (the timer of the
// first incarnation does nothing), and sub, still subscribed, gets NodeDown x. Its probes count
// from seq 1 again.
func TestATCLU21RestartKeepsTable(t *testing.T) {
	const tellAt = 2300 * time.Millisecond
	const failAt = tellAt + time.Millisecond
	const retention = 3 * time.Second
	sim.Simulate(t, simOpts(5, failAt+DefaultSuspicionTimeout+3*time.Second), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		c := newSimCluster(s)
		cfg := Config{Seeds: []string{"n1"}, JoinRetryInterval: 500 * time.Millisecond, DeadRetention: retention}
		sub := c.subNode("n1", cfg, true)
		c.node("n2", cfg)
		c.node("n3", cfg)
		w.RunFor(tellAt)
		told := syncResp{From: toUpdate(c.view("n1").Self), Members: []update{
			{Node: "x", ID: 7, Inc: 2, State: Suspect}, {Node: "y", ID: 8, State: Alive}, {Node: "y", ID: 8, State: Dead},
		}}
		if err := sim.Send(s, "n1", lockstep.PIDAt[memberMsg]("n1", actorPath), memberMsg(told)); err != nil {
			tt.Fatal(err)
		}
		w.RunFor(failAt - tellAt)
		before := c.view("n1")
		sigsBefore := len(sub.log)
		if got := sub.sigs()[2:]; !slices.Equal(got, []string{"up x 7", "up y 8", "down y 8 left=false"}) {
			tt.Fatalf("sub's signals for x and y %q", got)
		}
		if err := s.InjectFail(lockstep.PIDAt[memberMsg]("n1", actorPath).Address(), errors.New("boom")); err != nil {
			tt.Fatal(err)
		}
		w.RunFor(DefaultSuspicionTimeout + 2*time.Second)

		starts := c.recs("n1", "start")
		if len(starts) != 2 || attrs(starts[1]) != "node=n1 id="+uitoa(c.id("n1"))+" inc=0 seeds=1 restart=true" {
			tt.Fatalf("n1's start records %s, want a second one with restart=true", describe(starts))
		}
		restart := starts[1].At
		if restart < vt(failAt) {
			tt.Fatalf("n1 restarted at %s, before the failure at %s", restart, vt(failAt))
		}
		// Nothing between the failure and x's suspicion timer but y's reap: no other view, no member,
		// no signal.
		var after []string
		for _, r := range clusterRecs(c, "n1") {
			if k := strings.TrimPrefix(r.Kind, recordPrefix); r.At >= vt(failAt) && r.At < restart.Add(DefaultSuspicionTimeout) &&
				(k == "view" || k == "member" || k == "node.up" || k == "node.down" || k == "reap") {
				after = append(after, describe([]kernel.Record{r}))
			}
		}
		if len(after) != 2 || !strings.HasSuffix(after[0], ".reap node=y id=8 inc=0") ||
			!strings.HasSuffix(after[1], fmt.Sprintf(".view version=%d members=4 up=4", before.Version+1)) {
			tt.Errorf("n1's records from the failure until SuspicionTimeout after the restart: %q, want only y's reap and its view", after)
		}
		if reaps := c.recs("n1", "reap"); len(reaps) != 1 || reaps[0].At != restart.Add(retention) {
			tt.Errorf("n1's reap records %s, want y's at %s, DeadRetention after the restart", describe(reaps), restart.Add(retention))
		}
		var xDead []kernel.Record
		for _, r := range c.recs("n1", "member") {
			if attr(r, "node") == "x" && attr(r, "to") == "dead" {
				xDead = append(xDead, r)
			}
		}
		if len(xDead) != 1 || xDead[0].At != restart.Add(DefaultSuspicionTimeout) || attrs(xDead[0]) != "node=x id=7 inc=2 from=suspect to=dead source=suspicion" {
			tt.Errorf("n1's records of x going Dead %s, want one by suspicion at %s", describe(xDead), restart.Add(DefaultSuspicionTimeout))
		}
		got := sub.log[sigsBefore:]
		if len(got) != 1 || describeSig(got[0].sig) != "down x 7 left=false" || got[0].at < xDead[0].At {
			tt.Errorf("sub's signals after the restart %v, want only NodeDown x after x went Dead", got)
		}
		// The first view after the restart is the next version, from the restored table.
		views := c.recs("n1", "view")
		var first kernel.Record
		for _, r := range views {
			if r.At >= vt(failAt) {
				first = r
				break
			}
		}
		if attr(first, "version") != strconv.FormatUint(before.Version+1, 10) {
			tt.Errorf("n1's first view after the restart %s, want version %d", describe([]kernel.Record{first}), before.Version+1)
		}
		var probes []kernel.Record
		for _, r := range c.recs("n1", "probe") {
			if r.At >= restart {
				probes = append(probes, r)
			}
		}
		if len(probes) == 0 || attr(probes[0], "seq") != "1" {
			tt.Errorf("n1's probes after the restart %s, want them to count from seq 1", describe(probes))
		}
		want := []Member{alive("n1", c.id("n1")), alive("n2", c.id("n2")), alive("n3", c.id("n3")), {Node: "x", ID: 7, Incarnation: 2, State: Dead}}
		expectView(tt, "n1", c.view("n1"), want)
		expectAlternation(tt, sub)
	})
}

// AT-CLU-23, CLU-510, CLU-130. n3 of three nodes crashes at 3s and restarts with a new ID at
// 3.2s. When n1 and n2 learn of the new ID, each records node.down of the old ID and node.up of
// the new one in the same turn, without ever having n3 Dead, and sub on n1 gets NodeDown{n3, old,
// false}, then NodeUp{n3, new}. The NodeUp cleared n3's down mark at n1: a Watch of n3's new actor
// goes to the transport and does not end.
func TestATCLU23RestartNewID(t *testing.T) {
	names := []string{"n1", "n2", "n3"}
	sim.Simulate(t, simOpts(5, 8*time.Second), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		c := newSimCluster(s)
		sub := c.subNode("n1", simConfig(names...), true)
		c.node("n2", simConfig(names...))
		target := c.subNode("n3", simConfig(names...), false)
		w.RunFor(3 * time.Second)
		oldID := c.id("n3")
		w.Faults.Inject(fault.Event{Kind: fault.KindCrash, Node: "n3"})
		w.RunFor(200 * time.Millisecond)
		w.Faults.Inject(fault.Event{Kind: fault.KindRestart, Node: "n3"})
		w.RunFor(3 * time.Second)
		newID := c.id("n3")
		if newID == oldID {
			tt.Fatalf("n3 restarted with its old ID %d", oldID)
		}
		for _, n := range names[:2] {
			var got []string
			var at []kernel.Time
			for _, r := range clusterRecs(c, n) {
				if k := strings.TrimPrefix(r.Kind, recordPrefix); (k == "node.up" || k == "node.down") && attr(r, "node") == "n3" {
					got = append(got, k+" "+attrs(r))
					at = append(at, r.At)
				}
			}
			want := []string{"node.up node=n3 id=" + uitoa(oldID), "node.down node=n3 id=" + uitoa(oldID) + " left=false", "node.up node=n3 id=" + uitoa(newID)}
			if !slices.Equal(got, want) || at[1] != at[2] || at[1] < vt(3200*time.Millisecond) {
				tt.Errorf("%s's signals of n3 %q at %v, want %q, the last two in one turn after the restart", n, got, at, want)
			}
			for _, r := range c.recs(n, "member") {
				if attr(r, "node") == "n3" && attr(r, "to") == "dead" {
					tt.Errorf("%s had n3 Dead: %s", n, describe([]kernel.Record{r}))
				}
			}
			expectView(tt, n, c.view(n), []Member{alive("n1", c.id("n1")), alive("n2", c.id("n2")), alive("n3", newID)})
		}
		var n3 []string
		for _, sg := range sub.sigs() {
			if strings.HasPrefix(sg, "up n3 ") || strings.HasPrefix(sg, "down n3 ") {
				n3 = append(n3, sg)
			}
		}
		if want := []string{"up n3 " + uitoa(oldID), "down n3 " + uitoa(oldID) + " left=false", "up n3 " + uitoa(newID)}; !slices.Equal(n3, want) {
			tt.Errorf("sub's signals of n3 %q, want %q", n3, want)
		}

		mark := len(sub.log)
		if err := sim.Send(s, "n1", sub.pid, subMsg{watch: target.pid.Address()}); err != nil {
			tt.Fatal(err)
		}
		w.RunFor(1500 * time.Millisecond)
		if got := sub.log[mark:]; len(got) != 0 {
			tt.Errorf("a Watch of n3's new actor after the NodeUp gave %v, want nothing: the mark is cleared", got)
		}
		expectAlternation(tt, sub)
	})
}

// AT-CLU-22, CLU-520. On n1, s1, s2 and s3 subscribe and each gets NodeUp a. After
// Unsubscribe(s1) (twice, and of an address that never subscribed) and the stop of s2, a's going
// Dead reaches only s3, and s2's refused delivery removes it. Subscribe of the zero Address, of an
// address without a UID (s3's path) and of an address on another node fail; the stopped s2
// refuses its NodeUp b and is not added; subscribing s3 again does nothing, and s1, subscribing again twice, gets
// NodeUp b once and is added once, after s3.
func TestATCLU22Unsubscribe(t *testing.T) {
	sim.Simulate(t, simOpts(3, 2*time.Second), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		c := newSimCluster(s)
		c.node("n1", simConfig())
		w.RunFor(10 * time.Millisecond)
		s1, s2, s3 := c.spawnSub("s1"), c.spawnSub("s2"), c.spawnSub("s3")
		m := c.ms["n1"]
		for _, l := range []*subLog{s1, s2, s3} {
			if err := m.Subscribe(l.pid.Address()); err != nil {
				tt.Fatal(err)
			}
		}
		tell := func(us ...update) {
			tt.Helper()
			msg := syncResp{From: toUpdate(c.view("n1").Self), Members: us}
			if err := sim.Send(s, "n1", lockstep.PIDAt[memberMsg]("n1", actorPath), memberMsg(msg)); err != nil {
				tt.Fatal(err)
			}
			w.RunFor(10 * time.Millisecond)
		}
		tell(update{Node: "a", ID: 1, State: Alive})
		for _, l := range []*subLog{s1, s2, s3} {
			if got := l.sigs(); !slices.Equal(got, []string{"up a 1"}) {
				tt.Errorf("%s's signals %q, want [up a 1]", l.name, got)
			}
		}
		m.Unsubscribe(s1.pid.Address())
		m.Unsubscribe(s1.pid.Address())
		m.Unsubscribe(lockstep.PIDAt[subMsg]("n1", "/user/never").Address())
		if err := s.StopActor(s2.pid.Address()); err != nil {
			tt.Fatal(err)
		}
		w.RunFor(10 * time.Millisecond)
		if got := subsOf(m); !slices.Equal(got, []lockstep.Address{s2.pid.Address(), s3.pid.Address()}) {
			tt.Errorf("subscribers before the delivery %v, want s2 and s3", got)
		}
		tell(update{Node: "a", ID: 1, State: Dead})
		if got := s1.sigs(); len(got) != 1 {
			tt.Errorf("the unsubscribed s1 got %q", got[1:])
		}
		if got := s3.sigs(); !slices.Equal(got, []string{"up a 1", "down a 1 left=false"}) {
			tt.Errorf("s3's signals %q, want NodeUp a, NodeDown a", got)
		}
		if got := subsOf(m); !slices.Equal(got, []lockstep.Address{s3.pid.Address()}) {
			tt.Errorf("subscribers after the refused delivery %v, want only s3", got)
		}

		tell(update{Node: "b", ID: 2, State: Alive})
		for _, e := range []struct {
			addr lockstep.Address
			want error
		}{
			{lockstep.Address{}, lockstep.ErrNotFound},
			{lockstep.PIDAt[subMsg]("n1", "/user/s3").Address(), lockstep.ErrNotFound}, // no UID
			{lockstep.PIDAt[subMsg]("n2", "/user/s").Address(), lockstep.ErrNotLocal},
		} {
			err := m.Subscribe(e.addr)
			if !errors.Is(err, e.want) || err.Error() != fmt.Sprintf("cluster: subscribe %v: %v", e.addr, e.want) {
				tt.Errorf("Subscribe(%v) = %v, want cluster: subscribe %v: %v", e.addr, err, e.addr, e.want)
			}
		}
		for _, addr := range []lockstep.Address{s2.pid.Address(), s3.pid.Address(), s1.pid.Address(), s1.pid.Address()} {
			if err := m.Subscribe(addr); err != nil {
				tt.Errorf("Subscribe(%v): %v", addr, err)
			}
		}
		w.RunFor(10 * time.Millisecond)
		if got := subsOf(m); !slices.Equal(got, []lockstep.Address{s3.pid.Address(), s1.pid.Address()}) {
			tt.Errorf("subscribers %v, want s3 then s1", got)
		}
		if got := s3.sigs(); !slices.Equal(got, []string{"up a 1", "down a 1 left=false", "up b 2"}) {
			tt.Errorf("s3's signals %q: subscribing again sends nothing", got)
		}
		if got := s1.sigs(); !slices.Equal(got, []string{"up a 1", "up b 2"}) {
			tt.Errorf("s1's signals %q, want NodeUp b from its second Subscribe", got)
		}
		for _, l := range []*subLog{s1, s3} {
			expectAlternation(tt, l)
		}
	})
}

// The helpers of the signal tests.

// subMsg is the message of a subscriber: watch the address watch.
type subMsg struct{ watch lockstep.Address }

// sigEntry is a signal a subscriber received, with the time.
type sigEntry struct {
	at  kernel.Time
	sig lockstep.Signal
}

// subLog is the log of one subscriber: the PID of its current boot and every signal it got.
type subLog struct {
	name string
	pid  lockstep.PID[subMsg]
	log  []sigEntry
}

// sigs returns the signals of l as text (describeSig).
func (l *subLog) sigs() []string {
	out := make([]string, len(l.log))
	for i, e := range l.log {
		out[i] = describeSig(e.sig)
	}
	return out
}

// subscriber is a test actor that watches what it is told to and logs every signal.
type subscriber struct {
	now func() kernel.Time
	l   *subLog
}

func (a *subscriber) Receive(ctx *lockstep.Context[subMsg], msg subMsg) {
	if err := lockstep.Watch(ctx, msg.watch); err != nil {
		panic(err)
	}
}

func (a *subscriber) Signal(_ *lockstep.Context[subMsg], sig lockstep.Signal) {
	a.l.log = append(a.l.log, sigEntry{a.now(), sig})
}

// describeSig returns sig as "up <node> <id>", "down <node> <id> left=<bool>" or "terminated
// <addr> <reason>".
func describeSig(sig lockstep.Signal) string {
	switch sg := sig.(type) {
	case lockstep.NodeUp:
		return "up " + sg.Node + " " + uitoa(sg.ID)
	case lockstep.NodeDown:
		return fmt.Sprintf("down %s %s left=%t", sg.Node, uitoa(sg.ID), sg.Left)
	case lockstep.Terminated:
		return "terminated " + sg.Addr.String() + " " + sg.Reason.String()
	}
	return fmt.Sprintf("%#v", sig)
}

// spawnSub spawns the subscriber /user/<name> on n1, which must be up, and returns its log. It
// does not subscribe it.
func (c *simCluster) spawnSub(name string) *subLog {
	l := &subLog{name: name}
	sys := c.s.System("n1")
	if sys == nil {
		panic("spawnSub: n1 is not up")
	}
	pid, err := lockstep.Spawn(sys, name, func() lockstep.Actor[subMsg] { return &subscriber{c.s.World().Sim.Now, l} })
	if err != nil {
		panic(err)
	}
	l.pid = pid
	return l
}

// subNode adds node name, which joins with cfg at every boot and spawns the subscriber /user/sub,
// subscribed at once when subscribe is set, and returns the subscriber's log.
func (c *simCluster) subNode(name string, cfg Config, subscribe bool) *subLog {
	l := &subLog{name: name + "/sub"}
	c.n[name] = c.s.Node(name, func(sys *lockstep.System) {
		m, err := Join(sys, cfg)
		if err != nil {
			panic(err)
		}
		c.ms[name] = m
		pid, err := lockstep.Spawn(sys, "sub", func() lockstep.Actor[subMsg] { return &subscriber{c.s.World().Sim.Now, l} })
		if err != nil {
			panic(err)
		}
		l.pid = pid
		if subscribe {
			if err := m.Subscribe(pid.Address()); err != nil {
				panic(err)
			}
		}
	})
	return l
}

// subsOf returns the subscribers of m. Call it from body only, between the turns of the
// simulation.
func subsOf(m *Membership) []lockstep.Address { return slices.Clone(m.subs) }

// sameSet reports whether a and b hold the same strings, in any order.
func sameSet(a, b []string) bool {
	a, b = slices.Clone(a), slices.Clone(b)
	slices.Sort(a)
	slices.Sort(b)
	return slices.Equal(a, b)
}

// expectAlternation fails tt unless, per node name, the NodeUp and NodeDown signals of l alternate,
// starting with NodeUp (CLU-840), each NodeDown with the ID of the NodeUp before it.
func expectAlternation(tt testing.TB, l *subLog) {
	tt.Helper()
	upID := map[string]uint32{} // the ID of each node that is up; read by key
	for _, e := range l.log {
		switch sg := e.sig.(type) {
		case lockstep.NodeUp:
			if _, ok := upID[sg.Node]; ok {
				tt.Errorf("%s got NodeUp of %s at %s while it was up", l.name, sg.Node, e.at)
			}
			upID[sg.Node] = sg.ID
		case lockstep.NodeDown:
			if id, ok := upID[sg.Node]; !ok || id != sg.ID {
				tt.Errorf("%s got NodeDown of %s (ID %d) at %s, which was not up with that ID", l.name, sg.Node, sg.ID, e.at)
			}
			delete(upID, sg.Node)
		}
	}
}
