// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package cluster

import (
	"fmt"
	"math/bits"
	"slices"
	"strings"
	"time"

	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/lockstep"
)

// The ground truth and the bounds of the property tests (CLU §5.9). A truth follows every node's
// processes through s.System, which is nil while the node is down and whose Incarnation is the
// process's node ID, and reads the views of the live processes. Its checks are meant to run as
// faultline invariants, after every event.

// bounds holds CLU §5.9's bounds for a cluster of n nodes.
type bounds struct {
	detect   time.Duration // D(N) = 2*(N-1)*PI + PT + ST + PI (CLU-800)
	gossip   time.Duration // G(N) = RetransmitMult * bits.Len(N) * PI
	converge time.Duration // R(N) = 2*SI + D(N) + G(N) (CLU-830)

	// transient is CLU-820's bound: a cut shorter than ST - (2N-3)*PI kills no one. After the heal,
	// a suspicion is surely cleared only when its holder next pings the suspect: the ping carries
	// the suspicion as the forced update, the suspect refutes, and its ack's From carries the new
	// incarnation. That ping can take 2N-3 ticks (the two rounds of D(N), less the probe that was
	// made), and the earliest suspicion comes about PI after the cut began, so its timer fires
	// about ST + PI after it. The design's ST - PI is the case N = 2.
	transient time.Duration
}

// boundsOf returns the bounds of a cluster of n nodes that all join with cfg, after its defaults.
func boundsOf(cfg Config, n int) bounds {
	if err := cfg.Validate(); err != nil {
		panic(err)
	}
	pi := cfg.ProbeInterval
	d := 2*time.Duration(n-1)*pi + cfg.ProbeTimeout + cfg.SuspicionTimeout + pi
	g := time.Duration(cfg.RetransmitMult*bits.Len(uint(n))) * pi
	return bounds{detect: d, gossip: g, converge: 2*cfg.SyncInterval + d + g, transient: cfg.SuspicionTimeout - time.Duration(2*n-3)*pi}
}

// proc is one process of a node: a boot of it, which ends when the node crashes.
type proc struct {
	node   string
	id     uint32 // the process's node ID: its System's Incarnation
	sys    *lockstep.System
	booted kernel.Time // when the truth first saw it
	ended  kernel.Time // when the truth saw it gone; 0 while it is live
}

// truth follows the processes of the nodes names (sorted) of one simulation.
type truth struct {
	c     *simCluster
	names []string
	procs []proc // every process seen, in the order seen
	cur   []int  // per name, the index in procs of its live process, or -1
}

// newTruth returns the truth of the nodes names of c. Call observe before reading it.
func newTruth(c *simCluster, names []string) *truth {
	names = slices.Sorted(slices.Values(names))
	cur := make([]int, len(names))
	for i := range cur {
		cur[i] = -1
	}
	return &truth{c: c, names: names, cur: cur}
}

// observe brings the truth up to date with s.System: a node whose System changed has a new process,
// and the old one ended now. Every check calls it first, so it sees each crash and boot at the
// event that made it.
func (tr *truth) observe() {
	now := tr.c.s.World().Sim.Now()
	for i, name := range tr.names {
		sys := tr.c.s.System(name)
		if j := tr.cur[i]; j >= 0 && tr.procs[j].sys == sys {
			continue
		} else if j >= 0 {
			tr.procs[j].ended = now
			tr.cur[i] = -1
		}
		if sys != nil {
			tr.procs = append(tr.procs, proc{node: name, id: sys.Incarnation(), sys: sys, booted: now})
			tr.cur[i] = len(tr.procs) - 1
		}
	}
}

// live returns the live processes, by node name.
func (tr *truth) live() []proc {
	var out []proc
	for _, j := range tr.cur {
		if j >= 0 {
			out = append(out, tr.procs[j])
		}
	}
	return out
}

// view returns the last view of the live process p. It panics when the node's Membership is not
// the one of p's System, which would make every check meaningless.
func (tr *truth) view(p proc) *View {
	m := tr.c.ms[p.node]
	if m == nil || m.sys != p.sys {
		panic(fmt.Sprintf("cluster test: %s's Membership is not the one of its live process %d", p.node, p.id))
	}
	return m.View()
}

// converged returns nil when the view of every live process has as up members exactly the live
// processes, with their IDs (CLU-830): the views agree on the up set, and every process that
// ended is down or gone. Otherwise it describes the first view that differs.
func (tr *truth) converged() error {
	live := tr.live()
	want := make([]string, len(live))
	for i, p := range live {
		want[i] = p.node + ":" + uitoa(p.id)
	}
	for _, p := range live {
		v := tr.view(p)
		if got := upIDs(v); !slices.Equal(got, want) {
			return fmt.Errorf("%s's view %d has up %v, want the live processes %v (view: %v)", p.node, v.Version, got, want, v.Members)
		}
	}
	return nil
}

// upIDs returns the up members of v as "node:id", sorted by node.
func upIDs(v *View) []string {
	var out []string
	for _, m := range v.Members {
		if m.State.Up() {
			out = append(out, m.Node+":"+uitoa(m.ID))
		}
	}
	return out
}

// subscribe spawns the subscriber /user/<actor> on sys and subscribes it to m, and returns its log,
// named "<node>:<id>/<actor>".
func (c *simCluster) subscribe(sys *lockstep.System, m *Membership, actor string) *subLog {
	l := &subLog{name: sys.Node() + ":" + uitoa(sys.Incarnation()) + "/" + actor}
	pid, err := lockstep.Spawn(sys, actor, func() lockstep.Actor[subMsg] { return &subscriber{c.s.World().Sim.Now, l} })
	if err != nil {
		panic(err)
	}
	l.pid = pid
	if err := m.Subscribe(pid.Address()); err != nil {
		panic(err)
	}
	return l
}

// alternation checks CLU-840 on subscriber logs as they grow: per subscriber and node name, the
// signals alternate NodeUp, NodeDown, NodeUp, ..., starting with NodeUp, and each NodeDown has the
// ID of the NodeUp before it.
type alternation struct {
	logs *[]*subLog
	seen []int      // per log, the signals already checked
	up   [][]string // per log, the nodes that are up for it by its signals, as "node:id", sorted
}

// check checks the signals that arrived since the last call and returns the first violation.
func (a *alternation) check() error {
	for i, l := range *a.logs {
		if i == len(a.seen) {
			a.seen = append(a.seen, 0)
			a.up = append(a.up, nil)
		}
		for ; a.seen[i] < len(l.log); a.seen[i]++ {
			e := l.log[a.seen[i]]
			switch sg := e.sig.(type) {
			case lockstep.NodeUp:
				j, found := slices.BinarySearchFunc(a.up[i], sg.Node, compareUpNode)
				if found {
					return fmt.Errorf("%s got NodeUp of %s:%d at %s while %s was up: %s", l.name, sg.Node, sg.ID, e.at, a.up[i][j], sigTail(l, a.seen[i]))
				}
				a.up[i] = slices.Insert(a.up[i], j, sg.Node+":"+uitoa(sg.ID))
			case lockstep.NodeDown:
				j, found := slices.BinarySearchFunc(a.up[i], sg.Node, compareUpNode)
				if !found || a.up[i][j] != sg.Node+":"+uitoa(sg.ID) {
					return fmt.Errorf("%s got NodeDown of %s:%d at %s, which was not up with that ID: %s", l.name, sg.Node, sg.ID, e.at, sigTail(l, a.seen[i]))
				}
				a.up[i] = slices.Delete(a.up[i], j, j+1)
			}
		}
	}
	return nil
}

// compareUpNode orders an entry "node:id" of alternation.up by its node against node.
func compareUpNode(e, node string) int {
	n, _, _ := strings.Cut(e, ":")
	return strings.Compare(n, node)
}

// sigTail returns up to the last eight signals of l before and at i, for failure messages.
func sigTail(l *subLog, i int) string {
	var b strings.Builder
	for _, e := range l.log[max(0, i-7) : i+1] {
		fmt.Fprintf(&b, "\n  %s %s", e.at, describeSig(e.sig))
	}
	return b.String()
}
