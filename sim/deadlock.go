// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim

import (
	"errors"
	"slices"
	"strconv"
	"strings"

	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/gograph"
	"github.com/hmdsefi/gograph/connectivity"
	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/hooks"
)

// quiescent reports whether no lockstep activity is left (SIM-250): no pending node or world event
// of class activity that sim scheduled, and no simnet copy in flight. Pending ask timeouts, one
// per pending ask, and events of class background may remain.
func (s *Sim) quiescent() bool {
	return s.events.activity == 0 && s.world.activity == 0 && s.w.Net.Stats().InFlight == 0
}

// checkDeadlock is the lockstep/deadlock invariant, which runs after every event (SIM-250,
// SIM-300). It returns the failure stored in its slot, if any. Otherwise, when the world is
// quiescent with asks pending and the pending asks changed since it last read them, it builds the
// wait-for graph of the asks that count (SIM-260) and finds its cycles (SIM-270). With a cycle it
// emits lockstep.deadlock, stores the failure and returns it (SIM-280).
func (s *Sim) checkDeadlock() error {
	i := slot(checkDeadlock)
	if s.slots[i] != nil {
		return s.slots[i]
	}
	if s.events.asks == 0 || !s.quiescent() || s.askGen == s.graphGen {
		return nil
	}
	s.graphGen = s.askGen
	g := s.waitFor()
	if g == nil {
		return nil
	}
	cycles := findCycles(g)
	if len(cycles) == 0 {
		return nil
	}
	path := strings.Join(cycles[0], " → ")
	msg := "lockstep: deadlock: " + path
	if n := len(cycles) - 1; n > 0 {
		msg += " (and " + strconv.Itoa(n) + " more cycles)"
	}
	s.emit(0, "lockstep.deadlock", "deadlock", []engine.Attr{
		{Key: "cycle", Value: path},
		{Key: "cycles", Value: strconv.Itoa(len(cycles))},
	})
	s.store(checkDeadlock, errors.New(msg))
	return s.slots[i]
}

// waitFor returns the wait-for graph of the pending asks that count (SIM-260), or nil when no
// pending ask counts. An ask counts when an actor asked it and its target is not the zero address;
// a NewReply ask has the zero target. Vertices are Address.String() values; for each live node
// system in NodeID order and each of its asks that count in ask-counter order, the asker and the
// target are added, and the edge from the asker to the target unless it exists.
func (s *Sim) waitFor() gograph.Graph[string] {
	var g gograph.Graph[string]
	for _, ln := range s.nodes {
		if ln.sys == nil || ln.n.State() == kernel.NodeDown {
			continue
		}
		for _, p := range hooks.PendingAsks(ln.sys) {
			if !p.ByActor || p.Target == (engine.Address{}) {
				continue
			}
			if g == nil {
				g = gograph.New[string](gograph.Directed())
			}
			from, to := vertex(g, p.Asker.String()), vertex(g, p.Target.String())
			if _, err := g.AddEdge(from, to); err != nil && !errors.Is(err, gograph.ErrEdgeAlreadyExists) {
				panic("lockstep: sim: wait-for graph: " + err.Error()) // unreachable: from and to are not nil
			}
		}
	}
	return g
}

// vertex returns the vertex of g labeled label, adding it if g has none.
func vertex(g gograph.Graph[string], label string) *gograph.Vertex[string] {
	if v := g.GetVertexByID(label); v != nil {
		return v
	}
	return g.AddVertexByLabel(label)
}

// findCycles returns the cycles of g, ordered by start label (SIM-270): each strongly connected
// component with two or more vertices, or with one vertex that has an edge to itself, as the path
// from its start, its vertex with the smallest label in byte order, back to the start.
func findCycles(g gograph.Graph[string]) [][]string {
	var cycles [][]string
	for _, comp := range connectivity.Tarjan(g) {
		if len(comp) == 1 && !comp[0].HasNeighbor(comp[0]) {
			continue
		}
		in := make(map[string]bool, len(comp))
		start := comp[0].Label()
		for _, v := range comp {
			in[v.Label()] = true
			start = min(start, v.Label())
		}
		cycles = append(cycles, cyclePath(g, start, in))
	}
	slices.SortFunc(cycles, func(a, b []string) int { return strings.Compare(a[0], b[0]) })
	return cycles
}

// cyclePath returns the shortest path from start back to start in g, through vertices in only,
// found by breadth-first search over Vertex.Neighbors in their order (SIM-270). The path begins
// and ends with start. start belongs to a cycle inside in, so the path exists.
func cyclePath(g gograph.Graph[string], start string, in map[string]bool) []string {
	parent := map[string]string{} // the vertex each reached vertex was first reached from
	queue := []string{start}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, nb := range g.GetVertexByID(cur).Neighbors() {
			label := nb.Label()
			if label == start {
				path := []string{start}
				for v := cur; v != start; v = parent[v] {
					path = append(path, v)
				}
				slices.Reverse(path[1:])
				return append(path, start)
			}
			if _, seen := parent[label]; seen || !in[label] {
				continue
			}
			parent[label] = cur
			queue = append(queue, label)
		}
	}
	panic("lockstep: sim: no cycle through " + start) // unreachable: start is on a cycle inside in
}
