// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package cluster_test

import (
	"context"
	"fmt"
	"log"
	"slices"
	"testing"
	"time"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/faultline/kernel/fault"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/cluster"
	"github.com/hmdsefi/lockstep/codec"
	"github.com/hmdsefi/lockstep/remote"
	"github.com/hmdsefi/lockstep/sim"
)

// Watcher logs every member that comes up or goes down.
type Watcher struct{}

func (Watcher) Receive(*lockstep.Context[struct{}], struct{}) {}

func (Watcher) Signal(ctx *lockstep.Context[struct{}], sig lockstep.Signal) {
	switch s := sig.(type) {
	case lockstep.NodeUp:
		ctx.Logf("%s is up", s.Node)
	case lockstep.NodeDown:
		ctx.Logf("%s is down (left: %t)", s.Node, s.Left)
	}
}

func newWatcher() lockstep.Actor[struct{}] { return Watcher{} }

// ExampleJoin makes node n1 a member of a cluster of three nodes, n1, n2 and n3, and subscribes
// an actor to the members that come up and go down. Every node runs the same code with its own
// name and addresses. The example has no Output line because it needs the other two nodes.
func ExampleJoin() {
	sys := lockstep.New(lockstep.WithNodeName("n1"))
	if err := remote.Enable(sys, remote.Config{
		Listen: "10.0.0.1:7000",
		Peers:  map[string]string{"n2": "10.0.0.2:7000", "n3": "10.0.0.3:7000"},
	}); err != nil {
		log.Fatal(err)
	}
	m, err := cluster.Join(sys, cluster.Config{Seeds: []string{"n1", "n2", "n3"}})
	if err != nil {
		log.Fatal(err)
	}
	watcher, err := lockstep.Spawn(sys, "watcher", newWatcher)
	if err != nil {
		log.Fatal(err)
	}
	if err := m.Subscribe(watcher.Address()); err != nil {
		log.Fatal(err)
	}
}

// ExampleMembership_View joins a cluster of one node and reads its view.
func ExampleMembership_View() {
	sys := lockstep.New(lockstep.WithNodeName("n1"))
	m, err := cluster.Join(sys, cluster.Config{}) // no seeds: a cluster of one
	if err != nil {
		log.Fatal(err)
	}
	v := m.View()
	for _, member := range v.Up() {
		fmt.Println(v.Version, member.Node, member.State)
	}
	if err := sys.Shutdown(context.Background()); err != nil {
		log.Fatal(err)
	}
	// Output: 1 n1 alive
}

// CounterMsg is every message a counter grain accepts.
type CounterMsg interface{ isCounterMsg() }

// Add adds N to the grain's total.
type Add struct{ N int }

// Get asks for the grain's total.
type Get struct{ Reply lockstep.ReplyTo[int] }

func (Add) isCounterMsg() {}
func (Get) isCounterMsg() {}

// Messages for a grain on another node cross nodes, so they have wire names.
func init() { //nolint:gochecknoinits // the README's form
	codec.Register[Add]("example.Add")
	codec.Register[Get]("example.Get")
}

// Counter is the activation of one counter grain: the actor that runs it on its owner.
type Counter struct{ total int }

func (c *Counter) Receive(ctx *lockstep.Context[CounterMsg], msg CounterMsg) {
	switch m := msg.(type) {
	case Add:
		c.total += m.N
	case Get:
		m.Reply.Send(ctx, c.total)
	}
}

// counterKind returns the kind of the counter grains. Its factory gets the grain's key.
func counterKind() cluster.Kind {
	return cluster.NewKind("counter", func(key string) lockstep.Actor[CounterMsg] {
		return &Counter{}
	}, cluster.WithIdleTimeout(time.Minute))
}

func getTotal(r lockstep.ReplyTo[int]) CounterMsg { return Get{Reply: r} }

// ExampleGrainOf runs the counter grain "user-42" on a cluster of one node, sends it three Adds
// and asks for its total.
func ExampleGrainOf() {
	sys := lockstep.New(lockstep.WithNodeName("n1"))
	m, err := cluster.Join(sys, cluster.Config{Kinds: []cluster.Kind{counterKind()}})
	if err != nil {
		log.Fatal(err)
	}
	// Grain messages are refused until the node has joined, which takes one turn.
	for h, _ := m.Host("counter"); !h.Joined; h, _ = m.Host("counter") {
		time.Sleep(time.Millisecond)
	}

	counter := cluster.GrainOf[CounterMsg](m, "counter", "user-42")
	for _, n := range []int{1, 2, 3} {
		if err := counter.Send(sys, Add{N: n}); err != nil {
			log.Fatal(err)
		}
	}
	total, err := cluster.Ask(sys, counter, getTotal).Await(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	h, _ := m.Host("counter")
	fmt.Println("total:", total, "active:", h.Active)

	if err := sys.Shutdown(context.Background()); err != nil {
		log.Fatal(err)
	}
	// Output: total: 6 active: [user-42]
}

// TestCounterGrain is the simulation snippet of the README: three nodes host the counter grains,
// client c1 sends one grain 20 Adds while faultline crashes nodes and cuts the network, and a final
// check asks the grain for its total once the faults have stopped.
func TestCounterGrain(t *testing.T) {
	opts := sim.Options{Options: faultline.Options{Seeds: 20, Duration: 50 * time.Second}}
	sim.Simulate(t, opts, func(s *sim.Sim) {
		w := s.World()
		nodes := []string{"n1", "n2", "n3"}
		for _, name := range nodes {
			s.Node(name, func(sys *lockstep.System) {
				if _, err := cluster.Join(sys, cluster.Config{Seeds: nodes, Kinds: []cluster.Kind{counterKind()}}); err != nil {
					panic(err)
				}
			})
		}
		var m *cluster.Membership
		s.Client("c1", func(sys *lockstep.System) {
			var err error
			if m, err = cluster.Join(sys, cluster.Config{Seeds: nodes}); err != nil { // hosts no kind
				panic(err)
			}
		})
		w.Plan(&fault.Random{MaxDown: 1, Quiet: 25 * time.Second, Rules: []fault.Rule{
			{Kind: fault.KindCrash, Every: 10 * time.Second, MaxFor: 3 * time.Second},
			{Kind: fault.KindPartition, Every: 10 * time.Second, MaxFor: 3 * time.Second},
		}}) // faults stop at 25s, when the Adds end

		w.RunFor(5 * time.Second)
		counter := cluster.GrainOf[CounterMsg](m, "counter", "user-42")
		for range 20 {
			w.RunFor(time.Second)
			_ = counter.Send(s.System("c1"), Add{N: 1}) // the Add may be lost
		}
		w.RunFor(w.RecoveryStart().Sub(w.Sim.Now()) + 10*time.Second) // time to recover
		reply := cluster.Ask(s.System("c1"), counter, getTotal)
		w.RunFor(6 * time.Second) // longer than the ask's timeout, so the reply has resolved
		total, err := reply.Await(context.Background())
		w.Final("the grain answers, and no Add counts twice", func() error {
			if err != nil {
				return err
			}
			if total > 20 {
				return fmt.Errorf("total is %d after 20 adds", total)
			}
			return nil
		})
	})
}

// upMembers returns the up members of v as "node:id", sorted by node.
func upMembers(v *cluster.View) []string {
	var out []string
	for _, m := range v.Up() {
		out = append(out, fmt.Sprintf("%s:%d", m.Node, m.ID))
	}
	return out
}

// TestViewsAgree is the membership snippet of docs/distribution.md: four nodes join each other
// while faultline crashes them and cuts the network, and a final check compares their views at
// the end of the run.
func TestViewsAgree(t *testing.T) {
	opts := sim.Options{Options: faultline.Options{Seeds: 20, Duration: 80 * time.Second}}
	sim.Simulate(t, opts, func(s *sim.Sim) {
		w := s.World()
		nodes := []string{"n1", "n2", "n3", "n4"}
		members := make(map[string]*cluster.Membership)
		for _, name := range nodes {
			s.Node(name, func(sys *lockstep.System) {
				m, err := cluster.Join(sys, cluster.Config{Seeds: nodes, SyncInterval: 5 * time.Second})
				if err != nil {
					panic(err)
				}
				members[name] = m // the Membership of the node's current process
			})
		}
		w.Plan(&fault.Random{MaxDown: 1, Quiet: 40 * time.Second, Rules: []fault.Rule{
			{Kind: fault.KindCrash, Every: 10 * time.Second, MaxFor: 5 * time.Second},
			{Kind: fault.KindPartition, Every: 10 * time.Second, MaxFor: 10 * time.Second},
		}}) // faults stop at 40s

		w.Final("every node sees the same up members", func() error {
			want := upMembers(members["n1"].View())
			for _, name := range nodes[1:] {
				if got := upMembers(members[name].View()); !slices.Equal(got, want) {
					return fmt.Errorf("%s sees %v, n1 sees %v", name, got, want)
				}
			}
			if len(want) != len(nodes) {
				return fmt.Errorf("up members %v, want all %d nodes", want, len(nodes))
			}
			return nil
		})
	})
}
