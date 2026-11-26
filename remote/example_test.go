// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package remote_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/faultline/kernel/fault"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/codec"
	"github.com/hmdsefi/lockstep/remote"
	"github.com/hmdsefi/lockstep/sim"
)

// Greet asks for a greeting for Name.
type Greet struct {
	Name  string
	Reply lockstep.ReplyTo[string]
}

// Greet crosses nodes, so it has a wire name, the same on every node.
func init() { codec.Register[Greet]("example.Greet") } //nolint:gochecknoinits // the README's form

// Greeter answers every Greet with the name of the node it runs on.
type Greeter struct{}

func (Greeter) Receive(ctx *lockstep.Context[Greet], g Greet) {
	g.Reply.Send(ctx, "hello "+g.Name+" from "+ctx.Self().Address().Node())
}

func newGreeter() lockstep.Actor[Greet] { return Greeter{} }

func greet(name string) func(lockstep.ReplyTo[string]) Greet {
	return func(r lockstep.ReplyTo[string]) Greet { return Greet{Name: name, Reply: r} }
}

// Example runs two nodes, a and b, in one process and asks the greeter on b from a over TCP. In
// production each node is its own process, and the code of each is the same. The example has no
// Output line because it listens on fixed ports; it prints "hello a from b".
func Example() {
	ctx := context.Background()

	// Node b runs the greeter and listens on 127.0.0.1:7102.
	b := lockstep.New(lockstep.WithNodeName("b"))
	if err := remote.Enable(b, remote.Config{Listen: "127.0.0.1:7102"}); err != nil {
		log.Fatal(err)
	}
	if _, err := lockstep.Spawn(b, "greeter", newGreeter); err != nil {
		log.Fatal(err)
	}

	// Node a knows b's address and dials it on the first message.
	a := lockstep.New(lockstep.WithNodeName("a"))
	if err := remote.Enable(a, remote.Config{
		Listen: "127.0.0.1:7101",
		Peers:  map[string]string{"b": "127.0.0.1:7102"},
	}); err != nil {
		log.Fatal(err)
	}

	greeter, err := remote.Lookup[Greet](ctx, a, "b", "/user/greeter")
	if err != nil {
		log.Fatal(err)
	}
	answer, err := lockstep.Ask(a, greeter, greet("a")).Await(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(answer)

	for _, sys := range []*lockstep.System{a, b} {
		if err := sys.Shutdown(ctx); err != nil {
			log.Fatal(err)
		}
	}
}

// ExampleError shows how a failure reported by another node is matched: errors.As finds the
// *remote.Error, and errors.Is finds the lockstep error its code stands for.
func ExampleError() {
	var err error = &remote.Error{Node: "b", Code: remote.CodeNotFound, Message: "no actor at /user/greeter"}

	var re *remote.Error
	if errors.As(err, &re) {
		fmt.Println(re.Node, re.Code)
	}
	fmt.Println(errors.Is(err, lockstep.ErrNotFound))
	fmt.Println(err)
	// Output:
	// b not-found
	// true
	// remote: b: no actor at /user/greeter
}

// TestGreeterUnderPartitions is the simulation snippet of docs/distribution.md: the greeter runs
// on node b, node a asks it once a second while faultline cuts the network between them, and a
// final check asks once more after the faults have stopped.
func TestGreeterUnderPartitions(t *testing.T) {
	opts := sim.Options{Options: faultline.Options{Seeds: 20, Duration: 20 * time.Second}}
	sim.Simulate(t, opts, func(s *sim.Sim) {
		w := s.World()
		s.Node("b", func(sys *lockstep.System) {
			if _, err := lockstep.Spawn(sys, "greeter", newGreeter); err != nil {
				panic(err)
			}
		})
		s.Node("a", func(*lockstep.System) {})
		w.Plan(&fault.Random{Rules: []fault.Rule{
			{Kind: fault.KindPartition, Every: 3 * time.Second, MaxFor: 2 * time.Second},
		}})

		greeter := lockstep.PIDAt[Greet]("b", "/user/greeter")
		answered := 0
		for range 10 {
			w.RunFor(time.Second)
			if _, err := sim.Ask(s, "a", greeter, greet("a"), lockstep.WithTimeout(500*time.Millisecond)); err == nil {
				answered++
			}
		}
		w.RunFor(w.RecoveryStart().Sub(w.Sim.Now()) + time.Second) // a second after the faults stop
		answer, err := sim.Ask(s, "a", greeter, greet("a"))
		w.Final("the greeter answers after the faults", func() error {
			if err != nil {
				return fmt.Errorf("%w (%d of 10 asks answered during the faults)", err, answered)
			}
			if answer != "hello a from b" {
				return fmt.Errorf("answer %q", answer)
			}
			return nil
		})
	})
}
