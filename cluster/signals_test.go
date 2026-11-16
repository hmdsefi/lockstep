// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package cluster

import (
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/sim"
)

// CLU-510: the transitions of every kind of change.
func TestTransitions(t *testing.T) {
	up := func(node string, id uint32) transition { return transition{up: true, node: node, id: id} }
	down := func(node string, id uint32, left bool) transition {
		return transition{node: node, id: id, left: left}
	}
	for _, c := range []struct {
		name string
		c    change
		want []transition
	}{
		{"addition", change{Node: "a", ID: 1, To: Alive}, []transition{up("a", 1)}},
		{"addition as suspect", change{Node: "a", ID: 1, To: Suspect}, []transition{up("a", 1)}},
		{"revive", change{Node: "a", ID: 1, Inc: 3, From: Dead, To: Alive}, []transition{up("a", 1)}},
		{"tombstone restored up", change{Node: "a", ID: 1, Inc: 2, From: Dead, To: Suspect}, []transition{up("a", 1)}},
		{"tombstone restored dead", change{Node: "a", ID: 1, Inc: 2, From: Dead, To: Dead}, nil},
		{"new id meets a tombstone", change{Node: "a", ID: 2, To: Alive}, []transition{up("a", 2)}},
		{"new id replaces a dead entry", change{Node: "a", ID: 2, From: Dead, To: Alive, OldID: 1, Superseded: true}, []transition{up("a", 2)}},
		{"new id replaces a left entry", change{Node: "a", ID: 2, From: Left, To: Suspect, OldID: 1, Superseded: true}, []transition{up("a", 2)}},
		{"dead to left", change{Node: "a", ID: 1, From: Dead, To: Left}, nil},
		{"alive to dead", change{Node: "a", ID: 1, From: Alive, To: Dead}, []transition{down("a", 1, false)}},
		{"suspect to dead", change{Node: "a", ID: 1, From: Suspect, To: Dead}, []transition{down("a", 1, false)}},
		{"alive to left", change{Node: "a", ID: 1, From: Alive, To: Left}, []transition{down("a", 1, true)}},
		{"suspect to left", change{Node: "a", ID: 1, From: Suspect, To: Left}, []transition{down("a", 1, true)}},
		{"supersede of a suspect", change{Node: "a", ID: 2, From: Suspect, To: Alive, OldID: 1, Superseded: true}, []transition{down("a", 1, false), up("a", 2)}},
		{"supersede of an alive", change{Node: "a", ID: 2, From: Alive, To: Suspect, OldID: 1, Superseded: true}, []transition{down("a", 1, false), up("a", 2)}},
		{"alive to suspect", change{Node: "a", ID: 1, From: Alive, To: Suspect}, nil},
		{"suspect to alive", change{Node: "a", ID: 1, Inc: 1, From: Suspect, To: Alive}, nil},
		{"alive at a higher incarnation", change{Node: "a", ID: 1, Inc: 1, From: Alive, To: Alive}, nil},
	} {
		if got := transitions(c.c); !slices.Equal(got, c.want) {
			t.Errorf("%s: transitions(%+v) = %+v, want %+v", c.name, c.c, got, c.want)
		}
	}

	// Changes of the own entry give none.
	a := &memberActor{tbl: newTable(Member{Node: "self", ID: 1, State: Alive}, time.Minute, time.Time{})}
	a.noteTransitions(change{Node: "self", ID: 1, From: Alive, To: Left})
	a.noteTransitions(change{Node: "b", ID: 2, To: Alive})
	if want := []transition{up("b", 2)}; !slices.Equal(a.trans, want) {
		t.Errorf("noted transitions %+v, want only %+v", a.trans, want)
	}
}

// CLU-520, CLU-600: the membership actor has its own FailSend mailbox, whatever the system's
// default (here a capacity of 1 that drops the newest): every Subscribe and Leave that fits is
// kept, and one that does not fit returns ErrMailboxFull, after which Leave can be called again.
func TestMembershipMailbox(t *testing.T) {
	opts := simOpts(2, time.Second)
	opts.System = []lockstep.Option{lockstep.WithDefaultMailbox(1, lockstep.DropNewest)}
	sim.Simulate(t, opts, func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		c := newSimCluster(s)
		c.node("n1", simConfig())
		w.RunFor(10 * time.Millisecond)
		s1 := c.spawnSub("s1")
		m := c.ms["n1"]
		for i := range mailboxCapacity {
			if err := m.Subscribe(s1.pid.Address()); err != nil {
				tt.Fatalf("Subscribe %d: %v", i, err)
			}
		}
		if err := m.Subscribe(s1.pid.Address()); !errors.Is(err, lockstep.ErrMailboxFull) {
			tt.Errorf("Subscribe to the full mailbox = %v, want ErrMailboxFull", err)
		}
		for range 2 {
			if err := m.Leave(); !errors.Is(err, lockstep.ErrMailboxFull) || errors.Is(err, ErrLeft) {
				tt.Errorf("Leave with the full mailbox = %v, want ErrMailboxFull", err)
			}
		}
		w.RunFor(10 * time.Millisecond)
		if got := subsOf(m); !slices.Equal(got, []lockstep.Address{s1.pid.Address()}) {
			tt.Errorf("subscribers %v, want s1 once", got)
		}
		if err := m.Leave(); err != nil {
			tt.Fatalf("Leave once the mailbox has room: %v", err)
		}
		w.RunFor(10 * time.Millisecond)
		if self := c.view("n1").Self; self.State != Left {
			tt.Errorf("own entry %v after Leave, want Left", self)
		}
	})
}

// countingSubscriber counts the signals it receives; it is safe to read from another goroutine.
type countingSubscriber struct{ n *atomic.Int64 }

func (countingSubscriber) Receive(*lockstep.Context[subMsg], subMsg) {}

func (a countingSubscriber) Signal(*lockstep.Context[subMsg], lockstep.Signal) { a.n.Add(1) }

// CLU-520, CLU-600 on a production System: Subscribe, Unsubscribe and Leave called from many
// goroutines while the membership actor merges changes. Every Subscribe returns nil or ErrLeft,
// exactly one of the concurrent Leaves succeeds, and the node ends Left. Run it with -race.
func TestMembershipConcurrent(t *testing.T) {
	sys := newSystem(t)
	m, err := Join(sys, Config{})
	if err != nil {
		t.Fatal(err)
	}
	var signals atomic.Int64
	var subs []lockstep.Address
	for i := range 8 {
		pid, err := lockstep.Spawn(sys, fmt.Sprintf("s%d", i), func() lockstep.Actor[subMsg] { return countingSubscriber{&signals} })
		if err != nil {
			t.Fatal(err)
		}
		subs = append(subs, pid.Address())
	}
	to := lockstep.PIDAt[memberMsg]("n1", actorPath)
	self := toUpdate(m.View().Self)
	var wg sync.WaitGroup
	var left atomic.Int64
	for i, addr := range subs {
		wg.Go(func() {
			for j := range 50 {
				if err := m.Subscribe(addr); err != nil && !errors.Is(err, ErrLeft) {
					t.Errorf("Subscribe(%v): %v", addr, err)
				}
				if j%3 == 0 {
					m.Unsubscribe(addr)
				}
				st := Alive
				if j%2 == 1 {
					st = Dead
				}
				msg := syncResp{From: self, Members: []update{{Node: fmt.Sprintf("x%d", i), ID: 1, Inc: uint64(j), State: st}}}
				if err := lockstep.Send(sys, to, memberMsg(msg)); err != nil && !errors.Is(err, lockstep.ErrNotFound) {
					t.Errorf("Send: %v", err)
				}
			}
		})
		wg.Go(func() {
			time.Sleep(time.Millisecond)
			switch err := m.Leave(); {
			case err == nil:
				left.Add(1)
			case !errors.Is(err, ErrLeft):
				t.Errorf("Leave: %v", err)
			}
		})
	}
	wg.Wait()
	if n := left.Load(); n != 1 {
		t.Errorf("%d of 8 concurrent Leaves succeeded, want 1", n)
	}
	for i := 0; i < 1000 && m.View().Self.State != Left; i++ {
		time.Sleep(time.Millisecond)
	}
	if st := m.View().Self.State; st != Left {
		t.Errorf("own state %v after Leave, want Left", st)
	}
	if err := m.Subscribe(subs[0]); !errors.Is(err, ErrLeft) {
		t.Errorf("Subscribe after Leave = %v, want ErrLeft", err)
	}
	t.Logf("%d signals delivered", signals.Load())
}
