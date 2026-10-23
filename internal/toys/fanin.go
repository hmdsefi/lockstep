// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package toys

import (
	"fmt"

	"github.com/hmdsefi/lockstep"
)

// CollectorMsg is the message type of the collector.
type CollectorMsg interface{ collectorMsg() }

// Item is the N-th item of a producer.
type Item struct{ Producer, N int }

func (Item) collectorMsg() {}

// Describe returns "item p<Producer> <N>".
func (m Item) Describe() string { return fmt.Sprintf("item p%d %d", m.Producer, m.N) }

// idle is the message type of the producers, which receive nothing.
type idle struct{}

// Describe returns "idle".
func (idle) Describe() string { return "idle" }

// FanInProbe is what the collector reports to the test.
type FanInProbe struct {
	Items     int // items received
	Reordered int // items whose N was not the producer's previous N + 1
}

// SpawnFanIn spawns the actor "collector", then the producers "p1" to "p<producers>" in that
// order, under the user guardian of sys. When it starts, producer i sends Item{i, 1} to
// Item{i, items} to the collector, in order, and does nothing else. The collector counts the
// items and the ones that arrive out of a producer's order in p (DET-220). It returns an error
// for producers or items below 1, before it spawns anything.
func SpawnFanIn(sys *lockstep.System, producers, items int, p *FanInProbe) error {
	if producers < 1 {
		return invalid("producers", producers)
	}
	if items < 1 {
		return invalid("items", items)
	}
	collector, err := lockstep.Spawn(sys, "collector", func() lockstep.Actor[CollectorMsg] {
		return &collector{p: p, last: make([]int, producers+1)}
	})
	if err != nil {
		return err
	}
	for i := 1; i <= producers; i++ {
		_, err := lockstep.Spawn(sys, fmt.Sprintf("p%d", i), func() lockstep.Actor[idle] {
			return &producer{id: i, items: items, to: collector}
		})
		if err != nil {
			return err
		}
	}
	return nil
}

type collector struct {
	p    *FanInProbe
	last []int // the last N seen from each producer, indexed by producer number
}

func (a *collector) Receive(_ *lockstep.Context[CollectorMsg], msg CollectorMsg) {
	m, ok := msg.(Item)
	if !ok {
		return
	}
	a.p.Items++
	if m.N != a.last[m.Producer]+1 {
		a.p.Reordered++
	}
	a.last[m.Producer] = m.N
}

type producer struct {
	id, items int
	to        lockstep.PID[CollectorMsg]
}

func (a *producer) Started(ctx *lockstep.Context[idle]) {
	for n := 1; n <= a.items; n++ {
		_ = lockstep.Send(ctx, a.to, CollectorMsg(Item{Producer: a.id, N: n}))
	}
}

func (*producer) Receive(*lockstep.Context[idle], idle) {}
