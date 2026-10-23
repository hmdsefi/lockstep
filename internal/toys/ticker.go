// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package toys

import (
	"time"

	"github.com/hmdsefi/lockstep"
)

// TickerMsg is the message type of the ticker.
type TickerMsg interface{ tickerMsg() }

// Tick is the message of the ticker's timer.
type Tick struct{}

func (Tick) tickerMsg() {}

// Describe returns "tick".
func (Tick) Describe() string { return "tick" }

// TickerProbe is what the ticker reports to the test.
type TickerProbe struct {
	Starts  int  // incarnations started
	Stopped bool // an incarnation reached its tick limit and stopped
}

// SpawnTicker spawns the actor "ticker" under the user guardian of sys. Every incarnation ticks
// every 10 ms and counts its own ticks. At each tick it draws once from its random stream and
// panics with "toys: planned failure" if the draw is 0 out of 64, or at the third tick of the
// first incarnation. Otherwise, at tick number ticks it records that it stopped in p and stops.
// The first incarnation therefore fails once, and the default supervision restarts it after
// the backoff (DET-230). It returns an error for ticks below 1, before it spawns anything.
func SpawnTicker(sys *lockstep.System, ticks int, p *TickerProbe) error {
	if ticks < 1 {
		return invalid("ticks", ticks)
	}
	_, err := lockstep.Spawn(sys, "ticker", func() lockstep.Actor[TickerMsg] {
		return &ticker{p: p, ticks: ticks}
	})
	return err
}

type ticker struct {
	p     *TickerProbe
	ticks int
	count int // the ticks of this incarnation
}

func (a *ticker) Started(ctx *lockstep.Context[TickerMsg]) {
	a.p.Starts++
	ctx.Every(10*time.Millisecond, TickerMsg(Tick{}))
}

func (a *ticker) Receive(ctx *lockstep.Context[TickerMsg], msg TickerMsg) {
	if _, ok := msg.(Tick); !ok {
		return
	}
	a.count++
	d := ctx.Rand().IntN(64)
	switch {
	case a.p.Starts == 1 && a.count == 3, d == 0:
		panic("toys: planned failure")
	case a.count == a.ticks:
		a.p.Stopped = true
		ctx.Stop()
	}
}
