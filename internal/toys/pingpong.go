// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package toys

import (
	"fmt"

	"github.com/hmdsefi/lockstep"
)

// PingerMsg is the message type of the pinger.
type PingerMsg interface{ pingerMsg() }

// PongerMsg is the message type of the ponger.
type PongerMsg interface{ pongerMsg() }

// StartPing tells the pinger to play Rounds rounds with Peer.
type StartPing struct {
	Peer   lockstep.PID[PongerMsg]
	Rounds int
}

// Pong answers a Ping with its N.
type Pong struct{ N int }

// Ping asks the ponger to answer From with a Pong of N.
type Ping struct {
	N    int
	From lockstep.PID[PingerMsg]
}

func (StartPing) pingerMsg() {}
func (Pong) pingerMsg()      {}
func (Ping) pongerMsg()      {}

// Describe returns "start-ping <Rounds>".
func (m StartPing) Describe() string { return fmt.Sprintf("start-ping %d", m.Rounds) }

// Describe returns "pong <N>".
func (m Pong) Describe() string { return fmt.Sprintf("pong %d", m.N) }

// Describe returns "ping <N>".
func (m Ping) Describe() string { return fmt.Sprintf("ping %d", m.N) }

// PingPongProbe is what the pinger reports to the test.
type PingPongProbe struct{ Rounds int } // the last N the pinger received

// SpawnPinger spawns the actor "pinger" under the user guardian of sys. On StartPing it sends
// Ping{1} to the peer, and on every Pong{N} it records N in p and, while N is below the rounds
// asked for, sends Ping{N + 1} (DET-210).
func SpawnPinger(sys *lockstep.System, p *PingPongProbe) (lockstep.PID[PingerMsg], error) {
	return lockstep.Spawn(sys, "pinger", func() lockstep.Actor[PingerMsg] { return &pinger{p: p} })
}

// SpawnPonger spawns the actor "ponger" under the user guardian of sys. It answers every Ping{N}
// with Pong{N} (DET-210).
func SpawnPonger(sys *lockstep.System) (lockstep.PID[PongerMsg], error) {
	return lockstep.Spawn(sys, "ponger", func() lockstep.Actor[PongerMsg] { return &ponger{} })
}

type pinger struct {
	p      *PingPongProbe
	peer   lockstep.PID[PongerMsg]
	rounds int
}

func (a *pinger) Receive(ctx *lockstep.Context[PingerMsg], msg PingerMsg) {
	switch m := msg.(type) {
	case StartPing:
		a.peer, a.rounds = m.Peer, m.Rounds
		_ = lockstep.Send(ctx, a.peer, PongerMsg(Ping{N: 1, From: ctx.Self()}))
	case Pong:
		a.p.Rounds = m.N
		if m.N < a.rounds {
			_ = lockstep.Send(ctx, a.peer, PongerMsg(Ping{N: m.N + 1, From: ctx.Self()}))
		}
	}
}

type ponger struct{}

func (*ponger) Receive(ctx *lockstep.Context[PongerMsg], msg PongerMsg) {
	if m, ok := msg.(Ping); ok {
		_ = lockstep.Send(ctx, m.From, PingerMsg(Pong{N: m.N}))
	}
}
