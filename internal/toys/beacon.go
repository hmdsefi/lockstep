// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package toys

import (
	"fmt"
	"time"

	"github.com/hmdsefi/lockstep"
)

// BeaconMsg is the message type of the beacon.
type BeaconMsg interface{ beaconMsg() }

// StartBeacon tells the beacon to send Beats beats to Peer.
type StartBeacon struct {
	Peer  lockstep.PID[ListenerMsg]
	Beats int
}

// beat is the beacon's timer message: send the next beat.
type beat struct{}

// ListenerMsg is the message type of the listener.
type ListenerMsg interface{ listenerMsg() }

// Beat is the N-th beat of the beacon.
type Beat struct{ N int }

func (StartBeacon) beaconMsg() {}
func (beat) beaconMsg()        {}
func (Beat) listenerMsg()      {}

// Describe returns "start-beacon <Beats>".
func (m StartBeacon) Describe() string { return fmt.Sprintf("start-beacon %d", m.Beats) }

// Describe returns "beat".
func (beat) Describe() string { return "beat" }

// Describe returns "beat <N>".
func (m Beat) Describe() string { return fmt.Sprintf("beat %d", m.N) }

// BeaconProbe is what the beacon and the listener report to the test: the beats sent and the
// beats heard.
type BeaconProbe struct{ Sent, Heard int }

// SpawnBeacon spawns the actor "beacon" under the user guardian of sys. On StartBeacon it sends a
// Beat to the peer every 10 ms, counting them in p.Sent, and stops its timer after the last one
// (DET-250).
func SpawnBeacon(sys *lockstep.System, p *BeaconProbe) (lockstep.PID[BeaconMsg], error) {
	return lockstep.Spawn(sys, "beacon", func() lockstep.Actor[BeaconMsg] { return &beacon{p: p} })
}

// SpawnListener spawns the actor "listener" under the user guardian of sys. It counts the beats
// it receives in p.Heard (DET-250).
func SpawnListener(sys *lockstep.System, p *BeaconProbe) (lockstep.PID[ListenerMsg], error) {
	return lockstep.Spawn(sys, "listener", func() lockstep.Actor[ListenerMsg] { return &listener{p: p} })
}

type beacon struct {
	p     *BeaconProbe
	peer  lockstep.PID[ListenerMsg]
	beats int
	timer lockstep.TimerID
	sent  int
}

func (a *beacon) Receive(ctx *lockstep.Context[BeaconMsg], msg BeaconMsg) {
	switch m := msg.(type) {
	case StartBeacon:
		a.peer, a.beats = m.Peer, m.Beats
		a.timer = ctx.Every(10*time.Millisecond, BeaconMsg(beat{}))
	case beat:
		a.sent++
		a.p.Sent = a.sent
		_ = lockstep.Send(ctx, a.peer, ListenerMsg(Beat{N: a.sent}))
		if a.sent == a.beats {
			ctx.CancelTimer(a.timer)
		}
	}
}

type listener struct{ p *BeaconProbe }

func (a *listener) Receive(_ *lockstep.Context[ListenerMsg], msg ListenerMsg) {
	if _, ok := msg.(Beat); ok {
		a.p.Heard++
	}
}
