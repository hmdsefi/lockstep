// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package toys

import (
	"errors"
	"fmt"
	"time"

	"github.com/hmdsefi/lockstep"
)

// ResponderMsg is the message type of the responder.
type ResponderMsg interface{ responderMsg() }

// Request asks the responder for the square of N. With a non-zero Via the responder hands the
// request on to Via, with the same ReplyTo, instead of answering it.
type Request struct {
	N       int
	ReplyTo lockstep.ReplyTo[int]
	Via     lockstep.PID[ResponderMsg]
}

// respond is the responder's own message that sends the reply to a request once its delay is over.
type respond struct {
	N       int
	ReplyTo lockstep.ReplyTo[int]
}

// RequesterMsg is the message type of the requester.
type RequesterMsg interface{ requesterMsg() }

// StartRound tells the requester to ask Count questions, the odd ones of A and the even ones of B.
// The question number Count/2 goes to A with Via set to B, so that A forwards it to B.
type StartRound struct {
	A, B  lockstep.PID[ResponderMsg]
	Count int
}

// nextAsk is the requester's timer message: ask the next question.
type nextAsk struct{}

// outcome is the requester's own message that carries the reply to question N.
type outcome struct {
	N, V int
	Err  error
}

func (Request) responderMsg()    {}
func (respond) responderMsg()    {}
func (StartRound) requesterMsg() {}
func (nextAsk) requesterMsg()    {}
func (outcome) requesterMsg()    {}

// Describe returns "request <N>".
func (m Request) Describe() string { return fmt.Sprintf("request %d", m.N) }

// Describe returns "respond <N>".
func (m respond) Describe() string { return fmt.Sprintf("respond %d", m.N) }

// Describe returns "start-round <Count>".
func (m StartRound) Describe() string { return fmt.Sprintf("start-round %d", m.Count) }

// Describe returns "next-ask".
func (nextAsk) Describe() string { return "next-ask" }

// Describe returns "outcome <N>".
func (m outcome) Describe() string { return fmt.Sprintf("outcome %d", m.N) }

// RequestsProbe is what the requester reports to the test.
type RequestsProbe struct {
	Answered int // asks answered
	TimedOut int // asks that ended with ErrTimeout
	Other    int // asks that ended with another error
	Wrong    int // answers that were not the square of the question
}

// SpawnResponder spawns the actor "responder" under the user guardian of sys. It answers
// Request{N} with N*N after a delay of 5 ms plus a draw from its random stream between 0 and 29
// ms, or hands the request on to its Via (AT-MSG-33).
func SpawnResponder(sys *lockstep.System) (lockstep.PID[ResponderMsg], error) {
	return lockstep.Spawn(sys, "responder", func() lockstep.Actor[ResponderMsg] { return &responder{} })
}

// SpawnRequester spawns the actor "requester" under the user guardian of sys. On StartRound it
// asks one question every 10 ms, Count in all, each with a timeout of 40 ms, and counts the
// outcomes in p.
func SpawnRequester(sys *lockstep.System, p *RequestsProbe) (lockstep.PID[RequesterMsg], error) {
	return lockstep.Spawn(sys, "requester", func() lockstep.Actor[RequesterMsg] { return &requester{p: p} })
}

type responder struct{}

func (*responder) Receive(ctx *lockstep.Context[ResponderMsg], msg ResponderMsg) {
	switch m := msg.(type) {
	case Request:
		if !m.Via.IsZero() {
			_ = lockstep.Send(ctx, m.Via, ResponderMsg(Request{N: m.N, ReplyTo: m.ReplyTo}))
			return
		}
		k := ctx.Rand().IntN(30)
		ctx.After(time.Duration(5+k)*time.Millisecond, ResponderMsg(respond{N: m.N, ReplyTo: m.ReplyTo}))
	case respond:
		m.ReplyTo.Send(ctx, m.N*m.N)
	}
}

type requester struct {
	p     *RequestsProbe
	round StartRound
	timer lockstep.TimerID
	n     int // the questions asked so far
}

func (a *requester) Receive(ctx *lockstep.Context[RequesterMsg], msg RequesterMsg) {
	switch m := msg.(type) {
	case StartRound:
		a.round = m
		a.timer = ctx.Every(10*time.Millisecond, RequesterMsg(nextAsk{}))
	case nextAsk:
		a.n++
		n := a.n
		to := a.round.A
		if n%2 == 0 {
			to = a.round.B
		}
		var via lockstep.PID[ResponderMsg]
		if n == a.round.Count/2 {
			via = a.round.B
		}
		r := lockstep.Ask(ctx, to, func(r lockstep.ReplyTo[int]) ResponderMsg { return Request{N: n, ReplyTo: r, Via: via} },
			lockstep.WithTimeout(40*time.Millisecond))
		lockstep.Pipe(ctx, r, func(v int, err error) RequesterMsg { return outcome{N: n, V: v, Err: err} })
		if n == a.round.Count {
			ctx.CancelTimer(a.timer)
		}
	case outcome:
		switch {
		case m.Err == nil:
			a.p.Answered++
			if m.V != m.N*m.N {
				a.p.Wrong++
			}
		case errors.Is(m.Err, lockstep.ErrTimeout):
			a.p.TimedOut++
		default:
			a.p.Other++
		}
	}
}
