// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package toys

import (
	"errors"
	"fmt"
	"time"

	"github.com/hmdsefi/lockstep"
)

// ServerMsg is the message type of the server.
type ServerMsg interface{ serverMsg() }

// Query asks the server for the square of N.
type Query struct {
	N       int
	ReplyTo lockstep.ReplyTo[int]
}

// answer is the server's own message that sends the reply to a Query once its delay is over.
type answer struct {
	N       int
	ReplyTo lockstep.ReplyTo[int]
}

// ClientMsg is the message type of the client.
type ClientMsg interface{ clientMsg() }

// StartAsks tells the client to ask Server for Count squares.
type StartAsks struct {
	Server lockstep.PID[ServerMsg]
	Count  int
}

// next is the client's timer message: ask the next question.
type next struct{}

// result is the client's own message that carries the reply to question N.
type result struct {
	N, V int
	Err  error
}

func (Query) serverMsg()     {}
func (answer) serverMsg()    {}
func (StartAsks) clientMsg() {}
func (next) clientMsg()      {}
func (result) clientMsg()    {}

// Describe returns "query <N>".
func (m Query) Describe() string { return fmt.Sprintf("query %d", m.N) }

// Describe returns "answer <N>".
func (m answer) Describe() string { return fmt.Sprintf("answer %d", m.N) }

// Describe returns "start-asks <Count>".
func (m StartAsks) Describe() string { return fmt.Sprintf("start-asks %d", m.Count) }

// Describe returns "next".
func (next) Describe() string { return "next" }

// Describe returns "result <N>".
func (m result) Describe() string { return fmt.Sprintf("result %d", m.N) }

// AskProbe is what the client reports to the test.
type AskProbe struct{ Answered, TimedOut, Wrong int }

// SpawnServer spawns the actor "server" under the user guardian of sys. It answers Query{N} with
// N*N after a delay of 5 ms plus a draw from its random stream between 0 and 29 ms (DET-240).
func SpawnServer(sys *lockstep.System) (lockstep.PID[ServerMsg], error) {
	return lockstep.Spawn(sys, "server", func() lockstep.Actor[ServerMsg] { return &server{} })
}

// SpawnClient spawns the actor "client" under the user guardian of sys. On StartAsks it asks
// the server a question every 10 ms, Count questions in all, each with a timeout of 25 ms, and
// counts in p the answers, the timeouts and the wrong answers (DET-240).
func SpawnClient(sys *lockstep.System, p *AskProbe) (lockstep.PID[ClientMsg], error) {
	return lockstep.Spawn(sys, "client", func() lockstep.Actor[ClientMsg] { return &client{p: p} })
}

type server struct{}

func (*server) Receive(ctx *lockstep.Context[ServerMsg], msg ServerMsg) {
	switch m := msg.(type) {
	case Query:
		k := ctx.Rand().IntN(30)
		ctx.After(time.Duration(5+k)*time.Millisecond, ServerMsg(answer(m)))
	case answer:
		m.ReplyTo.Send(ctx, m.N*m.N)
	}
}

type client struct {
	p      *AskProbe
	server lockstep.PID[ServerMsg]
	count  int
	timer  lockstep.TimerID
	n      int // the questions asked so far
}

func (a *client) Receive(ctx *lockstep.Context[ClientMsg], msg ClientMsg) {
	switch m := msg.(type) {
	case StartAsks:
		a.server, a.count = m.Server, m.Count
		a.timer = ctx.Every(10*time.Millisecond, ClientMsg(next{}))
	case next:
		a.n++
		n := a.n
		r := lockstep.Ask(ctx, a.server, func(r lockstep.ReplyTo[int]) ServerMsg { return Query{N: n, ReplyTo: r} },
			lockstep.WithTimeout(25*time.Millisecond))
		lockstep.Pipe(ctx, r, func(v int, err error) ClientMsg { return result{N: n, V: v, Err: err} })
		if n == a.count {
			ctx.CancelTimer(a.timer)
		}
	case result:
		switch {
		case m.Err == nil:
			a.p.Answered++
			if m.V != m.N*m.N {
				a.p.Wrong++
			}
		case errors.Is(m.Err, lockstep.ErrTimeout):
			a.p.TimedOut++
		}
	}
}
