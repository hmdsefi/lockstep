// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package protoactor runs the benchmark workloads on protoactor-go.
//
// The system uses protoactor-go's defaults, which include an unbounded mailbox and a dispatcher
// throughput of 300 messages per turn, with one exception: it discards its log.
//
// The messages are the harness values, passed as any. A fan-out ack carries the index of its
// consumer, so that every message between actors goes out with Send and none carries a sender.
package protoactor

import (
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/asynkron/protoactor-go/actor"

	"github.com/hmdsefi/lockstep/bench/harness"
)

const (
	// credits is how many items the fan-out producer may send a consumer before that consumer
	// acknowledges any; ackEvery is how many items a consumer acknowledges with one Ack.
	credits  = 512
	ackEvery = 256

	// askTimeout is the timeout of every ask.
	askTimeout = 5 * time.Second
)

// Lib is protoactor-go.
type Lib struct{}

// Name returns "protoactor".
func (Lib) Name() string { return "protoactor" }

// Start builds a system with actor.NewActorSystem and protoactor-go's defaults, except that the
// system discards its log: protoactor-go logs to standard error by default, and a line written
// while a benchmark runs would break its result line.
func (Lib) Start() (harness.System, error) {
	sys := actor.NewActorSystem(actor.WithLoggerFactory(discardLogger))
	return &system{sys: sys}, nil
}

// discardLogger is the system's logger factory.
func discardLogger(*actor.ActorSystem) *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

// system is a running protoactor-go system.
type system struct {
	sys  *actor.ActorSystem
	pids []*actor.PID // every actor spawned but the idle ones, which idle holds

	// The idle workload, from PrepareIdle.
	idle      []*actor.PID
	idleDone  *latch
	idleProps *actor.Props
}

// spawn spawns an actor from outside the system and remembers it for Close.
func (s *system) spawn(produce actor.Producer) *actor.PID {
	pid := s.sys.Root.Spawn(actor.PropsFromProducer(produce))
	s.pids = append(s.pids, pid)
	return pid
}

// Close poisons every actor, waits until each has stopped, and shuts the system down.
func (s *system) Close() error {
	futures := make([]actor.Future, 0, len(s.pids)+len(s.idle))
	for _, pid := range s.pids {
		futures = append(futures, s.sys.Root.PoisonFuture(pid))
	}
	for _, pid := range s.idle {
		if pid != nil { // SpawnIdle may have stopped early
			futures = append(futures, s.sys.Root.PoisonFuture(pid))
		}
	}
	var first error
	failed := 0
	for _, f := range futures {
		if err := f.Wait(); err != nil {
			if first == nil {
				first = err
			}
			failed++
		}
	}
	s.sys.Shutdown()
	if first != nil {
		return fmt.Errorf("protoactor: %d of %d actors did not stop: %w", failed, len(futures), first)
	}
	return nil
}

// latch is the completion signal of a run: done closes when the count reaches zero.
type latch struct {
	left atomic.Int64
	done chan struct{}
}

// reset arms l for a run that ends after n count-downs. Every count-down of the previous run
// must have happened.
func (l *latch) reset(n int) {
	l.left.Store(int64(n))
	l.done = make(chan struct{})
	if n <= 0 {
		close(l.done)
	}
}

// countDown counts one part of the run as done.
func (l *latch) countDown() {
	if l.left.Add(-1) == 0 {
		close(l.done)
	}
}

// wait waits for the run to end.
func (l *latch) wait() { <-l.done }

// Ping-pong (BEN-040). The pair's quota travels in Seq: a Ping's Seq is the number of messages
// the pair still delivers, counting that Ping, and its Pong carries the same Seq. A starts with
// Ping{Seq: quota}. B counts a Ping and answers it unless its Seq is 1; A counts a Pong and sends
// Ping{Seq: Seq-2} unless its Seq is 2. The actor that receives the last message counts the pair
// down; a pair with quota 0 sends nothing and A counts it down at its start. So an odd quota ends
// on a Ping that B does not answer, and neither actor keeps the quota or shares state with the
// other.

// pinger is a pair's first actor A: it receives harness.PingStart and harness.Pong, and sends the
// Pings.
type pinger struct {
	peer  *actor.PID
	done  *latch
	count int64
}

func (a *pinger) Receive(ctx actor.Context) {
	switch m := ctx.Message().(type) {
	case harness.PingStart:
		if m.Quota <= 0 {
			a.done.countDown()
			return
		}
		ctx.Send(a.peer, harness.Ping{Seq: int64(m.Quota)})
	case harness.Pong:
		a.count++
		if m.Seq <= 2 {
			a.done.countDown()
			return
		}
		ctx.Send(a.peer, harness.Ping{Seq: m.Seq - 2})
	}
}

// ponger is a pair's second actor B: it answers each Ping but the pair's last with a Pong.
type ponger struct {
	peer  *actor.PID
	done  *latch
	count int64
}

func (b *ponger) Receive(ctx actor.Context) {
	m, ok := ctx.Message().(harness.Ping)
	if !ok {
		return
	}
	b.count++
	if m.Seq <= 1 {
		b.done.countDown()
		return
	}
	ctx.Send(b.peer, harness.Pong(m))
}

// pingPong is a spawned ping-pong workload.
type pingPong struct {
	sys     *actor.ActorSystem
	as      []*actor.PID
	pingers []*pinger
	pongers []*ponger
	done    *latch
}

// PingPong spawns pairs pairs. Each producer returns an actor the workload keeps, so Run can read
// its count once the run is done; these actors never fail, so no producer runs twice.
func (s *system) PingPong(pairs int) (harness.Workload, error) {
	w := &pingPong{
		sys:     s.sys,
		as:      make([]*actor.PID, pairs),
		pingers: make([]*pinger, pairs),
		pongers: make([]*ponger, pairs),
		done:    &latch{},
	}
	for i := range pairs {
		a := &pinger{done: w.done}
		pa := s.spawn(func() actor.Actor { return a })
		b := &ponger{done: w.done}
		pb := s.spawn(func() actor.Actor { return b })
		// Neither actor reads its peer before Run's PingStart, which Run sends after these writes.
		a.peer, b.peer = pb, pa
		w.as[i], w.pingers[i], w.pongers[i] = pa, a, b
	}
	return w, nil
}

// Run gives pair i a quota of n/pairs messages, plus one for i < n%pairs. It must not run
// concurrently with itself.
func (w *pingPong) Run(n int) (int, error) {
	pairs := len(w.as)
	if n <= 0 || pairs == 0 {
		return 0, nil
	}
	before := w.delivered()
	w.done.reset(pairs)
	for i, pa := range w.as {
		quota := n / pairs
		if i < n%pairs {
			quota++
		}
		w.sys.Root.Send(pa, harness.PingStart{Quota: quota})
	}
	w.done.wait()
	return int(w.delivered() - before), nil
}

// delivered sums the counts of every actor of w.
func (w *pingPong) delivered() int64 {
	var sum int64
	for i := range w.pingers {
		sum += w.pingers[i].count + w.pongers[i].count
	}
	return sum
}

// Fan-out (BEN-050).

// ack is a consumer's harness.Ack. It names the consumer, since a message sent with Send does not
// tell the producer who sent it.
type ack struct {
	harness.Ack
	consumer int
}

// producer is P: it receives harness.FanStart and ack, and sends item j to consumer
// j%len(consumers), in order, while that consumer has credit, and otherwise waits for its ack.
type producer struct {
	consumers []*actor.PID
	credits   []int // per consumer
	n, next   int   // the items of this run, and the next one to send
}

func (p *producer) Receive(ctx actor.Context) {
	switch m := ctx.Message().(type) {
	case harness.FanStart:
		p.n, p.next = m.N, 0
	case ack:
		p.credits[m.consumer] += ackEvery
	default:
		return
	}
	for p.next < p.n {
		c := p.next % len(p.consumers)
		if p.credits[c] == 0 {
			return
		}
		ctx.Send(p.consumers[c], harness.Item{Seq: int64(p.next)})
		p.credits[c]--
		p.next++
	}
}

// consumer counts its items and acks every ackEvery of them.
type consumer struct {
	index    int
	producer *actor.PID
	run      *fanOut
	count    int64
}

func (c *consumer) Receive(ctx actor.Context) {
	item, ok := ctx.Message().(harness.Item)
	if !ok {
		return
	}
	c.count++
	if c.count%ackEvery == 0 {
		ctx.Send(c.producer, ack{consumer: c.index})
	}
	// The consumer's last item of the run is the one with no item for it after it.
	if item.Seq+int64(len(c.run.consumers)) >= c.run.n {
		c.run.done.countDown()
	}
}

// fanOut is a spawned fan-out workload.
type fanOut struct {
	sys       *actor.ActorSystem
	producer  *actor.PID
	consumers []*consumer
	n         int64 // the items of the current run; read by the consumers
	done      *latch
}

// FanOut spawns the producer and consumers consumers. As in PingPong, each producer returns an
// actor the workload keeps.
func (s *system) FanOut(consumers int) (harness.Workload, error) {
	w := &fanOut{sys: s.sys, consumers: make([]*consumer, consumers), done: &latch{}}
	p := &producer{consumers: make([]*actor.PID, consumers), credits: make([]int, consumers)}
	pp := s.spawn(func() actor.Actor { return p })
	w.producer = pp
	for i := range consumers {
		c := &consumer{index: i, producer: pp, run: w}
		pc := s.spawn(func() actor.Actor { return c })
		// P reads its consumers and credits only after Run's FanStart, sent after these writes.
		p.consumers[i], p.credits[i] = pc, credits
		w.consumers[i] = c
	}
	return w, nil
}

// Run sends n items. It must not run concurrently with itself.
func (w *fanOut) Run(n int) (int, error) {
	if n <= 0 || len(w.consumers) == 0 {
		return 0, nil
	}
	before := w.delivered()
	w.n = int64(n)
	// Each consumer that gets an item counts down once, at its last item.
	w.done.reset(min(n, len(w.consumers)))
	w.sys.Root.Send(w.producer, harness.FanStart{N: n})
	w.done.wait()
	return int(w.delivered() - before), nil
}

// delivered sums the consumers' counts.
func (w *fanOut) delivered() int64 {
	var sum int64
	for _, c := range w.consumers {
		sum += c.count
	}
	return sum
}

// Idle actors (BEN-060).

// idler counts its Hello and counts down the spawn.
type idler struct {
	done  *latch
	count int64
}

func (a *idler) Receive(ctx actor.Context) {
	if _, ok := ctx.Message().(harness.Hello); ok {
		a.count++
		a.done.countDown()
	}
}

// PrepareIdle allocates the PID slice, the completion signal and the props of n actors.
func (s *system) PrepareIdle(n int) {
	s.idle = make([]*actor.PID, n)
	done := &latch{}
	done.reset(n)
	s.idleDone = done
	s.idleProps = actor.PropsFromProducer(func() actor.Actor { return &idler{done: done} })
}

// SpawnIdle spawns the prepared actors, sends each a Hello, and waits until each has counted it.
func (s *system) SpawnIdle() error {
	if s.idleDone == nil {
		return errors.New("protoactor: SpawnIdle before PrepareIdle")
	}
	for i := range s.idle {
		pid := s.sys.Root.Spawn(s.idleProps)
		s.idle[i] = pid
		s.sys.Root.Send(pid, harness.Hello{})
	}
	s.idleDone.wait()
	return nil
}

// Echo (BEN-070, BEN-080).

// echo records the latency of each harness.Stamp in rec and then signals the sender; it answers
// each harness.EchoReq with 1.
type echo struct {
	rec    *harness.Recorder
	signal chan struct{}
}

func (e *echo) Receive(ctx actor.Context) {
	switch m := ctx.Message().(type) {
	case harness.Stamp:
		e.rec.Add(e.rec.Now() - m.At)
		e.signal <- struct{}{}
	case harness.EchoReq:
		ctx.Respond(int64(1))
	}
}

// echoRef is a spawned echo actor.
type echoRef struct {
	sys    *actor.ActorSystem
	pid    *actor.PID
	rec    *harness.Recorder
	signal chan struct{}
}

// Echo spawns an echo actor that records into rec.
func (s *system) Echo(rec *harness.Recorder) (harness.Echo, error) {
	signal := make(chan struct{}, 1)
	pid := s.spawn(func() actor.Actor { return &echo{rec: rec, signal: signal} })
	return &echoRef{sys: s.sys, pid: pid, rec: rec, signal: signal}, nil
}

// Send sends a stamp and waits until the echo actor has recorded it. It must not run
// concurrently with itself.
func (e *echoRef) Send() error {
	e.sys.Root.Send(e.pid, harness.Stamp{At: e.rec.Now()})
	<-e.signal
	return nil
}

// Ask asks the echo actor and waits for its answer.
func (e *echoRef) Ask() error {
	v, err := e.sys.Root.RequestFuture(e.pid, harness.EchoReq{}, askTimeout).Result()
	if err != nil {
		return err
	}
	if n, ok := v.(int64); !ok || n != 1 {
		return fmt.Errorf("protoactor: echo answered %v, want 1", v)
	}
	return nil
}
