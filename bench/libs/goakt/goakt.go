// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package goakt runs the benchmark workloads on GoAkt, the system actor.NewActorSystem builds.
//
// The system uses GoAkt's defaults with two exceptions: it discards its log, since GoAkt's default
// logger writes errors to standard error, and every actor is spawned long-lived, so GoAkt's
// passivation never stops an idle one. The messages are the harness values, passed as GoAkt's any.
// An actor that needs to know who sent a message asks GoAkt: the fan-out consumers answer the
// producer that sent an item, and the producer tells its consumers apart by the sender of an ack.
//
// An actor sends with ReceiveContext.Tell, which records a failed send in the receive context
// instead of returning it. The adapter does not observe such a failure: the run never ends, and Run
// blocks until the go test timeout stops the binary.
package goakt

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/tochemey/goakt/v4/actor"
	"github.com/tochemey/goakt/v4/log"

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

// Lib is GoAkt.
type Lib struct{}

// Name returns "goakt".
func (Lib) Name() string { return "goakt" }

// Start builds and starts a system with GoAkt's defaults, except that the system discards its
// log: GoAkt's default logger writes errors to standard error, and a line written while a
// benchmark runs would break its result line.
func (Lib) Start() (harness.System, error) {
	sys, err := actor.NewActorSystem("bench", actor.WithLogger(log.DiscardLogger))
	if err != nil {
		return nil, err
	}
	if err := sys.Start(context.Background()); err != nil {
		return nil, err
	}
	return &system{sys: sys}, nil
}

// system is a running GoAkt system.
type system struct {
	sys   actor.ActorSystem
	names int // how many actor names name has handed out

	// The idle workload, from PrepareIdle.
	idle      []*actor.PID
	idleNames []string
	idleDone  *latch
}

// Close stops every actor and the system. GoAkt bounds the wait with its own shutdown timeout.
func (s *system) Close() error { return s.sys.Stop(context.Background()) }

// name returns an actor name that starts with prefix and that no other name of s equals. GoAkt
// accepts a name only once per system.
func (s *system) name(prefix string) string {
	s.names++
	return prefix + "-" + strconv.Itoa(s.names)
}

// spawn spawns a under name. The actor lives until the system stops: GoAkt passivates an idle
// actor otherwise.
func (s *system) spawn(name string, a actor.Actor) (*actor.PID, error) {
	return s.sys.Spawn(context.Background(), name, a, actor.WithLongLived())
}

// plain gives an actor the PreStart and PostStop that GoAkt's Actor interface requires and these
// actors do not need. Embedded first, it adds nothing to the actor's size.
type plain struct{}

func (plain) PreStart(*actor.Context) error { return nil }
func (plain) PostStop(*actor.Context) error { return nil }

// latch is the completion signal of a run: done closes when the count reaches zero, or at the
// first failure.
type latch struct {
	left   atomic.Int64
	closed atomic.Bool
	err    error // the first failure; read after done is closed
	done   chan struct{}
}

// reset arms l for a run that ends after n count-downs. Every count-down of the previous run
// must have happened.
func (l *latch) reset(n int) {
	l.left.Store(int64(n))
	l.closed.Store(false)
	l.err = nil
	l.done = make(chan struct{})
	if n <= 0 {
		l.close(nil)
	}
}

// countDown counts one part of the run as done.
func (l *latch) countDown() {
	if l.left.Add(-1) == 0 {
		l.close(nil)
	}
}

// fail ends the run with err.
func (l *latch) fail(err error) { l.close(err) }

func (l *latch) close(err error) {
	if l.closed.CompareAndSwap(false, true) {
		l.err = err
		close(l.done)
	}
}

// wait waits for the run to end and returns its failure.
func (l *latch) wait() error {
	<-l.done
	return l.err
}

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
	plain
	peer  *actor.PID
	done  *latch
	count int64
}

func (a *pinger) Receive(rctx *actor.ReceiveContext) {
	switch m := rctx.Message().(type) {
	case harness.PingStart:
		if m.Quota <= 0 {
			a.done.countDown()
			return
		}
		rctx.Tell(a.peer, harness.Ping{Seq: int64(m.Quota)})
	case harness.Pong:
		a.count++
		if m.Seq <= 2 {
			a.done.countDown()
			return
		}
		rctx.Tell(a.peer, harness.Ping{Seq: m.Seq - 2})
	}
}

// ponger is a pair's second actor B: it answers each Ping but the pair's last with a Pong.
type ponger struct {
	plain
	peer  *actor.PID
	done  *latch
	count int64
}

func (b *ponger) Receive(rctx *actor.ReceiveContext) {
	m, ok := rctx.Message().(harness.Ping)
	if !ok {
		return
	}
	b.count++
	if m.Seq <= 1 {
		b.done.countDown()
		return
	}
	rctx.Tell(b.peer, harness.Pong(m))
}

// pingPong is a spawned ping-pong workload.
type pingPong struct {
	as      []*actor.PID
	pingers []*pinger
	pongers []*ponger
	done    *latch
}

// PingPong spawns pairs pairs. Each pair knows its peer's PID before Run starts it.
func (s *system) PingPong(pairs int) (harness.Workload, error) {
	w := &pingPong{
		as:      make([]*actor.PID, pairs),
		pingers: make([]*pinger, pairs),
		pongers: make([]*ponger, pairs),
		done:    &latch{},
	}
	for i := range pairs {
		a := &pinger{done: w.done}
		pa, err := s.spawn(s.name("pinger"), a)
		if err != nil {
			return nil, err
		}
		b := &ponger{peer: pa, done: w.done}
		pb, err := s.spawn(s.name("ponger"), b)
		if err != nil {
			return nil, err
		}
		// A reads peer only after Run's PingStart, which Run sends after this write.
		a.peer = pb
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
		if err := actor.Tell(context.Background(), pa, harness.PingStart{Quota: quota}); err != nil {
			return 0, err
		}
	}
	if err := w.done.wait(); err != nil {
		return 0, err
	}
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

// producer is P: it receives harness.FanStart and harness.Ack, and sends item j to consumer
// j%len(consumers), in order, while that consumer has credit, and otherwise waits for its ack.
type producer struct {
	plain
	consumers []*actor.PID
	index     map[*actor.PID]int // the position of each consumer in consumers
	credits   []int              // per consumer
	n, next   int                // the items of this run, and the next one to send
	done      *latch
}

func (p *producer) Receive(rctx *actor.ReceiveContext) {
	switch m := rctx.Message().(type) {
	case harness.FanStart:
		p.n, p.next = m.N, 0
	case harness.Ack:
		c, ok := p.index[rctx.Sender()]
		if !ok {
			p.next = p.n
			p.done.fail(errors.New("goakt: ack from a sender that is not a consumer"))
			return
		}
		p.credits[c] += ackEvery
	default:
		return
	}
	for p.next < p.n {
		c := p.next % len(p.consumers)
		if p.credits[c] == 0 {
			return
		}
		rctx.Tell(p.consumers[c], harness.Item{Seq: int64(p.next)})
		p.credits[c]--
		p.next++
	}
}

// consumer counts its items and acks every ackEvery of them to the producer that sent the item.
type consumer struct {
	plain
	run   *fanOut
	count int64
}

func (c *consumer) Receive(rctx *actor.ReceiveContext) {
	item, ok := rctx.Message().(harness.Item)
	if !ok {
		return
	}
	c.count++
	if c.count%ackEvery == 0 {
		rctx.Tell(rctx.Sender(), harness.Ack{})
	}
	// The consumer's last item of the run is the one with no item for it after it.
	if item.Seq+int64(len(c.run.consumers)) >= c.run.n {
		c.run.done.countDown()
	}
}

// fanOut is a spawned fan-out workload.
type fanOut struct {
	producer  *actor.PID
	consumers []*consumer
	n         int64 // the items of the current run; read by the consumers
	done      *latch
}

// FanOut spawns the producer and consumers consumers.
func (s *system) FanOut(consumers int) (harness.Workload, error) {
	w := &fanOut{consumers: make([]*consumer, consumers), done: &latch{}}
	p := &producer{
		consumers: make([]*actor.PID, consumers),
		index:     make(map[*actor.PID]int, consumers),
		credits:   make([]int, consumers),
		done:      w.done,
	}
	pp, err := s.spawn(s.name("producer"), p)
	if err != nil {
		return nil, err
	}
	w.producer = pp
	for i := range consumers {
		c := &consumer{run: w}
		pc, err := s.spawn(s.name("consumer"), c)
		if err != nil {
			return nil, err
		}
		// P reads its consumers and credits only after Run's FanStart, sent after these writes.
		p.consumers[i], p.credits[i], p.index[pc] = pc, credits, i
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
	if err := actor.Tell(context.Background(), w.producer, harness.FanStart{N: n}); err != nil {
		return 0, err
	}
	if err := w.done.wait(); err != nil {
		return 0, err
	}
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
	plain
	done  *latch
	count int64
}

func (a *idler) Receive(rctx *actor.ReceiveContext) {
	if _, ok := rctx.Message().(harness.Hello); !ok {
		return
	}
	a.count++
	a.done.countDown()
}

// PrepareIdle allocates the PID slice, the actor names and the completion signal of n actors:
// GoAkt wants a name for each actor, and building them here keeps that cost out of the spawn.
func (s *system) PrepareIdle(n int) {
	s.idle = make([]*actor.PID, n)
	s.idleNames = make([]string, n)
	for i := range s.idleNames {
		s.idleNames[i] = s.name("idle")
	}
	done := &latch{}
	done.reset(n)
	s.idleDone = done
}

// SpawnIdle spawns the prepared actors, sends each a Hello, and waits until each has counted it.
func (s *system) SpawnIdle() error {
	if s.idleDone == nil {
		return errors.New("goakt: SpawnIdle before PrepareIdle")
	}
	for i, name := range s.idleNames {
		pid, err := s.spawn(name, &idler{done: s.idleDone})
		if err != nil {
			return err
		}
		s.idle[i] = pid
		if err := actor.Tell(context.Background(), pid, harness.Hello{}); err != nil {
			return err
		}
	}
	return s.idleDone.wait()
}

// Echo (BEN-070, BEN-080).

// echo records the latency of each harness.Stamp in rec and then signals the sender; it answers
// each harness.EchoReq with int64(1).
type echo struct {
	plain
	rec    *harness.Recorder
	signal chan struct{}
}

func (e *echo) Receive(rctx *actor.ReceiveContext) {
	switch m := rctx.Message().(type) {
	case harness.Stamp:
		e.rec.Add(e.rec.Now() - m.At)
		e.signal <- struct{}{}
	case harness.EchoReq:
		rctx.Response(int64(1))
	}
}

// echoRef is a spawned echo actor.
type echoRef struct {
	pid    *actor.PID
	rec    *harness.Recorder
	signal chan struct{}
}

// Echo spawns an echo actor that records into rec.
func (s *system) Echo(rec *harness.Recorder) (harness.Echo, error) {
	signal := make(chan struct{}, 1)
	pid, err := s.spawn(s.name("echo"), &echo{rec: rec, signal: signal})
	if err != nil {
		return nil, err
	}
	return &echoRef{pid: pid, rec: rec, signal: signal}, nil
}

// Send sends a stamp and waits until the echo actor has recorded it. It must not run
// concurrently with itself.
func (e *echoRef) Send() error {
	if err := actor.Tell(context.Background(), e.pid, harness.Stamp{At: e.rec.Now()}); err != nil {
		return err
	}
	<-e.signal
	return nil
}

// Ask asks the echo actor and waits for its answer.
func (e *echoRef) Ask() error {
	v, err := actor.Ask(context.Background(), e.pid, harness.EchoReq{}, askTimeout)
	if err != nil {
		return err
	}
	if got, ok := v.(int64); !ok || got != 1 {
		return fmt.Errorf("goakt: echo answered %v, want 1", v)
	}
	return nil
}
