// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package hollywood runs the benchmark workloads on hollywood, the actor library
// github.com/anthdm/hollywood.
//
// The engine is the one actor.NewEngine builds from actor.NewEngineConfig, and every actor has
// hollywood's default inbox, scheduler and restart settings, with one exception: the fan-out
// producer's inbox starts with room for 4 messages per consumer. hollywood's inbox grows when it
// is full, so that size is where it starts, not a limit.
//
// hollywood has no call that shuts an engine down, so Close stops every actor the system spawned
// and waits for each to stop. The engine's own event-stream actor, which every engine starts,
// stays registered; it runs a goroutine only while it handles an event.
//
// hollywood logs through slog's default logger. It logs the life cycle of an actor at the debug
// level, which the default logger drops, and it warns when an actor answers a message that has no
// sender, which the workloads never do: they answer only requests. So the workloads write nothing
// to standard output or standard error.
//
// The messages are the harness values, passed as any. An actor ignores the messages it does not
// know, among them hollywood's own life-cycle messages (actor.Initialized, actor.Started and
// actor.Stopped). hollywood tells an actor who sent a message, so a fan-out consumer's
// acknowledgement is a plain harness.Ack and the producer finds the consumer from its sender.
package hollywood

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/anthdm/hollywood/actor"

	"github.com/hmdsefi/lockstep/bench/harness"
)

const (
	// credits is how many items the fan-out producer may send a consumer before that consumer
	// acknowledges any; ackEvery is how many items a consumer acknowledges with one Ack.
	credits  = 512
	ackEvery = 256

	// askTimeout is the timeout of every ask. closeTimeout bounds how long Close waits for the
	// actors to stop.
	askTimeout   = 5 * time.Second
	closeTimeout = time.Minute
)

// The kinds of the actors the workloads spawn. hollywood registers an actor under its kind and its
// ID, and keeps "eventstream" for its own.
const (
	kindPinger   = "pinger"
	kindPonger   = "ponger"
	kindProducer = "producer"
	kindConsumer = "consumer"
	kindIdler    = "idler"
	kindEcho     = "echo"
)

// Lib is hollywood.
type Lib struct{}

// Name returns "hollywood".
func (Lib) Name() string { return "hollywood" }

// Start builds an engine with actor.NewEngine and the default configuration.
func (Lib) Start() (harness.System, error) {
	e, err := actor.NewEngine(actor.NewEngineConfig())
	if err != nil {
		return nil, err
	}
	return &system{engine: e}, nil
}

// system is a running hollywood engine. Its methods must not be called concurrently.
type system struct {
	engine *actor.Engine
	seq    int          // the last actor ID handed out
	pids   []*actor.PID // every actor spawned, for Close

	// The idle workload, from PrepareIdle. The IDs are built there so SpawnIdle does not time
	// them.
	idleIDs  []string
	idleDone *latch
	newIdler actor.Producer
}

// spawn spawns r as an actor of the given kind with an ID no other actor of the system has, and
// remembers it for Close. hollywood spawns an actor whose ID is taken as a no-op, so every actor
// needs its own. The producer given to hollywood returns r every time, so a restart would keep
// the actor's state; these actors never fail, so none restarts.
func (s *system) spawn(kind string, r actor.Receiver, opts ...actor.OptFunc) *actor.PID {
	s.seq++
	opts = append(opts, actor.WithID(strconv.Itoa(s.seq)))
	pid := s.engine.Spawn(func() actor.Receiver { return r }, kind, opts...)
	s.pids = append(s.pids, pid)
	return pid
}

// Close stops every actor the system spawned, gracefully, and waits until each has stopped. It
// starts every stop before it waits for the first.
func (s *system) Close() error {
	stopped := make([]context.Context, len(s.pids))
	for i, pid := range s.pids {
		stopped[i] = s.engine.Poison(pid)
	}
	timeout := time.NewTimer(closeTimeout)
	defer timeout.Stop()
	for i, done := range stopped {
		select {
		case <-done.Done():
		case <-timeout.C:
			return fmt.Errorf("hollywood: actor %s still running %s after Close", s.pids[i].ID, closeTimeout)
		}
	}
	return nil
}

// latch is the completion signal of a run: done closes when the count reaches zero.
type latch struct {
	left atomic.Int64
	done chan struct{}
}

// reset arms l for a run that ends after n count-downs. Every count-down of the previous run must
// have happened.
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

func (a *pinger) Receive(ctx *actor.Context) {
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

func (b *ponger) Receive(ctx *actor.Context) {
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
	engine  *actor.Engine
	as      []*actor.PID
	pingers []*pinger
	pongers []*ponger
	done    *latch
}

// PingPong spawns pairs pairs.
func (s *system) PingPong(pairs int) (harness.Workload, error) {
	w := &pingPong{
		engine:  s.engine,
		as:      make([]*actor.PID, pairs),
		pingers: make([]*pinger, pairs),
		pongers: make([]*ponger, pairs),
		done:    &latch{},
	}
	for i := range pairs {
		a := &pinger{done: w.done}
		pa := s.spawn(kindPinger, a)
		b := &ponger{peer: pa, done: w.done}
		pb := s.spawn(kindPonger, b)
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
		w.engine.Send(pa, harness.PingStart{Quota: quota})
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

// producer is P: it receives harness.FanStart and harness.Ack, and sends item j to consumer
// j%len(consumers), in order, while that consumer has credit, and otherwise waits for its ack.
type producer struct {
	consumers []*actor.PID
	index     map[string]int // the position in consumers of the consumer whose PID has this ID
	credits   []int          // per consumer
	n, next   int            // the items of this run, and the next one to send
}

func (p *producer) Receive(ctx *actor.Context) {
	switch m := ctx.Message().(type) {
	case harness.FanStart:
		p.n, p.next = m.N, 0
	case harness.Ack:
		p.credits[p.index[ctx.Sender().ID]] += ackEvery
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
	producer *actor.PID
	run      *fanOut
	count    int64
}

func (c *consumer) Receive(ctx *actor.Context) {
	item, ok := ctx.Message().(harness.Item)
	if !ok {
		return
	}
	c.count++
	if c.count%ackEvery == 0 {
		ctx.Send(c.producer, harness.Ack{})
	}
	// The consumer's last item of the run is the one with no item for it after it.
	if item.Seq+int64(len(c.run.consumers)) >= c.run.n {
		c.run.done.countDown()
	}
}

// fanOut is a spawned fan-out workload.
type fanOut struct {
	engine    *actor.Engine
	producer  *actor.PID
	consumers []*consumer
	n         int64 // the items of the current run; read by the consumers
	done      *latch
}

// FanOut spawns the producer and consumers consumers. The producer's inbox starts with room for
// 4*consumers messages; it needs at least one consumer, since hollywood's inbox of size 0 divides
// by zero.
func (s *system) FanOut(consumers int) (harness.Workload, error) {
	if consumers < 1 {
		return nil, fmt.Errorf("hollywood: FanOut(%d): need at least one consumer", consumers)
	}
	w := &fanOut{engine: s.engine, consumers: make([]*consumer, consumers), done: &latch{}}
	p := &producer{
		consumers: make([]*actor.PID, consumers),
		index:     make(map[string]int, consumers),
		credits:   make([]int, consumers),
	}
	w.producer = s.spawn(kindProducer, p, actor.WithInboxSize(4*consumers))
	for i := range consumers {
		c := &consumer{producer: w.producer, run: w}
		pc := s.spawn(kindConsumer, c)
		// P reads its consumers and credits only after Run's FanStart, sent after these writes.
		p.consumers[i], p.index[pc.ID], p.credits[i] = pc, i, credits
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
	w.engine.Send(w.producer, harness.FanStart{N: n})
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

func (a *idler) Receive(ctx *actor.Context) {
	if _, ok := ctx.Message().(harness.Hello); ok {
		a.count++
		a.done.countDown()
	}
}

// PrepareIdle allocates what SpawnIdle needs for n actors: their IDs, room for their PIDs, and the
// completion signal.
func (s *system) PrepareIdle(n int) {
	s.idleIDs = make([]string, n)
	for i := range s.idleIDs {
		s.seq++
		s.idleIDs[i] = strconv.Itoa(s.seq)
	}
	s.pids = slices.Grow(s.pids, n)
	done := &latch{}
	done.reset(n)
	s.idleDone = done
	s.newIdler = func() actor.Receiver { return &idler{done: done} }
}

// SpawnIdle spawns the prepared actors, sends each a Hello, and waits until each has counted it.
func (s *system) SpawnIdle() error {
	if s.idleDone == nil {
		return errors.New("hollywood: SpawnIdle before PrepareIdle")
	}
	for _, id := range s.idleIDs {
		pid := s.engine.Spawn(s.newIdler, kindIdler, actor.WithID(id))
		s.pids = append(s.pids, pid)
		s.engine.Send(pid, harness.Hello{})
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

func (e *echo) Receive(ctx *actor.Context) {
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
	engine *actor.Engine
	pid    *actor.PID
	rec    *harness.Recorder
	signal chan struct{}
}

// Echo spawns an echo actor that records into rec.
func (s *system) Echo(rec *harness.Recorder) (harness.Echo, error) {
	signal := make(chan struct{}, 1)
	pid := s.spawn(kindEcho, &echo{rec: rec, signal: signal})
	return &echoRef{engine: s.engine, pid: pid, rec: rec, signal: signal}, nil
}

// Send sends a stamp and waits until the echo actor has recorded it. It must not run
// concurrently with itself.
func (e *echoRef) Send() error {
	e.engine.Send(e.pid, harness.Stamp{At: e.rec.Now()})
	<-e.signal
	return nil
}

// Ask asks the echo actor and waits for its answer.
func (e *echoRef) Ask() error {
	v, err := e.engine.Request(e.pid, harness.EchoReq{}, askTimeout).Result()
	if err != nil {
		return err
	}
	if got, ok := v.(int64); !ok || got != 1 {
		return fmt.Errorf("echo answered %v, want 1", v)
	}
	return nil
}
