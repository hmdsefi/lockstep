// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package lockstep runs the benchmark workloads on lockstep's production engine, the system
// lockstep.New builds.
//
// The system uses lockstep's defaults with three exceptions: it discards its log; the fan-out
// producer's mailbox holds 4 messages per consumer and refuses a send when full (FailSend); and an
// idle workload of more than 900,000 actors runs on a system whose actor cap is 2,000,000.
//
// An actor that receives several message types takes M = any and switches on the type; an actor
// that receives one type takes that type as M. The messages are the harness values, wrapped where
// lockstep needs more in the message itself: a fan-out ack names its consumer, since lockstep does
// not tell an actor who sent a message, and the echo request carries the handle its answer goes
// to. The ask-with-pipe workload, which only lockstep runs, has messages of its own.
package lockstep

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/bench/harness"
)

const (
	// credits is how many items the fan-out producer may send a consumer before that consumer
	// acknowledges any; ackEvery is how many items a consumer acknowledges with one Ack.
	credits  = 512
	ackEvery = 256

	// largeIdle is the most idle actors a system with lockstep's default actor cap holds;
	// largeCap is the cap of a system built for more.
	largeIdle = 900_000
	largeCap  = 2_000_000
)

// askTimeout is the timeout of every ask, built once so an ask allocates no option.
var askTimeout = lockstep.WithTimeout(5 * time.Second)

// Lib is lockstep.
type Lib struct{}

// Name returns "lockstep".
func (Lib) Name() string { return "lockstep" }

// Start builds a system with lockstep.New and lockstep's defaults, except that the system
// discards its log: lockstep logs to slog's default logger, which writes to standard error, and
// a line written while a benchmark runs would break its result line.
func (Lib) Start() (harness.System, error) {
	return &system{sys: newSystem(0), idleLimit: largeIdle, idleCap: largeCap}, nil
}

// newSystem builds a system whose actor cap is maxActors, or lockstep's default when maxActors is
// 0.
func newSystem(maxActors int) *lockstep.System {
	opts := []lockstep.Option{lockstep.WithLogger(slog.New(slog.DiscardHandler))}
	if maxActors > 0 {
		opts = append(opts, lockstep.WithMaxActors(maxActors))
	}
	return lockstep.New(opts...)
}

// system is a running lockstep system.
type system struct {
	sys *lockstep.System
	// idleLimit is the most idle actors PrepareIdle runs on the started system; for more, it
	// replaces the system, once, with one whose actor cap is idleCap. Start sets largeIdle and
	// largeCap; TestPrepareIdleActorCap sets small values.
	idleLimit, idleCap int
	capped             bool  // PrepareIdle replaced the system
	err                error // from shutting down the system PrepareIdle replaced

	// The idle workload, from PrepareIdle.
	idle     []lockstep.PID[harness.Hello]
	idleDone *latch
	newIdler func() lockstep.Actor[harness.Hello]

	// fanProbe, when set, observes the credit flow of the fan-outs spawned afterwards
	// (TestATBEN04FanOutCredits).
	fanProbe fanProbe
	// skipShutdown makes Close return without stopping the system (TestATBEN13LeakCaught).
	skipShutdown bool
}

// Close shuts the system down and waits until every actor has stopped, however long that takes.
func (s *system) Close() error {
	if s.skipShutdown {
		return s.err
	}
	return errors.Join(s.err, s.sys.Shutdown(context.Background()))
}

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
		close(l.done) //lockstep:allow LS100 benchmark completion signal
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
	peer  lockstep.PID[harness.Ping]
	done  *latch
	count int64
}

func (a *pinger) Receive(ctx *lockstep.Context[any], msg any) {
	switch m := msg.(type) {
	case harness.PingStart:
		if m.Quota <= 0 {
			a.done.countDown()
			return
		}
		a.ping(ctx, int64(m.Quota))
	case harness.Pong:
		a.count++
		if m.Seq <= 2 {
			a.done.countDown()
			return
		}
		a.ping(ctx, m.Seq-2)
	}
}

func (a *pinger) ping(ctx *lockstep.Context[any], seq int64) {
	if err := lockstep.Send(ctx, a.peer, harness.Ping{Seq: seq}); err != nil {
		a.done.fail(err)
	}
}

// ponger is a pair's second actor B: it answers each Ping but the pair's last with a Pong.
type ponger struct {
	peer  lockstep.PID[any]
	done  *latch
	count int64
}

func (b *ponger) Receive(ctx *lockstep.Context[harness.Ping], m harness.Ping) {
	b.count++
	if m.Seq <= 1 {
		b.done.countDown()
		return
	}
	if err := lockstep.Send[any](ctx, b.peer, harness.Pong(m)); err != nil {
		b.done.fail(err)
	}
}

// pingPong is a spawned ping-pong workload.
type pingPong struct {
	sys     *lockstep.System
	as      []lockstep.PID[any]
	pingers []*pinger
	pongers []*ponger
	done    *latch
}

// PingPong spawns pairs pairs. Each factory returns an actor the workload keeps, so Run can read
// its count once the run is done; these actors never fail, so no factory runs twice.
func (s *system) PingPong(pairs int) (harness.Workload, error) {
	w := &pingPong{
		sys:     s.sys,
		as:      make([]lockstep.PID[any], pairs),
		pingers: make([]*pinger, pairs),
		pongers: make([]*ponger, pairs),
		done:    &latch{},
	}
	for i := range pairs {
		a := &pinger{done: w.done}
		pa, err := lockstep.Spawn(s.sys, "", func() lockstep.Actor[any] { return a })
		if err != nil {
			return nil, err
		}
		b := &ponger{peer: pa, done: w.done}
		pb, err := lockstep.Spawn(s.sys, "", func() lockstep.Actor[harness.Ping] { return b })
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
		if err := lockstep.Send[any](w.sys, pa, harness.PingStart{Quota: quota}); err != nil {
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

// ack is a consumer's harness.Ack. It names the consumer, since a lockstep actor is not told who
// sent a message.
type ack struct {
	harness.Ack
	consumer int
}

// fanProbe observes a fan-out's credit flow.
type fanProbe interface {
	sent(consumer int)     // after P sent consumer an item
	stopped(consumer int)  // when P stops because consumer has no credit
	received(consumer int) // when consumer receives an item, before it acks
	acked(consumer int)    // after consumer sent an ack
}

// producer is P: it receives harness.FanStart and ack, and sends item j to consumer
// j%len(consumers), in order, while that consumer has credit, and otherwise waits for its ack.
type producer struct {
	consumers []lockstep.PID[harness.Item]
	credits   []int // per consumer
	n, next   int   // the items of this run, and the next one to send
	done      *latch
	probe     fanProbe
}

func (p *producer) Receive(ctx *lockstep.Context[any], msg any) {
	switch m := msg.(type) {
	case harness.FanStart:
		p.n, p.next = m.N, 0
	case ack:
		p.credits[m.consumer] += ackEvery
	}
	for p.next < p.n {
		c := p.next % len(p.consumers)
		if p.credits[c] == 0 {
			if p.probe != nil {
				p.probe.stopped(c)
			}
			return
		}
		if err := lockstep.Send(ctx, p.consumers[c], harness.Item{Seq: int64(p.next)}); err != nil {
			p.next = p.n
			p.done.fail(err)
			return
		}
		p.credits[c]--
		p.next++
		if p.probe != nil {
			p.probe.sent(c)
		}
	}
}

// consumer counts its items and acks every ackEvery of them.
type consumer struct {
	index    int
	producer lockstep.PID[any]
	run      *fanOut
	probe    fanProbe
	count    int64
}

func (c *consumer) Receive(ctx *lockstep.Context[harness.Item], item harness.Item) {
	c.count++
	if c.probe != nil {
		c.probe.received(c.index)
	}
	if c.count%ackEvery == 0 {
		if err := lockstep.Send[any](ctx, c.producer, ack{consumer: c.index}); err != nil {
			c.run.done.fail(err)
			return
		}
		if c.probe != nil {
			c.probe.acked(c.index)
		}
	}
	// The consumer's last item of the run is the one with no item for it after it.
	if item.Seq+int64(len(c.run.consumers)) >= c.run.n {
		c.run.done.countDown()
	}
}

// fanOut is a spawned fan-out workload.
type fanOut struct {
	sys       *lockstep.System
	producer  lockstep.PID[any]
	consumers []*consumer
	n         int64 // the items of the current run; read by the consumers
	done      *latch
}

// FanOut spawns the producer and consumers consumers. As in PingPong, each factory returns an
// actor the workload keeps.
func (s *system) FanOut(consumers int) (harness.Workload, error) {
	w := &fanOut{sys: s.sys, consumers: make([]*consumer, consumers), done: &latch{}}
	p := &producer{
		consumers: make([]lockstep.PID[harness.Item], consumers),
		credits:   make([]int, consumers),
		done:      w.done,
		probe:     s.fanProbe,
	}
	pp, err := lockstep.Spawn(s.sys, "", func() lockstep.Actor[any] { return p },
		lockstep.WithMailbox(4*consumers, lockstep.FailSend))
	if err != nil {
		return nil, err
	}
	w.producer = pp
	for i := range consumers {
		c := &consumer{index: i, producer: pp, run: w, probe: s.fanProbe}
		pc, err := lockstep.Spawn(s.sys, "", func() lockstep.Actor[harness.Item] { return c })
		if err != nil {
			return nil, err
		}
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
	if err := lockstep.Send[any](w.sys, w.producer, harness.FanStart{N: n}); err != nil {
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
	done  *latch
	count int64
}

func (a *idler) Receive(_ *lockstep.Context[harness.Hello], _ harness.Hello) {
	a.count++
	a.done.countDown()
}

// PrepareIdle allocates the PID slice and the completion signal of n actors. With n above
// idleLimit it first replaces the started system, which must have no actors yet, with one whose
// actor cap is idleCap.
func (s *system) PrepareIdle(n int) {
	if n > s.idleLimit && !s.capped {
		s.err = s.sys.Shutdown(context.Background())
		s.sys = newSystem(s.idleCap)
		s.capped = true
	}
	s.idle = make([]lockstep.PID[harness.Hello], n)
	done := &latch{}
	done.reset(n)
	s.idleDone = done
	s.newIdler = func() lockstep.Actor[harness.Hello] { return &idler{done: done} }
}

// SpawnIdle spawns the prepared actors, sends each a Hello, and waits until each has counted it.
func (s *system) SpawnIdle() error {
	if s.err != nil {
		return s.err
	}
	if s.idleDone == nil {
		return errors.New("lockstep: SpawnIdle before PrepareIdle")
	}
	for i := range s.idle {
		pid, err := lockstep.Spawn(s.sys, "", s.newIdler)
		if err != nil {
			return err
		}
		s.idle[i] = pid
		if err := lockstep.Send(s.sys, pid, harness.Hello{}); err != nil {
			return err
		}
	}
	return s.idleDone.wait()
}

// Echo (BEN-070, BEN-080).

// echoAsk is harness.EchoReq with the handle of the ask it belongs to, which lockstep's Ask
// puts in the request.
type echoAsk struct {
	harness.EchoReq
	reply lockstep.ReplyTo[int64]
}

// newEchoAsk builds the request of an ask to the echo actor.
func newEchoAsk(rt lockstep.ReplyTo[int64]) any { return newAsk(rt) }

// newAsk builds the request of an ask to the answerer.
func newAsk(rt lockstep.ReplyTo[int64]) echoAsk { return echoAsk{reply: rt} }

// echo records the latency of each harness.Stamp in rec and then signals the sender; it answers
// each echoAsk with 1.
type echo struct {
	rec    *harness.Recorder
	signal chan struct{}
}

func (e *echo) Receive(ctx *lockstep.Context[any], msg any) {
	switch m := msg.(type) {
	case harness.Stamp:
		e.rec.Add(e.rec.Now() - m.At)
		e.signal <- struct{}{} //lockstep:allow LS100 benchmark completion signal
	case echoAsk:
		m.reply.Send(ctx, 1)
	}
}

// echoRef is a spawned echo actor.
type echoRef struct {
	sys    *lockstep.System
	pid    lockstep.PID[any]
	rec    *harness.Recorder
	signal chan struct{}
}

// Echo spawns an echo actor that records into rec.
func (s *system) Echo(rec *harness.Recorder) (harness.Echo, error) {
	signal := make(chan struct{}, 1)
	pid, err := lockstep.Spawn(s.sys, "", func() lockstep.Actor[any] {
		return &echo{rec: rec, signal: signal}
	})
	if err != nil {
		return nil, err
	}
	return &echoRef{sys: s.sys, pid: pid, rec: rec, signal: signal}, nil
}

// Send sends a stamp and waits until the echo actor has recorded it. It must not run
// concurrently with itself.
func (e *echoRef) Send() error {
	if err := lockstep.Send[any](e.sys, e.pid, harness.Stamp{At: e.rec.Now()}); err != nil {
		return err
	}
	<-e.signal
	return nil
}

// Ask asks the echo actor and waits for its answer.
func (e *echoRef) Ask() error {
	v, err := lockstep.Ask(e.sys, e.pid, newEchoAsk, askTimeout).Await(context.Background())
	if err != nil {
		return err
	}
	if v != 1 {
		return fmt.Errorf("echo answered %d, want 1", v)
	}
	return nil
}

// Ask with pipe (BEN-090).

// askStart starts a run of n round trips.
type askStart struct{ n int }

// piped is the resolution of A's ask, which lockstep.Pipe delivers to A.
type piped struct {
	v   int64
	err error
}

// toPiped turns the resolution of an ask into A's message.
func toPiped(v int64, err error) any { return piped{v: v, err: err} }

// answerer is the actor B that A asks: it answers each ask with 1.
type answerer struct{}

func (answerer) Receive(ctx *lockstep.Context[echoAsk], m echoAsk) { m.reply.Send(ctx, 1) }

// asker is the asking actor A: it receives askStart and piped. It asks the answerer B, pipes the
// answer into its own mailbox, and asks again until its run is done, recording each round trip in
// rec.
type asker struct {
	b     lockstep.PID[echoAsk]
	rec   *harness.Recorder
	done  *latch
	left  int   // round trips left in this run
	sent  int64 // rec.Now() when the pending ask was sent
	count int64
}

func (a *asker) Receive(ctx *lockstep.Context[any], msg any) {
	switch m := msg.(type) {
	case askStart:
		a.left = m.n
	case piped:
		if m.err != nil {
			a.done.fail(m.err)
			return
		}
		if m.v != 1 {
			a.done.fail(fmt.Errorf("echo answered %d, want 1", m.v))
			return
		}
		a.rec.Add(a.rec.Now() - a.sent)
		a.count++
		a.left--
	}
	if a.left <= 0 {
		a.done.countDown()
		return
	}
	a.sent = a.rec.Now()
	lockstep.Pipe(ctx, lockstep.Ask(ctx, a.b, newAsk, askTimeout), toPiped)
}

// askPipe is a spawned ask-with-pipe workload.
type askPipe struct {
	sys   *lockstep.System
	pid   lockstep.PID[any]
	asker *asker
	done  *latch
}

// AskPipe spawns the asking actor A and the answerer B it asks; A records each round trip in
// rec. As in PingPong, A's factory returns an actor the workload keeps.
func (s *system) AskPipe(rec *harness.Recorder) (harness.Workload, error) {
	pb, err := lockstep.Spawn(s.sys, "", func() lockstep.Actor[echoAsk] { return answerer{} })
	if err != nil {
		return nil, err
	}
	w := &askPipe{sys: s.sys, done: &latch{}}
	a := &asker{b: pb, rec: rec, done: w.done}
	pa, err := lockstep.Spawn(s.sys, "", func() lockstep.Actor[any] { return a })
	if err != nil {
		return nil, err
	}
	w.pid, w.asker = pa, a
	return w, nil
}

// Run makes n round trips. It must not run concurrently with itself.
func (w *askPipe) Run(n int) (int, error) {
	if n <= 0 {
		return 0, nil
	}
	before := w.asker.count
	w.done.reset(1)
	if err := lockstep.Send[any](w.sys, w.pid, askStart{n: n}); err != nil {
		return 0, err
	}
	if err := w.done.wait(); err != nil {
		return 0, err
	}
	return int(w.asker.count - before), nil
}
