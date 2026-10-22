// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"fmt"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/hmdsefi/lockstep/internal/engine"
)

// askTable is a system's ask state (MSG §11): the pending asks whose asker is this system, and the
// asks this system's actors track (MSG-340). One mutex guards both, so applying a reply attempt
// is atomic with every other resolution of the same ask (MSG-420). Lock order: a caller may hold
// the table's lock while it takes the uid allocator's lock, the engine's timer lock (Cancel) or
// the tracer's lock; nothing that holds those calls back into the table.
type askTable struct {
	mu      sync.Mutex
	counter uint64                 // the number of the last ask allocated: /temp/<counter> (MSG-300 step 4)
	pending map[string]*pendingAsk // the pending asks by ask path; the uid is compared on use (MSG-420)
	order   []*pendingAsk          // the pending asks in counter order, with removed ones until compaction
	removed int                    // removed asks still in order

	tracked map[Address]actorCell // tracked ask address -> the actor that tracks it (MSG-340)
	byActor map[actorCell]*trackList
}

// pendingAsk is one pending ask of the asker system (MSG-370).
type pendingAsk struct {
	addr    Address       // the ask address
	asker   Address       // the asking actor, or the system address for a *System sender
	target  Address       // the request's target
	timeout time.Duration // the ask's timeout (MSG-320)
	cancel  engine.Cancel // the timeout timer's Cancel; nil until Ask arms it
	// resolve is the typed Reply's untyped resolver: it asserts v to R, stores the result and
	// closes the reply's done channel. It reports whether this call resolved the reply.
	resolve func(v any, err error) bool
	removed bool // no longer pending: resolved, or dropped after a builder panic
}

// trackList is the asks one actor tracks, in tracking order. Entries that left the table's
// tracked map stay until the list is compacted.
type trackList struct {
	asks []Address
	live int
}

// how is the kind of a resolution attempt; it decides the record (MSG §8).
type how uint8

const (
	byReply   how = iota + 1 // a reply attempt (MSG-420): lockstep.reply resolved, or late when not pending
	byRouting                // Ask's routing outcome (MSG-310): lockstep.reply resolved; nothing when not pending
	byTimeout                // the timer (MSG-320): lockstep.timeout; nothing when not pending
)

// resolution is one attempt to resolve an ask.
type resolution struct {
	how  how
	from Address // the replier (byReply) or the target (byRouting)
	v    any     // the answer; nil with an error
	err  error
}

// newAsk allocates the next ask address of s, /temp/<n> with a fresh uid, and enters a pending
// ask for it with the given asker, target, timeout and resolver (MSG-300 step 4). The number and
// the uid are allocated under the table's lock, so ask numbers and uids rise together.
func (s *System) newAsk(asker, target Address, timeout time.Duration, resolve func(any, error) bool) *pendingAsk {
	t := &s.asks
	t.mu.Lock()
	defer t.mu.Unlock()
	t.counter++
	p := &pendingAsk{
		addr:    Address{node: s.node, path: "/temp/" + strconv.FormatUint(t.counter, 10), uid: s.allocUID()},
		asker:   asker,
		target:  target,
		timeout: timeout,
		resolve: resolve,
	}
	if t.pending == nil {
		t.pending = make(map[string]*pendingAsk)
	}
	t.pending[p.addr.path] = p
	t.order = append(t.order, p)
	return p
}

// drop removes p without resolving it: its builder panicked (MSG-300 step 5).
func (t *askTable) drop(p *pendingAsk) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !p.removed {
		t.remove(p)
	}
}

// remove takes the pending p out of the table. The caller holds t.mu.
func (t *askTable) remove(p *pendingAsk) {
	delete(t.pending, p.addr.path)
	p.removed = true
	t.removed++
	if t.removed*2 > len(t.order) {
		t.order = slices.DeleteFunc(t.order, func(q *pendingAsk) bool { return q.removed })
		t.removed = 0
	}
}

// arm stores the Cancel of p's timeout timer. If p already resolved (a reply attempt can arrive
// through a ReplyTo that build leaked to another goroutine), it cancels the timer instead.
func (t *askTable) arm(p *pendingAsk, cancel engine.Cancel) {
	t.mu.Lock()
	if !p.removed {
		p.cancel = cancel
		t.mu.Unlock()
		return
	}
	t.mu.Unlock()
	cancel()
}

// len returns the number of pending asks.
func (t *askTable) len() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.pending)
}

// settle is the one place an ask resolves (MSG-430). Under the table's lock it looks up the
// pending ask with exactly the address ask (path and uid); if there is one, it removes it,
// cancels its timer, emits the record of res.how and resolves the reply, which closes its done
// channel. Otherwise the ask resolved already, or is unknown here: a reply attempt is recorded as
// late and every other attempt is dropped silently.
func (s *System) settle(ask Address, res resolution) {
	t := &s.asks
	t.mu.Lock()
	defer t.mu.Unlock()
	p := t.pending[ask.path]
	if p == nil || p.addr.uid != ask.uid {
		if res.how == byReply {
			s.recordReply(ask, res.from, "late", res.err)
		}
		return
	}
	t.remove(p)
	if p.cancel != nil {
		p.cancel()
	}
	if res.how == byTimeout {
		s.recordTimeout(p)
	} else {
		s.recordReply(ask, res.from, "resolved", res.err)
	}
	p.resolve(res.v, res.err)
}

// askTimedOut is the timeout timer's function (MSG-320): it resolves p with ErrTimeout if p is
// still pending.
func (s *System) askTimedOut(p *pendingAsk) {
	s.settle(p.addr, resolution{
		how: byTimeout,
		err: fmt.Errorf("%w: ask %s to %s after %s", ErrTimeout, p.addr, p.target, p.timeout),
	})
}

// replyAttempt issues one reply attempt for ask from the sender from on s (MSG-410), after the
// ReplyTo checks: a zero ask is only recorded; otherwise the ask leaves s's tracked asks and a
// local attempt is applied to s's table before replyAttempt returns (MSG-420). v is the answer, or
// nil with a non-nil err.
func (s *System) replyAttempt(ask, from Address, v any, err error) {
	if ask.IsZero() {
		s.recordReply(ask, from, "zero", err)
		return
	}
	// A reply attempt from a stopped system is dropped without a record (MSG-410); that check
	// arrives with Shutdown.
	s.asks.untrack(ask)
	if ask.node != s.node {
		// Not local: Transport.Deliver, a dead letter without one (MSG-410), is not handled yet.
		return
	}
	s.settle(ask, resolution{how: byReply, from: from, v: v, err: err})
}

// track makes the actor c track ask (MSG-340). The router calls it before it enqueues the
// request, so a reply attempt that follows the enqueue always finds the entry to remove.
func (t *askTable) track(ask Address, c actorCell) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.tracked == nil {
		t.tracked = make(map[Address]actorCell)
		t.byActor = make(map[actorCell]*trackList)
	}
	t.tracked[ask] = c
	l := t.byActor[c]
	if l == nil {
		l = &trackList{}
		t.byActor[c] = l
	}
	l.asks = append(l.asks, ask)
	l.live++
}

// untrack removes ask from the tracked asks, if it is there (MSG-410, MSG-345).
func (t *askTable) untrack(ask Address) {
	t.mu.Lock()
	defer t.mu.Unlock()
	c, ok := t.tracked[ask]
	if !ok {
		return
	}
	delete(t.tracked, ask)
	l := t.byActor[c]
	l.live--
	switch {
	case l.live == 0:
		delete(t.byActor, c)
	case l.live*2 < len(l.asks):
		l.asks = slices.DeleteFunc(l.asks, func(a Address) bool { return t.tracked[a] != c })
	}
}

// trackedBy returns the asks c tracks, in tracking order (MSG-340).
func (t *askTable) trackedBy(c actorCell) []Address {
	t.mu.Lock()
	defer t.mu.Unlock()
	l := t.byActor[c]
	if l == nil {
		return nil
	}
	out := make([]Address, 0, l.live)
	for _, a := range l.asks {
		if t.tracked[a] == c {
			out = append(out, a)
		}
	}
	return out
}

// recordAsk emits lockstep.ask (MSG §8) before the request is routed.
func (s *System) recordAsk(from Address, p *pendingAsk, msg any) {
	tr := s.eng.Tracer
	if !tr.Enabled() {
		return
	}
	tr.Record("lockstep.ask",
		engine.Attr{Key: "from", Value: from.String()},
		engine.Attr{Key: "to", Value: p.target.String()},
		engine.Attr{Key: "ask", Value: p.addr.String()},
		engine.Attr{Key: "timeout", Value: p.timeout.String()},
		engine.Attr{Key: "msg", Value: tr.Describe(msg)},
	)
}

// recordReply emits lockstep.reply (MSG §8) with the given outcome; err, if not nil, is described
// in a last attribute.
func (s *System) recordReply(ask, from Address, outcome string, err error) {
	tr := s.eng.Tracer
	if !tr.Enabled() {
		return
	}
	attrs := []engine.Attr{
		{Key: "ask", Value: ask.String()},
		{Key: "from", Value: from.String()},
		{Key: "outcome", Value: outcome},
	}
	if err != nil {
		attrs = append(attrs, engine.Attr{Key: "err", Value: tr.Describe(err)})
	}
	tr.Record("lockstep.reply", attrs...)
}

// recordTimeout emits lockstep.timeout (MSG §8) for p.
func (s *System) recordTimeout(p *pendingAsk) {
	tr := s.eng.Tracer
	if !tr.Enabled() {
		return
	}
	tr.Record("lockstep.timeout",
		engine.Attr{Key: "ask", Value: p.addr.String()},
		engine.Attr{Key: "to", Value: p.target.String()},
		engine.Attr{Key: "after", Value: p.timeout.String()},
	)
}
