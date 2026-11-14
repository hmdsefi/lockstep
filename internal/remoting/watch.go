// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package remoting

import (
	"cmp"
	"maps"
	"slices"
	"strings"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/hooks"
)

// watchPair is one watch across nodes: watcher watches watched. One of them is on this node.
type watchPair struct{ watcher, watched engine.Address }

// comparePairs orders watch pairs by the text of watched, then of watcher, as hooks.WatchesDown
// orders the watches it ends.
func comparePairs(a, b watchPair) int {
	return cmp.Or(cmp.Compare(a.watched.String(), b.watched.String()), cmp.Compare(a.watcher.String(), b.watcher.String()))
}

// sortedPairs returns the keys of m in comparePairs order.
func sortedPairs[V any](m map[watchPair]V) []watchPair {
	return slices.SortedFunc(maps.Keys(m), comparePairs)
}

// inWatch is one watch of a peer's actor on a local actor, which the core registered with
// hooks.WatchFor for a WATCH frame (REM-280).
type inWatch struct {
	n      uint64 // the watcher's watch number, which the TERMINATED echoes
	cancel func() // the registration's cancel, once hooks.WatchFor returned; nil before and after the end
	// ended is set when the watch ends: the watched actor ended or was not alive, an UNWATCH or a
	// newer WATCH of the pair came, the peer restarted, or the core shut down. A registration that
	// returns after the end is cancelled at once.
	ended bool
}

// receiveWatch applies a WATCH of pair numbered n from the peer p (REM-280): the watch replaces
// one of the same pair, and is registered with hooks.WatchFor once the lock is released. If the
// watched actor is not alive the watcher gets TERMINATED no-actor at once; otherwise it gets
// TERMINATED with the actor's stop reason when the actor ends, unless an UNWATCH comes first.
func (c *Core) receiveWatch(p *peer, pair watchPair, n uint64) {
	if old := p.watchers[pair]; old != nil {
		// hooks.WatchFor replaces the old registration: its function will not run, and its cancel
		// would do nothing.
		old.ended = true
		old.cancel = nil
	}
	w := &inWatch{n: n}
	if p.watchers == nil {
		p.watchers = make(map[watchPair]*inWatch)
	}
	p.watchers[pair] = w
	c.deferred = append(c.deferred, func() { c.register(p, pair, w) })
}

// receiveUnwatch applies an UNWATCH of pair numbered n from the peer p (REM-280): it cancels the
// watch of the pair if its number is n. An UNWATCH of another number, of an earlier watch of the
// pair, changes nothing.
func (c *Core) receiveUnwatch(p *peer, pair watchPair, n uint64) {
	w := p.watchers[pair]
	if w == nil || w.n != n {
		return
	}
	delete(p.watchers, pair)
	c.endWatch(w)
}

// endWatch ends w and cancels its registration once the lock is released. A registration still
// on its way is cancelled when hooks.WatchFor returns (register).
func (c *Core) endWatch(w *inWatch) {
	w.ended = true
	if cancel := w.cancel; cancel != nil {
		w.cancel = nil
		c.deferred = append(c.deferred, cancel)
	}
}

// endWatches ends every watch of p's actors on local actors, in comparePairs order.
func (c *Core) endWatches(p *peer) {
	for _, pair := range sortedPairs(p.watchers) {
		c.endWatch(p.watchers[pair])
	}
	p.watchers = nil
}

// register registers w, the watch of pair from the peer p, with hooks.WatchFor. It runs without
// the lock, as a deferred call, and takes the lock for the result: a watched actor that is not
// alive is answered with TERMINATED no-actor, unless the watch ended meanwhile; a registration
// whose watch ended meanwhile is cancelled; any other is kept for UNWATCH. The registration's
// function may run on another goroutine before register takes the lock; watchEnded and register
// then agree through w.ended.
func (c *Core) register(p *peer, pair watchPair, w *inWatch) {
	alive, cancel := hooks.WatchFor(c.sys, pair.watcher, pair.watched, func(reason any) {
		c.watchEnded(p, pair, w, reason)
	})
	c.mu.Lock()
	defer c.unlock()
	switch {
	case !alive:
		if p.watchers[pair] == w {
			delete(p.watchers, pair)
		}
		if !w.ended {
			w.ended = true
			c.sendTerminated(pair, lockstep.NoActor, w.n)
		}
	case w.ended:
		c.deferred = append(c.deferred, cancel)
	default:
		w.cancel = cancel
	}
}

// watchEnded is the function of w's registration: the watched actor ended with reason, a
// lockstep.StopReason. Unless w ended already, the watcher gets TERMINATED with the reason and w's
// number. It runs in engine context, without the lock.
func (c *Core) watchEnded(p *peer, pair watchPair, w *inWatch, reason any) {
	c.mu.Lock()
	defer c.unlock()
	if w.ended {
		return
	}
	w.ended = true
	w.cancel = nil
	if p.watchers[pair] == w {
		delete(p.watchers, pair)
	}
	r, _ := reason.(lockstep.StopReason) // the runtime passes the stop reason
	c.sendTerminated(pair, r, w.n)
}

// sendTerminated queues a TERMINATED of pair numbered n with reason r for the watcher's node.
// After Shutdown it sends nothing.
func (c *Core) sendTerminated(pair watchPair, r lockstep.StopReason, n uint64) {
	if c.shut {
		return
	}
	c.enqueue(pair.watcher.Node, c.terminatedFrame(pair.watched, pair.watcher, n, r))
}

// terminatedFrame returns the TERMINATED frame from watched to watcher that ends the watch
// numbered n with reason r. A failure's error goes as its code and message; a message that would
// make the frame larger than MinFrameLimit, the smallest frame limit any peer announces, is cut at
// a rune boundary, so that every peer takes the frame. It reads only settings that do not change.
func (c *Core) terminatedFrame(watched, watcher engine.Address, n uint64, r lockstep.StopReason) *outFrame {
	t := Terminated{Watch: n, Kind: stopKindOf(r)}
	if t.Kind == StopFailed && r.Err != nil {
		t.Failure = Failure{Code: c.errs.Code(r.Err), Message: strings.ToValidUTF8(r.Err.Error(), "�")}
	}
	f := Frame{Kind: KindTerminated, Sender: addrText(watched), Recipient: addrText(watcher)}
	f.Payload = AppendTerminated(nil, &t)
	b := AppendFrame(nil, &f)
	if over := len(b) - prefixLen - MinFrameLimit; over > 0 && len(t.Failure.Message) > 0 {
		t.Failure.Message = cutUTF8(t.Failure.Message, max(len(t.Failure.Message)-over, 0))
		f.Payload = AppendTerminated(nil, &t)
		b = AppendFrame(nil, &f)
	}
	return &outFrame{b: b, kind: KindTerminated, from: watched, to: watcher, msg: r, watch: n}
}

// stopKindOf returns the stop kind a TERMINATED carries for r. A kind the wire has no byte for,
// StopRestarting (which never reaches a watcher), counts as failed when r has an error and as
// normal otherwise.
func stopKindOf(r lockstep.StopReason) StopKind {
	switch r.Kind {
	case lockstep.StopNormal:
		return StopNormal
	case lockstep.StopFailed:
		return StopFailed
	case lockstep.StopParentStopping:
		return StopParentStopping
	case lockstep.StopShutdown:
		return StopShutdown
	case lockstep.StopNoActor:
		return StopNoActor
	}
	if r.Err != nil {
		return StopFailed
	}
	return StopNormal
}

// stopReason returns the lockstep.StopReason of t, a TERMINATED from node: a failure becomes
// Failed with the error of Errors.Remote.
func (c *Core) stopReason(node string, t *Terminated) lockstep.StopReason {
	switch t.Kind {
	case StopFailed:
		return lockstep.Failed(c.errs.Remote(node, t.Failure.Code, t.Failure.Message))
	case StopParentStopping:
		return lockstep.ParentStopping
	case StopShutdown:
		return lockstep.Shutdown
	case StopNoActor:
		return lockstep.NoActor
	}
	return lockstep.Normal
}

// receiveTerminated applies a TERMINATED that s accepted (REM-282): the outgoing watch it names
// ends if its number is the one the TERMINATED echoes, and the host gets RemoteTerminated with the
// reason and the number. The runtime skips one whose watch Unwatch or a newer Watch replaced.
func (c *Core) receiveTerminated(s *session, f *Frame, in *inbound) {
	p := s.peer
	pair := watchPair{watcher: in.to, watched: in.from}
	if n, ok := p.watches[pair]; ok && n == in.term.Watch {
		delete(p.watches, pair)
	}
	c.inbound(s, f, in, hooks.RemotePayload{Kind: hooks.RemoteTerminated, Reason: c.stopReason(p.name, &in.term), Seq: in.term.Watch})
}

// watchFrame returns the WATCH or UNWATCH frame of pair numbered n, stamped with incarnation inc.
func watchFrame(k Kind, pair watchPair, n uint64, inc uint32) *outFrame {
	f := Frame{Kind: k, Sender: addrText(pair.watcher), Recipient: addrText(pair.watched), Payload: AppendWatch(nil, n)}
	return &outFrame{b: AppendFrame(nil, &f), kind: k, from: pair.watcher, to: pair.watched, watch: n, inc: inc}
}

// resendWatches puts in front of p's queue a WATCH for every outgoing watch on p's actors whose
// WATCH is not queued already, in comparePairs order (REM-284). An outgoing session that comes up
// calls it before it writes the queue, so a session to the same incarnation of p carries every
// watch again, whatever the last session lost; a restart has ended them all before.
func (c *Core) resendWatches(p *peer) {
	if len(p.watches) == 0 {
		return
	}
	queued := make(map[watchPair]uint64)
	for _, f := range p.queue {
		if f.kind == KindWatch {
			queued[watchPair{watcher: f.from, watched: f.to}] = f.watch
		}
	}
	var fs []*outFrame
	for _, pair := range sortedPairs(p.watches) {
		n := p.watches[pair]
		if q, ok := queued[pair]; ok && q == n {
			continue
		}
		fs = append(fs, watchFrame(KindWatch, pair, n, p.inc))
	}
	p.queue = append(fs, p.queue...)
}
