// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"cmp"
	"fmt"
	"slices"
	"sync"

	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/hooks"
	"github.com/hmdsefi/lockstep/internal/registry"
)

// Watch asks for a Terminated signal when the actor at addr stops for good. If addr is not alive,
// the signal is delivered with reason NoActor. A path address (PIDAt) watches the actor
// registered at its path at the time of the call. An address on another node is watched through
// the system's transport, which delivers the Terminated when that actor stops, or with a Failed
// reason when its node restarts. Errors: ErrNotFound for the zero address and guardian
// addresses; ErrNotLocal for an address on another node when the system has no transport;
// ErrStopped when the calling actor is stopping or restarting (inside a restart's Stopped too);
// the transport's error, wrapped, when it cannot take the watch of an address on another node.
func Watch[M any](ctx *Context[M], addr Address) error {
	// ACT-600's checks, in its order.
	ctx.mustInTurn()
	c := ctx.cell
	sys := c.sys
	if addr.IsZero() || sys.isGuardian(addr) {
		return fmt.Errorf("%w: %s", ErrNotFound, addr)
	}
	var tr engine.Transport // set for an address on another node (ACT-602)
	if addr.node != sys.node {
		if tr = sys.transportOf(); tr == nil {
			return fmt.Errorf("%w: %s", ErrNotLocal, addr)
		}
	}
	if c.ending() { // only Stopped runs in a turn of an incarnation that stops or restarts
		return fmt.Errorf("%w: %s", ErrStopped, c.addr)
	}
	if addr == c.addr {
		return nil
	}
	// ACT-620, ACT-602: a watch in effect stays one entry and gives one Terminated. A not-alive
	// address is in the set from the call until its Terminated is delivered or Unwatch removes it.
	if ctx.watched.has(addr) {
		return nil
	}

	// The watch's sequence number goes with it to the Terminated that ends it (ACT-630).
	c.watchSeq++
	seq := c.watchSeq
	if tr != nil {
		return watchRemote(ctx, tr, addr, seq)
	}
	// The alive check and the registration are one registry operation (ACT-600).
	w := registry.Watcher{Addr: registryAddr(c.addr), Inc: ctx.inc}
	alive, added := sys.reg.AddWatch(registryAddr(addr), w, seq)
	ctx.watched.add(addr, seq)
	if alive {
		if added {
			sys.recordWatch(c.addr, addr, "true")
		}
		return nil
	}
	// Not alive: Terminated{addr, NoActor} is on the system queue before Watch returns (SUP-335).
	sys.recordWatch(c.addr, addr, "false")
	c.pushSystem(signalMsg{sig: Terminated{Addr: addr, Reason: NoActor}, seq: seq}, c)
	return nil
}

// watchRemote is Watch's branch for an address on another node (ACT-602): addr joins the watched
// set and the system's outgoing watches under seq, and tr gets a RemoteWatch envelope from the
// watcher to addr carrying seq. A Deliver error takes addr out again and is returned wrapped, with
// no record; otherwise lockstep.watch is recorded with alive=remote. The Terminated comes back with
// seq, through Host.Inbound (remoteTerminated) or hooks.WatchesDown.
func watchRemote[M any](ctx *Context[M], tr engine.Transport, addr Address, seq uint64) error {
	c := ctx.cell
	sys := c.sys
	ctx.watched.add(addr, seq)
	// Listed before Deliver, so a WatchesDown that runs meanwhile ends this watch too.
	sys.rwatch.addOut(c.addr, addr, seq)
	err := tr.Deliver(engine.Envelope{From: c.addr.engine(), To: addr.engine(), Payload: watchPayload{kind: hooks.RemoteWatch, seq: seq}})
	if err != nil {
		ctx.watched.remove(addr)
		sys.rwatch.removeOut(c.addr, addr, seq)
		return fmt.Errorf("lockstep: watch %s: %w", addr, err)
	}
	sys.recordWatch(c.addr, addr, "remote")
	return nil
}

// Unwatch cancels Watch. After it returns, the incarnation receives no Terminated for addr.
func Unwatch[M any](ctx *Context[M], addr Address) {
	ctx.mustInTurn()
	// An address that is not in the watched set, the zero address among them, is a no-op
	// (ACT-630).
	seq, _ := ctx.watched.seq(addr)
	if !ctx.watched.remove(addr) {
		return
	}
	c := ctx.cell
	if addr.node != c.sys.node {
		// ACT-630: the transport gets a RemoteUnwatch. A Deliver error changes nothing: the watch
		// has left the set, so a Terminated that still comes is skipped.
		c.sys.unwatchRemote(c.addr, addr, seq)
		c.sys.recordUnwatch(c.addr, addr)
		return
	}
	// A not-alive watch is in the set but on no registry list, so RemoveWatch's result does not
	// decide the record (ACT-630, ACT-660): the Terminated that Watch queued is skipped at delivery
	// instead, and so is it after a later Watch of addr, which has another sequence number.
	c.sys.reg.RemoveWatch(registryAddr(addr), registry.Watcher{Addr: registryAddr(c.addr), Inc: ctx.inc})
	c.sys.recordUnwatch(c.addr, addr)
}

// isGuardian reports whether a names the user or the system guardian, by uid or as a path address
// (ACT-030, ACT-600).
func (s *System) isGuardian(a Address) bool {
	if a.node != s.node {
		return false
	}
	for _, g := range [...]Address{s.user, s.system} {
		if a.path == g.path && (a.uid == 0 || a.uid == g.uid) {
			return true
		}
	}
	return false
}

// registryAddr converts a to the registry's address type.
func registryAddr(a Address) registry.Addr {
	return registry.Addr{Node: a.node, Path: a.path, UID: a.uid}
}

// registryWatchers returns the watchers on the watcher list of the actor registered at addr's
// path, in the order they were added and the order they are notified (ACT-650), for tests
// (AT-ACT-30, AT-ACT-31). It returns nil if no actor is registered there.
func (s *System) registryWatchers(addr Address) []Address {
	ws := s.reg.Watchers(addr.path)
	if len(ws) == 0 {
		return nil
	}
	out := make([]Address, len(ws))
	for i, w := range ws {
		out[i] = Address{node: w.Watcher.Addr.Node, path: w.Watcher.Addr.Path, uid: w.Watcher.Addr.UID}
	}
	return out
}

// watchedSet is an incarnation's watched set (ACT-600, ACT §11): the addresses it passed to Watch
// whose Terminated it has not received, in insertion order, each with the sequence number of the
// Watch call that added it (ACT-630). The slice keeps the order for Phase 3's RemoteUnwatch pass
// (ACT-640); the map answers lookups only and is never ranged over. Only turns of the owning
// incarnation touch it, so it needs no lock. A new incarnation has a new Context and so an empty
// set.
type watchedSet struct {
	list []Address
	in   map[Address]uint64
}

// has reports whether a is in the set.
func (w *watchedSet) has(a Address) bool {
	_, ok := w.in[a]
	return ok
}

// seq returns the sequence number stored for a, and whether a is in the set.
func (w *watchedSet) seq(a Address) (uint64, bool) {
	s, ok := w.in[a]
	return s, ok
}

// skips reports whether m is a Terminated that a delivery against the watched set w would skip:
// its Addr is not in w, or is there under another Watch call's sequence number (ACT-630). Such a
// signal answers no watch of the incarnation, so the not-running paths discard it without a
// record, as a delivery would. A signal of hooks.Signal answers no watch and is never skipped.
func (w *watchedSet) skips(m signalMsg) bool {
	t, ok := m.sig.(Terminated)
	if !ok || m.direct {
		return false
	}
	seq, in := w.seq(t.Addr)
	return !in || seq != m.seq
}

// add puts a in the set with sequence number seq if it is not there.
func (w *watchedSet) add(a Address, seq uint64) {
	if w.has(a) {
		return
	}
	if w.in == nil {
		w.in = make(map[Address]uint64)
	}
	w.in[a] = seq
	w.list = append(w.list, a)
}

// remove takes a out of the set and reports whether it was there. It searches the list from both
// ends (indexNearEnds): a monitor that watches many actors gets their Terminated newest first in a
// shutdown, and the list is in watch order.
func (w *watchedSet) remove(a Address) bool {
	if !w.has(a) {
		return false
	}
	delete(w.in, a)
	if i := indexNearEnds(w.list, func(x Address) bool { return x == a }); i >= 0 {
		w.list = slices.Delete(w.list, i, i+1)
	}
	return true
}

// indexNearEnds returns the index of the element of s nearest an end for which match is true, the
// newest end first, or -1 if there is none. Actors stopped together stop newest first (ACT-050,
// SUP-060, SUP-070), and actors that stop on their own tend to stop oldest first, so either finds
// an actor's entry in a few steps for any length of s.
func indexNearEnds[E any](s []E, match func(E) bool) int {
	for lo, hi := 0, len(s)-1; lo <= hi; lo, hi = lo+1, hi-1 {
		if match(s[hi]) {
			return hi
		}
		if match(s[lo]) {
			return lo
		}
	}
	return -1
}

// recordWatch emits lockstep.watch (ACT-660, ACT §8). alive is "true" or "false", or "remote" for
// an address on another node (ACT-602).
func (s *System) recordWatch(watcher, addr Address, alive string) {
	tr := s.eng.Tracer
	if !tr.Enabled() {
		return
	}
	tr.Record("lockstep.watch",
		engine.Attr{Key: "watcher", Value: watcher.String()},
		engine.Attr{Key: "addr", Value: addr.String()},
		engine.Attr{Key: "alive", Value: alive},
	)
}

// recordUnwatch emits lockstep.unwatch (ACT-660, ACT §8).
func (s *System) recordUnwatch(watcher, addr Address) {
	tr := s.eng.Tracer
	if !tr.Enabled() {
		return
	}
	tr.Record("lockstep.unwatch",
		engine.Attr{Key: "watcher", Value: watcher.String()},
		engine.Attr{Key: "addr", Value: addr.String()},
	)
}

// unwatchRemote ends the watch of watcher, a local actor, of addr on another node, whose watch
// number is seq: the watch leaves the outgoing watches and the transport gets a RemoteUnwatch
// envelope from watcher to addr carrying seq, whose Deliver error is ignored (ACT-630, ACT-640).
func (s *System) unwatchRemote(watcher, addr Address, seq uint64) {
	s.rwatch.removeOut(watcher, addr, seq)
	if tr := s.transportOf(); tr != nil { // always: a transport never detaches
		_ = tr.Deliver(engine.Envelope{From: watcher.engine(), To: addr.engine(), Payload: watchPayload{kind: hooks.RemoteUnwatch, seq: seq}})
	}
}

// unwatchAllRemote ends, in insertion order, the watches in w, the watched set of watcher's ending
// incarnation, of addresses on another node (ACT-640): one RemoteUnwatch each, and no record, as
// the incarnation's local watches leave their lists without one (endIncarnationWork).
func (s *System) unwatchAllRemote(watcher Address, w *watchedSet) {
	for _, a := range w.list {
		if a.node != s.node {
			seq, _ := w.seq(a)
			s.unwatchRemote(watcher, a, seq)
		}
	}
}

// remoteTerminated applies an inbound RemoteTerminated from watched to watcher that echoes the
// watch number seq (REM-282, ACT-630). If watcher's watch of watched is an outgoing watch under
// exactly that number, it leaves the outgoing watches and Terminated{watched, reason} is pushed to
// watcher's system queue with the number: so it is pushed at most once per watch, and its delivery
// skips it when Unwatch or the end of the incarnation came first. Otherwise it is dropped, also
// when it answers an earlier watch of the pair that Unwatch ended before a new Watch.
func (s *System) remoteTerminated(watcher, watched Address, reason StopReason, seq uint64) {
	p := remotePair{watcher: watcher, watched: watched}
	if s.rwatch.takeOut(p, seq) {
		s.pushRemoteTerminated(p, seq, reason)
	}
}

// watchesDown ends every outgoing watch of an address on node (hooks.WatchesDown): each watcher
// gets Terminated{watched, Failed(err)} with the watch's sequence number, in ascending (watched,
// watcher) text order.
func (s *System) watchesDown(node string, err error) {
	reason := Failed(err)
	ps, seqs := s.rwatch.downs(node)
	for i, p := range ps {
		s.pushRemoteTerminated(p, seqs[i], reason)
	}
}

// pushRemoteTerminated pushes Terminated{p.watched, reason} with seq to the system queue of the
// local actor p.watcher, if an actor is registered under that address.
func (s *System) pushRemoteTerminated(p remotePair, seq uint64, reason StopReason) {
	c, ok := s.lookupCell(registryAddr(p.watcher))
	if !ok {
		return
	}
	c.pushSystem(signalMsg{sig: Terminated{Addr: p.watched, Reason: reason}, seq: seq}, nil)
}

// remotePair is one watch across nodes: watcher watches watched, the address passed to Watch or
// hooks.WatchFor; one of them is on this node and the other is not.
type remotePair struct{ watcher, watched Address }

// remoteWatches holds a system's watches across nodes. out mirrors the part of the live
// incarnations' watched sets that is on other nodes, each address with its Watch call's sequence
// number: the sets belong to turns, and an inbound RemoteTerminated or WatchesDown runs outside
// them. in holds the watches of actors on other nodes on local actors (hooks.WatchFor), each with
// the sequence number of its entry on the target's watcher list and the function its end runs.
// The maps are read by key; only downs ranges over out, and it sorts what it finds. mu is taken
// before the registry's lock and never while another lock is held.
type remoteWatches struct {
	mu   sync.Mutex
	out  map[remotePair]uint64
	in   map[remotePair]remoteWatcher
	next uint64 // the sequence number of the last WatchFor registration
}

// remoteWatcher is one watch of hooks.WatchFor: its sequence number and the function its end runs.
type remoteWatcher struct {
	seq uint64
	fn  func(reason any)
}

// addOut lists watcher's watch of watched under seq.
func (r *remoteWatches) addOut(watcher, watched Address, seq uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.out == nil {
		r.out = make(map[remotePair]uint64)
	}
	r.out[remotePair{watcher: watcher, watched: watched}] = seq
}

// removeOut removes watcher's watch of watched if it is listed under seq.
func (r *remoteWatches) removeOut(watcher, watched Address, seq uint64) {
	r.takeOut(remotePair{watcher: watcher, watched: watched}, seq)
}

// takeOut removes the outgoing watch p and reports whether it was listed under seq; a watch listed
// under another number stays.
func (r *remoteWatches) takeOut(p remotePair, seq uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if s, ok := r.out[p]; ok && s == seq {
		delete(r.out, p)
		return true
	}
	return false
}

// downs removes every outgoing watch of an address on node and returns them in ascending
// (watched, watcher) text order, with their sequence numbers.
func (r *remoteWatches) downs(node string) ([]remotePair, []uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var ps []remotePair
	for p := range r.out { //lockstep:maporder the pairs are sorted below
		if p.watched.node == node {
			ps = append(ps, p)
		}
	}
	slices.SortFunc(ps, func(a, b remotePair) int {
		return cmp.Or(cmp.Compare(a.watched.String(), b.watched.String()),
			cmp.Compare(a.watcher.String(), b.watcher.String()))
	})
	seqs := make([]uint64, len(ps))
	for i, p := range ps {
		seqs[i] = r.out[p]
		delete(r.out, p)
	}
	return ps, seqs
}

// watchFor registers watcher, an address on another node, on the watcher list of the local actor
// at watched (hooks.WatchFor, ACT-605): the registry's alive check and registration are one
// operation, as Watch's are (ACT-600). A second registration of the same pair replaces the first,
// whose fn then never runs and whose cancel does nothing.
func (s *System) watchFor(watcher, watched Address, fn func(reason any)) (bool, func()) {
	w := registry.Watcher{Addr: registryAddr(watcher)}
	key := registryAddr(watched)
	p := remotePair{watcher: watcher, watched: watched}
	r := &s.rwatch
	r.mu.Lock()
	defer r.mu.Unlock()
	r.next++
	seq := r.next
	if alive, _ := s.reg.AddWatch(key, w, seq); !alive {
		return false, func() {}
	}
	if r.in == nil {
		r.in = make(map[remotePair]remoteWatcher)
	}
	r.in[p] = remoteWatcher{seq: seq, fn: fn}
	return true, func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		if e, ok := r.in[p]; ok && e.seq == seq {
			delete(r.in, p)
			s.reg.RemoveWatch(key, w)
		}
	}
}

// endRemoteWatch ends the watch of w, an entry of a stopped actor's watcher list whose watcher is
// on another node (pushTerminated): unless its cancel ran first, the watch's function runs with
// reason, in the engine context of the stop.
func (s *System) endRemoteWatch(w registry.Watch, reason StopReason) {
	r := &s.rwatch
	p := remotePair{
		watcher: Address{node: w.Watcher.Addr.Node, path: w.Watcher.Addr.Path, uid: w.Watcher.Addr.UID},
		watched: Address{node: w.Key.Node, path: w.Key.Path, uid: w.Key.UID},
	}
	r.mu.Lock()
	e, ok := r.in[p]
	if ok && e.seq == w.Seq {
		delete(r.in, p)
	}
	r.mu.Unlock()
	if ok && e.seq == w.Seq {
		e.fn(reason)
	}
}
