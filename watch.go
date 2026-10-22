// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"fmt"
	"slices"

	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/registry"
)

// Watch asks for a Terminated signal (SUP) when the actor at addr stops for good. If addr is not
// alive, the signal is delivered with reason NoActor (SUP). A path address (PIDAt) watches the
// actor registered at its path at the time of the call. From Phase 3 an address on another node
// is watched through remoting (ACT-602). Errors: ErrNotFound for the zero address and guardian
// addresses; ErrNotLocal for another node's address in Phase 1 and 2, and from Phase 3 on a
// system without a transport; ErrStopped when the watching incarnation is stopping; a wrapped
// transport error (Phase 3).
func Watch[M any](ctx *Context[M], addr Address) error {
	// ACT-600's checks, in its order.
	ctx.mustInTurn()
	c := ctx.cell
	sys := c.sys
	if addr.IsZero() || sys.isGuardian(addr) {
		return fmt.Errorf("%w: %s", ErrNotFound, addr)
	}
	if addr.node != sys.node {
		return fmt.Errorf("%w: %s", ErrNotLocal, addr)
	}
	// Hook for 1.2-11: the watching incarnation stopping returns fmt.Errorf("%w: %s", ErrStopped,
	// c.addr) here, after the locality check and before the self check.
	if addr == c.addr {
		return nil
	}
	// ACT-620: a watch in effect stays one entry and gives one Terminated. A not-alive address is
	// in the set from the call until its Terminated is delivered or Unwatch removes it.
	if ctx.watched.has(addr) {
		return nil
	}

	// The alive check and the registration are one registry operation (ACT-600).
	w := registry.Watcher{Addr: registryAddr(c.addr), Inc: ctx.inc}
	alive, added := sys.reg.AddWatch(registryAddr(addr), w)
	ctx.watched.add(addr)
	if alive {
		if added {
			sys.recordWatch(c.addr, addr, "true")
		}
		return nil
	}
	// Not alive: Terminated{addr, NoActor} is on the system queue before Watch returns (SUP-335).
	sys.recordWatch(c.addr, addr, "false")
	c.pushSignal(Terminated{Addr: addr, Reason: NoActor})
	return nil
}

// Unwatch cancels Watch. After it returns, the incarnation receives no Terminated for addr.
func Unwatch[M any](ctx *Context[M], addr Address) {
	ctx.mustInTurn()
	// An address that is not in the watched set, the zero address and (in Phase 1 and 2) every
	// address on another node among them, is a no-op (ACT-630).
	if !ctx.watched.remove(addr) {
		return
	}
	// A not-alive watch is in the set but on no registry list, so RemoveWatch's result does not
	// decide the record (m21): the Terminated that Watch queued is skipped at delivery instead.
	c := ctx.cell
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
// whose Terminated it has not received, in insertion order. The slice keeps the order for Phase
// 3's RemoteUnwatch pass (ACT-640); the map answers membership only and is never ranged over.
// Only turns of the owning incarnation touch it, so it needs no lock. A new incarnation has a new
// Context and so an empty set.
type watchedSet struct {
	list []Address
	in   map[Address]struct{}
}

// has reports whether a is in the set.
func (w *watchedSet) has(a Address) bool {
	_, ok := w.in[a]
	return ok
}

// add puts a in the set if it is not there.
func (w *watchedSet) add(a Address) {
	if w.has(a) {
		return
	}
	if w.in == nil {
		w.in = make(map[Address]struct{})
	}
	w.in[a] = struct{}{}
	w.list = append(w.list, a)
}

// remove takes a out of the set and reports whether it was there.
func (w *watchedSet) remove(a Address) bool {
	if !w.has(a) {
		return false
	}
	delete(w.in, a)
	if i := slices.Index(w.list, a); i >= 0 {
		w.list = slices.Delete(w.list, i, i+1)
	}
	return true
}

// recordWatch emits lockstep.watch (ACT-660, ACT §8). alive is "true" or "false".
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
