// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package overload

import (
	"sync"
	"sync/atomic"

	"github.com/hmdsefi/lockstep/internal/engine"
)

// Gate combines admission sources and implements engine.Admission (OVL-150). The System sheds
// while at least one source is on. It is safe for concurrent use; AdmitSpawn and AdmitSend read
// one atomic word and take no lock.
type Gate struct {
	// refusal is the error AdmitSpawn and AdmitSend return: it names the source that turned
	// shedding on and, after that source turns off, the one that has been on longest. Nil means
	// admitting.
	refusal atomic.Pointer[engine.AdmissionError]

	// mu guards the fields below and is never held while onChange or usage runs.
	mu       sync.Mutex
	bound    bool
	onChange func(engine.AdmissionChange)
	usage    func() engine.Usage
	on       []engine.AdmissionSource // sources that are on, in the order they turned on

	// pending holds the changes made but not yet reported, oldest first. delivering is true while
	// one Set call drains it, which is what keeps onChange from running concurrently with itself.
	pending    []engine.AdmissionChange
	delivering bool
}

// NewGate returns a Gate with every source off.
func NewGate() *Gate { return new(Gate) }

// Bind implements engine.Admission.Bind. A second call panics with "overload: Gate.Bind called
// twice". A nil onChange reports nothing; a nil usage makes Usage return the zero value.
func (g *Gate) Bind(onChange func(engine.AdmissionChange), usage func() engine.Usage) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.bound {
		panic("overload: Gate.Bind called twice")
	}
	g.bound = true
	g.onChange = onChange
	g.usage = usage
}

// Set turns src on or off with the measured values of the change (OVL-150). A call that does not
// change src's state does nothing. Otherwise the change is reported to the bound onChange, its
// Shedding being the combined state after the change, outside every lock of the Gate.
//
// Changes are reported one at a time, in the order they were made. A Set made while onChange is
// running, from inside onChange or from another goroutine, only queues its change and returns; the
// goroutine already reporting delivers it after the current onChange returns. So onChange may call
// Set, and a Set from another goroutine may return before its change is reported. If onChange
// panics the panic reaches the Set that was reporting, the Gate stays usable, and changes still
// queued are reported by the next Set that changes something.
//
// Set panics with "overload: Gate.Set before Bind" before Bind.
func (g *Gate) Set(src engine.AdmissionSource, on bool, live, high, low uint64) {
	g.mu.Lock()
	if !g.bound {
		g.mu.Unlock()
		panic("overload: Gate.Set before Bind")
	}
	i := g.indexOf(src)
	if (i >= 0) == on {
		g.mu.Unlock()
		return
	}
	if on {
		g.on = append(g.on, src)
	} else {
		g.on = append(g.on[:i], g.on[i+1:]...)
	}
	shedding := len(g.on) > 0
	if shedding {
		g.refusal.Store(&engine.AdmissionError{Source: g.on[0]})
	} else {
		g.refusal.Store(nil)
	}
	g.pending = append(g.pending, engine.AdmissionChange{
		Source: src, On: on, Live: live, High: high, Low: low, Shedding: shedding,
	})
	if g.delivering {
		g.mu.Unlock()
		return
	}
	g.delivering = true
	g.mu.Unlock()
	g.drain()
}

// drain reports the pending changes in order until none is left. The caller has set delivering.
func (g *Gate) drain() {
	drained := false
	defer func() {
		if drained {
			return
		}
		// onChange panicked: hand the delivery role back so the next Set can take it.
		g.mu.Lock()
		g.delivering = false
		g.mu.Unlock()
	}()
	for {
		g.mu.Lock()
		if len(g.pending) == 0 {
			g.pending = nil
			g.delivering = false
			drained = true
			g.mu.Unlock()
			return
		}
		c := g.pending[0]
		g.pending = g.pending[1:]
		onChange := g.onChange
		g.mu.Unlock()
		if onChange != nil {
			onChange(c)
		}
	}
}

// indexOf returns the position of src in g.on, or -1. The caller holds g.mu.
func (g *Gate) indexOf(src engine.AdmissionSource) int {
	for i, s := range g.on {
		if s == src {
			return i
		}
	}
	return -1
}

// Usage returns the bound usage function's result, or the zero Usage before Bind.
func (g *Gate) Usage() engine.Usage {
	g.mu.Lock()
	usage := g.usage
	g.mu.Unlock()
	if usage == nil {
		return engine.Usage{}
	}
	return usage()
}

// AdmitSpawn returns nil, or an *engine.AdmissionError while shedding. A reserve spawn is never
// refused (OVL-100).
func (g *Gate) AdmitSpawn(reserve bool) error {
	if reserve {
		return nil
	}
	return g.admit()
}

// AdmitSend returns nil, or an *engine.AdmissionError while shedding (OVL-110).
func (g *Gate) AdmitSend() error { return g.admit() }

func (g *Gate) admit() error {
	if e := g.refusal.Load(); e != nil {
		return e
	}
	return nil
}

var _ engine.Admission = (*Gate)(nil)
