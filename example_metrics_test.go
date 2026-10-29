// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hmdsefi/lockstep"
)

// mapMetrics adapts Metrics to a set of counters kept in a map, keyed by the series in the form
// name{label="value",...}. A real backend registers each series with its metrics library instead.
type mapMetrics struct {
	mu       sync.Mutex
	counters map[string]*atomic.Uint64
}

// seriesKey returns the key of a series: its name and its labels, in order.
func seriesKey(name string, labels []lockstep.MetricLabel) string {
	parts := make([]string, len(labels))
	for i, l := range labels {
		parts[i] = fmt.Sprintf("%s=%q", l.Name, l.Value)
	}
	return name + "{" + strings.Join(parts, ",") + "}"
}

// mapCounter is one counter of a mapMetrics.
type mapCounter struct{ n *atomic.Uint64 }

func (c mapCounter) Add(n uint64) { c.n.Add(n) }

// Counter registers the counter of one series and returns it.
func (m *mapMetrics) Counter(name, _ string, labels ...lockstep.MetricLabel) lockstep.Counter {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := new(atomic.Uint64)
	m.counters[seriesKey(name, labels)] = n
	return mapCounter{n}
}

// value returns the count of the series key, or 0 for a series that is not registered.
func (m *mapMetrics) value(key string) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	if n := m.counters[key]; n != nil {
		return n.Load()
	}
	return 0
}

// GaugeFunc ignores the gauge: this backend keeps counters only.
func (m *mapMetrics) GaugeFunc(string, string, func() int64, ...lockstep.MetricLabel) {}

type hello struct{}

type greeter struct{}

func (greeter) Receive(*lockstep.Context[hello], hello) {}

// ExampleWithMetrics reports a system's counters to a map. New registers every series with the
// backend before it returns, each with the node label first, and the system adds to a series each
// time its event happens.
func ExampleWithMetrics() {
	m := &mapMetrics{counters: make(map[string]*atomic.Uint64)}
	sys := lockstep.New(lockstep.WithNodeName("example"), lockstep.WithMetrics(m))
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = sys.Shutdown(ctx)
	}()

	if _, err := lockstep.Spawn(sys, "greeter", func() lockstep.Actor[hello] { return greeter{} }); err != nil {
		fmt.Println(err)
		return
	}

	const spawned = `lockstep_actors_spawned_total{node="example"}`
	fmt.Println(spawned, m.value(spawned))
	// Output: lockstep_actors_spawned_total{node="example"} 1
}
