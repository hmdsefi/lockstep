// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package overload

import (
	"bytes"
	"io"
	"log/slog"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
)

const (
	// monitorEvery is how many sampler ticks pass between two goroutine samples (OVL-600).
	monitorEvery = 10
	// summaryItems is the number of functions the warning's summary lists (OVL-620).
	summaryItems = 10
)

// numGoroutineHook, when set, replaces runtime.NumGoroutine (OVL §10).
var numGoroutineHook atomic.Pointer[func() int]

// goroutineProfileHook, when set, replaces the write of the goroutine profile at debug 1 (OVL §10).
var goroutineProfileHook atomic.Pointer[func(io.Writer) error]

func numGoroutine() int {
	if h := numGoroutineHook.Load(); h != nil {
		return (*h)()
	}
	return runtime.NumGoroutine()
}

func writeGoroutineProfile(w io.Writer) error {
	if h := goroutineProfileHook.Load(); h != nil {
		return (*h)(w)
	}
	return pprof.Lookup("goroutine").WriteTo(w, 1)
}

// Monitor samples the goroutine count and warns when it reaches a bound (production). The Sampler
// holds one and calls Sample on every 10th tick (OVL-600).
type Monitor struct {
	bound int
	log   *slog.Logger

	// armed belongs to Sample, which the sampler never runs concurrently with itself.
	armed bool
}

// NewMonitor returns a Monitor that warns through log when a sample reaches bound. A bound of 0
// turns the warning off. log may be nil, which logs nothing. The monitor starts armed (OVL-610).
func NewMonitor(bound int, log *slog.Logger) *Monitor {
	return &Monitor{bound: bound, log: log, armed: true}
}

// Sample reads the goroutine count. While armed, a count of at least the bound logs one warning
// and disarms the monitor; while disarmed, a count below bound - bound/5 re-arms it (OVL-610).
// With a bound of 0 it does nothing. The Sampler calls it on every 10th tick; callers must not
// run it concurrently with itself.
func (m *Monitor) Sample() {
	if m.bound <= 0 {
		return
	}
	n := numGoroutine()
	if !m.armed {
		m.armed = n < m.bound-m.bound/5
		return
	}
	if n < m.bound {
		return
	}
	m.armed = false
	if m.log == nil {
		return
	}
	m.log.Warn("goroutine count above bound",
		slog.Int("goroutines", n), slog.Int("bound", m.bound), slog.String("summary", goroutineSummary()))
}

// goroutineSummary lists the ten functions with the most goroutines, as OVL-620 describes. It is
// empty when the profile cannot be written.
func goroutineSummary() string {
	var buf bytes.Buffer
	if err := writeGoroutineProfile(&buf); err != nil {
		return ""
	}
	return summarizeProfile(buf.String())
}

// summarizeProfile reads a goroutine profile in pprof's debug-1 text form. A record is a line
// "<count> @ <pcs>" followed by "# labels:" and frame lines; a frame line reads
// "#<tab><pc><tab><function>+0x<offset><tab><file>:<line>". Each record's count goes to the function
// of its first frame. The result lists the ten functions with the largest sums, by count
// descending then name in byte order, as "<count> <function>" joined with "; ".
func summarizeProfile(text string) string {
	counts := map[string]int{}
	count, first := 0, false // the current record's count, and whether its first frame is next
	for line := range strings.SplitSeq(text, "\n") {
		if frame, ok := strings.CutPrefix(line, "#\t"); ok {
			if first {
				first = false
				counts[frameFunction(frame)] += count
			}
			continue
		}
		head, _, ok := strings.Cut(line, " @ ")
		n, err := strconv.Atoi(head)
		if ok && err == nil {
			count, first = n, true
		}
	}

	type item struct {
		fn    string
		count int
	}
	items := make([]item, 0, len(counts))
	for fn, n := range counts {
		items = append(items, item{fn, n})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].count != items[j].count {
			return items[i].count > items[j].count
		}
		return items[i].fn < items[j].fn
	})
	items = items[:min(len(items), summaryItems)]

	parts := make([]string, len(items))
	for i, it := range items {
		parts[i] = strconv.Itoa(it.count) + " " + it.fn
	}
	return strings.Join(parts, "; ")
}

// frameFunction returns the function of a frame line without its "#<tab>" prefix: the second
// tab-separated field, without the "+0x<offset>" suffix.
func frameFunction(frame string) string {
	_, rest, _ := strings.Cut(frame, "\t")
	fn, _, _ := strings.Cut(rest, "\t")
	if i := strings.LastIndex(fn, "+0x"); i >= 0 {
		fn = fn[:i]
	}
	return fn
}
