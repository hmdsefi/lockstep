// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package overload

import (
	"io"
	"runtime/metrics"
	"unsafe"
)

// SetReadMetrics replaces the sampler's metrics.Read until restore is called. Tests only.
func SetReadMetrics(read func([]metrics.Sample)) (restore func()) {
	prev := readHook.Swap(&read)
	return func() { readHook.Store(prev) }
}

// SetNumGoroutine replaces the monitor's runtime.NumGoroutine until restore is called. Tests only.
func SetNumGoroutine(num func() int) (restore func()) {
	prev := numGoroutineHook.Swap(&num)
	return func() { numGoroutineHook.Store(prev) }
}

// SetGoroutineProfile replaces the monitor's write of the goroutine profile at debug 1 until
// restore is called. Tests only.
func SetGoroutineProfile(write func(w io.Writer) error) (restore func()) {
	prev := goroutineProfileHook.Swap(&write)
	return func() { goroutineProfileHook.Store(prev) }
}

// SetStack replaces the runtime.Stack call of GoroutineStack until restore is called. Tests only.
func SetStack(stack func(buf []byte, all bool) int) (restore func()) {
	prev := stackHook.Swap(&stack)
	return func() { stackHook.Store(prev) }
}

// Uint64Value returns a metrics.Value of kind KindUint64 holding v, which a read hook needs
// because the runtime/metrics package offers no constructor for one. Tests only.
func Uint64Value(v uint64) metrics.Value {
	// value mirrors the layout of metrics.Value; the check below catches a layout change.
	type value struct {
		kind    metrics.ValueKind
		scalar  uint64
		pointer unsafe.Pointer
	}
	var out metrics.Value
	*(*value)(unsafe.Pointer(&out)) = value{kind: metrics.KindUint64, scalar: v}
	if out.Kind() != metrics.KindUint64 || out.Uint64() != v {
		panic("overload: Uint64Value: runtime/metrics.Value layout changed")
	}
	return out
}
