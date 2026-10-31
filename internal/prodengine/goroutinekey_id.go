// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

//go:build !(amd64 || arm64) || purego

package prodengine

// goroutineKeyCheap is false: goroutineKey walks the caller's stack.
const goroutineKeyCheap = false

// goroutineKey returns a key that tells the calling goroutine apart from every other live
// goroutine (SCH-115): its goroutine ID (SCH-270). No other goroutine ever has that ID, but
// reading it costs a walk of the caller's stack.
func goroutineKey() uint64 {
	return goroutineID()
}
