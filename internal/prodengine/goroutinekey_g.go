// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

//go:build (amd64 || arm64) && !purego

package prodengine

// goroutineKeyCheap is true: goroutineKey reads one register or TLS slot.
const goroutineKeyCheap = true

// goroutineKey returns a key that tells the calling goroutine apart from every other live
// goroutine (SCH-115): the address of its runtime g. A goroutine's g does not move while it runs,
// so its key does not change, but the runtime reuses the g of an exited goroutine for a new one,
// so the key of a goroutine that has exited may come back as another goroutine's key.
func goroutineKey() uint64 {
	return uint64(getg())
}
