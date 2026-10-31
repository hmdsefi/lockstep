// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

//go:build (amd64 || arm64) && !purego

package prodengine

// getg is alone in this file because it has no Go body: a file that holds only bodyless
// declarations gets no coverage blocks, so coverage tools that read every function body of a
// covered file never parse it.

// getg returns the address of the calling goroutine's runtime g. It is written in assembly
// (getg_amd64.s, getg_arm64.s), so it needs no go:linkname into the runtime.
func getg() uintptr
