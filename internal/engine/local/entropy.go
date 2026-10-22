// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package local

import (
	crand "crypto/rand"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"strings"
)

// entropy is the bundle's Entropy (ENG-440). It is not deterministic by design.
type entropy struct{}

// Rand returns a new PCG stream seeded with 16 bytes from crypto/rand. It panics for a label
// without the "lockstep/" prefix (ENG-100).
func (entropy) Rand(label string) *rand.Rand {
	if !strings.HasPrefix(label, "lockstep/") {
		panic(fmt.Sprintf("local: Entropy label %q does not start with \"lockstep/\"", label))
	}
	var b [16]byte
	_, _ = crand.Read(b[:])                                                                            // never fails (crypto/rand.Read, Go 1.24+)
	return rand.New(rand.NewPCG(binary.LittleEndian.Uint64(b[:8]), binary.LittleEndian.Uint64(b[8:]))) //nolint:gosec // G404: seeded from crypto/rand; callers need a *rand.Rand
}
