// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import "github.com/hmdsefi/lockstep/internal/engine"

// host is a system's engine.Host: the engine hands it the envelopes that arrive for the system's
// node (ENG-080). It is a type of its own so that *System has no exported Inbound method.
type host struct{ sys *System }

// Inbound drops env: inbound delivery arrives with Send (MSG §4.1).
func (host) Inbound(engine.Envelope) {}
