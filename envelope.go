// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import "reflect"

// userPayload is the engine.Envelope.Payload of a user message: a Send, or an Ask request,
// that crosses to another node. Until REM (Phase 3) encodes messages, it travels by
// reference over the simulated network (MSG-030).
type userPayload struct {
	msg any          // the M value, not copied (MSG-010)
	ask Address      // MBX-092's ask address of an Ask request, zero for every other message
	typ reflect.Type // the sender's M: a path address resolves only to an actor of this type (ACT-440); nil: any
}
