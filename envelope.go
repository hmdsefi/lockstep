// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import "reflect"

// userPayload is the engine.Envelope.Payload of a user message: a Send, or an Ask request
// (1.2-07), that crosses to another node. Until REM (Phase 3) encodes messages, it travels by
// reference over the simulated network (MSG-030).
type userPayload struct {
	msg any          // the M value, not copied (MSG-010)
	ask Address      // MBX-092's ask address of an Ask request, zero for every other message
	typ reflect.Type // the sender's M: a path address resolves only to an actor of this type (ACT-440); nil: any
}

// replyPayload is the engine.Envelope.Payload of a reply attempt for the ask of another node
// (MSG-410): Envelope.To is the ask address and Envelope.From the replier. The asker's system
// applies it to its table on arrival (MSG-420). Like userPayload, it travels by reference until
// REM (Phase 3).
type replyPayload struct {
	v   any   // the answer, an R; nil with an error
	err error // the error of a Fail, unchanged (MSG §7); nil with an answer
}
