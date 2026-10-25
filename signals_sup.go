// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

// Terminated reports that a watched actor stopped for good, or that the watched address was not
// alive (Reason NoActor). Reason is the actor's final stop reason, or NoActor.
type Terminated struct {
	Addr   Address
	Reason StopReason
}

func (Terminated) signal() {}

// ChildFailed reports a child's failure after the supervisor's strategy has handled it.
type ChildFailed struct {
	Child     Address
	Err       error
	Directive Directive // the directive applied; 0 when Cleanup is true
	Cleanup   bool      // the failure happened in Stopped; no directive was applied
}

func (ChildFailed) signal() {}
