// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package supervise holds the pure logic of actor lifecycle and supervision: the cell state
// machine, directives, restart limits, sliding-window restart intensity and capped exponential
// backoff (SUP-010, SUP-180, SUP-270, SUP-280, SUP-370).
//
// The package has no clock, no goroutines and no tracer. Callers pass the current time to Admit
// and drive every transition themselves.
package supervise

import "strconv"

// State is a cell's lifecycle state.
type State uint8

const (
	Starting   State = iota + 1 // cell created; factory and Started run in this state
	Running                     // processing user messages
	Suspended                   // failed; waiting for the supervisor's command
	Restarting                  // ending the incarnation for a restart, or waiting for begin
	Stopping                    // stopping children, then Stopped, then drain
	Stopped                     // final; the cell is gone from the registry
)

// String returns "starting", "running", "suspended", "restarting", "stopping", "stopped", or
// "State(<n>)" for a value outside Starting..Stopped.
func (s State) String() string {
	switch s {
	case Starting:
		return "starting"
	case Running:
		return "running"
	case Suspended:
		return "suspended"
	case Restarting:
		return "restarting"
	case Stopping:
		return "stopping"
	case Stopped:
		return "stopped"
	}
	return "State(" + strconv.Itoa(int(s)) + ")"
}

// CanTransition reports whether from -> to is one of the 12 edges of SUP-010. A state may not
// move to itself, and Stopped has no outgoing edge.
func CanTransition(from, to State) bool {
	switch from {
	case Starting:
		return to == Running || to == Suspended || to == Stopping
	case Running:
		return to == Suspended || to == Restarting || to == Stopping
	case Suspended:
		return to == Running || to == Restarting || to == Stopping
	case Restarting:
		return to == Starting || to == Stopping
	case Stopping:
		return to == Stopped
	}
	return false
}
