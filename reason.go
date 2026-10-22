// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import "strconv"

// StopKind classifies a StopReason.
type StopKind uint8

const (
	StopNormal         StopKind = iota + 1 // Stop(from, addr) or ctx.Stop
	StopFailed                             // the supervisor's directive was to stop a failed actor
	StopParentStopping                     // the parent is stopping or restarting
	StopShutdown                           // System.Shutdown
	StopRestarting                         // the incarnation ends for a restart; only Stopped sees it
	StopNoActor                            // Terminated only: the watched address was not alive
)

// String returns "normal", "failed", "parent-stopping", "shutdown", "restarting", "no-actor", or
// "StopKind(<n>)".
func (k StopKind) String() string {
	switch k {
	case StopNormal:
		return "normal"
	case StopFailed:
		return "failed"
	case StopParentStopping:
		return "parent-stopping"
	case StopShutdown:
		return "shutdown"
	case StopRestarting:
		return "restarting"
	case StopNoActor:
		return "no-actor"
	}
	return "StopKind(" + strconv.Itoa(int(k)) + ")"
}

// StopReason tells why an incarnation ended, or, in Terminated, why a watched actor is gone. Err is
// non-nil exactly for StopFailed and StopRestarting.
type StopReason struct {
	Kind StopKind
	Err  error
}

var (
	Normal         = StopReason{Kind: StopNormal}
	ParentStopping = StopReason{Kind: StopParentStopping}
	Shutdown       = StopReason{Kind: StopShutdown}
	// NoActor is the reason of a Terminated for an address that was not alive when it was watched
	// (Erlang's noproc). It never reaches Stopped.
	NoActor = StopReason{Kind: StopNoActor}
)

// Failed returns a StopFailed reason carrying err: the reason of an actor stopped by its
// supervisor after failing with err, and of a Terminated whose watched actor was lost for a reason
// another package reports, such as cluster.ErrNodeDown or REM's ErrPeerRestarted. It
// panics if err is nil.
func Failed(err error) StopReason {
	if err == nil {
		panic("lockstep: Failed with nil error")
	}
	return StopReason{Kind: StopFailed, Err: err}
}

// Restarting returns the reason Stopped receives when the incarnation ends for a restart caused
// by err. It panics if err is nil.
func Restarting(err error) StopReason {
	if err == nil {
		panic("lockstep: Restarting with nil error")
	}
	return StopReason{Kind: StopRestarting, Err: err}
}

// String returns Kind.String(), followed by ": " and Err.Error() when Err is non-nil.
func (r StopReason) String() string {
	if r.Err == nil {
		return r.Kind.String()
	}
	return r.Kind.String() + ": " + r.Err.Error()
}

// Terminated reports that a watched actor stopped for good, or that the watched address was not
// alive (Reason NoActor). Reason is the actor's final stop reason, NoActor, or a Failed reason
// set by REM or CLU for an actor lost with its node or connection.
type Terminated struct {
	Addr   Address
	Reason StopReason
}

func (Terminated) signal() {}
