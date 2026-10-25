// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"reflect"
	"strconv"
)

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
// supervisor after failing with err. It panics if err is nil.
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

// PanicError is the error of a failure caused by a panic. errors.Unwrap returns Value when it is
// an error.
type PanicError struct {
	Value any    // the value passed to panic
	Stack []byte // runtime/debug.Stack() at recovery; never recorded in traces
}

// Error returns "lockstep: panic: " followed by a description of Value: Value.Error() for an
// error, Value.String() for a fmt.Stringer, the value itself for a string, integer or bool kind,
// and otherwise the type name ("nil" for a nil Value). The text never depends on a %v verb, so it
// is the same on every run.
func (e *PanicError) Error() string {
	return "lockstep: panic: " + describePanic(e.Value)
}

// Unwrap returns Value if it is an error, otherwise nil.
func (e *PanicError) Unwrap() error {
	err, _ := e.Value.(error)
	return err
}

// describePanic renders v by the rules of SUP-145: v.Error() if it is an error, v.String() if it
// is a fmt.Stringer, the string itself for a string kind, the decimal number for an integer kind,
// "true" or "false" for a bool kind, and otherwise the type name ("nil" for a nil v). It never
// uses a %v verb, so the text is the same on every run.
//
// A panic inside the Error or String method of v, for example a typed nil pointer whose method
// dereferences its receiver, is recovered and v is described by its type name, SUP-145's
// "otherwise" case: the runtime calls PanicError.Error while it handles a failure, so a second
// panic there must not escape.
func describePanic(v any) (s string) {
	defer func() {
		if recover() != nil {
			s = reflect.TypeOf(v).String()
		}
	}()
	switch x := v.(type) {
	case nil:
		return "nil"
	case error:
		return x.Error()
	case interface{ String() string }:
		return x.String()
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.String:
		return rv.String()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(rv.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return strconv.FormatUint(rv.Uint(), 10)
	case reflect.Bool:
		return strconv.FormatBool(rv.Bool())
	}
	return rv.Type().String()
}
