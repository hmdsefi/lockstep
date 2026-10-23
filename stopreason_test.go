// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"errors"
	"fmt"
	"strconv"
	"testing"
)

// Terminated and ChildFailed are Signals (MSG-800).
var (
	_ Signal = Terminated{}
	_ Signal = ChildFailed{}
)

// AT-SUP-36: SUP-380.
func TestATSUP36StringsAndReasons(t *testing.T) {
	for _, c := range []struct {
		name string
		f    func()
		want string
	}{
		{"Failed", func() { Failed(nil) }, "lockstep: Failed with nil error"},
		{"Restarting", func() { Restarting(nil) }, "lockstep: Restarting with nil error"},
	} {
		got := mustPanic(t, c.f)
		if s, ok := got.(string); !ok || s != c.want {
			t.Errorf("%s(nil) panic = %#v, want %q", c.name, got, c.want)
		}
	}

	errNodeDown := errors.New("node down")
	if r := Failed(errNodeDown); r.Kind != StopFailed || r.Err != errNodeDown {
		t.Errorf("Failed = %+v", r)
	}
	if r := Restarting(errNodeDown); r.Kind != StopRestarting || r.Err != errNodeDown {
		t.Errorf("Restarting = %+v", r)
	}
	if got := Failed(errors.New("x")).String(); got != "failed: x" {
		t.Errorf("Failed(x).String() = %q", got)
	}
	if got := Failed(fmt.Errorf("x: %w", errNodeDown)).String(); got != "failed: x: node down" {
		t.Errorf("wrapped = %q", got)
	}
	if got := Restarting(errNodeDown).String(); got != "restarting: node down" {
		t.Errorf("Restarting.String() = %q", got)
	}

	for _, c := range []struct {
		r    StopReason
		kind StopKind
		want string
	}{
		{Normal, StopNormal, "normal"},
		{ParentStopping, StopParentStopping, "parent-stopping"},
		{Shutdown, StopShutdown, "shutdown"},
		{NoActor, StopNoActor, "no-actor"},
	} {
		if c.r.Kind != c.kind || c.r.Err != nil || c.r.String() != c.want {
			t.Errorf("%+v: kind %v err %v string %q, want %v and %q", c.r, c.r.Kind, c.r.Err, c.r.String(), c.kind, c.want)
		}
	}

	for k, want := range map[StopKind]string{
		StopNormal: "normal", StopFailed: "failed", StopParentStopping: "parent-stopping",
		StopShutdown: "shutdown", StopRestarting: "restarting", StopNoActor: "no-actor",
		0: "StopKind(0)", 7: "StopKind(7)", 255: "StopKind(255)",
	} {
		if k.String() != want {
			t.Errorf("StopKind(%d).String() = %q, want %q", uint8(k), k.String(), want)
		}
	}
	if StopNormal != 1 || StopFailed != 2 || StopParentStopping != 3 || StopShutdown != 4 || StopRestarting != 5 || StopNoActor != 6 {
		t.Error("StopKind constants are not 1..6 in SUP §4.1 order")
	}

	for d, want := range map[Directive]string{
		DirectiveRestart: "restart", DirectiveStop: "stop", DirectiveEscalate: "escalate",
		DirectiveResume: "resume", 0: "Directive(0)", 5: "Directive(5)", 9: "Directive(9)",
	} {
		if d.String() != want {
			t.Errorf("Directive(%d).String() = %q, want %q", uint8(d), d.String(), want)
		}
	}
	if DirectiveRestart != 1 || DirectiveStop != 2 || DirectiveEscalate != 3 || DirectiveResume != 4 {
		t.Error("Directive constants are not 1..4 in SUP §4.1 order")
	}

	// A Terminated is comparable and carries its address and reason.
	a := Address{node: "n", path: "/user/a", uid: 1}
	if (Terminated{Addr: a, Reason: Normal}) != (Terminated{Addr: a, Reason: Normal}) {
		t.Error("equal Terminated values differ")
	}
}

// stringerValue is a fmt.Stringer that is not an error.
type stringerValue struct{ n int }

func (s stringerValue) String() string { return "stringer-" + strconv.Itoa(s.n) }

// errStringer implements both error and fmt.Stringer; the error wins (SUP-145).
type errStringer struct{}

func (errStringer) Error() string  { return "as-error" }
func (errStringer) String() string { return "as-stringer" }

// namedInt, namedString and namedBool have no methods, so SUP-145 goes by their kind.
type (
	namedInt    int16
	namedString string
	namedBool   bool
)

// SUP-145: the text never uses %v and does not change between runs.
func TestPanicErrorText(t *testing.T) {
	errX := errors.New("x")
	for _, c := range []struct {
		name  string
		value any
		want  string
	}{
		{"int", 7, "lockstep: panic: 7"},
		{"negative int8", int8(-3), "lockstep: panic: -3"},
		{"uint64", uint64(1) << 63, "lockstep: panic: 9223372036854775808"},
		{"named int", namedInt(12), "lockstep: panic: 12"},
		{"string", "boom", "lockstep: panic: boom"},
		{"empty string", "", "lockstep: panic: "},
		{"named string", namedString("named"), "lockstep: panic: named"},
		{"true", true, "lockstep: panic: true"},
		{"false", false, "lockstep: panic: false"},
		{"named bool", namedBool(true), "lockstep: panic: true"},
		{"error", errX, "lockstep: panic: x"},
		{"error and Stringer", errStringer{}, "lockstep: panic: as-error"},
		{"Stringer", stringerValue{4}, "lockstep: panic: stringer-4"},
		{"pointer to struct", &struct{}{}, "lockstep: panic: *struct {}"},
		{"struct", struct{ A int }{1}, "lockstep: panic: struct { A int }"},
		{"float", 1.5, "lockstep: panic: float64"},
		{"slice", []int{1}, "lockstep: panic: []int"},
		{"nil", nil, "lockstep: panic: nil"},
	} {
		e := &PanicError{Value: c.value, Stack: []byte("never in the text")}
		if got := e.Error(); got != c.want {
			t.Errorf("%s: Error() = %q, want %q", c.name, got, c.want)
		}
		if got := e.Error(); got != c.want {
			t.Errorf("%s: second Error() = %q, want %q", c.name, got, c.want)
		}
	}
}

// PanicError.Unwrap returns Value only when it is an error.
func TestPanicErrorUnwrap(t *testing.T) {
	errX := errors.New("x")
	if got := errors.Unwrap(&PanicError{Value: errX}); got != errX { //nolint:errorlint // the same value
		t.Errorf("Unwrap(error value) = %v, want the value", got)
	}
	wrapped := &PanicError{Value: fmt.Errorf("w: %w", errX)}
	if !errors.Is(wrapped, errX) {
		t.Error("errors.Is does not see through a PanicError holding a wrapping error")
	}
	for _, v := range []any{nil, 7, "x", stringerValue{1}, &struct{}{}} {
		if got := (&PanicError{Value: v}).Unwrap(); got != nil {
			t.Errorf("Unwrap(%#v) = %v, want nil", v, got)
		}
	}
	var pe *PanicError
	if !errors.As(fmt.Errorf("f: %w", &PanicError{Value: 1}), &pe) || pe.Value != 1 {
		t.Error("errors.As does not find the PanicError")
	}
}
