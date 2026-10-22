// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"errors"
	"fmt"
	"testing"
)

// Terminated is a Signal (MSG-800).
var _ Signal = Terminated{}

// AT-SUP-36 (the declared part), SUP-380.
func TestStopReasonValues(t *testing.T) {
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

	// A Terminated is comparable and carries its address and reason.
	a := Address{node: "n", path: "/user/a", uid: 1}
	if (Terminated{Addr: a, Reason: Normal}) != (Terminated{Addr: a, Reason: Normal}) {
		t.Error("equal Terminated values differ")
	}
}
