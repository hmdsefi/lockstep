// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package engine_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep/internal/engine"
)

// AT-ENG-01, ENG-020
func TestATENG01AddressString(t *testing.T) {
	tests := []struct {
		addr engine.Address
		want string
	}{
		{engine.Address{Node: "n1", Path: "/user/orders/o-17", UID: 42}, "lockstep://n1/user/orders/o-17#2a"},
		{engine.Address{}, "<zero>"},
		{engine.Address{Node: "local", Path: "/"}, "lockstep://local/#0"},
		{engine.Address{Node: "a", Path: "/system", UID: 1<<64 - 1}, "lockstep://a/system#ffffffffffffffff"},
	}
	for _, tt := range tests {
		if got := tt.addr.String(); got != tt.want {
			t.Errorf("%#v.String() = %q, want %q", tt.addr, got, tt.want)
		}
	}
}

// fakes implements every interface of the Engine bundle with methods that do nothing.
type fakes struct{}

func (fakes) Ready(engine.ActorRef) {}
func (fakes) InTurn() bool          { return false }
func (fakes) Now() time.Time        { return time.Time{} }
func (fakes) AfterFunc(time.Duration, string, func()) engine.Cancel {
	return func() bool { return false }
}
func (fakes) Deliver(engine.Envelope) error { return nil }
func (fakes) Start(engine.EffectRequest, func(any, error)) engine.Cancel {
	return func() bool { return false }
}
func (fakes) Rand(string) *rand.Rand                                 { return nil }
func (fakes) Enabled() bool                                          { return false }
func (fakes) Record(string, ...engine.Attr)                          {}
func (fakes) Describe(any) string                                    { return "" }
func (fakes) AdmitSpawn(bool) error                                  { return nil }
func (fakes) AdmitSend() error                                       { return nil }
func (fakes) Bind(func(engine.AdmissionChange), func() engine.Usage) {}
func (fakes) Fail(error)                                             {}
func (fakes) Wait(context.Context, <-chan struct{}) error            { return nil }
func (fakes) Interpose(_, _ engine.Address, _ any, _ func()) bool    { return false }

// full returns a bundle with every field set.
func full() engine.Engine {
	f := fakes{}
	return engine.Engine{
		Node:       "n1",
		Epoch:      1,
		Blocking:   true,
		Scheduler:  f,
		Clock:      f,
		Transport:  f,
		Effects:    f,
		Entropy:    f,
		Tracer:     f,
		Admission:  f,
		Failer:     f,
		Waiter:     f,
		Interposer: f,
		Stop:       func() {},
	}
}

// AT-ENG-02, ENG-030
func TestATENG02Validate(t *testing.T) {
	if err := full().Validate(); err != nil {
		t.Fatalf("full bundle: Validate() = %v, want nil", err)
	}
	tests := []struct {
		name  string
		clear func(e *engine.Engine)
		want  string
	}{
		{"Node", func(e *engine.Engine) { e.Node = "" }, "lockstep: engine: Node is empty"},
		{"Epoch", func(e *engine.Engine) { e.Epoch = 0 }, "lockstep: engine: Epoch is 0"},
		{"Scheduler", func(e *engine.Engine) { e.Scheduler = nil }, "lockstep: engine: Scheduler is nil"},
		{"Clock", func(e *engine.Engine) { e.Clock = nil }, "lockstep: engine: Clock is nil"},
		{"Effects", func(e *engine.Engine) { e.Effects = nil }, "lockstep: engine: Effects is nil"},
		{"Entropy", func(e *engine.Engine) { e.Entropy = nil }, "lockstep: engine: Entropy is nil"},
		{"Tracer", func(e *engine.Engine) { e.Tracer = nil }, "lockstep: engine: Tracer is nil"},
		{"Admission", func(e *engine.Engine) { e.Admission = nil }, "lockstep: engine: Admission is nil"},
		{"Failer", func(e *engine.Engine) { e.Failer = nil }, "lockstep: engine: Failer is nil"},
		{"Waiter", func(e *engine.Engine) { e.Waiter = nil }, "lockstep: engine: Waiter is nil"},
		{"Node and Clock", func(e *engine.Engine) { e.Node, e.Clock = "", nil }, "lockstep: engine: Node is empty"},
		{"optional fields", func(e *engine.Engine) { e.Transport, e.Interposer, e.Stop = nil, nil, nil }, ""},
	}
	for _, tt := range tests {
		e := full()
		tt.clear(&e)
		err := e.Validate()
		got := ""
		if err != nil {
			got = err.Error()
		}
		if got != tt.want {
			t.Errorf("%s cleared: Validate() = %q, want %q", tt.name, got, tt.want)
		}
	}
}

// AT-ENG-06 (first half), ENG-120
func TestATENG06ErrOverloaded(t *testing.T) {
	if got, want := engine.ErrOverloaded.Error(), "lockstep: overloaded"; got != want {
		t.Errorf("ErrOverloaded.Error() = %q, want %q", got, want)
	}
}

// AT-ENG-27 (String, Error and errors.Is clauses), ENG-120
func TestAdmissionValues(t *testing.T) {
	sources := []struct {
		s    engine.AdmissionSource
		want string
	}{
		{engine.SourceMemory, "memory"},
		{engine.SourceBudget, "budget"},
		{engine.SourcePressure, "pressure"},
		{engine.AdmissionSource(0), "AdmissionSource(0)"},
		{engine.AdmissionSource(9), "AdmissionSource(9)"},
	}
	for _, tt := range sources {
		if got := tt.s.String(); got != tt.want {
			t.Errorf("AdmissionSource(%d).String() = %q, want %q", uint8(tt.s), got, tt.want)
		}
	}

	if got, want := (&engine.AdmissionError{Source: engine.SourcePressure}).Error(), "lockstep: overloaded (pressure)"; got != want {
		t.Errorf("AdmissionError{SourcePressure}.Error() = %q, want %q", got, want)
	}
	if !errors.Is(fmt.Errorf("x: %w", &engine.AdmissionError{Source: engine.SourceBudget}), engine.ErrOverloaded) {
		t.Error("errors.Is(wrapped *AdmissionError, ErrOverloaded) = false, want true")
	}
	if errors.Is(&engine.AdmissionError{Source: engine.SourceMemory}, io.EOF) {
		t.Error("errors.Is(*AdmissionError, io.EOF) = true, want false")
	}
}
