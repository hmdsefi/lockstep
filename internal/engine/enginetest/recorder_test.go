// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package enginetest_test

import (
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/engine/enginetest"
)

type describer struct{}

func (describer) Describe() string { return "ping 1" }

// both has every method Describe checks; Describe wins, then String, then Error.
type both struct{}

func (both) Describe() string { return "from Describe" }
func (both) String() string   { return "from String" }
func (both) Error() string    { return "from Error" }

type stringerErr struct{}

func (stringerErr) String() string { return "from String" }
func (stringerErr) Error() string  { return "from Error" }

type level int
type flags uint16

// AT-ENG-44, ENG-110, ENG-550
func TestATENG44Recorder(t *testing.T) {
	t.Run("record", func(t *testing.T) {
		r := enginetest.NewRecorder()
		if !r.Enabled() {
			t.Error("Enabled() = false")
		}
		if got := r.Records(); len(got) != 0 {
			t.Errorf("Records() of a new Recorder = %v, want empty", got)
		}
		attrs := []engine.Attr{{Key: "k", Value: "v"}}
		r.Record("lockstep.x", attrs...)
		r.Record("lockstep.y")
		attrs[0].Value = "changed"
		attrs[0].Key = "changed"

		want := []engine.Record{
			{Kind: "lockstep.x", Attrs: []engine.Attr{{Key: "k", Value: "v"}}},
			{Kind: "lockstep.y"},
		}
		got := r.Records()
		if !reflect.DeepEqual(got, want) {
			t.Errorf("Records() = %v, want %v", got, want)
		}

		// The returned slice is a copy too.
		got[0].Attrs[0].Value = "mutated"
		got[1].Kind = "mutated"
		if again := r.Records(); !reflect.DeepEqual(again, want) {
			t.Errorf("Records() after mutating a returned copy = %v, want %v", again, want)
		}
	})

	t.Run("panics", func(t *testing.T) {
		r := enginetest.NewRecorder()
		wantPanic(t, `enginetest: record kind "x" does not start with "lockstep."`, func() { r.Record("x") })
		wantPanic(t, `enginetest: record kind "" does not start with "lockstep."`, func() { r.Record("") })
		wantPanic(t, `enginetest: empty attribute key in "lockstep.x"`, func() {
			r.Record("lockstep.x", engine.Attr{Key: ""})
		})
		wantPanic(t, `enginetest: empty attribute key in "lockstep.x"`, func() {
			r.Record("lockstep.x", engine.Attr{Key: "ok"}, engine.Attr{Value: "v"})
		})
		if got := r.Records(); len(got) != 0 {
			t.Errorf("a panicking Record kept %v", got)
		}
	})

	t.Run("concurrent", func(t *testing.T) {
		r := enginetest.NewRecorder()
		var wg sync.WaitGroup
		for g := 0; g < 8; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 1000; i++ {
					r.Record("lockstep.c", engine.Attr{Key: "k", Value: "v"})
					if i%100 == 0 {
						_ = r.Records()
					}
				}
			}()
		}
		wg.Wait()
		if got := len(r.Records()); got != 8000 {
			t.Errorf("len(Records()) = %d, want 8000", got)
		}
	})

	t.Run("describe", func(t *testing.T) {
		r := enginetest.NewRecorder()
		tests := []struct {
			name string
			v    any
			want string
		}{
			{"nil", nil, "<nil>"},
			{"Describe method", describer{}, "ping 1"},
			{"Describe before String and Error", both{}, "from Describe"},
			{"String before Error", stringerErr{}, "from String"},
			{"Stringer", time.Duration(1500), "1.5µs"},
			{"error", errors.New("e"), "e"},
			{"string", "a\"b", "\"a\\\"b\""},
			{"true", true, "true"},
			{"false", false, "false"},
			{"int8", int8(-3), "-3"},
			{"int", 42, "42"},
			{"named int", level(7), "7"},
			{"named uint16", flags(65535), "65535"},
			{"uint64", uint64(1<<64 - 1), "18446744073709551615"},
			{"uintptr", uintptr(9), "9"},
			{"struct", struct{ X int }{1}, "struct { X int }"},
			{"slice", []int{1}, "[]int"},
			{"typed nil pointer", (*int)(nil), "*int"},
		}
		for _, tt := range tests {
			if got := r.Describe(tt.v); got != tt.want {
				t.Errorf("%s: Describe(%#v) = %q, want %q", tt.name, tt.v, got, tt.want)
			}
		}
	})
}
