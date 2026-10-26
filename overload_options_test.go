// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"errors"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep/internal/engine/enginetest"
	"github.com/hmdsefi/lockstep/internal/hooks"
)

// AT-OVL-29 (the panic half), OVL-350: a non-zero WithAdmission pair that does not satisfy
// 0 < low < high makes New panic with an ErrInvalidOption error ending with OVL-350's detail;
// WithAdmission(0, 0) is valid.
func TestATOVL29InvalidAdmission(t *testing.T) {
	for _, tc := range []struct{ high, low uint64 }{{1000, 1000}, {5, 0}, {0, 5}, {10, 20}} {
		detail := "WithAdmission: need 0 < low < high (got low=" + strconv.FormatUint(tc.low, 10) +
			" high=" + strconv.FormatUint(tc.high, 10) + ")"
		err := mustPanicError(t, func() { New(WithAdmission(tc.high, tc.low)) })
		if !errors.Is(err, ErrInvalidOption) || !strings.HasSuffix(err.Error(), detail) {
			t.Errorf("New(WithAdmission(%d, %d)) panicked with %q, want ErrInvalidOption ending with %q",
				tc.high, tc.low, err, detail)
		}
	}
	mustNotPanic(t, func() { newLocalSystem(t, WithAdmission(0, 0)) })
	mustNotPanic(t, func() { newLocalSystem(t, WithAdmission(2000, 1000)) })
}

// OVL §7, OVL-010, R44: WithWatchdog and WithGoroutineWarning reject a negative value with the
// §7 detail, from New (a panic) and from hooks.NewSystem (a returned error, no panic); no option
// constructor panics for any argument and each returns a non-nil Option.
func TestOverloadOptionValidation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		opt    Option
		detail string
	}{
		{"WithWatchdog(-1)", WithWatchdog(-1), "WithWatchdog: d must be >= 0 (got -1ns)"},
		{"WithWatchdog(-1.5s)", WithWatchdog(-1500 * time.Millisecond), "WithWatchdog: d must be >= 0 (got -1.5s)"},
		{"WithGoroutineWarning(-1)", WithGoroutineWarning(-1), "WithGoroutineWarning: bound must be >= 0 (got -1)"},
		{"WithGoroutineWarning(-9)", WithGoroutineWarning(-9), "WithGoroutineWarning: bound must be >= 0 (got -9)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want := "lockstep: invalid option: " + tc.detail
			err := mustPanicError(t, func() { New(tc.opt) })
			if !errors.Is(err, ErrInvalidOption) || err.Error() != want {
				t.Errorf("New panicked with %q, want ErrInvalidOption %q", err, want)
			}
			var built hooks.Built
			var buildErr error
			mustNotPanic(t, func() {
				built, buildErr = hooks.NewSystem(enginetest.New("n1", 1).Engine(), []Option{tc.opt})
			})
			if !errors.Is(buildErr, ErrInvalidOption) || buildErr.Error() != want {
				t.Errorf("hooks.NewSystem: err %v, want ErrInvalidOption %q", buildErr, want)
			}
			if built.System != nil {
				t.Errorf("hooks.NewSystem returned a system %v with an error", built.System)
			}
		})
	}

	// Boundary values are valid.
	for _, opts := range [][]Option{
		{WithWatchdog(0), WithGoroutineWarning(0)},
		{WithWatchdog(1), WithGoroutineWarning(1)},
		{WithWatchdog(math.MaxInt64), WithGoroutineWarning(math.MaxInt)},
	} {
		mustNotPanic(t, func() { newLocalSystem(t, opts...) })
	}

	// R44: the constructors neither panic nor validate when called.
	var opts []Option
	mustNotPanic(t, func() {
		for _, d := range []time.Duration{math.MinInt64, -time.Hour, -1, 0, 1, time.Hour, math.MaxInt64} {
			opts = append(opts, WithWatchdog(d))
		}
		for _, b := range []int{math.MinInt, -1, 0, 1, math.MaxInt} {
			opts = append(opts, WithGoroutineWarning(b))
		}
		for _, p := range [][2]uint64{{0, 0}, {5, 0}, {0, 5}, {1000, 1000}, {10, 20}, {2000, 1000}, {math.MaxUint64, math.MaxUint64}} {
			opts = append(opts, WithAdmission(p[0], p[1]))
		}
		for _, n := range []int{math.MinInt, -1, 0, 1, math.MaxInt} {
			opts = append(opts, WithMaxActors(n))
		}
	})
	if len(opts) != 24 {
		t.Fatalf("built %d options, want 24", len(opts))
	}
	for i, o := range opts {
		if o == nil {
			t.Errorf("option constructor %d returned nil", i)
		}
	}
}
