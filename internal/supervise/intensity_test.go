// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package supervise

import (
	"testing"
	"time"
)

var epoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// TestATSUP27Intensity checks the unit part of AT-SUP-27.
func TestATSUP27Intensity(t *testing.T) {
	l := Limits{MaxRestarts: 3, Window: 10 * time.Second}
	var in Intensity
	for _, c := range []struct {
		at        time.Duration
		wantCount int
		wantOK    bool
	}{
		{0, 1, true},
		{1 * time.Second, 2, true},
		{2 * time.Second, 3, true},
		{3 * time.Second, 3, false}, // three already in the window: refused, not added
		{10 * time.Second, 3, true}, // t=0 has aged out (now-s >= Window); 1s and 2s remain
		{10500 * time.Millisecond, 3, false},
	} {
		count, ok := in.Admit(epoch.Add(c.at), l)
		if count != c.wantCount || ok != c.wantOK {
			t.Fatalf("Admit at %v = %d, %v; want %d, %v", c.at, count, ok, c.wantCount, c.wantOK)
		}
	}
}

func TestIntensityClockJumpBackwards(t *testing.T) {
	l := Limits{MaxRestarts: 3, Window: 10 * time.Second}
	var in Intensity
	for i := 1; i <= 2; i++ {
		if count, ok := in.Admit(epoch.Add(100*time.Second), l); count != i || !ok {
			t.Fatalf("Admit at 100s = %d, %v; want %d, true", count, ok, i)
		}
	}
	// A timestamp later than now stays in the window.
	if count, ok := in.Admit(epoch.Add(50*time.Second), l); count != 3 || !ok {
		t.Fatalf("Admit at 50s = %d, %v; want 3, true", count, ok)
	}
	if count, ok := in.Admit(epoch.Add(50*time.Second), l); count != 3 || ok {
		t.Fatalf("second Admit at 50s = %d, %v; want 3, false", count, ok)
	}
}

func TestIntensityZeroMaxRestarts(t *testing.T) {
	var in Intensity
	if count, ok := in.Admit(epoch, Limits{MaxRestarts: 0, Window: time.Minute}); count != 0 || ok {
		t.Fatalf("Admit with MaxRestarts 0 = %d, %v; want 0, false", count, ok)
	}
}

func TestIntensityAgedOutTimestampsAreForgotten(t *testing.T) {
	l := Limits{MaxRestarts: 1, Window: time.Second}
	var in Intensity
	for i := 0; i < 5; i++ {
		now := epoch.Add(time.Duration(i) * time.Second)
		if count, ok := in.Admit(now, l); count != 1 || !ok {
			t.Fatalf("Admit at %v = %d, %v; want 1, true", now, count, ok)
		}
	}
	if len(in.times) != 1 {
		t.Fatalf("holds %d timestamps, want 1", len(in.times))
	}
}

func TestLimitsValidate(t *testing.T) {
	ok := Limits{MaxRestarts: 10, Window: time.Minute, MinBackoff: 10 * time.Millisecond, MaxBackoff: 10 * time.Second}
	if err := ok.Validate(); err != nil {
		t.Fatalf("Validate(valid) = %v, want nil", err)
	}
	if err := (Limits{MaxRestarts: 0, Window: 1, MinBackoff: 0, MaxBackoff: 0}).Validate(); err != nil {
		t.Fatalf("Validate(minimal) = %v, want nil", err)
	}
	with := func(f func(*Limits)) Limits { l := ok; f(&l); return l }
	for _, c := range []struct {
		name string
		l    Limits
		want string
	}{
		{"negative MaxRestarts", with(func(l *Limits) { l.MaxRestarts = -1 }), "MaxRestarts must not be negative, got -1"},
		{"zero Window", with(func(l *Limits) { l.Window = 0 }), "Window must be positive, got 0s"},
		{"negative Window", with(func(l *Limits) { l.Window = -time.Second }), "Window must be positive, got -1s"},
		{"negative MinBackoff", with(func(l *Limits) { l.MinBackoff = -1 }), "MinBackoff must not be negative, got -1ns"},
		{"Max below Min", with(func(l *Limits) { l.MaxBackoff, l.MinBackoff = time.Second, 2*time.Second }),
			"MaxBackoff 1s is less than MinBackoff 2s"},
		// The first failing check wins.
		{"all fail", Limits{MaxRestarts: -2, Window: -1, MinBackoff: -3, MaxBackoff: -4}, "MaxRestarts must not be negative, got -2"},
		{"window before backoff", Limits{MaxRestarts: 0, Window: 0, MinBackoff: -3, MaxBackoff: -4}, "Window must be positive, got 0s"},
		{"min before max", Limits{MaxRestarts: 0, Window: 1, MinBackoff: -3, MaxBackoff: -4}, "MinBackoff must not be negative, got -3ns"},
	} {
		err := c.l.Validate()
		if err == nil || err.Error() != c.want {
			t.Errorf("%s: Validate() = %v, want %q", c.name, err, c.want)
		}
	}
}
