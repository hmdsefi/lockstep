// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package supervise

import (
	"errors"
	"strconv"
	"time"
)

// Limits are a strategy's restart limits.
type Limits struct {
	MaxRestarts            int
	Window                 time.Duration
	MinBackoff, MaxBackoff time.Duration
}

// Validate returns the first failing limit check of SUP-370 as errors.New(text), or nil. The
// checks run in this order: MaxRestarts, Window, MinBackoff, then MaxBackoff against MinBackoff.
// Durations are formatted with time.Duration.String.
func (l Limits) Validate() error {
	switch {
	case l.MaxRestarts < 0:
		return errors.New("MaxRestarts must not be negative, got " + strconv.Itoa(l.MaxRestarts))
	case l.Window <= 0:
		return errors.New("Window must be positive, got " + l.Window.String())
	case l.MinBackoff < 0:
		return errors.New("MinBackoff must not be negative, got " + l.MinBackoff.String())
	case l.MaxBackoff < l.MinBackoff:
		return errors.New("MaxBackoff " + l.MaxBackoff.String() + " is less than MinBackoff " + l.MinBackoff.String())
	}
	return nil
}

// Intensity holds restart timestamps for one key: a child (OneForOne) or a group (AllForOne).
// The zero value is empty and ready to use. It is not safe for concurrent use.
type Intensity struct {
	times []time.Time // restarts in the window, oldest first unless the clock jumped backwards
}

// Admit applies SUP-270 at now. It removes every timestamp s with now.Sub(s) >= l.Window; then, if
// at least l.MaxRestarts timestamps remain, it returns ok false without adding. Otherwise it adds
// now and returns the new count and ok true. When ok is false, count is the number of restarts
// already in the window. A timestamp later than now, which a clock jump backwards leaves behind,
// stays in the window.
func (in *Intensity) Admit(now time.Time, l Limits) (count int, ok bool) {
	kept := in.times[:0]
	for _, s := range in.times {
		if now.Sub(s) < l.Window {
			kept = append(kept, s)
		}
	}
	clear(in.times[len(kept):])
	in.times = kept
	if len(in.times) >= l.MaxRestarts {
		return len(in.times), false
	}
	in.times = append(in.times, now)
	return len(in.times), true
}

// Count returns the number of restarts in the window at now, the count Admit would start from:
// the timestamps s with now.Sub(s) < l.Window. Unlike Admit it changes nothing, so a supervisor
// can record the count for a directive that is not a restart (SUP §8).
func (in *Intensity) Count(now time.Time, l Limits) int {
	n := 0
	for _, s := range in.times {
		if now.Sub(s) < l.Window {
			n++
		}
	}
	return n
}
