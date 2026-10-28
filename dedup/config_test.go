// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package dedup

import (
	"errors"
	"testing"
	"time"
)

// AT-DLV-01, DLV-010: Capacity is checked first, then TTL; every error wraps ErrInvalidConfig.
func TestATDLV01ConfigValidate(t *testing.T) {
	for _, tc := range []struct {
		cfg  Config
		want string // "" for nil
	}{
		{Config{0, 0}, "dedup: invalid config: Capacity must be at least 1 (got 0)"},
		{Config{-1, 0}, "dedup: invalid config: Capacity must be at least 1 (got -1)"},
		{Config{1, -time.Nanosecond}, "dedup: invalid config: TTL must not be negative (got -1ns)"},
		{Config{0, -time.Nanosecond}, "dedup: invalid config: Capacity must be at least 1 (got 0)"},
		{Config{1, 0}, ""},
		{Config{1, time.Nanosecond}, ""},
	} {
		err := tc.cfg.Validate()
		if tc.want == "" {
			if err != nil {
				t.Errorf("%+v.Validate() = %v, want nil", tc.cfg, err)
			}
			continue
		}
		if err == nil || err.Error() != tc.want {
			t.Errorf("%+v.Validate() = %v, want %q", tc.cfg, err, tc.want)
		}
		if !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("%+v.Validate() = %v, does not wrap ErrInvalidConfig", tc.cfg, err)
		}
	}
}
