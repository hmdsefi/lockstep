// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package dedup

import (
	"errors"
	"fmt"
	"time"
)

// ErrInvalidConfig is wrapped by the error of Config.Validate.
var ErrInvalidConfig = errors.New("dedup: invalid config")

// ErrInvalidEncoding is wrapped by every error UnmarshalJSON returns.
var ErrInvalidEncoding = errors.New("dedup: invalid encoding")

// Config bounds a Window, Replies or Versions value.
type Config struct {
	// Capacity is the most entries the value holds. It must be at least 1. A write that adds an
	// entry to a full value first removes the oldest entry.
	Capacity int
	// TTL is how long an entry stays visible after it was written: an entry written at t is
	// visible while now - t < TTL. 0 means entries never expire and only Capacity removes them.
	// It must not be negative.
	TTL time.Duration
}

// Validate returns nil if c is valid, and otherwise an error wrapping ErrInvalidConfig that
// names the first invalid field: Capacity below 1, then a negative TTL.
func (c Config) Validate() error {
	// DLV-010: Capacity first, then TTL.
	if c.Capacity < 1 {
		return fmt.Errorf("%w: Capacity must be at least 1 (got %d)", ErrInvalidConfig, c.Capacity)
	}
	if c.TTL < 0 {
		return fmt.Errorf("%w: TTL must not be negative (got %s)", ErrInvalidConfig, c.TTL)
	}
	return nil
}
