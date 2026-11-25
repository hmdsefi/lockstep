// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package cluster

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep/codec"
)

// TestConfigValidate checks CLU-010: zero fields get their defaults, negative ones do not, and the
// rows are checked in field order with the first failing row reported.
func TestConfigValidate(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		var c Config
		if err := c.Validate(); err != nil {
			t.Fatalf("Validate(zero) = %v", err)
		}
		d := DefaultConfig()
		if c.Seeds != nil || c.ProbeInterval != d.ProbeInterval || c.ProbeTimeout != d.ProbeTimeout ||
			c.IndirectProbes != d.IndirectProbes || c.SuspicionTimeout != d.SuspicionTimeout ||
			c.SyncInterval != d.SyncInterval || c.JoinRetryInterval != d.JoinRetryInterval ||
			c.RetransmitMult != d.RetransmitMult || c.MaxPiggyback != d.MaxPiggyback ||
			c.DeadRetention != d.DeadRetention || c.MaxHops != d.MaxHops || c.Registry != d.Registry ||
			c.Codec != d.Codec {
			t.Fatalf("Validate(zero) left %+v, want %+v", c, d)
		}
		want := Config{
			ProbeInterval: time.Second, ProbeTimeout: 500 * time.Millisecond, IndirectProbes: 3,
			SuspicionTimeout: 5 * time.Second, SyncInterval: 30 * time.Second, JoinRetryInterval: time.Second,
			RetransmitMult: 4, MaxPiggyback: 8, DeadRetention: time.Minute,
		}
		if d.Seeds != nil || d.ProbeInterval != want.ProbeInterval || d.ProbeTimeout != want.ProbeTimeout ||
			d.IndirectProbes != want.IndirectProbes || d.SuspicionTimeout != want.SuspicionTimeout ||
			d.SyncInterval != want.SyncInterval || d.JoinRetryInterval != want.JoinRetryInterval ||
			d.RetransmitMult != want.RetransmitMult || d.MaxPiggyback != want.MaxPiggyback ||
			d.DeadRetention != want.DeadRetention {
			t.Fatalf("DefaultConfig() = %+v, want %+v", d, want)
		}
		// The grain fields: MaxHops 3, the process registry and the JSON codec.
		if d.MaxHops != 3 || DefaultMaxHops != 3 || d.Registry != codec.Default() || d.Codec != (codec.JSON{}) {
			t.Fatalf("DefaultConfig() grain fields: MaxHops %d, Registry %p, Codec %#v", d.MaxHops, d.Registry, d.Codec)
		}
	})

	t.Run("grain fields kept", func(t *testing.T) {
		r := codec.NewRegistry()
		c := Config{MaxHops: 1, Registry: r, Codec: stubCodec{}}
		if err := c.Validate(); err != nil {
			t.Fatalf("Validate = %v", err)
		}
		if c.MaxHops != 1 || c.Registry != r || c.Codec != (stubCodec{}) {
			t.Fatalf("Validate changed set grain fields: %+v", c)
		}
	})

	t.Run("max hops before kinds", func(t *testing.T) {
		c := Config{MaxHops: -1, Kinds: []Kind{validKind("Bad")}}
		if err := c.Validate(); err == nil || err.Error() != "cluster: invalid config: MaxHops: -1 is less than 1" {
			t.Fatalf("Validate = %v, want the MaxHops row", err)
		}
	})

	t.Run("set fields kept", func(t *testing.T) {
		c := Config{Seeds: []string{"a", "b.1", "c_2-x"}, ProbeInterval: 2 * time.Second, IndirectProbes: 1,
			SuspicionTimeout: 2 * time.Second, MaxPiggyback: 1, RetransmitMult: 1}
		if err := c.Validate(); err != nil {
			t.Fatalf("Validate = %v", err)
		}
		if c.ProbeInterval != 2*time.Second || c.IndirectProbes != 1 || c.SuspicionTimeout != 2*time.Second ||
			c.MaxPiggyback != 1 || c.RetransmitMult != 1 || c.ProbeTimeout != DefaultProbeTimeout ||
			len(c.Seeds) != 3 {
			t.Fatalf("Validate changed set fields: %+v", c)
		}
	})

	t.Run("limits against defaults", func(t *testing.T) {
		for _, c := range []struct {
			pi   time.Duration
			want string
		}{
			{6 * time.Second, "cluster: invalid config: SuspicionTimeout: 5s is less than ProbeInterval 6s"},
			{500 * time.Millisecond, "cluster: invalid config: ProbeTimeout: 500ms is not less than ProbeInterval 500ms"},
		} {
			cfg := Config{ProbeInterval: c.pi}
			if err := cfg.Validate(); err == nil || err.Error() != c.want {
				t.Errorf("Validate(ProbeInterval %s) = %v, want %q", c.pi, err, c.want)
			}
		}
	})

	// The rows of CLU-010 in order. Each case breaks its row only; the "first row wins" cases
	// below break two rows at once.
	long := strings.Repeat("n", 65)
	rows := []struct {
		name string
		set  func(*Config)
		want string
	}{
		{"seed empty", func(c *Config) { c.Seeds = []string{"a", ""} }, `Seeds: seed "" is not a valid node name`},
		{"seed first byte", func(c *Config) { c.Seeds = []string{".a"} }, `Seeds: seed ".a" is not a valid node name`},
		{"seed byte", func(c *Config) { c.Seeds = []string{"a b"} }, `Seeds: seed "a b" is not a valid node name`},
		{"seed long", func(c *Config) { c.Seeds = []string{long} }, `Seeds: seed "` + long + `" is not a valid node name`},
		{"seed duplicate", func(c *Config) { c.Seeds = []string{"a", "b", "a"} }, `Seeds: duplicate seed "a"`},
		{"seed invalid before duplicate", func(c *Config) { c.Seeds = []string{"a", "a", "-"} }, `Seeds: duplicate seed "a"`},
		{"seed duplicate before invalid", func(c *Config) { c.Seeds = []string{"a", "-", "a"} }, `Seeds: seed "-" is not a valid node name`},
		{"probe interval", func(c *Config) { c.ProbeInterval = -time.Second }, "ProbeInterval: -1s is not positive"},
		{"probe timeout", func(c *Config) { c.ProbeTimeout = -time.Millisecond }, "ProbeTimeout: -1ms is not positive"},
		{"probe timeout equal", func(c *Config) { c.ProbeTimeout = time.Second }, "ProbeTimeout: 1s is not less than ProbeInterval 1s"},
		{"probe timeout above", func(c *Config) { c.ProbeInterval, c.ProbeTimeout = 100*time.Millisecond, 200*time.Millisecond },
			"ProbeTimeout: 200ms is not less than ProbeInterval 100ms"},
		{"indirect probes", func(c *Config) { c.IndirectProbes = -1 }, "IndirectProbes: -1 is negative"},
		{"suspicion timeout", func(c *Config) { c.SuspicionTimeout = 999 * time.Millisecond },
			"SuspicionTimeout: 999ms is less than ProbeInterval 1s"},
		{"suspicion timeout negative", func(c *Config) { c.SuspicionTimeout = -time.Second },
			"SuspicionTimeout: -1s is less than ProbeInterval 1s"},
		{"sync interval", func(c *Config) { c.SyncInterval = -time.Second }, "SyncInterval: -1s is not positive"},
		{"join retry interval", func(c *Config) { c.JoinRetryInterval = -time.Second }, "JoinRetryInterval: -1s is not positive"},
		{"retransmit mult", func(c *Config) { c.RetransmitMult = -1 }, "RetransmitMult: -1 is less than 1"},
		{"max piggyback", func(c *Config) { c.MaxPiggyback = -2 }, "MaxPiggyback: -2 is less than 1"},
		{"dead retention", func(c *Config) { c.DeadRetention = -time.Minute }, "DeadRetention: -1m0s is not positive"},
		{"max hops", func(c *Config) { c.MaxHops = -2 }, "MaxHops: -2 is less than 1"},
	}
	check := func(t *testing.T, c Config, want string) {
		t.Helper()
		err := c.Validate()
		if !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("Validate = %v, want an error wrapping ErrInvalidConfig", err)
		}
		if got, want := err.Error(), "cluster: invalid config: "+want; got != want {
			t.Fatalf("Validate = %q, want %q", got, want)
		}
	}
	for _, r := range rows {
		t.Run(r.name, func(t *testing.T) {
			var c Config
			r.set(&c)
			check(t, c, r.want)
		})
	}
	// Every row, broken together with a row of a later field, is the one reported: the rows run in
	// field order.
	field := func(want string) string { return strings.SplitN(want, ":", 2)[0] }
	for i, r := range rows {
		for _, later := range rows[i+1:] {
			if field(later.want) == field(r.want) {
				continue
			}
			var c Config
			later.set(&c)
			r.set(&c)
			t.Run(r.name+" before "+later.name, func(t *testing.T) { check(t, c, r.want) })
		}
	}
}

// stubCodec is a codec.Codec that Validate must keep as it is.
type stubCodec struct{ codec.JSON }

func (stubCodec) Name() string { return "stub" }
