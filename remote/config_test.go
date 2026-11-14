// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package remote_test

import (
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep/codec"
	"github.com/hmdsefi/lockstep/remote"
)

func TestSentinelTexts(t *testing.T) {
	for err, want := range map[error]string{
		remote.ErrAlreadyEnabled: "remote: already enabled",
		remote.ErrFrameTooLarge:  "remote: frame too large",
		remote.ErrUnreachable:    "remote: node unreachable",
		remote.ErrPeerRestarted:  "remote: peer restarted",
	} {
		if err.Error() != want {
			t.Errorf("%q, want %q", err.Error(), want)
		}
	}
	if remote.ProtocolVersion != 1 || remote.DefaultMaxFrame != 1<<20 {
		t.Errorf("ProtocolVersion %d, DefaultMaxFrame %d", remote.ProtocolVersion, remote.DefaultMaxFrame)
	}
}

// AT-REM-01, REM-015
func TestATREM01Config(t *testing.T) {
	t.Run("DefaultConfig", func(t *testing.T) {
		c := remote.DefaultConfig()
		if c.Registry != codec.Default() {
			t.Error("Registry is not codec.Default()")
		}
		if _, ok := c.Codec.(codec.JSON); !ok {
			t.Errorf("Codec = %T, want codec.JSON", c.Codec)
		}
		if c.MaxFrame != remote.DefaultMaxFrame || c.OutboundQueue != 1024 ||
			c.Heartbeat != time.Second || c.IdleTimeout != 10*time.Second ||
			c.DialTimeout != 5*time.Second || c.RedialMin != 100*time.Millisecond ||
			c.RedialMax != 10*time.Second {
			t.Errorf("unexpected defaults: %+v", c)
		}
		if c.Listen != "" || c.Advertise != "" || c.Peers != nil || c.Resolve != nil || c.TLS != nil {
			t.Errorf("network fields are set: %+v", c)
		}
		if err := c.Validate(); err != nil {
			t.Errorf("DefaultConfig().Validate() = %v", err)
		}
	})

	t.Run("ValidateAppliesDefaults", func(t *testing.T) {
		var c remote.Config
		if err := c.Validate(); err != nil {
			t.Fatal(err)
		}
		want := remote.DefaultConfig()
		if c.Registry != want.Registry || c.Codec != want.Codec || c.MaxFrame != want.MaxFrame ||
			c.OutboundQueue != want.OutboundQueue || c.Heartbeat != want.Heartbeat ||
			c.IdleTimeout != want.IdleTimeout || c.DialTimeout != want.DialTimeout ||
			c.RedialMin != want.RedialMin || c.RedialMax != want.RedialMax {
			t.Errorf("Validate defaults = %+v, want %+v", c, want)
		}
	})

	t.Run("ValidateKeepsSetFields", func(t *testing.T) {
		reg := codec.NewRegistry()
		c := remote.Config{Registry: reg, MaxFrame: 8192, Heartbeat: 2 * time.Second}
		if err := c.Validate(); err != nil {
			t.Fatal(err)
		}
		if c.Registry != reg || c.MaxFrame != 8192 || c.Heartbeat != 2*time.Second ||
			c.IdleTimeout != 10*time.Second {
			t.Errorf("unexpected config %+v", c)
		}
	})

	const p = "remote: invalid config: "
	tests := []struct {
		name string
		c    remote.Config
		want string
	}{
		{"MaxFrame low", remote.Config{MaxFrame: 4095}, p + "MaxFrame 4095 out of range [4096, 67108864]"},
		{"MaxFrame negative", remote.Config{MaxFrame: -1}, p + "MaxFrame -1 out of range [4096, 67108864]"},
		{"MaxFrame high", remote.Config{MaxFrame: 64<<20 + 1}, p + "MaxFrame 67108865 out of range [4096, 67108864]"},
		{"OutboundQueue", remote.Config{OutboundQueue: -3}, p + "OutboundQueue -3 is less than 1"},
		{"Heartbeat", remote.Config{Heartbeat: -time.Second}, p + "Heartbeat -1s is not positive"},
		{"IdleTimeout", remote.Config{Heartbeat: 3 * time.Second, IdleTimeout: 5 * time.Second},
			p + "IdleTimeout 5s is less than twice Heartbeat 3s"},
		{"IdleTimeout default", remote.Config{Heartbeat: 6 * time.Second},
			p + "IdleTimeout 10s is less than twice Heartbeat 6s"},
		{"DialTimeout", remote.Config{DialTimeout: -time.Millisecond}, p + "DialTimeout -1ms is not positive"},
		{"RedialMin", remote.Config{RedialMin: -time.Millisecond}, p + "RedialMin -1ms is not positive"},
		{"RedialMax", remote.Config{RedialMin: time.Second, RedialMax: 500 * time.Millisecond},
			p + "RedialMax 500ms is less than RedialMin 1s"},
		{"peer name", remote.Config{Peers: map[string]string{"ok": "h:1", "bad name": "h:2"}},
			p + "peer name \"bad name\" is not a valid node name"},
		{"peer address", remote.Config{Peers: map[string]string{"b": "", "a": "h:1"}},
			p + "peer \"b\" has an empty address"},
		// Rows are checked in order: an earlier row wins over a later one.
		{"row order", remote.Config{MaxFrame: 1, OutboundQueue: -1, Peers: map[string]string{"!": ""}},
			p + "MaxFrame 1 out of range [4096, 67108864]"},
		{"row order queue before heartbeat", remote.Config{OutboundQueue: -1, Heartbeat: -1},
			p + "OutboundQueue -1 is less than 1"},
		{"row order heartbeat before idle", remote.Config{Heartbeat: -1, IdleTimeout: -5},
			p + "Heartbeat -1ns is not positive"},
		{"row order dial before redial", remote.Config{DialTimeout: -1, RedialMin: -1},
			p + "DialTimeout -1ns is not positive"},
		{"row order redial before peers", remote.Config{RedialMin: -1, Peers: map[string]string{"!": ""}},
			p + "RedialMin -1ns is not positive"},
		// Peers are checked in ascending name order; a bad name beats an empty address.
		{"peer order", remote.Config{Peers: map[string]string{"b": "", "a!": "x"}},
			p + "peer name \"a!\" is not a valid node name"},
		{"peer name before address", remote.Config{Peers: map[string]string{"-x": ""}},
			p + "peer name \"-x\" is not a valid node name"},
		{"peer name too long", remote.Config{Peers: map[string]string{strings.Repeat("n", 65): "h:1"}},
			p + "peer name \"" + strings.Repeat("n", 65) + "\" is not a valid node name"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.c.Validate()
			if err == nil || err.Error() != tc.want {
				t.Errorf("Validate() = %v, want %q", err, tc.want)
			}
		})
	}

	t.Run("huge Heartbeat", func(t *testing.T) {
		c := remote.Config{Heartbeat: time.Duration(1<<63 - 1), IdleTimeout: time.Hour}
		want := p + "IdleTimeout 1h0m0s is less than twice Heartbeat " + time.Duration(1<<63-1).String()
		if err := c.Validate(); err == nil || err.Error() != want {
			t.Errorf("Validate() = %v, want %q", err, want)
		}
	})

	t.Run("valid edges", func(t *testing.T) {
		for name, c := range map[string]remote.Config{
			"MaxFrame min":       {MaxFrame: 4096},
			"MaxFrame max":       {MaxFrame: 64 << 20},
			"OutboundQueue 1":    {OutboundQueue: 1},
			"Idle == 2*Heart":    {Heartbeat: 3 * time.Second, IdleTimeout: 6 * time.Second},
			"RedialMax == Min":   {RedialMin: time.Second, RedialMax: time.Second},
			"1ns durations":      {Heartbeat: 1, IdleTimeout: 2, DialTimeout: 1, RedialMin: 1, RedialMax: 1},
			"Idle > 2*Heart odd": {Heartbeat: 3, IdleTimeout: 7},
		} {
			if err := c.Validate(); err != nil {
				t.Errorf("%s: Validate() = %v", name, err)
			}
		}
		c := remote.Config{Heartbeat: 3, IdleTimeout: 5}
		if err := c.Validate(); err == nil {
			t.Error("IdleTimeout 5ns with Heartbeat 3ns accepted")
		}
	})

	t.Run("defaults kept on error", func(t *testing.T) {
		c := remote.Config{Heartbeat: -1}
		if err := c.Validate(); err == nil {
			t.Fatal("Validate() = nil")
		}
		want := remote.DefaultConfig()
		if c.MaxFrame != want.MaxFrame || c.OutboundQueue != want.OutboundQueue ||
			c.IdleTimeout != want.IdleTimeout || c.DialTimeout != want.DialTimeout ||
			c.RedialMin != want.RedialMin || c.RedialMax != want.RedialMax ||
			c.Registry != want.Registry || c.Codec != want.Codec || c.Heartbeat != -1 {
			t.Errorf("defaults not applied on error: %+v", c)
		}
	})

	t.Run("two bad peer names", func(t *testing.T) {
		c := remote.Config{Peers: map[string]string{"z!": "h:1", "b!": "h:2", "a": "h:3"}}
		want := p + "peer name \"b!\" is not a valid node name"
		if err := c.Validate(); err == nil || err.Error() != want {
			t.Errorf("Validate() = %v, want %q", err, want)
		}
	})

	t.Run("valid peers", func(t *testing.T) {
		c := remote.Config{Peers: map[string]string{"n1": "h:1", "0-a_b.c": "h:2"}}
		if err := c.Validate(); err != nil {
			t.Errorf("Validate() = %v", err)
		}
	})
}
