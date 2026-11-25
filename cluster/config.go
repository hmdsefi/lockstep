// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package cluster

import (
	"fmt"
	"time"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/internal/hooks"
)

// Defaults filled in for the zero fields of a Config.
const (
	DefaultProbeInterval     = time.Second
	DefaultProbeTimeout      = 500 * time.Millisecond
	DefaultIndirectProbes    = 3
	DefaultSuspicionTimeout  = 5 * time.Second
	DefaultSyncInterval      = 30 * time.Second
	DefaultJoinRetryInterval = time.Second
	DefaultRetransmitMult    = 4
	DefaultMaxPiggyback      = 8
	DefaultDeadRetention     = time.Minute
)

// Config configures cluster membership on one node. A zero field means its default; a negative
// one is invalid. Validate applies the defaults.
//
// Validate checks the fields after the defaults are filled in, so the limits between fields hold
// against the defaults of the fields left zero. Setting only ProbeInterval to more than the default
// SuspicionTimeout (5s), or to no more than the default ProbeTimeout (500ms), fails; set the
// related fields together.
type Config struct {
	// Seeds names the nodes to join the cluster through, and may include this node. Empty means a
	// cluster of one, which other nodes may join.
	Seeds []string
	// ProbeInterval is the time between two probes of other members. Zero means 1s.
	ProbeInterval time.Duration
	// ProbeTimeout is how long a probe waits for an answer before other members are asked to probe
	// the target. It must be less than ProbeInterval. Zero means 500ms.
	ProbeTimeout time.Duration
	// IndirectProbes is the number of members asked to probe a target that did not answer. Zero
	// means 3.
	IndirectProbes int
	// SuspicionTimeout is how long a member stays suspected before it is declared dead. It must
	// not be less than ProbeInterval. Zero means 5s.
	SuspicionTimeout time.Duration
	// SyncInterval is the time between two exchanges of the whole member table with a random
	// member. Zero means 30s.
	SyncInterval time.Duration
	// JoinRetryInterval is the time between two attempts to reach the seeds until this node has
	// joined. Zero means 1s.
	JoinRetryInterval time.Duration
	// RetransmitMult scales how many times each update is piggybacked: RetransmitMult times the
	// number of bits of the member count. Zero means 4.
	RetransmitMult int
	// MaxPiggyback is the largest number of updates piggybacked on one message. Zero means 8.
	MaxPiggyback int
	// DeadRetention is how long a dead or departed member is kept in the table after its last
	// change. Zero means 60s.
	DeadRetention time.Duration
	// Kinds lists the grain kinds this node hosts. Empty means a client-only node, which sends to
	// grains but hosts none. Kind names must be unique. Validate checks the kinds, in order, after
	// every other field: a kind needs a valid name, a factory, positive IdleTimeout,
	// HandoffTimeout and Buffer options, and valid spawn options. It does not change Kinds.
	Kinds []Kind
}

// DefaultConfig returns a Config with every default filled in and no seeds.
func DefaultConfig() Config {
	return Config{
		ProbeInterval:     DefaultProbeInterval,
		ProbeTimeout:      DefaultProbeTimeout,
		IndirectProbes:    DefaultIndirectProbes,
		SuspicionTimeout:  DefaultSuspicionTimeout,
		SyncInterval:      DefaultSyncInterval,
		JoinRetryInterval: DefaultJoinRetryInterval,
		RetransmitMult:    DefaultRetransmitMult,
		MaxPiggyback:      DefaultMaxPiggyback,
		DeadRetention:     DefaultDeadRetention,
	}
}

// Validate fills in the default of every zero field of c, then checks c field by field and returns
// the first problem it finds. Every error wraps ErrInvalidConfig, and its text is
// "cluster: invalid config: <Field>: <reason>".
func (c *Config) Validate() error {
	c.applyDefaults()

	invalid := func(field, format string, args ...any) error {
		return fmt.Errorf("%w: %s: %s", ErrInvalidConfig, field, fmt.Sprintf(format, args...))
	}
	seen := make(map[string]struct{}, len(c.Seeds))
	for _, s := range c.Seeds {
		if !hooks.ValidNodeName(s) {
			return invalid("Seeds", "seed %q is not a valid node name", s)
		}
		if _, dup := seen[s]; dup {
			return invalid("Seeds", "duplicate seed %q", s)
		}
		seen[s] = struct{}{}
	}
	switch {
	case c.ProbeInterval <= 0:
		return invalid("ProbeInterval", "%s is not positive", c.ProbeInterval)
	case c.ProbeTimeout <= 0:
		return invalid("ProbeTimeout", "%s is not positive", c.ProbeTimeout)
	case c.ProbeTimeout >= c.ProbeInterval:
		return invalid("ProbeTimeout", "%s is not less than ProbeInterval %s", c.ProbeTimeout, c.ProbeInterval)
	case c.IndirectProbes < 0:
		return invalid("IndirectProbes", "%d is negative", c.IndirectProbes)
	case c.SuspicionTimeout < c.ProbeInterval:
		return invalid("SuspicionTimeout", "%s is less than ProbeInterval %s", c.SuspicionTimeout, c.ProbeInterval)
	case c.SyncInterval <= 0:
		return invalid("SyncInterval", "%s is not positive", c.SyncInterval)
	case c.JoinRetryInterval <= 0:
		return invalid("JoinRetryInterval", "%s is not positive", c.JoinRetryInterval)
	case c.RetransmitMult < 1:
		return invalid("RetransmitMult", "%d is less than 1", c.RetransmitMult)
	case c.MaxPiggyback < 1:
		return invalid("MaxPiggyback", "%d is less than 1", c.MaxPiggyback)
	case c.DeadRetention <= 0:
		return invalid("DeadRetention", "%s is not positive", c.DeadRetention)
	}
	return c.validateKinds()
}

// validateKinds checks Kinds in order; the first failing row of a kind returns (VAC-010).
func (c *Config) validateKinds() error {
	seen := make(map[string]struct{}, len(c.Kinds))
	for i, k := range c.Kinds {
		invalid := func(format string, args ...any) error {
			return fmt.Errorf("%w: Kinds[%d]: %s", ErrInvalidConfig, i, fmt.Sprintf(format, args...))
		}
		if !validKindName(k.name) {
			return invalid("name %q is not a valid kind name", k.name)
		}
		if _, dup := seen[k.name]; dup {
			return invalid("duplicate kind %q", k.name)
		}
		seen[k.name] = struct{}{}
		switch {
		case k.spawn == nil:
			return invalid("nil factory")
		case k.idle <= 0:
			return invalid("IdleTimeout %s is not positive", k.idle)
		case k.handoff <= 0:
			return invalid("HandoffTimeout %s is not positive", k.handoff)
		case k.buffer <= 0:
			return invalid("Buffer %d is not positive", k.buffer)
		}
		if err := checkSpawnOptions(k.activation); err != nil {
			return fmt.Errorf("%w: Kinds[%d]: ActivationOptions: %w", ErrInvalidConfig, i, err)
		}
		if err := checkSpawnOptions(k.host); err != nil {
			return fmt.Errorf("%w: Kinds[%d]: HostOptions: %w", ErrInvalidConfig, i, err)
		}
	}
	return nil
}

// checkSpawnOptions returns the error a Spawn with opts would return.
func checkSpawnOptions(opts []lockstep.SpawnOption) error {
	if len(opts) == 0 {
		return nil
	}
	as := make([]any, len(opts))
	for i, o := range opts {
		as[i] = o
	}
	return hooks.CheckSpawnOptions(as)
}

// applyDefaults sets every zero field of c to its default; negative values stay and fail their
// row of Validate (CLU-010).
func (c *Config) applyDefaults() {
	d := DefaultConfig()
	if c.ProbeInterval == 0 {
		c.ProbeInterval = d.ProbeInterval
	}
	if c.ProbeTimeout == 0 {
		c.ProbeTimeout = d.ProbeTimeout
	}
	if c.IndirectProbes == 0 {
		c.IndirectProbes = d.IndirectProbes
	}
	if c.SuspicionTimeout == 0 {
		c.SuspicionTimeout = d.SuspicionTimeout
	}
	if c.SyncInterval == 0 {
		c.SyncInterval = d.SyncInterval
	}
	if c.JoinRetryInterval == 0 {
		c.JoinRetryInterval = d.JoinRetryInterval
	}
	if c.RetransmitMult == 0 {
		c.RetransmitMult = d.RetransmitMult
	}
	if c.MaxPiggyback == 0 {
		c.MaxPiggyback = d.MaxPiggyback
	}
	if c.DeadRetention == 0 {
		c.DeadRetention = d.DeadRetention
	}
}
