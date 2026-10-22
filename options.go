// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import "fmt"

// defaultNode is the node name of a system built by New without WithNodeName (ACT-020).
const defaultNode = "local"

// systemConfig is what the options of New configure. Option appliers only set fields and report
// a bad value through fail; New validates (ACT-010, R44).
type systemConfig struct {
	node  string
	fixed bool // the node name comes from an engine: WithNodeName is ignored (ACT-015)

	at     int // 1-based position of the option being applied
	nodeAt int // position of the WithNodeName whose name is in node; 0 if none
	err    error
	errAt  int // position of err
}

// fail records err for the option being applied. Only the first call counts.
func (c *systemConfig) fail(err error) {
	if c.err == nil {
		c.err, c.errAt = err, c.at
	}
}

// Option configures New. Options are applied in argument order; a later option overrides an
// earlier one for the same setting. Option values are validated by New, not by the option
// constructors: no option constructor panics (R44).
type Option func(*systemConfig)

// WithNodeName sets the system's node name. The name must be 1-64 bytes from [A-Za-z0-9._-],
// starting with a letter or digit (the faultline node-name rule). WithNodeName never panics; New
// panics with an error wrapping ErrInvalidOption on an invalid name.
// The default is "local". It is ignored on a system built on an engine (simulation), which takes
// the engine's node name.
func WithNodeName(name string) Option {
	return func(c *systemConfig) {
		if c.fixed {
			return
		}
		c.node, c.nodeAt = name, c.at
	}
}

// newConfig applies opts in order to the defaults and validates the result. A non-empty
// engineNode is the node name of the engine the system is built on (ACT-015): it replaces the
// default and WithNodeName is ignored. The error is the first invalid setting in argument order.
func newConfig(opts []Option, engineNode string) (*systemConfig, error) {
	c := &systemConfig{node: defaultNode}
	if engineNode != "" {
		c.node, c.fixed = engineNode, true
	}
	for i, o := range opts {
		c.at = i + 1
		if o == nil {
			c.fail(fmt.Errorf("%w: nil Option", ErrInvalidOption))
			continue
		}
		o(c)
	}
	// Only the last WithNodeName counts, so its name is checked once every option has run; its
	// position decides whether it or another option's error came first.
	if c.nodeAt > 0 && !validNode(c.node) && (c.err == nil || c.nodeAt < c.errAt) {
		return nil, fmt.Errorf("%w: node name %q: want 1-64 characters [A-Za-z0-9._-], starting with a letter or digit",
			ErrInvalidOption, c.node)
	}
	if c.err != nil {
		return nil, c.err
	}
	return c, nil
}
