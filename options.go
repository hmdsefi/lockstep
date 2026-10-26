// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"fmt"
	"runtime"
	"time"
)

// defaultNode is the node name of a system built by New without WithNodeName (ACT-020).
const defaultNode = "local"

// Defaults of the production options, used when an option is absent.
const (
	defaultThroughput    = 32
	defaultEffectWorkers = 64
	defaultEffectQueue   = 1024
)

// systemConfig is what the options of New configure. Option appliers only set fields and report
// a bad value through fail; New validates (ACT-010, R44).
type systemConfig struct {
	node  string
	fixed bool          // the node name comes from an engine: WithNodeName is ignored (ACT-015)
	mbx   mailboxConfig // the default mailbox of the system's actors (MBX-010)

	guardian Strategy // the user guardian's strategy (SUP-370)

	// The overload options (OVL §4.1). The cap and the estimates act on every engine, the admission
	// thresholds only in a system New builds (OVL-300, OVL-230); the watchdog timeout and the
	// goroutine bound are validated and kept for the features that read them.
	maxActors        int             // the actor cap (OVL-010)
	admission        admissionConfig // WithAdmission's thresholds (OVL-350)
	estimates        bool            // per-actor memory estimates are on (OVL-400)
	watchdog         time.Duration   // the stuck-turn timeout; 0 is off
	goroutineWarning int             // the goroutine warning bound; 0 is off

	// The production options (SCH §4.1). An invalid value is reported through fail; of several
	// valid options for a setting the last counts.
	workers       int // WithWorkers; default runtime.GOMAXPROCS(0) read by newConfig
	throughput    int // WithThroughput
	effectWorkers int // WithEffectPool's workers
	effectQueue   int // WithEffectPool's queue

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
// constructors: no option constructor panics.
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

// WithWorkers sets the number of worker goroutines that run actor turns. The default is
// runtime.GOMAXPROCS(0), read once when New runs. New panics with an error wrapping
// ErrInvalidOption if n < 1. The pool size never changes after New, even when GOMAXPROCS changes.
func WithWorkers(n int) Option {
	return func(c *systemConfig) {
		if n < 1 {
			c.fail(fmt.Errorf("%w: WithWorkers(%d): n must be at least 1", ErrInvalidOption, n))
			return
		}
		c.workers = n
	}
}

// WithThroughput sets how many user messages one actor may process in a turn before it
// yields its worker to other actors. The default is 32. New panics with an error wrapping
// ErrInvalidOption if n < 1. It applies to the production engine only; simulation always uses 1.
func WithThroughput(n int) Option {
	return func(c *systemConfig) {
		if n < 1 {
			c.fail(fmt.Errorf("%w: WithThroughput(%d): n must be at least 1", ErrInvalidOption, n))
			return
		}
		c.throughput = n
	}
}

// WithEffectPool sets the effect pool: at most workers effects run at once, and at most
// queue more wait for a free slot. A Do call that finds both full resolves with
// ErrOverloaded. The defaults are 64 and 1024. New panics with an error wrapping
// ErrInvalidOption if workers < 1 or queue < 0.
func WithEffectPool(workers, queue int) Option {
	return func(c *systemConfig) {
		if workers < 1 || queue < 0 {
			c.fail(fmt.Errorf("%w: WithEffectPool(%d, %d): workers must be at least 1 and queue at least 0",
				ErrInvalidOption, workers, queue))
			return
		}
		c.effectWorkers, c.effectQueue = workers, queue
	}
}

// newConfig applies opts in order to the defaults and validates the result. A non-empty
// engineNode is the node name of the engine the system is built on (ACT-015): it replaces the
// default and WithNodeName is ignored. The error is the first invalid setting in argument order.
func newConfig(opts []Option, engineNode string) (*systemConfig, error) {
	c := &systemConfig{
		node: defaultNode, mbx: defaultMailbox, guardian: OneForOne(nil),
		maxActors: DefaultMaxActors, watchdog: DefaultWatchdogTimeout, goroutineWarning: DefaultGoroutineWarning,
		workers: runtime.GOMAXPROCS(0), throughput: defaultThroughput,
		effectWorkers: defaultEffectWorkers, effectQueue: defaultEffectQueue,
	}
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
