// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package cluster

import (
	"fmt"
	"reflect"
	"slices"
	"time"

	"github.com/hmdsefi/lockstep"
)

// Defaults of the options of a Kind.
const (
	// DefaultIdleTimeout is how long a grain may go without a message before its activation is
	// stopped.
	DefaultIdleTimeout = 10 * time.Minute
	// DefaultHandoffTimeout is how long a node that takes over a grain waits for the previous
	// owners to hand it over before it activates the grain anyway.
	DefaultHandoffTimeout = 5 * time.Second
	// DefaultBuffer is the number of messages a node holds for the grains of a kind while their
	// owner is not settled.
	DefaultBuffer = 1024
)

// Kind describes one kind of grain a node hosts: its name, the factory of its activations and its
// options. Build one with NewKind and list it in Config.Kinds. The message type of the kind is
// fixed by NewKind and erased here. The zero Kind is invalid.
type Kind struct {
	name string
	kindConfig

	// The closures below are built by NewKind[M] and are the only code that knows M. They are nil
	// in the zero Kind; spawn is nil when the factory was nil.

	// spawn spawns an activation of the grain key under parent as a child named name, with opts,
	// and returns its address and its PID (a lockstep.PID[M], as an opaque value for send). It
	// calls the kind's factory with key at every start and restart of the activation. parent is any
	// lockstep.Spawner, the context of the host that owns the grain, whatever the host's message
	// type is.
	spawn func(parent lockstep.Spawner, name, key string, opts []lockstep.SpawnOption) (lockstep.Address, any, error)
	// accepts reports whether v is an M.
	accepts func(v any) bool
	// send sends msg, which must be an M, to the activation whose PID spawn returned.
	send func(from lockstep.Sender, to, msg any) error
	// typ is the reflect.Type of M, for the text of a wrong-type error.
	typ reflect.Type
}

// kindConfig holds the options of a Kind. NewKind sets the defaults first, then applies the options
// in order, so that a later option replaces an earlier one.
type kindConfig struct {
	idle       time.Duration
	handoff    time.Duration
	buffer     int
	activation []lockstep.SpawnOption
	host       []lockstep.SpawnOption
}

// KindOption configures a Kind. It is applied by NewKind, which does not check the value; Join
// checks it (see Config.Validate).
type KindOption func(*kindConfig)

// NewKind returns the Kind that hosts grains of the given name. The name must be 1 to 64 bytes
// of a-z, 0-9, '.', '_' and '-', and unique among the kinds of a node. An activation, the running
// actor of one grain, is made by calling factory with the grain's key every time it starts, and
// receives messages of type M. NewKind never panics and checks nothing: Join returns an error
// wrapping ErrInvalidConfig for an invalid kind, a nil factory included. Options are applied in
// order, and a later one replaces an earlier one of the same kind; a nil option is skipped.
func NewKind[M any](name string, factory func(key string) lockstep.Actor[M], opts ...KindOption) Kind {
	k := Kind{
		name:       name,
		kindConfig: kindConfig{idle: DefaultIdleTimeout, handoff: DefaultHandoffTimeout, buffer: DefaultBuffer},
		typ:        reflect.TypeFor[M](),
	}
	for _, o := range opts {
		if o != nil {
			o(&k.kindConfig)
		}
	}
	k.accepts = func(v any) bool {
		_, ok := v.(M)
		return ok
	}
	typ := k.typ
	k.send = func(from lockstep.Sender, to, msg any) error {
		pid, ok := to.(lockstep.PID[M])
		if !ok {
			return fmt.Errorf("cluster: kind %q: activation is a %T, want a lockstep.PID[%v]", name, to, typ)
		}
		m, ok := msg.(M)
		if !ok {
			return fmt.Errorf("cluster: kind %q: message is a %T, want %v", name, msg, typ)
		}
		return lockstep.Send(from, pid, m)
	}
	if factory != nil {
		k.spawn = func(parent lockstep.Spawner, actorName, key string, spawnOpts []lockstep.SpawnOption) (lockstep.Address, any, error) {
			pid, err := lockstep.Spawn(parent, actorName, func() lockstep.Actor[M] { return factory(key) }, spawnOpts...)
			if err != nil {
				return lockstep.Address{}, nil, err
			}
			return pid.Address(), pid, nil
		}
	}
	return k
}

// Name returns the kind's name.
func (k Kind) Name() string { return k.name }

// WithIdleTimeout sets how long a grain of the kind may go without a message before its
// activation is stopped to free its memory. It must be positive. The default is
// DefaultIdleTimeout.
func WithIdleTimeout(d time.Duration) KindOption {
	return func(c *kindConfig) { c.idle = d }
}

// WithHandoffTimeout sets how long a node that becomes the owner of a grain waits for the previous
// owners to stop their activations of it before it activates the grain anyway. It must be
// positive. The default is DefaultHandoffTimeout.
func WithHandoffTimeout(d time.Duration) KindOption {
	return func(c *kindConfig) { c.handoff = d }
}

// WithBuffer sets how many messages a node holds for the grains of the kind while it cannot yet
// deliver them, for instance because the grain's owner is still changing. It must be positive. The
// default is DefaultBuffer.
func WithBuffer(n int) KindOption {
	return func(c *kindConfig) { c.buffer = n }
}

// WithActivationOptions sets the spawn options of every activation of the kind. Invalid options
// are reported by Join.
func WithActivationOptions(opts ...lockstep.SpawnOption) KindOption {
	return func(c *kindConfig) { c.activation = slices.Clone(opts) }
}

// WithHostOptions sets the spawn options of the actor that hosts the kind's grains on a node.
// Invalid options are reported by Join.
func WithHostOptions(opts ...lockstep.SpawnOption) KindOption {
	return func(c *kindConfig) { c.host = slices.Clone(opts) }
}

// maxKindName is the longest kind name, in bytes.
const maxKindName = 64

// validKindName reports whether s is a valid kind name (VAC-010): 1 to 64 bytes of a-z, 0-9, '.',
// '_' and '-', and not "." or "..", which are not valid actor names and a kind's host is an actor
// named after it.
func validKindName(s string) bool {
	if len(s) == 0 || len(s) > maxKindName || s == "." || s == ".." {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '.' && c != '_' && c != '-' {
			return false
		}
	}
	return true
}

// kindNames returns the names of kinds, sorted, or nil when there are none. Names that Validate
// accepted are unique.
func kindNames(kinds []Kind) []string {
	if len(kinds) == 0 {
		return nil
	}
	names := make([]string, len(kinds))
	for i, k := range kinds {
		names[i] = k.name
	}
	slices.Sort(names)
	return names
}

// validKindList reports whether names is sorted, free of duplicates and made of valid kind names:
// the form a member's Kinds take on the wire (VAC-700).
func validKindList(names []string) bool {
	for i, n := range names {
		if !validKindName(n) || (i > 0 && names[i-1] >= n) {
			return false
		}
	}
	return true
}
