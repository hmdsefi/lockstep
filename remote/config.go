// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package remote

import (
	"crypto/tls"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/hmdsefi/lockstep/codec"
	"github.com/hmdsefi/lockstep/internal/engine"
)

// ProtocolVersion is the version byte of every frame.
const ProtocolVersion = 1

// DefaultMaxFrame is the default largest frame, in bytes: 1 MiB.
const DefaultMaxFrame = 1 << 20

// Frame size bounds that Config.Validate enforces on MaxFrame.
const (
	minMaxFrame = 4096
	maxMaxFrame = 64 << 20
)

// Defaults filled in for the zero fields of a Config.
const (
	defaultOutboundQueue = 1024
	defaultHeartbeat     = time.Second
	defaultIdleTimeout   = 10 * time.Second
	defaultDialTimeout   = 5 * time.Second
	defaultRedialMin     = 100 * time.Millisecond
	defaultRedialMax     = 10 * time.Second
)

var (
	// ErrAlreadyEnabled is returned when remoting is enabled twice on one System.
	ErrAlreadyEnabled = errors.New("remote: already enabled")
	// ErrFrameTooLarge is returned for a message whose frame exceeds the largest frame.
	ErrFrameTooLarge = errors.New("remote: frame too large")
	// ErrUnreachable is returned when a peer cannot be reached.
	ErrUnreachable = errors.New("remote: node unreachable")
	// ErrPeerRestarted is returned for work that was in flight when a peer restarted.
	ErrPeerRestarted = errors.New("remote: peer restarted")
)

// Config configures remoting on one node. A zero field means its default, except for the network
// fields (Listen, Advertise, Peers, Resolve, TLS), whose zero values mean what their comments say.
// Validate applies the defaults.
type Config struct {
	// Listen is the host:port this node listens on. Enable requires it in production.
	Listen string
	// Advertise is the host:port peers dial back. Empty means the listener's address.
	Advertise string
	// Peers maps a node name to its host:port.
	Peers map[string]string
	// Resolve is asked for a node that is not in Peers. Nil means none.
	Resolve func(node string) (string, error)
	// TLS, when not nil, makes both the listener and the dialer use TLS. Nil means plain TCP.
	TLS *tls.Config
	// Registry holds the message types that cross nodes. Nil means codec.Default().
	Registry *codec.Registry
	// Codec encodes and decodes messages. Nil means codec.JSON{}.
	Codec codec.Codec
	// MaxFrame is the largest frame in bytes, from 4096 to 67108864. Zero means DefaultMaxFrame.
	MaxFrame int
	// OutboundQueue is the number of frames per peer that are not yet written. Zero means 1024.
	OutboundQueue int
	// Heartbeat is the interval between pings. Zero means 1s.
	Heartbeat time.Duration
	// IdleTimeout is how long a connection may stay silent before it is dropped. It must be at
	// least twice Heartbeat. Zero means 10s.
	IdleTimeout time.Duration
	// DialTimeout bounds one dial. Zero means 5s.
	DialTimeout time.Duration
	// RedialMin is the first wait before a redial. Zero means 100ms.
	RedialMin time.Duration
	// RedialMax is the longest wait before a redial. It must not be less than RedialMin. Zero
	// means 10s.
	RedialMax time.Duration
}

// DefaultConfig returns a Config with every default filled in. Listen, Advertise, Peers, Resolve
// and TLS stay empty.
func DefaultConfig() Config {
	return Config{
		Registry:      codec.Default(),
		Codec:         codec.JSON{},
		MaxFrame:      DefaultMaxFrame,
		OutboundQueue: defaultOutboundQueue,
		Heartbeat:     defaultHeartbeat,
		IdleTimeout:   defaultIdleTimeout,
		DialTimeout:   defaultDialTimeout,
		RedialMin:     defaultRedialMin,
		RedialMax:     defaultRedialMax,
	}
}

// Validate fills in the default of every zero field of c, then checks c and returns the first
// problem it finds. Every error text starts with "remote: invalid config: ".
func (c *Config) Validate() error {
	if c.Registry == nil {
		c.Registry = codec.Default()
	}
	if c.Codec == nil {
		c.Codec = codec.JSON{}
	}
	if c.MaxFrame == 0 {
		c.MaxFrame = DefaultMaxFrame
	}
	if c.OutboundQueue == 0 {
		c.OutboundQueue = defaultOutboundQueue
	}
	if c.Heartbeat == 0 {
		c.Heartbeat = defaultHeartbeat
	}
	if c.IdleTimeout == 0 {
		c.IdleTimeout = defaultIdleTimeout
	}
	if c.DialTimeout == 0 {
		c.DialTimeout = defaultDialTimeout
	}
	if c.RedialMin == 0 {
		c.RedialMin = defaultRedialMin
	}
	if c.RedialMax == 0 {
		c.RedialMax = defaultRedialMax
	}

	const p = "remote: invalid config: "
	switch {
	case c.MaxFrame < minMaxFrame || c.MaxFrame > maxMaxFrame:
		return fmt.Errorf(p+"MaxFrame %d out of range [%d, %d]", c.MaxFrame, minMaxFrame, maxMaxFrame)
	case c.OutboundQueue < 1:
		return fmt.Errorf(p+"OutboundQueue %d is less than 1", c.OutboundQueue)
	case c.Heartbeat <= 0:
		return fmt.Errorf(p+"Heartbeat %s is not positive", c.Heartbeat)
	case c.IdleTimeout < 2*c.Heartbeat:
		return fmt.Errorf(p+"IdleTimeout %s is less than twice Heartbeat %s", c.IdleTimeout, c.Heartbeat)
	case c.DialTimeout <= 0:
		return fmt.Errorf(p+"DialTimeout %s is not positive", c.DialTimeout)
	case c.RedialMin <= 0:
		return fmt.Errorf(p+"RedialMin %s is not positive", c.RedialMin)
	case c.RedialMax < c.RedialMin:
		return fmt.Errorf(p+"RedialMax %s is less than RedialMin %s", c.RedialMax, c.RedialMin)
	}
	for _, n := range slices.Sorted(maps.Keys(c.Peers)) {
		if !engine.ValidNodeName(n) {
			return fmt.Errorf(p+"peer name %q is not a valid node name", n)
		}
		if c.Peers[n] == "" {
			return fmt.Errorf(p+"peer %q has an empty address", n)
		}
	}
	return nil
}
