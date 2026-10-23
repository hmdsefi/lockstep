// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"math/rand/v2"
	"strconv"
)

// random returns the live incarnation's stream (TMR-120). The first call of an incarnation asks
// the engine's Entropy for it with the TMR-110 label; every later call, Stopped's included,
// returns the same *rand.Rand. begin resets rng, so a restart gets a new stream and an
// incarnation that never calls random never reaches Entropy. The runtime draws nothing from it.
func (c *cell[M]) random() *rand.Rand {
	if c.rng == nil {
		c.rng = c.sys.eng.Entropy.Rand(c.randLabel())
	}
	return c.rng
}

// randLabel returns the stream label of the live incarnation (TMR-110):
// lockstep/<node>/<epoch>/<path>/<incarnation>. The path starts with "/" and the epoch is the
// system's own, so a uid rollover never changes a label.
func (c *cell[M]) randLabel() string {
	return "lockstep/" + c.sys.node + "/" + strconv.FormatUint(uint64(c.sys.incarnation), 10) +
		c.addr.path + "/" + strconv.FormatUint(uint64(c.inc), 10)
}
