// Package generic exercises the roots of a generic actor type and method values and method
// expressions on it (ANL-040, ANL-050).
package generic

import "github.com/hmdsefi/lockstep"

// Cell is a generic actor type with roots besides Receive.
type Cell[M any] struct{ v M }

func (c *Cell[M]) Started(ctx *lockstep.Context[M]) { // want `^actor code: \(\*Cell\[M\]\)\.Started$`
	g := c.reset
	g()
}

func (c *Cell[M]) Receive(ctx *lockstep.Context[M], msg M) { // want `^actor code: \(\*Cell\[M\]\)\.Receive$`
	f := (*Cell[M]).set
	f(c, msg)
}

// Stopped is not a root: its context's type argument is not the message type.
func (c *Cell[M]) Stopped(ctx *lockstep.Context[int], r lockstep.StopReason) {}

func (c *Cell[M]) set(v M) { c.v = v } // want `^actor code: \(\*Cell\[M\]\)\.Receive -> \(\*Cell\[M\]\)\.set$`

func (c *Cell[M]) reset() { // want `^actor code: \(\*Cell\[M\]\)\.Started -> \(\*Cell\[M\]\)\.reset$`
	var zero M
	c.v = zero
}
