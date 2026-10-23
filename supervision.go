// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"io"
	"reflect"
	"strconv"

	"github.com/hmdsefi/lockstep/internal/engine"
)

// Own registers r to be closed after the incarnation's Stopped hook returns. Resources are closed
// in reverse registration order. It panics if r is nil.
func (c *Context[M]) Own(r io.Closer) {
	c.mustInTurn()
	if r == nil {
		// Inside a handler this panic is recovered as a failure with this PanicError, inside
		// Stopped as a cleanup failure (SUP-120, SUP-140).
		panic("lockstep: Own called with nil resource")
	}
	c.owned = append(c.owned, r)
}

// closeOwned closes the incarnation's owned resources in reverse registration order, including
// those registered during Stopped (SUP-120). Each Close is a guarded call: an error, or a panic as
// a *PanicError, is recorded in lockstep.close and the next resource is closed. The list is empty
// afterwards. It runs in the turn that ends the incarnation, outside the in-turn window of the
// Context: a Close has no handle to the runtime.
func (c *cell[M]) closeOwned() {
	owned := c.ctx.owned
	c.ctx.owned = nil
	for i := len(owned) - 1; i >= 0; i-- {
		var err error
		if failed, perr := guard(func() { err = owned[i].Close() }); failed {
			err = perr
		}
		c.recordClose(i, owned[i], err)
	}
}

// recordClose emits lockstep.close (SUP §8) for the resource r registered at index, closed with
// error err (nil on success).
func (c *cell[M]) recordClose(index int, r io.Closer, err error) {
	tr := c.sys.eng.Tracer
	if !tr.Enabled() {
		return
	}
	desc := ""
	if err != nil {
		desc = tr.Describe(err)
	}
	tr.Record("lockstep.close",
		engine.Attr{Key: "actor", Value: c.addr.String()},
		engine.Attr{Key: "inc", Value: strconv.FormatUint(uint64(c.ctx.inc), 10)},
		engine.Attr{Key: "index", Value: strconv.Itoa(index)},
		engine.Attr{Key: "type", Value: reflect.TypeOf(r).String()},
		engine.Attr{Key: "err", Value: desc},
	)
}

// Fail marks the actor failed with err when the current handler returns. Only the first call per
// handler counts. It panics if err is nil.
func (c *Context[M]) Fail(err error) {
	c.mustInTurn()
	if err == nil {
		// Inside the handler this panic is recovered as a failure with this PanicError (SUP-140).
		panic("lockstep: Fail called with nil error")
	}
	if c.failure == nil {
		c.failure = err
	}
}

// Stop stops the actor with reason Normal when the current handler returns. No further user
// message is processed by this incarnation.
func (c *Context[M]) Stop() {
	c.mustInTurn()
	// Inside Stopped, the only handler of an ending incarnation, it does nothing (SUP-050,
	// SUP-100).
	if c.cell.ending() {
		return
	}
	// The request goes on the system queue, which the turn processes as soon as the handler
	// returns and before it pops another user message (SUP-010).
	c.cell.requestStop(Normal, c.cell)
}
