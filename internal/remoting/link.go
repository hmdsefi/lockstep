// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package remoting

import "errors"

// ErrUnknownNode is returned by Driver.Dial, and then by Core.Connect, when the driver knows no
// address for a node.
var ErrUnknownNode = errors.New("remoting: unknown node")

// Driver opens connections for a Core and carries their bytes: TCP connections in production and
// simulated datagrams in simulation. The core decides when to dial, what to write and when to
// close; the driver only moves frames.
//
// The core calls Dial, and the Link methods, while it holds its lock. They must never block and
// must never call back into the core: a driver reports what happens on a link later, through
// Core.Accepted, Core.Frame and Core.Closed. Those calls run in engine context, as an engine
// callback does, so a driver with goroutines of its own (a TCP reader, say) posts them to the
// engine instead of calling the core from its goroutine.
type Driver interface {
	// Dial starts a connection to node and returns its Link at once, before the connection is
	// established. advertise is the address node announced in its last HELLO to this core, or
	// empty; a driver tries it after the addresses it is configured with. Dial returns an error
	// wrapping ErrUnknownNode when it knows no address for node, and another error when it cannot
	// start a connection at all; it then returns no Link.
	//
	// The core writes the HELLO on the returned link right away, so a driver whose connections
	// take time to establish keeps the frames written before then and sends them, in order, once
	// connected. A connection that fails to establish is reported with Core.Closed.
	Dial(node, advertise string) (Link, error)
}

// Link is one connection: one TCP connection, or one connection ID between two simulated nodes.
// The core tells links apart by their value, so a Link must be comparable, a pointer for example,
// and IDs need not be unique between links in different directions.
type Link interface {
	// ID returns the connection's number, which appears in the core's records as "conn". It is
	// the same for the link's lifetime.
	ID() uint64
	// Write sends one frame, length prefix included. Frames go out in Write order. Write never
	// blocks and reports no error: a link that cannot write any more is reported with
	// Core.Closed. The link owns frame after the call.
	Write(frame []byte)
	// Close closes the connection after the frames written before it have been handed to the
	// network. It may be called more than once; calls after the first do nothing. The core calls
	// Close on every link it drops, including one the driver reported with Core.Closed, and
	// ignores calls about a link after it closed it.
	Close()
}
