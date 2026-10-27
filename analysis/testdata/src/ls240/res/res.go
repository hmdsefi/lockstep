// Package res declares resource types for the remote package, which sees the markers only through
// the facts the index analyzer exports.
package res

// Conn is a resource released by Close.
//
//lockstep:resource
type Conn struct{}

// Close closes the connection.
func (*Conn) Close() {}

// Lease is a resource released by Release.
//
//lockstep:resource
type Lease struct{}

// Release gives the lease back.
func (*Lease) Release() {}

// Plain is not a resource.
type Plain struct{}

// Close closes it.
func (*Plain) Close() {}
