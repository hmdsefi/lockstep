// Package marked declares marked types for the importer package, which sees the markers only
// through the facts the index analyzer exports.
package marked

// Batch is immutable, so its slice field is safe to share.
//
//lockstep:immutable
type Batch struct {
	Items []int
}

// Conn is a resource.
//
//lockstep:resource
type Conn struct{}

// Both carries both markers.
//
//lockstep:immutable
//lockstep:resource
type Both struct{}

// Frozen is a generic immutable type.
//
//lockstep:immutable
type Frozen[T any] struct {
	Values []T
}

// Plain has no marker.
type Plain struct {
	Items []int
}

// Wrapper is unmarked and holds a marked type, which the importer reaches without naming it.
type Wrapper struct {
	Batch Batch
}
