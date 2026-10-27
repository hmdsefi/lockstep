// Package good holds code that LS201 accepts: immutable types whose fields are immutable
// (ANL-250, AT-ANL-15), and construction and reads of their fields (ANL-260, AT-ANL-16).
package good

import (
	"slices"
	"time"

	"github.com/hmdsefi/lockstep"
)

// Msg is a message type.
type Msg int

// Header is immutable.
//
//lockstep:immutable
type Header struct{ ID int64 }

// Point is a value struct.
type Point struct{ X, Y int }

// Fields holds every allowed kind of field.
//
//lockstep:immutable
type Fields struct {
	ID     int64
	Name   string
	Err    error
	At     time.Time
	Every  time.Duration
	To     lockstep.PID[Msg]
	Reply  lockstep.ReplyTo[int]
	Addr   lockstep.Address
	Tags   []string
	Counts map[string]int
	Points [3]Point
	Head   *Header
	Heads  map[string][]*Header
	Anon   struct{ A, B int }
	Sub    Header
	Graph  Node
	Box    Box[*int]
	_      [0]func()
}

// Tree is recursive through its own marker.
//
//lockstep:immutable
type Tree struct{ Kids []Tree }

// Node is unmarked and recursive; it holds only allowed fields.
type Node struct {
	V    int
	Kids []Node
}

// Box is generic, and every instance is immutable because its marker says so.
//
//lockstep:immutable
type Box[T comparable] struct{ N int }

// Names is not a struct.
//
//lockstep:immutable
type Names []string

// Batch is immutable.
//
//lockstep:immutable
type Batch struct {
	Items []int
	Tags  map[string]int
	N     int
}

// Plain is not marked: writes to it are not checked.
type Plain struct {
	Items []int
	N     int
}

// Holder is an actor-like type that holds a Batch.
type Holder struct {
	batch Batch
	ptr   *Batch
}

// construct builds Batch values and reads their fields.
func construct(xs []int, dst []int) (Batch, *Batch, int) {
	b := Batch{Items: xs}
	var c Batch
	c.N = 2
	c.Items = xs
	c.Tags = map[string]int{"a": 1}
	p := &Batch{N: 1, Items: slices.Clone(xs)}
	n := b.Items[0] + c.Tags["a"] + len(p.Items)
	for _, v := range b.Items {
		n += v
	}
	sorted := slices.Clone(b.Items)
	slices.Sort(sorted)
	copy(dst, b.Items)
	dst = append(dst, b.Items...)
	n = len(b.Items)
	_ = append(b.Items[:0:0], b.Items...)
	_ = append(b.Items[:n:n], 1)
	_ = slices.Insert(b.Items[:n:n], 0, 1)
	_ = &b
	_ = &c.N
	return b, p, n + len(dst)
}

// replace assigns a new value to the holder's fields; the Batch itself is not written.
func (h *Holder) replace(xs []int) {
	h.batch = Batch{Items: xs}
	h.batch.N = len(xs)
	h.ptr = &Batch{N: 1}
}

// plain writes to an unmarked type.
func plain(p *Plain) {
	p.Items[0] = 1
	p.N = 2
	slices.Sort(p.Items)
	p.Items = append(p.Items, 1)
}

// sorter has a method named like an in-place mutator.
type sorter struct{}

// Sort does nothing.
func (sorter) Sort([]int) {}

// methods calls a method that is not a library mutator.
func methods(b Batch) {
	sorter{}.Sort(b.Items)
}

// unmarked declares a function-local type without a marker: its fields are not checked.
func unmarked() {
	type local struct{ C chan int }
	_ = local{}
}
