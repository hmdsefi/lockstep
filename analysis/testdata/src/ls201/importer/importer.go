// Package importer writes to the fields of a type marked in package bad, which it sees through the
// marker's fact (ANL-090, ANL-260, AT-ANL-16).
package importer

import (
	"slices"

	"ls201/bad"
)

// write writes to the shared fields of an imported immutable type.
func write(b bad.Batch, pb *bad.Batch) {
	b.Items[0] = 1       // want `^LS201: element assignment writes to field Items of immutable type bad\.Batch$`
	slices.Sort(b.Items) // want `^LS201: slices\.Sort writes to field Items of immutable type bad\.Batch$`
	pb.N = 3             // want `^LS201: assignment through a pointer writes to field N of immutable type bad\.Batch$`
}

// build makes a local copy; its field assignments are construction.
func build(xs []int) bad.Batch {
	var b bad.Batch
	b.Items = xs
	b.N = len(xs)
	return b
}
