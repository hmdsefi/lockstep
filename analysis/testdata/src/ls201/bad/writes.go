package bad

import (
	"cmp"
	"maps"
	"slices"
	"sort"
)

// Batch is immutable; the functions below write to its fields (AT-ANL-16).
//
//lockstep:immutable
type Batch struct {
	Items []int
	Tags  map[string]int
	N     int
}

// writes writes to the slice and map fields of a Batch value: the elements are shared with
// every copy of b.
func writes(b Batch, src []int, tags map[string]int) {
	b.Items[0] = 1   // want `^LS201: element assignment writes to field Items of immutable type Batch$`
	b.Items[0] += 1  // want `^LS201: element assignment writes to field Items of immutable type Batch$`
	b.Tags["x"] = 1  // want `^LS201: element assignment writes to field Tags of immutable type Batch$`
	b.Items[0]++     // want `^LS201: increment writes to field Items of immutable type Batch$`
	b.Items[0]--     // want `^LS201: increment writes to field Items of immutable type Batch$`
	p := &b.Items[0] // want `^LS201: taking the address of an element writes to field Items of immutable type Batch$`
	_ = p
	delete(b.Tags, "x")          // want `^LS201: delete writes to field Tags of immutable type Batch$`
	clear(b.Tags)                // want `^LS201: clear writes to field Tags of immutable type Batch$`
	copy(b.Items, src)           // want `^LS201: copy writes to field Items of immutable type Batch$`
	b.Items = append(b.Items, 1) // want `^LS201: append writes to field Items of immutable type Batch$`
	slices.Sort(b.Items)         // want `^LS201: slices\.Sort writes to field Items of immutable type Batch$`
	maps.Copy(b.Tags, tags)      // want `^LS201: maps\.Copy writes to field Tags of immutable type Batch$`
}

// throughPointer assigns the fields of a Batch through a pointer.
func throughPointer(pb *Batch, src []int) {
	pb.N = 3                       // want `^LS201: assignment through a pointer writes to field N of immutable type Batch$`
	pb.N *= 2                      // want `^LS201: assignment through a pointer writes to field N of immutable type Batch$`
	pb.N++                         // want `^LS201: assignment through a pointer writes to field N of immutable type Batch$`
	(*pb).N = 4                    // want `^LS201: assignment through a pointer writes to field N of immutable type Batch$`
	pb.Items, pb.Tags = src, nil   // want `^LS201: assignment through a pointer writes to field Items of immutable type Batch$` `^LS201: assignment through a pointer writes to field Tags of immutable type Batch$`
	pb.Items[0] = 1                // want `^LS201: element assignment writes to field Items of immutable type Batch$`
	pb.Items = append(pb.Items, 1) // want `^LS201: assignment through a pointer writes to field Items of immutable type Batch$` `^LS201: append writes to field Items of immutable type Batch$`
	for pb.N = range 3 {           // want `^LS201: assignment through a pointer writes to field N of immutable type Batch$`
	}
}

// mutators passes the fields of a Batch to the in-place mutators of ANL-260.
func mutators(b Batch) {
	slices.SortFunc(b.Items, cmp.Compare[int])                             // want `^LS201: slices\.SortFunc writes to field Items of immutable type Batch$`
	slices.SortStableFunc(b.Items, cmp.Compare[int])                       // want `^LS201: slices\.SortStableFunc writes to field Items of immutable type Batch$`
	slices.Reverse(b.Items)                                                // want `^LS201: slices\.Reverse writes to field Items of immutable type Batch$`
	_ = slices.Delete(b.Items, 0, 1)                                       // want `^LS201: slices\.Delete writes to field Items of immutable type Batch$`
	_ = slices.DeleteFunc(b.Items, func(int) bool { return true })         // want `^LS201: slices\.DeleteFunc writes to field Items of immutable type Batch$`
	_ = slices.Insert(b.Items, 0, 1)                                       // want `^LS201: slices\.Insert writes to field Items of immutable type Batch$`
	_ = slices.Replace(b.Items, 0, 1, 2)                                   // want `^LS201: slices\.Replace writes to field Items of immutable type Batch$`
	_ = slices.Compact(b.Items)                                            // want `^LS201: slices\.Compact writes to field Items of immutable type Batch$`
	_ = slices.CompactFunc(b.Items, func(x, y int) bool { return x == y }) // want `^LS201: slices\.CompactFunc writes to field Items of immutable type Batch$`
	sort.Ints(b.Items)                                                     // want `^LS201: sort\.Ints writes to field Items of immutable type Batch$`
	sort.Slice(b.Items, func(i, j int) bool { return i < j })              // want `^LS201: sort\.Slice writes to field Items of immutable type Batch$`
	sort.SliceStable(b.Items, func(i, j int) bool { return i < j })        // want `^LS201: sort\.SliceStable writes to field Items of immutable type Batch$`
	maps.DeleteFunc(b.Tags, func(string, int) bool { return true })        // want `^LS201: maps\.DeleteFunc writes to field Tags of immutable type Batch$`
	slices.Sort[[]int](b.Items)                                            // want `^LS201: slices\.Sort writes to field Items of immutable type Batch$`
	slices.Sort((b.Items))                                                 // want `^LS201: slices\.Sort writes to field Items of immutable type Batch$`
}

// Ledger is immutable; its fields reach the shared memory in other ways.
//
//lockstep:immutable
type Ledger struct {
	Rows   []Row
	Grid   [2]int
	Meta   Meta
	Names  Names
	Floats []float64
}

// Row and Meta are value structs.
type (
	Row  struct{ N int }
	Meta struct{ N int }
)

// Names is a named slice type.
type Names []string

// ledger writes to parts of fields: an element's field, a sub-slice, an array element and a nested
// field through a pointer.
func ledger(l Ledger, pl *Ledger, src []Row) {
	l.Rows[0].N = 1   // want `^LS201: element assignment writes to field Rows of immutable type Ledger$`
	p := &l.Rows[0].N // want `^LS201: taking the address of an element writes to field Rows of immutable type Ledger$`
	_ = p
	l.Names[0] = "x"               // want `^LS201: element assignment writes to field Names of immutable type Ledger$`
	sort.Strings(l.Names)          // want `^LS201: sort\.Strings writes to field Names of immutable type Ledger$`
	sort.Float64s(l.Floats)        // want `^LS201: sort\.Float64s writes to field Floats of immutable type Ledger$`
	copy(l.Rows[1:], src)          // want `^LS201: copy writes to field Rows of immutable type Ledger$`
	_ = append(l.Rows[:0], src...) // want `^LS201: append writes to field Rows of immutable type Ledger$`
	pl.Grid[0] = 1                 // want `^LS201: assignment through a pointer writes to field Grid of immutable type Ledger$`
	pl.Meta.N = 2                  // want `^LS201: assignment through a pointer writes to field Meta of immutable type Ledger$`
}

// Tagged embeds a Batch, so Items is a field of Tagged too.
//
//lockstep:immutable
type Tagged struct {
	Batch
}

// promoted writes to a promoted field.
func promoted(t Tagged, pt *Tagged) {
	t.Items[0] = 1 // want `^LS201: element assignment writes to field Items of immutable type Tagged$`
	pt.N = 1       // want `^LS201: assignment through a pointer writes to field N of immutable type Tagged$`
}

// Frozen is a generic immutable type.
//
//lockstep:immutable
type Frozen[T any] struct {
	Values []T // want `^LS201: field Values of immutable type Frozen\[T any\] contains type parameter T, which is not immutable$`
}

// frozen writes to an instance of Frozen.
func frozen(f Frozen[int]) {
	f.Values[0] = 1 // want `^LS201: element assignment writes to field Values of immutable type Frozen\[int\]$`
}

// Add appends to the receiver's field; a method of the type itself is not exempt.
func (f *Frozen[T]) Add(v T) {
	f.Values = append(f.Values, v) // want `^LS201: assignment through a pointer writes to field Values of immutable type Frozen\[T\]$` `^LS201: append writes to field Values of immutable type Frozen\[T\]$`
}

// literal writes inside a function literal.
var literal = func(b Batch) {
	b.Tags["y"] = 2 // want `^LS201: element assignment writes to field Tags of immutable type Batch$`
}
