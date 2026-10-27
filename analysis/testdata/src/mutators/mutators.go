// Package mutators calls the in-place mutators and the writing builtins, and functions that look
// like them. A probe reports "mutator <op>" and "builtin <name>" at each call it recognizes.
package mutators

import (
	"maps"
	"slices"
	"sort"

	other "mutators/sort"
)

type list []int

func (list) Sort() {}

func calls(s []int, m, n map[string]int, l list) {
	slices.Sort(s)                                                   // want `^mutator slices\.Sort$`
	slices.SortFunc(s, func(a, b int) int { return a - b })          // want `^mutator slices\.SortFunc$`
	slices.SortStableFunc(s, func(a, b int) int { return a - b })    // want `^mutator slices\.SortStableFunc$`
	slices.Reverse(s)                                                // want `^mutator slices\.Reverse$`
	s = slices.Delete(s, 0, 1)                                       // want `^mutator slices\.Delete$`
	s = slices.DeleteFunc(s, func(int) bool { return false })        // want `^mutator slices\.DeleteFunc$`
	s = slices.Insert(s, 0, 1)                                       // want `^mutator slices\.Insert$`
	s = slices.Replace(s, 0, 1, 2)                                   // want `^mutator slices\.Replace$`
	s = slices.Compact(s)                                            // want `^mutator slices\.Compact$`
	s = slices.CompactFunc(s, func(a, b int) bool { return a == b }) // want `^mutator slices\.CompactFunc$`
	_ = slices.Sort[[]int]                                           // nothing: not a call
	sort.Ints(s)                                                     // want `^mutator sort\.Ints$`
	sort.Strings(nil)                                                // want `^mutator sort\.Strings$`
	sort.Float64s(nil)                                               // want `^mutator sort\.Float64s$`
	sort.Slice(s, func(i, j int) bool { return false })              // want `^mutator sort\.Slice$`
	sort.SliceStable(s, func(i, j int) bool { return false })        // want `^mutator sort\.SliceStable$`
	maps.Copy(m, n)                                                  // want `^mutator maps\.Copy$`
	maps.DeleteFunc(m, func(string, int) bool { return false })      // want `^mutator maps\.DeleteFunc$`
	(slices.Sort[[]int])(s)                                          // want `^mutator slices\.Sort$`

	delete(m, "a")              // want `^builtin delete$`
	clear(s)                    // want `^builtin clear$`
	_ = copy(s, s)              // want `^builtin copy$`
	s = append(s, 1)            // want `^builtin append$`
	_ = len(s)                  // nothing: not a write
	_ = slices.Clone(s)         // nothing: not in the table
	_ = maps.Clone(m)           // nothing: not in the table
	sort.Sort(sort.IntSlice(s)) // nothing: not in the table
	other.Ints(s)               // nothing: another package named sort
	l.Sort()                    // nothing: a method
}

func shadowed(s []int) {
	copy := func(dst, src []int) int { return 0 }
	_ = copy(s, s) // nothing: not the builtin
}
