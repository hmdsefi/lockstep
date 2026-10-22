package dl010

import (
	"cmp"
	"maps"
	"slices"
)

func F(m map[string]int) {
	for k := range maps.Keys(m) { // want DL010
		_ = k
	}
	_ = maps.All(m)    // want DL010
	_ = maps.Values(m) // want DL010
	_ = slices.Sorted(maps.Keys(m))
	_ = slices.SortedFunc(maps.Values(m), cmp.Compare[int])
	_ = slices.SortedStableFunc(maps.Keys(m), cmp.Compare[string])
}
