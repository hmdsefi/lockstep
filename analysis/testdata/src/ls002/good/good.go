// Package good holds a directive with a known ID.
package good

//lockstep:allow LS110 r
func f() {}

// g carries the directives of lockstep's own determinism checks, maporder and blocking. They are
// not this analyzer's, so they are ignored rather than reported as unknown.
func g(m map[string]int) int {
	n := 0
	//lockstep:maporder the sum does not depend on the order
	for _, v := range m {
		n += v
	}
	//lockstep:blocking nothing sends, so this never returns
	select {}
}
