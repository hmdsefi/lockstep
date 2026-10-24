// Package good holds well-formed directives and comments that are not directives.
package good

// lockstep:allow is not a directive: a space follows the slashes.

/*lockstep:allow*/

/* lockstep:immutable */

//lockstep:allow LS100 reason here

//lockstep:allow LS100,LS110 two checks

//lockstep:allow	LS110	tabs	separate	the	fields

// Plain has an ungrouped marker in its doc comment.
//
//lockstep:immutable
type Plain struct{ N int }

// Handle is marked as a resource.
//
//lockstep:resource
type Handle struct{ fd int }

// Both carries both markers.
//
//lockstep:immutable
//lockstep:resource
type Both struct{}

// OneSpec is in a parenthesized declaration of a single spec.
//
//lockstep:immutable
type (
	OneSpec struct{}
)

type (
	// Grouped has the marker on its own spec.
	//
	//lockstep:immutable
	Grouped struct{}

	// Other is not marked.
	Other struct{}

	// Closer is an interface; only immutable is invalid on one.
	//
	//lockstep:resource
	Closer interface{ Close() error }
)

func use() {
	_ = 1 //lockstep:allow LS100 a trailing directive anywhere is well-formed
}
