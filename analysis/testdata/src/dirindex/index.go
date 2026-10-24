// Package dirindex checks the allow scopes and the marker bindings of the directive index. A probe
// analyzer reports "suppressed" or "visible" at every use of probe, according to whether a
// directive suppresses LS100 there, and "marker <names>" at every marked type.
package dirindex

const probe = 1

func scopes() {
	_ = probe /* want `^suppressed$` */ //lockstep:allow LS100 trailing comment

	//lockstep:allow LS100 own line above
	_ = probe // want `^suppressed$`

	// Several lines of text, the directive last.
	//lockstep:allow LS100 last line of the group
	_ = probe // want `^suppressed$`

	//lockstep:allow LS100 not the last line of its group
	// more text
	_ = probe // want `^visible$`

	_ = []int{
		//lockstep:allow LS100 not the last comment of its group, though the code is on the next line
		/* x */ probe, // want `^visible$`
	}

	//lockstep:allow LS100 blank line between

	_ = probe // want `^visible$`

	//lockstep:allow LS110 other check
	_ = probe // want `^visible$`

	//lockstep:maporder the names of the determinism checks suppress nothing here
	_ = probe // want `^visible$`

	//lockstep:allow LS110,LS100 two checks
	_ = probe // want `^suppressed$`

	_ = 0     //lockstep:allow LS100 a trailing directive covers its own line only
	_ = probe // want `^visible$`

	//lockstep:allow LS100
	_ = probe // want `^visible$`

	//lockstep:allow LS100;LS110 r
	_ = probe // want `^visible$`

	//lockstep:allow LS100,LS999 an unknown ID makes the directive ineffective
	_ = probe // want `^visible$`

	//lockstep:allow LS001 cannot be suppressed
	_ = probe // want `^visible$`

	_ = probe /* want `^suppressed$` */ //lockstep:allow LS100 trailing, so the next line stays visible
	_ = probe // want `^visible$`

	/* a block comment first */ //lockstep:allow LS100 text precedes the directive
	_ = probe                   // want `^visible$`
}

// documented has the directive in its doc comment: the whole declaration is covered.
//
//lockstep:allow LS100 whole function
func documented() {
	_ = probe // want `^suppressed$`
	func() {
		_ = probe // want `^suppressed$`
	}()
}

func undocumented() {
	_ = probe // want `^visible$`
}

// helper is called by a function with a directive; the directive stays with that function.
//
//lockstep:allow LS100 the function itself
func caller() {
	helper()
}

func helper() {
	_ = probe // want `^visible$`
}

type fields struct {
	// a has the directive in its doc comment.
	//
	//lockstep:allow LS100 field
	a [probe]int // want `^suppressed$`
	b [probe]int // want `^visible$`
	c [probe]int /* want `^suppressed$` */ //lockstep:allow LS100 trailing comment of the field
	d [probe]int // want `^visible$`
}

//lockstep:allow LS100 whole declaration
var (
	v1 = probe // want `^suppressed$`
	v2 = probe // want `^suppressed$`
)

var (
	// v3 has its own directive.
	//
	//lockstep:allow LS100 value spec
	v3 = probe // want `^suppressed$`
	v4 = probe // want `^visible$`
)

// Sized has the directive on its declaration.
//
//lockstep:allow LS100 type declaration
type Sized [probe]int // want `^suppressed$`

type (
	// Wide has the directive on its type spec.
	//
	//lockstep:allow LS100 type spec
	Wide [probe]int // want `^suppressed$`

	Narrow [probe]int // want `^visible$`
)

// Marked is an ungrouped type with a marker.
//
//lockstep:immutable
type Marked struct{} // want `^marker immutable$`

// Both has both markers.
//
//lockstep:resource
//lockstep:immutable
type Both struct{} // want `^marker immutable resource$`

type (
	// Spec is marked through its spec.
	//
	//lockstep:resource
	Spec struct{} // want `^marker resource$`

	Unmarked struct{}

	// Closer is an interface; resource is valid on it.
	//
	//lockstep:resource
	Closer interface{ Close() } // want `^marker resource$`

	// Face is an interface; immutable is invalid on it, so nothing is bound.
	//
	//lockstep:immutable
	Face interface{ M() }

	// Alias is an alias; nothing is bound to it or to Marked.
	//
	//lockstep:immutable
	Alias = Marked

	// Args has an argument, so nothing is bound.
	//
	//lockstep:resource now
	Args struct{}
)

// Pair has two specs, so the marker on the declaration marks neither.
//
//lockstep:immutable
type (
	P1 struct{}
	P2 struct{}
)
