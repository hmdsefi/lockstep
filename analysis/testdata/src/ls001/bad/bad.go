// Package bad holds malformed directives, each with the diagnostic LS001 expects.
package bad

type B struct{}

/* want `^LS001: missing directive name after //lockstep:$` */ //lockstep:

/* want `^LS001: //lockstep:allow needs a check ID and a reason: //lockstep:allow LSnnn <reason>$` */ //lockstep:allow

/* want `^LS001: malformed check ID list "LS10"; use LSnnn or LSnnn,LSnnn$` */ //lockstep:allow LS10 x

/* want `^LS001: malformed check ID list "LS100;LS110"; use LSnnn or LSnnn,LSnnn$` */ //lockstep:allow LS100;LS110 x

/* want `^LS001: malformed check ID list "LS100,"; use LSnnn or LSnnn,LSnnn$` */ //lockstep:allow LS100, x

/* want `^LS001: //lockstep:allow LS100 needs a reason$` */ //lockstep:allow LS100

/* want `^LS001: //lockstep:allow LS100,LS110 needs a reason$` */ //lockstep:allow	LS100,LS110

// NoArgs takes no argument text.
//
/* want `^LS001: //lockstep:immutable takes no arguments$` */ //lockstep:immutable now
type NoArgs struct{}

// Fn is a function: markers belong on types.
//
/* want `^LS001: //lockstep:resource must be in the doc comment of a type declaration$` */ //lockstep:resource
func Fn() {
	return
}

/* want `^LS001: //lockstep:immutable must be in the doc comment of a type declaration$` */ //lockstep:immutable
var V int

// Group has two specs, so the marker on the declaration marks neither.
//
/* want `^LS001: //lockstep:resource must be in the doc comment of a type declaration$` */ //lockstep:resource
type (
	G1 struct{}
	G2 struct{}
)

// Trailing is not a doc comment.
type Trailing struct{} /* want `^LS001: //lockstep:immutable must be in the doc comment of a type declaration$` */ //lockstep:immutable

func body() {
	/* want `^LS001: //lockstep:resource must be in the doc comment of a type declaration$` */ //lockstep:resource
	_ = 1
}

/* want `^LS001: //lockstep:immutable cannot mark alias A$` */ //lockstep:immutable
type A = B

/* want `^LS001: //lockstep:immutable cannot mark interface type I$` */ //lockstep:immutable
type I interface{ M() }

/* want `^LS001: //lockstep:resource takes no arguments$` `^LS001: //lockstep:resource cannot mark alias RA$` */ //lockstep:resource x
type RA = B
