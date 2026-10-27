// Package importer uses the types of package marked. A probe reports "marked <marker> <T>" at
// every use of a marked type, reading the markers of imported types from their facts.
package importer

import "facts/marked"

var (
	_ marked.Batch       // want `^marked immutable marked\.Batch$`
	_ *marked.Conn       // want `^marked resource marked\.Conn$`
	_ marked.Both        // want `^marked immutable marked\.Both$` `^marked resource marked\.Both$`
	_ marked.Frozen[int] // want `^marked immutable marked\.Frozen$`
	_ marked.Plain
	_ marked.Wrapper
)

// Local is marked in this package; the probe sees it through the same lookup.
//
//lockstep:resource
type Local struct{}

var _ Local // want `^marked resource Local$`
