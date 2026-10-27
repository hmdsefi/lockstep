// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package analysis

import (
	"go/types"

	"golang.org/x/tools/go/analysis"
)

// containsRefs reports whether a value of type t shares memory when it is copied (ANL-210). It is
// false for string, time.Time, lockstep.PID, lockstep.Address, lockstep.ReplyTo, types marked
// //lockstep:immutable and pointers to them. Otherwise it is true when t's underlying type is a
// pointer, slice, map, channel, function or unsafe.Pointer, an array whose element type contains
// references or a struct with a field whose type does; false for other types, interfaces and type
// parameters included. Markers come from the directive index of pass, so the analyzer calling it
// requires indexAnalyzer; without the index no type counts as marked.
func containsRefs(pass *analysis.Pass, t types.Type) bool {
	idx, _ := indexOf(pass)
	w := refsWalk{idx: idx, seen: make(map[*types.Named]bool)}
	return w.refs(t)
}

// refsWalk is the state of one containsRefs call.
type refsWalk struct {
	idx  *directiveIndex
	seen map[*types.Named]bool // named types already visited, so recursive types terminate
}

func (w *refsWalk) refs(t types.Type) bool {
	t = types.Unalias(t)
	if w.safe(t) {
		return false
	}
	if p, ok := t.(*types.Pointer); ok && w.idx.typeHasMarker(p.Elem(), markImmutable) {
		return false
	}
	if named, ok := t.(*types.Named); ok {
		// A named type visited before is either being visited (a cycle) or contains no
		// references: a true result would have ended the walk.
		if w.seen[named] {
			return false
		}
		w.seen[named] = true
	}
	switch u := t.Underlying().(type) {
	case *types.Basic:
		return u.Kind() == types.UnsafePointer
	case *types.Pointer, *types.Slice, *types.Map, *types.Chan, *types.Signature:
		return true
	case *types.Array:
		return w.refs(u.Elem())
	case *types.Struct:
		for f := range u.Fields() {
			if w.refs(f.Type()) {
				return true
			}
		}
	}
	return false
}

// safe reports whether t is one of the types ANL-210 names as safe: string, time.Time,
// lockstep.PID, lockstep.Address, lockstep.ReplyTo or an immutable type.
func (w *refsWalk) safe(t types.Type) bool {
	if b, ok := t.(*types.Basic); ok {
		return b.Info()&types.IsString != 0
	}
	named, ok := t.(*types.Named)
	if !ok {
		return false
	}
	obj := named.Obj()
	if obj.Pkg() != nil && obj.Pkg().Path() == "time" && obj.Name() == "Time" {
		return true
	}
	return isAPI(obj, "PID") || isAPI(obj, "Address") || isAPI(obj, "ReplyTo") ||
		w.idx.hasMarker(obj, markImmutable)
}
