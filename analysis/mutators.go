// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package analysis

import (
	"go/ast"
	"go/types"
	"slices"

	"golang.org/x/tools/go/types/typeutil"
)

// inPlaceMutators are the library functions that write into the memory of their first argument
// (ANL-260), by import path, in the table's order.
var inPlaceMutators = []struct {
	path  string
	names []string
}{
	{"slices", []string{"Sort", "SortFunc", "SortStableFunc", "Reverse", "Delete", "DeleteFunc", "Insert", "Replace", "Compact", "CompactFunc"}},
	{"sort", []string{"Ints", "Strings", "Float64s", "Slice", "SliceStable"}},
	{"maps", []string{"Copy", "DeleteFunc"}},
}

// builtinWrites are the builtins that write into their first argument (ANL-260, ANL-270).
var builtinWrites = []string{"delete", "clear", "copy", "append"}

// inPlaceMutator reports whether call calls one of the in-place mutators of ANL-260, such as
// slices.Sort or maps.Copy, and returns its name spelled <pkg>.<Name>: the op of LS201 and LS210
// messages. Functions match by package path and name, so a generic function matches through its
// origin and a same-named function of another package does not. The first argument of call is the
// written one. Builtins are not mutators here; see builtinWrite.
func inPlaceMutator(info *types.Info, call *ast.CallExpr) (op string, ok bool) {
	fn, isFunc := typeutil.Callee(info, call).(*types.Func)
	if !isFunc || fn.Pkg() == nil || fn.Signature().Recv() != nil {
		return "", false
	}
	for _, m := range inPlaceMutators {
		if fn.Pkg().Path() == m.path && slices.Contains(m.names, fn.Name()) {
			return m.path + "." + fn.Name(), true
		}
	}
	return "", false
}

// builtinWrite reports whether call calls one of the builtins delete, clear, copy or append, which
// write into their first argument, and returns the builtin's name: the op of LS201 and LS210
// messages. A function that shadows a builtin's name is not a builtin.
func builtinWrite(info *types.Info, call *ast.CallExpr) (name string, ok bool) {
	b, isBuiltin := typeutil.Callee(info, call).(*types.Builtin)
	if !isBuiltin || !slices.Contains(builtinWrites, b.Name()) {
		return "", false
	}
	return b.Name(), true
}
