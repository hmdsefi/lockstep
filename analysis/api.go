// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package analysis

import (
	"go/types"
	"strings"
)

// lockstepPath is the import path of package lockstep. The analyzers recognize lockstep objects by
// this path only; the module does not require lockstep (ANL-010).
const lockstepPath = "github.com/hmdsefi/lockstep"

// isAPI reports whether obj is the ANL-020 entry named entry: "Send", "Ask", "Spawn", "Pipe",
// "Do", "Actor", "Context", "Reply", "ReplyTo", "PID", "Address", or a method entry
// "Context.Own", "Context.After", "Context.Every", "Reply.Await", "ReplyTo.Send".
// Generic objects match through their origin; methods match on the receiver's origin type name.
func isAPI(obj types.Object, entry string) bool {
	if obj == nil || obj.Pkg() == nil || obj.Pkg().Path() != lockstepPath {
		return false
	}
	typeName, methodName, isMethod := strings.Cut(entry, ".")
	switch obj := obj.(type) {
	case *types.Func:
		fn := obj.Origin()
		recv := fn.Signature().Recv()
		if recv == nil {
			return !isMethod && isPackageLevel(fn, entry)
		}
		return isMethod && fn.Name() == methodName && receiverTypeName(recv.Type()) == typeName
	case *types.TypeName:
		return !isMethod && isPackageLevel(obj, entry)
	default:
		return false
	}
}

// isPackageLevel reports whether obj is the object declared as name at the scope of its package.
func isPackageLevel(obj types.Object, name string) bool {
	return obj.Name() == name && obj.Pkg().Scope().Lookup(name) == obj
}

// receiverTypeName returns the name of the origin of a method receiver's named type, after
// removing one pointer, or "" if the receiver has no named type.
func receiverTypeName(t types.Type) string {
	if p, ok := types.Unalias(t).(*types.Pointer); ok {
		t = p.Elem()
	}
	named, ok := types.Unalias(t).(*types.Named)
	if !ok {
		return ""
	}
	return named.Origin().Obj().Name()
}
