// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package dettest

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/types"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// APIPackages are the packages snapshotted in api/lockstep.txt, relative to the module path.
var APIPackages = []string{"", "sim", "persist", "persist/filestore", "codec", "remote", "cluster"}

// API returns the snapshot text of DET-110 for pkgs (relative to m.Path): the lines of every
// exported object, deduplicated and sorted bytewise, each ending in "\n". A missing directory, or
// one without non-test files for the host, contributes no lines.
func API(m Module, pkgs []string) (string, error) {
	l := newLinter()
	var lines []string
	for _, rel := range pkgs {
		dir := filepath.Join(m.Root, filepath.FromSlash(rel))
		entries, err := os.ReadDir(dir)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return "", err
		}
		var files []*ast.File
		for _, e := range entries {
			name := e.Name()
			if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}
			if ok, err := hostContext().MatchFile(dir, name); err != nil {
				return "", err
			} else if !ok {
				continue
			}
			f, err := parser.ParseFile(l.fset, filepath.Join(dir, name), nil, 0)
			if err != nil {
				return "", err
			}
			files = append(files, f)
		}
		if len(files) == 0 {
			continue
		}
		path := importPath(m, rel)
		pkg, err := (&types.Config{Importer: l.imp}).Check(path, l.fset, files, nil)
		if err != nil {
			return "", fmt.Errorf("dettest: type-checking %s: %w", path, err)
		}
		lines = append(lines, apiLines(pkg)...)
	}
	slices.Sort(lines)
	lines = slices.Compact(lines)
	var b strings.Builder
	for _, line := range lines {
		b.WriteString(line + "\n")
	}
	return b.String(), nil
}

// apiLines returns the lines of DET-110 for the exported objects of pkg.
func apiLines(pkg *types.Package) []string {
	qual := func(p *types.Package) string {
		if p == pkg {
			return ""
		}
		return p.Name()
	}
	T := func(t types.Type) string { return types.TypeString(stripNames(t), qual) }
	sig := func(s *types.Signature) string {
		var b strings.Builder
		b.WriteByte('(')
		for i := 0; i < s.Params().Len(); i++ {
			if i > 0 {
				b.WriteString(", ")
			}
			t := s.Params().At(i).Type()
			if s.Variadic() && i == s.Params().Len()-1 {
				b.WriteString("..." + T(t.(*types.Slice).Elem())) //nolint:errcheck // go/types: a variadic parameter is always a slice
			} else {
				b.WriteString(T(t))
			}
		}
		b.WriteByte(')')
		switch r := s.Results(); r.Len() {
		case 0:
		case 1:
			b.WriteString(" " + T(r.At(0).Type()))
		default:
			parts := make([]string, r.Len())
			for i := range parts {
				parts[i] = T(r.At(i).Type())
			}
			b.WriteString(" (" + strings.Join(parts, ", ") + ")")
		}
		return b.String()
	}
	// tparams returns "[T any, U comparable]" for a type parameter list, or "" if it is empty.
	tparams := func(tps *types.TypeParamList) string {
		if tps.Len() == 0 {
			return ""
		}
		parts := make([]string, tps.Len())
		for i := range parts {
			parts[i] = tps.At(i).Obj().Name() + " " + T(tps.At(i).Constraint())
		}
		return "[" + strings.Join(parts, ", ") + "]"
	}
	// recvNames returns "[A, B]" for the receiver type parameters of a method of a generic type,
	// or "" if it has none.
	recvNames := func(tps *types.TypeParamList) string {
		if tps.Len() == 0 {
			return ""
		}
		parts := make([]string, tps.Len())
		for i := range parts {
			parts[i] = tps.At(i).Obj().Name()
		}
		return "[" + strings.Join(parts, ", ") + "]"
	}
	prefix := "pkg " + pkg.Path() + ", "
	var out []string
	scope := pkg.Scope()
	for _, name := range scope.Names() {
		obj := scope.Lookup(name)
		if !obj.Exported() {
			continue
		}
		switch o := obj.(type) {
		case *types.Const:
			out = append(out, prefix+"const "+name+" = "+o.Val().ExactString())
			if b, ok := o.Type().(*types.Basic); ok && b.Info()&types.IsUntyped != 0 {
				out = append(out, prefix+"const "+name+" ideal-"+strings.TrimPrefix(b.Name(), "untyped "))
			} else {
				out = append(out, prefix+"const "+name+" "+T(o.Type()))
			}
		case *types.Var:
			out = append(out, prefix+"var "+name+" "+T(o.Type()))
		case *types.Func:
			out = append(out, prefix+"func "+name+tparams(o.Signature().TypeParams())+sig(o.Signature()))
		case *types.TypeName:
			if o.IsAlias() {
				tname := name
				if a, ok := o.Type().(*types.Alias); ok {
					tname += tparams(a.TypeParams())
				}
				out = append(out, prefix+"type "+tname+" = "+T(types.Unalias(o.Type())))
				continue
			}
			named := o.Type().(*types.Named) //nolint:errcheck // go/types: a non-alias type name in package scope is always Named
			tname := name + tparams(named.TypeParams())
			switch u := named.Underlying().(type) {
			case *types.Struct:
				out = append(out, prefix+"type "+tname+" struct")
				for i := 0; i < u.NumFields(); i++ {
					f := u.Field(i)
					if !f.Exported() {
						continue
					}
					if f.Embedded() {
						out = append(out, prefix+"type "+tname+" struct, embedded "+T(f.Type()))
					} else {
						out = append(out, prefix+"type "+tname+" struct, "+f.Name()+" "+T(f.Type()))
					}
				}
			case *types.Interface:
				if !u.IsMethodSet() {
					// A constraint interface with type terms or comparable: one line, no method lines.
					out = append(out, prefix+"type "+tname+" "+T(u))
					break
				}
				var names []string
				unexported := false
				for i := 0; i < u.NumMethods(); i++ {
					m := u.Method(i)
					if !m.Exported() {
						unexported = true
						continue
					}
					names = append(names, m.Name())
					out = append(out, prefix+"type "+tname+" interface, "+m.Name()+sig(m.Signature()))
				}
				if unexported {
					names = append(names, "unexported methods")
				}
				slices.Sort(names)
				if len(names) == 0 {
					out = append(out, prefix+"type "+tname+" interface {}")
				} else {
					out = append(out, prefix+"type "+tname+" interface { "+strings.Join(names, ", ")+" }")
				}
			default:
				out = append(out, prefix+"type "+tname+" "+T(u))
			}
			for i := 0; i < named.NumMethods(); i++ {
				m := named.Method(i)
				if !m.Exported() {
					continue
				}
				s := m.Signature()
				recv := "(" + name + recvNames(s.RecvTypeParams()) + ")"
				if _, ok := s.Recv().Type().(*types.Pointer); ok {
					recv = "(*" + name + recvNames(s.RecvTypeParams()) + ")"
				}
				out = append(out, prefix+"method "+recv+" "+m.Name()+sig(s))
			}
		}
	}
	return out
}

// stripNames returns t with the parameter and result names removed from every signature reachable
// through pointers, slices, arrays, maps, channels and signatures, so that names are never
// printed (DET-025).
func stripNames(t types.Type) types.Type {
	switch t := t.(type) {
	case *types.Signature:
		return types.NewSignatureType(nil, nil, nil, stripTuple(t.Params()), stripTuple(t.Results()), t.Variadic())
	case *types.Pointer:
		return types.NewPointer(stripNames(t.Elem()))
	case *types.Slice:
		return types.NewSlice(stripNames(t.Elem()))
	case *types.Array:
		return types.NewArray(stripNames(t.Elem()), t.Len())
	case *types.Map:
		return types.NewMap(stripNames(t.Key()), stripNames(t.Elem()))
	case *types.Chan:
		return types.NewChan(t.Dir(), stripNames(t.Elem()))
	}
	return t
}

func stripTuple(tu *types.Tuple) *types.Tuple {
	vars := make([]*types.Var, tu.Len())
	for i := range vars {
		vars[i] = types.NewParam(tu.At(i).Pos(), tu.At(i).Pkg(), "", stripNames(tu.At(i).Type()))
	}
	return types.NewTuple(vars...)
}
