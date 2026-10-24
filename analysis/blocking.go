// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package analysis

import (
	"go/types"
	"slices"
	"strings"
)

// blockingCalls holds the calls of ANL-140 that the blocking sub-rule of LS100 reports, by
// import path. Methods named Close are never blocking calls: LS240 asks for them in Stopped.
var blockingCalls = map[string]blockingPackage{
	"sync": {methods: []blockingMethods{
		{types: []string{"WaitGroup", "Cond"}, names: []string{"Wait"}},
		{types: []string{"Mutex"}, names: []string{"Lock"}},
		{types: []string{"RWMutex"}, names: []string{"Lock", "RLock"}},
	}},
	"net": {
		funcPrefixes: []string{"Dial", "Listen", "Lookup", "Resolve"},
		// Every type declared in net, the interfaces Conn, Listener and PacketConn included.
		methods: []blockingMethods{{prefixes: []string{"Read", "Write", "Accept", "Dial", "Listen", "Lookup"}}},
	},
	"net/http": {
		funcs: []string{"Get", "Head", "Post", "PostForm", "ListenAndServe", "ListenAndServeTLS", "Serve", "ServeTLS"},
		methods: []blockingMethods{
			{types: []string{"Client"}, names: []string{"Do", "Get", "Head", "Post", "PostForm"}},
			{types: []string{"Server"}, names: []string{"ListenAndServe", "ListenAndServeTLS", "Serve", "ServeTLS", "Shutdown"}},
		},
	},
	"net/rpc": {
		funcs:   []string{"Dial", "DialHTTP", "DialHTTPPath"},
		methods: []blockingMethods{{types: []string{"Client"}, names: []string{"Call"}}},
	},
	"database/sql": {
		funcs: []string{"Open"},
		methods: []blockingMethods{
			{
				types:    []string{"DB", "Conn", "Tx", "Stmt"},
				names:    []string{"Conn"},
				prefixes: []string{"Query", "Exec", "Ping", "Begin", "Prepare"},
			},
			{types: []string{"Rows"}, names: []string{"Next"}},
			{types: []string{"Row"}, names: []string{"Scan"}},
		},
	},
	"os/exec": {methods: []blockingMethods{
		{types: []string{"Cmd"}, names: []string{"Run", "Start", "Wait", "Output", "CombinedOutput"}},
	}},
	"os": {
		funcs: []string{
			"Open", "OpenFile", "Create", "ReadFile", "WriteFile", "ReadDir", "Stat", "Lstat",
			"Mkdir", "MkdirAll", "Remove", "RemoveAll", "Rename",
		},
		// Every method of File but Close, Name and Fd; Close is left out for every type.
		methods: []blockingMethods{{types: []string{"File"}, every: true, except: []string{"Name", "Fd"}}},
	},
}

// blockingPackage lists the blocking calls of one package: package-level functions by name or
// name prefix, and methods.
type blockingPackage struct {
	funcs        []string
	funcPrefixes []string
	methods      []blockingMethods
}

// blockingMethods matches the methods of the named types (every type of the package when types
// is empty) whose name is in names or starts with one of prefixes, or every method when every is
// set, except those in except.
type blockingMethods struct {
	types    []string
	names    []string
	prefixes []string
	every    bool
	except   []string
}

// isBlocking reports whether fn is one of the blocking calls of ANL-140.
func isBlocking(fn *types.Func) bool {
	if fn.Pkg() == nil || fn.Name() == "Close" {
		return false
	}
	p, ok := blockingCalls[fn.Pkg().Path()]
	if !ok {
		return false
	}
	name := fn.Name()
	recv := fn.Signature().Recv()
	if recv == nil {
		return slices.Contains(p.funcs, name) || hasAnyPrefix(name, p.funcPrefixes)
	}
	typeName := receiverName(recv.Type())
	for _, m := range p.methods {
		if (len(m.types) == 0 || slices.Contains(m.types, typeName)) &&
			(m.every || slices.Contains(m.names, name) || hasAnyPrefix(name, m.prefixes)) &&
			!slices.Contains(m.except, name) {
			return true
		}
	}
	return false
}

// receiverName returns the name of the type of a method's receiver t, a named type or a pointer
// to one, or "" for an interface literal.
func receiverName(t types.Type) string {
	if p, ok := t.(*types.Pointer); ok {
		t = p.Elem()
	}
	if n, ok := types.Unalias(t).(*types.Named); ok {
		return n.Obj().Name()
	}
	return ""
}

// hasAnyPrefix reports whether s starts with one of prefixes.
func hasAnyPrefix(s string, prefixes []string) bool {
	return slices.ContainsFunc(prefixes, func(p string) bool { return strings.HasPrefix(s, p) })
}
