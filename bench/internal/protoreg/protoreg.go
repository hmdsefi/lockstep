// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package protoreg lets one binary link both hollywood and protoactor-go. Both register the
// protobuf file actor.proto, with the message actor.PID, in the protobuf runtime's global
// registry, and the runtime panics on such a conflict while the binary initializes unless the
// environment variable GOLANG_PROTOBUF_REGISTRATION_CONFLICT says otherwise. Importing this
// package sets that variable to "ignore" when it is unset or empty, as the runtime treats an empty
// value as unset. Neither library uses the registry for local messages.
//
// The runtime reads the variable when the conflict happens, so this package must be initialized
// before any file is registered. Go initializes, at each step, the package with the smallest
// import path among those whose imports are all initialized. This package imports only os. Every
// file registration goes through google.golang.org/protobuf/reflect/protoregistry, which imports
// os too and has a larger import path, so this package is initialized before protoregistry and
// therefore before any package that registers a file. The order depends on this package's path
// sorting before google.golang.org. Package libs imports it.
package protoreg

import "os"

// conflictPolicy names the environment variable the protobuf runtime reads on a registration
// conflict.
const conflictPolicy = "GOLANG_PROTOBUF_REGISTRATION_CONFLICT"

var _ = setIfUnset(conflictPolicy, "ignore")

// setIfUnset sets the environment variable key to value if key is unset or empty, and reports
// whether it set it. It panics if it cannot set the variable.
func setIfUnset(key, value string) bool {
	if os.Getenv(key) != "" {
		return false
	}
	if err := os.Setenv(key, value); err != nil {
		panic("protoreg: " + err.Error())
	}
	return true
}
