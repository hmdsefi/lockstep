// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package protoreg lets one binary link both hollywood and protoactor-go. Both register the
// protobuf file actor.proto, with the message actor.PID, in the protobuf runtime's global
// registry, and the runtime panics on such a conflict while the binary initializes unless the
// environment variable GOLANG_PROTOBUF_REGISTRATION_CONFLICT says otherwise. Importing this
// package sets that variable to "ignore" when it is unset. Neither library uses the registry for
// local messages.
//
// The runtime reads the variable when the conflict happens. This package imports only os, so Go
// initializes it before any protobuf package and hence before either library registers its file.
// Package libs imports it.
package protoreg

import "os"

// conflictPolicy names the environment variable the protobuf runtime reads on a registration
// conflict.
const conflictPolicy = "GOLANG_PROTOBUF_REGISTRATION_CONFLICT"

var _ = setIfUnset(conflictPolicy, "ignore")

// setIfUnset sets the environment variable key to value unless key is set, and reports whether
// it set it. It panics if it cannot set the variable.
func setIfUnset(key, value string) bool {
	if _, ok := os.LookupEnv(key); ok {
		return false
	}
	if err := os.Setenv(key, value); err != nil {
		panic("protoreg: " + err.Error())
	}
	return true
}
