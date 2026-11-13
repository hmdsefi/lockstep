// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package codec

import (
	"math"
	"reflect"
	"time"

	"github.com/hmdsefi/lockstep"
)

// builtin is one built-in type and its wire name.
type builtin struct {
	name string
	typ  reflect.Type
}

// builtins are the types every registry starts with (COD-040), in the spec's table order. Their
// names carry the reserved prefix, so RegisterIn can never add or replace one.
//
// float32 and float64 are read from the parameter of math.Float32bits and math.Float64bits: codec
// is a core package, where the determinism lint (DL007) rejects any expression of a float type,
// a type argument included, and this table only names the types without computing with them.
var builtins = []builtin{
	{"lockstep.bool", reflect.TypeFor[bool]()},
	{"lockstep.string", reflect.TypeFor[string]()},
	{"lockstep.int", reflect.TypeFor[int]()},
	{"lockstep.int8", reflect.TypeFor[int8]()},
	{"lockstep.int16", reflect.TypeFor[int16]()},
	{"lockstep.int32", reflect.TypeFor[int32]()},
	{"lockstep.int64", reflect.TypeFor[int64]()},
	{"lockstep.uint", reflect.TypeFor[uint]()},
	{"lockstep.uint8", reflect.TypeFor[uint8]()},
	{"lockstep.uint16", reflect.TypeFor[uint16]()},
	{"lockstep.uint32", reflect.TypeFor[uint32]()},
	{"lockstep.uint64", reflect.TypeFor[uint64]()},
	{"lockstep.float32", reflect.TypeOf(math.Float32bits).In(0)}, // float32, see above
	{"lockstep.float64", reflect.TypeOf(math.Float64bits).In(0)}, // float64, see above
	{"lockstep.bytes", reflect.TypeFor[[]byte]()},
	{"lockstep.empty", reflect.TypeFor[struct{}]()},
	{"lockstep.duration", reflect.TypeFor[time.Duration]()},
	{"lockstep.time", reflect.TypeFor[time.Time]()},
	{"lockstep.address", reflect.TypeFor[lockstep.Address]()},
}
