package bad

import (
	"maps"
	"net/http"
	"reflect"
	"slices"
	"sync"
)

// sum ranges over a map field (maprange).
func (a *Account) sum() int {
	n := 0
	for k := range a.m { // want `LS100: range over map\[string\]int has random order; range over slices\.Sorted\(maps\.Keys\(m\)\) \(actor code: \(\*Account\)\.Receive -> \(\*Account\)\.sum\)`
		n += a.m[k]
	}
	return n
}

// counts is a named map type.
type counts map[string]int

// mapLike allows a map type; constraints that embed it allow one too.
type mapLike interface{ ~map[string]int }

// rangeKeys ranges over a type parameter whose constraint allows a map directly (maprange).
func rangeKeys[K interface{ ~map[string]int }](m K) {
	for k := range m { // want `^LS100: range over K has random order; range over slices\.Sorted\(maps\.Keys\(m\)\) \(actor code: \(\*Account\)\.Receive -> orders -> rangeKeys\)$`
		_ = k
	}
}

// rangeUnion ranges over a type parameter whose constraint allows a map as a union term.
func rangeUnion[M interface{ map[string]int | counts }](m M) {
	for k := range m { // want `^LS100: range over M has random order; range over slices\.Sorted\(maps\.Keys\(m\)\) \(actor code: \(\*Account\)\.Receive -> orders -> rangeUnion\)$`
		_ = k
	}
}

// rangeEmbedded ranges over a type parameter whose constraint allows a map through an embedded
// interface.
func rangeEmbedded[M interface{ mapLike }](m M) {
	for k := range m { // want `^LS100: range over M has random order; range over slices\.Sorted\(maps\.Keys\(m\)\) \(actor code: \(\*Account\)\.Receive -> orders -> rangeEmbedded\)$`
		_ = k
	}
}

// orders holds the map order findings other than sum's (maprange, unordered, mapiter). A type in
// the message is qualified by its package's name (ANL-110).
func orders(m map[string]int, c counts) {
	for k := range c { // want `^LS100: range over counts has random order; range over slices\.Sorted\(maps\.Keys\(m\)\) \(actor code: \(\*Account\)\.Receive -> orders\)$`
		_ = k
	}
	var h http.Header
	for k := range h { // want `^LS100: range over http\.Header has random order; range over slices\.Sorted\(maps\.Keys\(m\)\) \(actor code: \(\*Account\)\.Receive -> orders\)$`
		_ = k
	}
	rangeKeys(m)
	rangeUnion(m)
	rangeEmbedded(c)

	_ = slices.Collect(maps.Keys(m)) // want `^LS100: maps\.Keys yields in random order; wrap it in slices\.Sorted \(actor code: \(\*Account\)\.Receive -> orders\)$`
	for v := range maps.Values(m) {  // want `^LS100: maps\.Values yields in random order; wrap it in slices\.Sorted \(actor code: \(\*Account\)\.Receive -> orders\)$`
		_ = v
	}
	all := maps.All[map[string]int] // want `^LS100: maps\.All yields in random order; wrap it in slices\.Sorted \(actor code: \(\*Account\)\.Receive -> orders\)$`
	_ = all

	v := reflect.ValueOf(m)
	_ = v.MapKeys()  // want `^LS100: \(reflect\.Value\)\.MapKeys iterates in random order; sort the keys first \(actor code: \(\*Account\)\.Receive -> orders\)$`
	_ = v.MapRange() // want `^LS100: \(reflect\.Value\)\.MapRange iterates in random order; sort the keys first \(actor code: \(\*Account\)\.Receive -> orders\)$`
	var sm sync.Map
	sm.Range(func(k, v any) bool { return true })                    // want `^LS100: \(\*sync\.Map\)\.Range iterates in random order; sort the keys first \(actor code: \(\*Account\)\.Receive -> orders\)$`
	maps.DeleteFunc(m, func(k string, v int) bool { return v == 0 }) // want `^LS100: maps\.DeleteFunc iterates in random order; sort the keys first \(actor code: \(\*Account\)\.Receive -> orders\)$`
}
