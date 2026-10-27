// Package bad holds code that LS201 reports: fields of immutable types that are not immutable
// (ANL-250, AT-ANL-15) and writes to the fields of immutable types (ANL-260, AT-ANL-16).
package bad

import (
	"io"
	"time"
	"unsafe"
)

// Item is not marked.
type Item struct{ N int }

// fieldKinds declares the Batch of AT-ANL-15. It is local to the function so that it does not
// clash with the package-level Batch of the write checks.
func fieldKinds() {
	// Batch holds pointers to an unmarked type.
	//
	//lockstep:immutable
	type Batch struct {
		Items []*Item // want `^LS201: field Items of immutable type Batch contains pointer type \*Item, which is not immutable$`
	}
	_ = Batch{}
}

// WithChan holds a channel.
//
//lockstep:immutable
type WithChan struct {
	C chan int // want `^LS201: field C of immutable type WithChan contains channel type chan int, which is not immutable$`
}

// WithFunc holds a function.
//
//lockstep:immutable
type WithFunc struct {
	F func() // want `^LS201: field F of immutable type WithFunc contains function type func\(\), which is not immutable$`
}

// WithAny holds an interface.
//
//lockstep:immutable
type WithAny struct {
	V any // want `^LS201: field V of immutable type WithAny contains interface type any, which is not immutable$`
}

// Box holds a value of its type parameter.
//
//lockstep:immutable
type Box[T any] struct {
	V T // want `^LS201: field V of immutable type Box\[T any\] contains type parameter T, which is not immutable$`
}

// Deep holds a pointer inside an unnamed struct.
//
//lockstep:immutable
type Deep struct {
	Inner struct{ P *int } // want `^LS201: field Inner of immutable type Deep contains pointer type \*int, which is not immutable$`
}

// IDs is not a struct: it is reported at its type spec.
//
//lockstep:immutable
type IDs []*Item // want `^LS201: immutable type IDs contains pointer type \*Item, which is not immutable$`

// Callback is a named function type.
type Callback func()

// holder is unmarked and holds a pointer.
type holder struct {
	n  int
	at *time.Location
}

// Mixed holds the other kinds: unsafe.Pointer, a named interface of another package, a named
// function type, a pointer reached through an unmarked named type, a pointer in a map key, a
// pointer to a pointer to a marked type, one field with two names, and a struct whose first bad
// field is the one reported.
//
//lockstep:immutable
type Mixed struct {
	U     unsafe.Pointer // want `^LS201: field U of immutable type Mixed contains unsafe\.Pointer, which is not immutable$`
	R     io.Reader      // want `^LS201: field R of immutable type Mixed contains interface type io\.Reader, which is not immutable$`
	CB    Callback       // want `^LS201: field CB of immutable type Mixed contains function type Callback, which is not immutable$`
	H     holder         // want `^LS201: field H of immutable type Mixed contains pointer type \*time\.Location, which is not immutable$`
	K     map[*Item]int  // want `^LS201: field K of immutable type Mixed contains pointer type \*Item, which is not immutable$`
	PP    **WithChan     // want `^LS201: field PP of immutable type Mixed contains pointer type \*\*WithChan, which is not immutable$`
	A, B  []chan<- int   // want `^LS201: field A of immutable type Mixed contains channel type chan<- int, which is not immutable$` `^LS201: field B of immutable type Mixed contains channel type chan<- int, which is not immutable$`
	First struct {       // want `^LS201: field First of immutable type Mixed contains channel type chan int, which is not immutable$`
		C chan int
		F func()
	}
}

// Embeds embeds an unmarked pointer type.
//
//lockstep:immutable
type Embeds struct {
	*Item // want `^LS201: field Item of immutable type Embeds contains pointer type \*Item, which is not immutable$`
}

// Handler is not a struct and holds a function.
//
//lockstep:immutable
type Handler func(int) // want `^LS201: immutable type Handler contains function type func\(int\), which is not immutable$`
