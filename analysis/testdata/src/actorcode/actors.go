// Package actorcode exercises actor types, roots, reachability and chains (ANL-030 to ANL-060).
package actorcode

import (
	"sort"

	"github.com/hmdsefi/lockstep"
)

// Msg is Account's message type.
type Msg int

// Account is an actor type with a pointer receiver and all four roots.
type Account struct {
	balance int
	log     []int
}

// Started is declared before Receive, so the search reaches shared from Started first.
func (a *Account) Started(ctx *lockstep.Context[Msg]) { // want `^actor code: \(\*Account\)\.Started$`
	shared()
}

// Receive reaches apply and audit by calls, less and flush as function values, its first
// literal, generic by an instantiated call, and viaArg as an argument of a go call. The functions
// the go statements call and the interface call are not edges.
func (a *Account) Receive(ctx *lockstep.Context[Msg], msg Msg) { // want `^actor code: \(\*Account\)\.Receive$`
	a.balance = apply(a.balance, msg)
	sort.Slice(a.log, less)
	f := a.flush
	f()
	record := func(v int) { // want `^actor code: \(\*Account\)\.Receive -> \(\*Account\)\.Receive\$1$`
		a.log = append(a.log, v)
	}
	record(a.balance)
	go func() { tick() }()
	go spin()
	go run(viaArg)
	_ = generic[int](1)
	var s Runner = impl{}
	s.Run()
	shared()
}

// Stopped refers to flush through a method expression; flush is already reached from Receive. Its
// go literal is a closure, and is still not an edge.
func (a *Account) Stopped(ctx *lockstep.Context[Msg], r lockstep.StopReason) { // want `^actor code: \(\*Account\)\.Stopped$`
	g := (*Account).flush
	defer g(a)
	go func() { a.balance = 0 }()
}

// Signal calls early, declared at the top of others.go, before late, declared at the end of this
// file: successors are searched in file-name order first, so late comes before early.
func (a *Account) Signal(ctx *lockstep.Context[Msg], s lockstep.Signal) { // want `^actor code: \(\*Account\)\.Signal$`
	early()
	late()
}

// Stopped2 is not a root name.
func (a *Account) Stopped2(ctx *lockstep.Context[Msg]) {}

func (a *Account) flush() { // want `^actor code: \(\*Account\)\.Receive -> \(\*Account\)\.flush$`
	a.log = a.log[:0]
}

// apply reaches audit through a defer.
func apply(balance int, msg Msg) int { // want `^actor code: \(\*Account\)\.Receive -> apply$`
	defer audit()
	return balance + int(msg)
}

func audit() {} // want `^actor code: \(\*Account\)\.Receive -> apply -> audit$`

func less(i, j int) bool { return i < j } // want `^actor code: \(\*Account\)\.Receive -> less$`

func generic[T any](v T) T { return v } // want `^actor code: \(\*Account\)\.Receive -> generic$`

func viaArg() {} // want `^actor code: \(\*Account\)\.Receive -> viaArg$`

// run is the called function of a go statement, so it is not actor code.
func run(f func()) { f() }

func shared() {} // want `^actor code: \(\*Account\)\.Started -> shared$`

func tick() {}

func spin() {}

// setup is reached by no root, so neither it nor notReached is actor code.
func setup() { notReached() }

func notReached() {}

// Runner is implemented by impl in this package; a call through it is not an edge.
type Runner interface{ Run() }

type impl struct{}

func (impl) Run() {}

func late() {} // want `^actor code: \(\*Account\)\.Signal -> late$`
