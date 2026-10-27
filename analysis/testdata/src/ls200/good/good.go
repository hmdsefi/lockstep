// Package good holds code LS200 accepts (ANL-210 to ANL-230, AT-ANL-14): values without
// references, copies, kills, uses the send cannot reach, uses that do not read the sent memory,
// and what the check does not track.
package good

import (
	"slices"
	"time"

	"github.com/hmdsefi/lockstep"
)

// Msg is a message type of interface type.
type Msg interface{ msg() }

type Deposit struct{ Amount int }

type Batch struct{ Items []int }

type Row struct{ ID int }

// Snap is immutable, so it holds no references (ANL-210).
//
//lockstep:immutable
type Snap struct{ Rows []Row }

type Hello struct {
	From lockstep.PID[Hello]
	Name string
}

type Note struct{ Text string }

type Count struct{ N *int }

type Move struct{ X *int }

type Work struct{ Data []int }

type Job struct {
	Data    []int
	ID      int
	ReplyTo lockstep.ReplyTo[int]
}

// noReferences sends an int and changes it.
func noReferences(sys *lockstep.System, pid lockstep.PID[Deposit], n int) {
	lockstep.Send(sys, pid, Deposit{Amount: n})
	n++
	_ = n
}

// copied sends a copy: a call is not placed.
func copied(sys *lockstep.System, pid lockstep.PID[Batch], items []int) {
	lockstep.Send(sys, pid, Batch{Items: slices.Clone(items)})
	items[0] = 2
}

// killed assigns the bare variable right after the send.
func killed(sys *lockstep.System, pid lockstep.PID[Batch], items []int) []int {
	lockstep.Send(sys, pid, Batch{Items: items})
	items = nil
	items = append(items, 1)
	return items
}

// loopKilled is the loop of AT-ANL-13(e) with a kill after the send.
func loopKilled(sys *lockstep.System, pid lockstep.PID[Batch], in []int) {
	var buf []int
	for _, x := range in {
		buf = append(buf, x)
		if len(buf) == 3 {
			lockstep.Send(sys, pid, Batch{Items: buf})
			buf = nil
		}
	}
}

// notUses compares with nil and takes the length and capacity of the sent slice.
func notUses(sys *lockstep.System, pid lockstep.PID[Batch], items []int) int {
	lockstep.Send(sys, pid, Batch{Items: items})
	if items != nil && nil != items {
		return len(items) + cap(items)
	}
	return 0
}

// immutable sends an immutable message and reads its rows.
func immutable(sys *lockstep.System, pid lockstep.PID[Snap], rows []Row) int {
	lockstep.Send(sys, pid, Snap{Rows: rows})
	return rows[0].ID
}

// interfaceMessage sends a message of interface type twice: interfaces are not tracked.
func interfaceMessage(sys *lockstep.System, pid lockstep.PID[Msg], msg Msg) {
	lockstep.Send(sys, pid, msg)
	lockstep.Send(sys, pid, msg)
}

// safeFields sends a PID and a string and reuses them.
func safeFields(sys *lockstep.System, pid, to lockstep.PID[Hello], name string) {
	lockstep.Send(sys, to, Hello{From: pid, Name: name})
	lockstep.Send(sys, pid, Hello{From: to, Name: name})
}

// timer gives the slice to a timer: ctx.After is not a send site.
func timer(ctx *lockstep.Context[Batch], items []int) {
	ctx.After(time.Second, Batch{Items: items})
	items[0] = 1
}

// enclosingKill sends in a branch and kills in the enclosing block.
func enclosingKill(sys *lockstep.System, pid lockstep.PID[Batch], items []int, now bool) []int {
	if now {
		lockstep.Send(sys, pid, Batch{Items: items})
	}
	items = nil
	items = append(items, 1)
	return items
}

// redeclared kills with := redeclaring the variable.
func redeclared(sys *lockstep.System, pid lockstep.PID[Batch], load func() ([]int, error)) error {
	items, err := load()
	if err != nil {
		return err
	}
	lockstep.Send(sys, pid, Batch{Items: items})
	items, again := load()
	_ = items
	return again
}

// perIteration declares the slice inside the loop: no use is carried to the next iteration.
func perIteration(sys *lockstep.System, pid lockstep.PID[Batch], in []int) {
	for _, x := range in {
		buf := []int{x}
		buf = append(buf, x)
		lockstep.Send(sys, pid, Batch{Items: buf})
	}
}

// earlierUse uses the slice before the send, outside any loop.
func earlierUse(sys *lockstep.System, pid lockstep.PID[Batch], items []int) {
	items[0] = 1
	lockstep.Send(sys, pid, Batch{Items: items})
}

// Actor is an actor with state. Neither its receiver nor package variables are tracked; LS210
// covers them.
type Actor struct{ items []int }

var pending []int

// Receive sends receiver state and a package variable and keeps using them.
func (a *Actor) Receive(ctx *lockstep.Context[Batch], msg Batch) {
	lockstep.Send(ctx, ctx.Self(), Batch{Items: a.items})
	a.items[0] = 1
	lockstep.Send(ctx, ctx.Self(), Batch{Items: pending})
	pending[0] = 1
}

// askWithoutLiteral passes a build function that is not a literal: nothing is placed.
func askWithoutLiteral(sys *lockstep.System, pid lockstep.PID[Msg], build func(lockstep.ReplyTo[int]) Msg, items []int) {
	lockstep.Ask(sys, pid, build)
	items[0] = 1
}

// derivedAlias sends items; the alias declared before the send is not a root of the message, and
// ANL-220 tracks only roots of placed expressions.
func derivedAlias(sys *lockstep.System, pid lockstep.PID[Batch]) {
	items := []int{1}
	alias := items
	lockstep.Send(sys, pid, Batch{Items: items})
	_ = alias
}

// capturedVariable sends from a function literal a variable of the enclosing function: only
// variables declared in the sending function are tracked.
func capturedVariable(sys *lockstep.System, pid lockstep.PID[Batch]) {
	items := []int{1}
	send := func() { lockstep.Send(sys, pid, Batch{Items: items}) }
	send()
}

// stringNote sends a string, which holds no references.
func stringNote(sys *lockstep.System, pid lockstep.PID[Note], text string) string {
	lockstep.Send(sys, pid, Note{Text: text})
	return text + "!"
}

// otherBranch uses the slice in the else branch, which the send cannot reach (AT-ANL-14).
func otherBranch(sys *lockstep.System, pid lockstep.PID[Batch], items []int, c bool) {
	if c {
		lockstep.Send(sys, pid, Batch{Items: items})
	} else {
		items[0] = 2
	}
}

// earlyReturn returns after the send, so the use after the if statement is not reached
// (AT-ANL-14).
func earlyReturn(sys *lockstep.System, pid lockstep.PID[Batch], items []int, c bool) {
	if c {
		lockstep.Send(sys, pid, Batch{Items: items})
		return
	}
	items[0] = 2
}

// stringPrefix sends a slice of a string: strings are immutable, so the slice shares nothing.
func stringPrefix(sys *lockstep.System, pid lockstep.PID[Note], key string) string {
	lockstep.Send(sys, pid, Note{Text: key[:3]})
	return key
}

// forHeader sends the address of the loop variable: every iteration has its own.
func forHeader(sys *lockstep.System, pid lockstep.PID[Count], n int) {
	for i := 0; i < n; i++ {
		lockstep.Send(sys, pid, Count{N: &i})
	}
}

// rangeHeader sends the address of the range value: every iteration has its own.
func rangeHeader(sys *lockstep.System, pid lockstep.PID[Move], in []int) {
	for _, x := range in {
		lockstep.Send(sys, pid, Move{X: &x})
	}
}

// rangeAssign assigns the slice with range = before every iteration's send.
func rangeAssign(sys *lockstep.System, pid lockstep.PID[Batch], bufs [][]int) {
	var buf []int
	for _, buf = range bufs {
		lockstep.Send(sys, pid, Batch{Items: buf})
	}
}

// panicAfterSend ends the path with panic.
func panicAfterSend(sys *lockstep.System, pid lockstep.PID[Batch], items []int, bad bool) {
	if bad {
		lockstep.Send(sys, pid, Batch{Items: items})
		panic("bad batch")
	}
	items[0] = 1
}

// switchCases sends in one case and uses the slice in another.
func switchCases(sys *lockstep.System, pid lockstep.PID[Batch], items []int, k int) {
	switch k {
	case 0:
		lockstep.Send(sys, pid, Batch{Items: items})
	case 1:
		items[0] = 1
	}
}

// deferredSend sends when the function returns: the writes before that are the sender's own.
func deferredSend(sys *lockstep.System, pid lockstep.PID[Batch]) {
	items := []int{1}
	defer lockstep.Send(sys, pid, Batch{Items: items})
	items[0] = 2
}

// typeSwitchLoop writes the clause's variable before the send: every pass through the clause
// declares a new one (ANL-230).
func typeSwitchLoop(sys *lockstep.System, pid lockstep.PID[Batch], in []any) {
	for _, m := range in {
		switch x := m.(type) {
		case []int:
			x[0] = 1
			lockstep.Send(sys, pid, Batch{Items: x})
		}
	}
}

// Forwarder forwards one field of a message and replies with another.
type Forwarder struct{ next lockstep.PID[Work] }

// Receive forwards m.Data and replies with m.ID: neither the reply nor its ID is on the path of
// the forwarded slice (ANL-220).
func (w *Forwarder) Receive(ctx *lockstep.Context[Job], m Job) {
	lockstep.Send(ctx, w.next, Work{Data: m.Data})
	m.ReplyTo.Send(ctx, m.ID)
}

// aliasPerIteration sends each element through an alias: reading the source again for the next
// alias does not touch what was sent (ANL-220).
func aliasPerIteration(sys *lockstep.System, pid lockstep.PID[Batch], batches [][]int) {
	for i := range batches {
		b := batches[i]
		lockstep.Send(sys, pid, Batch{Items: b})
	}
}

// otherElement sends one element and reads another by a different constant index.
func otherElement(sys *lockstep.System, pid lockstep.PID[Batch], batches [][]int) int {
	lockstep.Send(sys, pid, Batch{Items: batches[0]})
	return len(batches[1])
}

// sharedDeclared sends the address of a variable declared in the loop: each iteration declares a
// new one.
func sharedDeclared(sys *lockstep.System, pid lockstep.PID[Count], in []int) {
	for _, x := range in {
		n := x
		lockstep.Send(sys, pid, Count{N: &n})
	}
}
