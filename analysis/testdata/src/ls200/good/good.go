// Package good holds code LS200 accepts (ANL-210 to ANL-230, AT-ANL-14): values without
// references, copies, kills, uses that do not read the sent memory, and what the check does not
// track.
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
