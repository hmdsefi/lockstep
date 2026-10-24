// Package allow checks the scopes of //lockstep:allow on LS100 findings (ANL-080, AT-ANL-07).
// Every case holds a time.Now() in actor code.
package allow

import (
	"time"

	"github.com/hmdsefi/lockstep"
)

// Msg is Account's message type.
type Msg int

// Account is an actor type.
type Account struct{}

// Receive has a directive in its doc comment. It covers Receive's own body, not the functions
// Receive calls.
//
//lockstep:allow LS100 the root reads the clock on purpose
func (a *Account) Receive(ctx *lockstep.Context[Msg], msg Msg) {
	_ = time.Now()
	lines()
	documented()
	stamp()
}

// lines holds the line scopes.
func lines() {
	_ = time.Now() //lockstep:allow LS100 replay uses ctx.Now

	//lockstep:allow LS100 own line above
	_ = time.Now()

	//lockstep:allow LS100 a blank line follows

	_ = time.Now() // want `^LS100: time\.Now uses the wall clock; use ctx\.Now, ctx\.After or ctx\.Every \(actor code: \(\*Account\)\.Receive -> lines\)$`

	// A comment group of two lines, the directive last.
	//lockstep:allow LS100 last line of the group
	_ = time.Now()

	//lockstep:allow LS110 wrong check
	_ = time.Now() // want `^LS100: time\.Now uses the wall clock; use ctx\.Now, ctx\.After or ctx\.Every \(actor code: \(\*Account\)\.Receive -> lines\)$`

	//lockstep:allow LS100
	_ = time.Now() // want `^LS100: time\.Now uses the wall clock; use ctx\.Now, ctx\.After or ctx\.Every \(actor code: \(\*Account\)\.Receive -> lines\)$`

	//lockstep:allow LS110,LS100 both
	_ = time.Now()
}

// documented has the directive in its doc comment, so nothing in it is reported, its function
// literal included.
//
//lockstep:allow LS100 the helper reads the clock on purpose
func documented() {
	_ = time.Now()
	func() {
		_ = time.Now()
	}()
}

// stamp is called by Receive; Receive's directive does not cover it.
func stamp() {
	_ = time.Now() // want `^LS100: time\.Now uses the wall clock; use ctx\.Now, ctx\.After or ctx\.Every \(actor code: \(\*Account\)\.Receive -> stamp\)$`
}
