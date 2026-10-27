// Package msgs declares sealed message interfaces for the actors of packages remote and good
// (ANL-320, AT-ANL-21, AT-ANL-22). Exhaustive exports a fact for each listing its exported
// members only.
package msgs

// Msg is sealed. Its members are A, B and the unexported c.
type Msg interface{ msg() } // want Msg:`^sealed\(A, B\)$`

// A is a member.
type A struct{}

// B is a member.
type B struct{ N int }

type c struct{}

func (A) msg() {}
func (B) msg() {}
func (c) msg() {}

// Cmd is sealed. Its members are Start and the pointer forms *Stop and *pause.
type Cmd interface{ cmd() } // want Cmd:`^sealed\(Start, \*Stop\)$`

// Start is a member.
type Start struct{}

// Stop is a member through its pointer.
type Stop struct{}

type pause struct{}

func (Start) cmd()  {}
func (*Stop) cmd()  {}
func (*pause) cmd() {}

// Open is not sealed: its only method is exported.
type Open interface{ Do() }

// AccountMsg is sealed, for the actors of package good. Its members are Deposit, Withdraw,
// GetBalance and *Close.
type AccountMsg interface{ accountMsg() } // want AccountMsg:`^sealed\(\*Close, Deposit, GetBalance, Withdraw\)$`

type (
	Deposit    struct{ N int }
	Withdraw   struct{ N int }
	GetBalance struct{}
	Close      struct{}
)

func (Deposit) accountMsg()    {}
func (Withdraw) accountMsg()   {}
func (GetBalance) accountMsg() {}
func (*Close) accountMsg()     {}

// Money is implemented by Deposit and Withdraw.
type Money interface{ Amount() int }

func (d Deposit) Amount() int  { return d.N }
func (w Withdraw) Amount() int { return -w.N }

// Note is sealed through the embedded base Base, which carries the sealing method. Base is not a
// member, because Memo and Ping embed it.
type Note interface{ note() } // want Note:`^sealed\(Memo, \*Ping\)$`

type Base struct{}

func (Base) note() {}

type Memo struct {
	Base
	Text string
}

type Ping struct{ *Base }

func (*Ping) note() {}
