// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package persist_test

import (
	"context"
	"fmt"
	"os"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/persist"
	"github.com/hmdsefi/lockstep/persist/filestore"
)

// Deposit adds Amount to an account and replies with the new balance.
type Deposit struct {
	Amount  int64
	ReplyTo lockstep.ReplyTo[int64]
}

// Deposited is the event a Deposit stores.
type Deposited struct{ Amount int64 }

// account is an event-sourced bank account whose state is its balance.
type account struct{}

func (account) Initial() int64 { return 0 }

func (account) Handle(ctx *persist.Context[Deposit, Deposited, int64], _ int64, d Deposit) []Deposited {
	ctx.Then(func(ctx *persist.Context[Deposit, Deposited, int64], balance int64) {
		d.ReplyTo.Send(ctx, balance) // runs once the event is committed
	})
	return []Deposited{{Amount: d.Amount}}
}

func (account) Apply(balance int64, e Deposited) int64 { return balance + e.Amount }

// A deposit replies only after its event is committed to the journal. A second system on the same
// directory recovers the balance from the stored events.
func Example() {
	dir, err := os.MkdirTemp("", "persist-example")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	deposit := func(amounts ...int64) {
		store, err := filestore.Open(dir)
		if err != nil {
			panic(err)
		}
		sys := lockstep.New()
		defer func() {
			if err := sys.Shutdown(context.Background()); err != nil {
				panic(err)
			}
		}()
		pid, err := persist.Spawn(sys, "account", "acct-1",
			func() persist.EventSourced[Deposit, Deposited, int64] { return account{} },
			persist.Config[Deposited, int64]{Journal: store, Events: persist.JSON[Deposited]()})
		if err != nil {
			panic(err)
		}
		for _, amount := range amounts {
			reply := lockstep.Ask(sys, pid, func(r lockstep.ReplyTo[int64]) Deposit {
				return Deposit{Amount: amount, ReplyTo: r}
			})
			balance, err := reply.Await(context.Background())
			if err != nil {
				panic(err)
			}
			fmt.Println("balance:", balance)
		}
	}
	deposit(5, 2)
	deposit(10) // a new system, as after a process restart
	// Output:
	// balance: 5
	// balance: 7
	// balance: 17
}
