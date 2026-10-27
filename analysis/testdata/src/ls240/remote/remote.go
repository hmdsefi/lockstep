// Package remote holds actors with fields of resource types of another package.
package remote

import (
	"github.com/hmdsefi/lockstep"
	"ls240/res"
)

// Msg is the message type of every actor here.
type Msg int

// Keeper never releases its lease.
type Keeper struct {
	l *res.Lease // want `^LS240: field l \(\*res\.Lease\) of actor Keeper is neither passed to ctx\.Own nor released in Stopped$`
}

// Receive handles a message.
func (k *Keeper) Receive(ctx *lockstep.Context[Msg], msg Msg) {}

// Releaser releases its lease and its connection.
type Releaser struct {
	l *res.Lease
	c res.Conn
}

// Receive handles a message.
func (r *Releaser) Receive(ctx *lockstep.Context[Msg], msg Msg) {}

// Stopped releases both.
func (r *Releaser) Stopped(ctx *lockstep.Context[Msg], reason lockstep.StopReason) {
	r.l.Release()
	r.c.Close()
}
