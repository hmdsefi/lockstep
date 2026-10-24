// Package apitable uses every entry of the lockstep API table once. Other lockstep objects and
// this package's own Send and Context get no report.
package apitable

import (
	"context"
	"io"
	"time"

	"github.com/hmdsefi/lockstep"
)

var _ lockstep.Address // want `^api Address$`

type effect struct{}

func (effect) Name() string                     { return "effect" }
func (effect) Run(context.Context) (int, error) { return 0, nil }

func use(
	sys *lockstep.System,
	ctx *lockstep.Context[int], // want `^api Context$`
	pid lockstep.PID[int], // want `^api PID$`
	pending *lockstep.Reply[string], // want `^api Reply$`
	c io.Closer,
) {
	_ = lockstep.Send[int](sys, pid, 1) // want `^api Send$`

	reply := lockstep.Ask(sys, pid, func(rt lockstep.ReplyTo[string]) int { // want `^api Ask$` `^api ReplyTo$`
		rt.Send(sys, "done") // want `^api ReplyTo\.Send$`
		return 0
	})
	await := reply.Await // want `^api Reply\.Await$`
	_, _ = await(context.Background())

	lockstep.Pipe(ctx, pending, func(string, error) int { return 0 }) // want `^api Pipe$`

	_, _ = lockstep.Spawn(sys, "child", func() lockstep.Actor[int] { return nil }) // want `^api Spawn$` `^api Actor$`

	_ = lockstep.Do(ctx, effect{}, func(int, error) int { return 0 }) // want `^api Do$`

	ctx.Own(c)                    // want `^api Context\.Own$`
	_ = ctx.After(time.Second, 1) // want `^api Context\.After$`
	_ = ctx.Every(time.Second, 2) // want `^api Context\.Every$`
	_, _, _ = ctx.Self(), ctx.Now(), ctx.Rand()
	var _ lockstep.TimerID
}

// Send and Context share names with lockstep objects but belong to this package.

func Send(to string) {}

type Context struct{}

func local() {
	Send("peer")
	var _ Context
}
