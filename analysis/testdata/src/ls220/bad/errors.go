package bad

import (
	"github.com/hmdsefi/lockstep"
)

func spawnFactory() lockstep.Actor[Msg] { return nil }

// sends discards the error of Send in every statement form.
func sends(ctx *lockstep.Context[Msg], pid lockstep.PID[Msg], m Msg) {
	lockstep.Send(ctx, pid, m)               // want `^LS220: the error returned by lockstep\.Send is ignored$`
	_ = lockstep.Send(ctx, pid, m)           // want `^LS220: the error returned by lockstep\.Send is ignored$`
	defer lockstep.Send(ctx, pid, m)         // want `^LS220: the error returned by lockstep\.Send is ignored$`
	go lockstep.Send(ctx, pid, m)            // want `^LS220: the error returned by lockstep\.Send is ignored$`
	var _ = lockstep.Send(ctx, pid, m)       // want `^LS220: the error returned by lockstep\.Send is ignored$`
	var _ error = lockstep.Send(ctx, pid, m) // want `^LS220: the error returned by lockstep\.Send is ignored$`
	lockstep.Send[Msg](ctx, pid, m)          // want `^LS220: the error returned by lockstep\.Send is ignored$`
	(lockstep.Send(ctx, pid, m))             // want `^LS220: the error returned by lockstep\.Send is ignored$`
	_ = (lockstep.Send(ctx, pid, m))         // want `^LS220: the error returned by lockstep\.Send is ignored$`
}

// pairs shows that the error position of a tuple assignment is the one that counts.
func pairs(ctx *lockstep.Context[Msg], pid lockstep.PID[Msg], m Msg) {
	var err error
	_, err = lockstep.Spawn(ctx, "a", spawnFactory) // not reported: err is a named variable
	_ = err
	pid, _ = lockstep.Spawn(ctx, "a", spawnFactory)    // want `^LS220: the error returned by lockstep\.Spawn is ignored$`
	other, _ := lockstep.Spawn(ctx, "b", spawnFactory) // want `^LS220: the error returned by lockstep\.Spawn is ignored$`
	_, _ = lockstep.Spawn(ctx, "c", spawnFactory)      // want `^LS220: the error returned by lockstep\.Spawn is ignored$`
	var _, _ = lockstep.Spawn(ctx, "d", spawnFactory)  // want `^LS220: the error returned by lockstep\.Spawn is ignored$`
	lockstep.Spawn(ctx, "e", spawnFactory)             // want `^LS220: the error returned by lockstep\.Spawn is ignored$`
	defer lockstep.Spawn(ctx, "f", spawnFactory)       // want `^LS220: the error returned by lockstep\.Spawn is ignored$`
	go lockstep.Spawn(ctx, "g", spawnFactory)          // want `^LS220: the error returned by lockstep\.Spawn is ignored$`
	_, _ = pid, other

	// The blank error is the second of two assignments, not the first.
	var n int
	n, _ = 1, lockstep.Send(ctx, pid, m) // want `^LS220: the error returned by lockstep\.Send is ignored$`
	_ = n
}
