package good

import (
	"github.com/hmdsefi/lockstep"
)

func factory() lockstep.Actor[Msg] { return nil }

func checked(ctx *lockstep.Context[Msg], pid lockstep.PID[Msg], m Msg) error {
	if err := lockstep.Send(ctx, pid, m); err != nil {
		return err
	}
	child, err := lockstep.Spawn(ctx, "a", factory)
	if err != nil {
		return err
	}
	_ = child
	return lockstep.Send(ctx, pid, m)
}

func returnedSpawn(ctx *lockstep.Context[Msg]) (lockstep.PID[Msg], error) {
	return lockstep.Spawn(ctx, "a", factory)
}

// notChecked assigns the errors to named variables that nothing reads: that is not checked further.
func notChecked(ctx *lockstep.Context[Msg], pid lockstep.PID[Msg], m Msg) {
	var err error
	err = lockstep.Send(ctx, pid, m)
	child, spawnErr := lockstep.Spawn(ctx, "b", factory)
	_, _ = child, spawnErr
	_ = err
}

// pair keeps the error of Send in a field next to a value: the error position of the assignment
// is the field.
func pair(ctx *lockstep.Context[Msg], pid lockstep.PID[Msg], h *holder, r *lockstep.Reply[int], m Msg) {
	h.two, h.err = r, lockstep.Send(ctx, pid, m)
}

func asArgument(ctx *lockstep.Context[Msg], pid lockstep.PID[Msg], m Msg, handle func(error)) {
	handle(lockstep.Send(ctx, pid, m))
}

func kept(ctx *lockstep.Context[Msg], pid lockstep.PID[Msg], m Msg) {
	errs := []error{lockstep.Send(ctx, pid, m)}
	_ = errs
	ch := make(chan error, 1)
	ch <- lockstep.Send(ctx, pid, m)
}
