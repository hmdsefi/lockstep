// Package good holds the LS220 uses: every reply is awaited, piped or kept, and every Send and
// Spawn error is handled or assigned (ANL-290, ANL-300, AT-ANL-19, AT-ANL-20).
package good

import (
	"context"

	"github.com/hmdsefi/lockstep"
)

// Msg is the message type of the asked actors.
type Msg struct{ To lockstep.ReplyTo[int] }

func build(rt lockstep.ReplyTo[int]) Msg { return Msg{To: rt} }

// holder keeps replies in its fields.
type holder struct {
	pending map[int]*lockstep.Reply[int]
	list    []*lockstep.Reply[int]
	one     *lockstep.Reply[int]
	two     *lockstep.Reply[int]
	err     error
}

// pkgReply is a package variable: storing a reply in it is a use.
var pkgReply *lockstep.Reply[int]

// pkgInit holds the reply from the start.
var pkgInit = lockstep.Ask(nil, lockstep.PID[Msg]{}, build)

func await(ctx context.Context, sys *lockstep.System, pid lockstep.PID[Msg]) (int, error) {
	return lockstep.Ask(sys, pid, build).Await(ctx)
}

func method(ctx context.Context, sys *lockstep.System, pid lockstep.PID[Msg]) {
	defer lockstep.Ask(sys, pid, build).Await(ctx)
	f := lockstep.Ask(sys, pid, build).Await
	_, _ = f(ctx)
}

func transitive(ctx *lockstep.Context[Msg], sys *lockstep.System, pid lockstep.PID[Msg]) {
	r := lockstep.Ask(sys, pid, build)
	r2 := r
	lockstep.Pipe(ctx, r2, func(int, error) Msg { return Msg{} })
}

func returned(sys *lockstep.System, pid lockstep.PID[Msg]) *lockstep.Reply[int] {
	return lockstep.Ask(sys, pid, build)
}

func returnedVariable(sys *lockstep.System, pid lockstep.PID[Msg]) *lockstep.Reply[int] {
	r := lockstep.Ask(sys, pid, build)
	return r
}

func namedResult(sys *lockstep.System, pid lockstep.PID[Msg]) (r *lockstep.Reply[int]) {
	r = lockstep.Ask(sys, pid, build)
	return
}

func appended(sys *lockstep.System, pid lockstep.PID[Msg]) []*lockstep.Reply[int] {
	var replies []*lockstep.Reply[int]
	replies = append(replies, lockstep.Ask(sys, pid, build))
	return replies
}

func sent(sys *lockstep.System, pid lockstep.PID[Msg], ch chan *lockstep.Reply[int]) {
	ch <- lockstep.Ask(sys, pid, build)
	r := lockstep.Ask(sys, pid, build)
	ch <- r
}

func stored(h *holder, sys *lockstep.System, pid lockstep.PID[Msg], id int) {
	h.pending[id] = lockstep.Ask(sys, pid, build)
	h.one = lockstep.Ask(sys, pid, build)
	h.list[0] = lockstep.Ask(sys, pid, build)
	pkgReply = lockstep.Ask(sys, pid, build)
	r := lockstep.Ask(sys, pid, build)
	h.two = r
}

func literals(sys *lockstep.System, pid lockstep.PID[Msg]) {
	_ = holder{one: lockstep.Ask(sys, pid, build)}
	_ = []*lockstep.Reply[int]{lockstep.Ask(sys, pid, build)}
	_ = map[string]*lockstep.Reply[int]{"a": lockstep.Ask(sys, pid, build)}
	r := lockstep.Ask(sys, pid, build)
	_ = holder{two: r}
}

// pointer stores the reply through a pointer, as sim's tickProbe does in Started.
type pointer struct {
	reply **lockstep.Reply[int]
}

func (p *pointer) store(sys *lockstep.System, pid lockstep.PID[Msg], out **lockstep.Reply[int]) {
	*out = lockstep.Ask(sys, pid, build)
	*p.reply = lockstep.Ask(sys, pid, build)
	r := lockstep.Ask(sys, pid, build)
	*out = r
}

func captured(ctx context.Context, sys *lockstep.System, pid lockstep.PID[Msg]) {
	r := lockstep.Ask(sys, pid, build)
	go func() { _, _ = r.Await(ctx) }()
}

// assignedInLiteral asks inside a literal and keeps the reply in a variable of the enclosing
// function, which another literal awaits.
func assignedInLiteral(ctx context.Context, sys *lockstep.System, pid lockstep.PID[Msg]) {
	var reply *lockstep.Reply[int]
	start := func() { reply = lockstep.Ask(sys, pid, build) }
	get := func() (int, error) { return reply.Await(ctx) }
	start()
	_, _ = get()
}

// assignedInLiteralLater awaits the reply of a variable of the enclosing function after the
// literal that asked ran.
func assignedInLiteralLater(ctx context.Context, sys *lockstep.System, pid lockstep.PID[Msg]) {
	var reply *lockstep.Reply[int]
	func() { reply = lockstep.Ask(sys, pid, build) }()
	_, _ = reply.Await(ctx)
}

func addressTaken(sys *lockstep.System, pid lockstep.PID[Msg], keep func(**lockstep.Reply[int])) {
	r := lockstep.Ask(sys, pid, build)
	keep(&r)
}

func passed(sys *lockstep.System, pid lockstep.PID[Msg], keep func(*lockstep.Reply[int])) {
	keep(lockstep.Ask(sys, pid, build))
}

func reassigned(ctx context.Context, sys *lockstep.System, pid lockstep.PID[Msg]) {
	r := lockstep.Ask(sys, pid, build)
	r = lockstep.Ask(sys, pid, build)
	_, _ = r.Await(ctx)
}

// capturedOnly mentions the reply in a literal without awaiting it: a capture is a use.
func capturedOnly(sys *lockstep.System, pid lockstep.PID[Msg]) func() bool {
	r := lockstep.Ask(sys, pid, build)
	return func() bool { return r == nil }
}

// assignedOnlyInLiteral assigns the reply to a variable the enclosing function declares: the
// literal captures the variable.
func assignedOnlyInLiteral(sys *lockstep.System, pid lockstep.PID[Msg]) func() {
	var reply *lockstep.Reply[int]
	_ = reply == nil
	return func() { reply = lockstep.Ask(sys, pid, build) }
}
