// Package good holds actors whose resources are owned or released, and types LS240 does not
// treat as resources (ANL-340, ANL-350, AT-ANL-24).
package good

import (
	"io"
	"net"
	"os"
	"time"

	"github.com/hmdsefi/lockstep"
)

// Msg is the message type of every actor here.
type Msg int

// Owner hands its connection to the runtime in Started.
type Owner struct {
	conn net.Conn
}

// Receive handles a message.
func (o *Owner) Receive(ctx *lockstep.Context[Msg], msg Msg) {}

// Started owns the connection through the field.
func (o *Owner) Started(ctx *lockstep.Context[Msg]) {
	ctx.Own(o.conn)
}

// Opener opens a file and owns it through a local variable that it stores in the field.
type Opener struct {
	f *os.File
}

// Receive handles a message.
func (o *Opener) Receive(ctx *lockstep.Context[Msg], msg Msg) {}

// Started opens the file, owns it and stores it.
func (o *Opener) Started(ctx *lockstep.Context[Msg]) {
	f, err := os.Open("x")
	if err != nil {
		return
	}
	ctx.Own(f)
	o.f = f
}

// Dialer stores the connection before it owns the local variable.
type Dialer struct {
	conn net.Conn
	n    int
}

// Receive handles a message.
func (d *Dialer) Receive(ctx *lockstep.Context[Msg], msg Msg) {}

// Started stores a connection in the field and owns the same local.
func (d *Dialer) Started(ctx *lockstep.Context[Msg]) {
	c, _ := net.Dial("tcp", "x")
	d.conn, d.n = c, 1
	ctx.Own(c)
}

// Ticking stops its ticker in Stopped.
type Ticking struct {
	t *time.Ticker
}

// Receive handles a message.
func (a *Ticking) Receive(ctx *lockstep.Context[Msg], msg Msg) {}

// Stopped stops the ticker.
func (a *Ticking) Stopped(ctx *lockstep.Context[Msg], reason lockstep.StopReason) {
	a.t.Stop()
}

// Deferring closes its database with a deferred call.
type Deferring struct {
	db io.ReadWriteCloser
}

// Receive handles a message.
func (a *Deferring) Receive(ctx *lockstep.Context[Msg], msg Msg) {}

// Stopped defers the close.
func (a *Deferring) Stopped(ctx *lockstep.Context[Msg], reason lockstep.StopReason) {
	defer a.db.Close()
}

// Helper releases its connection in a function that Stopped calls.
type Helper struct {
	conn net.Conn
}

// Receive handles a message.
func (a *Helper) Receive(ctx *lockstep.Context[Msg], msg Msg) {}

// Stopped shuts down.
func (a *Helper) Stopped(ctx *lockstep.Context[Msg], reason lockstep.StopReason) {
	a.shutdown()
}

func (a *Helper) shutdown() {
	a.finish()
}

func (a *Helper) finish() {
	_ = a.conn.Close()
}

// Passing hands its file to a function.
type Passing struct {
	file *os.File
}

// Receive handles a message.
func (a *Passing) Receive(ctx *lockstep.Context[Msg], msg Msg) {}

// Stopped passes the file to closeQuietly.
func (a *Passing) Stopped(ctx *lockstep.Context[Msg], reason lockstep.StopReason) {
	closeQuietly(a.file)
}

func closeQuietly(c io.Closer) { _ = c.Close() }

// Embedding embeds a file and releases it through the promoted Close.
type Embedding struct {
	*os.File
}

// Receive handles a message.
func (a *Embedding) Receive(ctx *lockstep.Context[Msg], msg Msg) {}

// Stopped closes the embedded file.
func (a *Embedding) Stopped(ctx *lockstep.Context[Msg], reason lockstep.StopReason) {
	_ = a.Close()
}

// Handle is a resource released by Release.
//
//lockstep:resource
type Handle struct{}

// Release frees the handle.
func (h *Handle) Release() {}

// Pool releases its handle in Stopped.
type Pool struct {
	h *Handle
}

// Receive handles a message.
func (p *Pool) Receive(ctx *lockstep.Context[Msg], msg Msg) {}

// Stopped releases the handle.
func (p *Pool) Stopped(ctx *lockstep.Context[Msg], reason lockstep.StopReason) {
	p.h.Release()
}

// Inline holds its handle by value.
type Inline struct {
	h Handle
}

// Receive handles a message.
func (a *Inline) Receive(ctx *lockstep.Context[Msg], msg Msg) {}

// Stopped releases the handle.
func (a *Inline) Stopped(ctx *lockstep.Context[Msg], reason lockstep.StopReason) {
	a.h.Release()
}

// Many holds a slice of closers, which is not a resource field.
type Many struct {
	all []io.Closer
}

// Receive handles a message.
func (a *Many) Receive(ctx *lockstep.Context[Msg], msg Msg) {}

// Plain is not an actor: its connection needs no release by this check.
type Plain struct {
	conn net.Conn
}

// Close is not a root.
func (p *Plain) Close() {}

// Deferred closes inside a function literal that Stopped defers.
type Deferred struct {
	c io.Closer
}

// Receive handles a message.
func (a *Deferred) Receive(ctx *lockstep.Context[Msg], msg Msg) {}

// Stopped defers a closure.
func (a *Deferred) Stopped(ctx *lockstep.Context[Msg], reason lockstep.StopReason) {
	defer func() { _ = a.c.Close() }()
}

// Napper embeds a timer and stops it through the promoted Stop.
type Napper struct {
	*time.Timer
}

// Receive handles a message.
func (a *Napper) Receive(ctx *lockstep.Context[Msg], msg Msg) {}

// Stopped stops the embedded timer.
func (a *Napper) Stopped(ctx *lockstep.Context[Msg], reason lockstep.StopReason) {
	a.Stop()
}

// Timing passes its timer to a function.
type Timing struct {
	t *time.Timer
}

// Receive handles a message.
func (a *Timing) Receive(ctx *lockstep.Context[Msg], msg Msg) {}

// Stopped hands the timer over.
func (a *Timing) Stopped(ctx *lockstep.Context[Msg], reason lockstep.StopReason) {
	cancel(a.t)
}

func cancel(t *time.Timer) { t.Stop() }

// Box is a generic actor that owns its resource.
type Box[M any] struct {
	c io.Closer
}

// Receive handles a message.
func (b *Box[M]) Receive(ctx *lockstep.Context[M], msg M) {}

// Started owns the closer.
func (b *Box[M]) Started(ctx *lockstep.Context[M]) {
	ctx.Own(b.c)
}

// Value has value receivers.
type Value struct {
	c io.Closer
}

// Receive handles a message.
func (v Value) Receive(ctx *lockstep.Context[Msg], msg Msg) {}

// Stopped closes c.
func (v Value) Stopped(ctx *lockstep.Context[Msg], reason lockstep.StopReason) {
	_ = v.c.Close()
}

// Lease is a resource with a Stop method on its pointer type.
//
//lockstep:resource
type Lease struct{}

// Stop ends the lease.
func (*Lease) Stop() {}

// Token is a resource whose release method is promoted from an embedded type.
//
//lockstep:resource
type Token struct {
	Lease
}

// Counter has fields that are not resources.
type Counter struct {
	n     int
	names []string
	done  chan struct{}
	stop  func()
	err   error
}

// Receive handles a message.
func (c *Counter) Receive(ctx *lockstep.Context[Msg], msg Msg) {}

// Slot is a generic resource type with a Close method.
//
//lockstep:resource
type Slot[T any] struct{}

// Close frees the slot.
func (*Slot[T]) Close() {}

// Literal owns its closer from a function literal and with a method expression.
type Literal struct {
	a io.Closer
	b io.Closer
}

// Receive handles a message.
func (l *Literal) Receive(ctx *lockstep.Context[Msg], msg Msg) {}

// Started owns a in a literal and b through the method expression.
func (l *Literal) Started(ctx *lockstep.Context[Msg]) {
	func() { ctx.Own(l.a) }()
	(*lockstep.Context[Msg]).Own(ctx, l.b)
}
