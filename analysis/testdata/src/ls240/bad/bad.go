// Package bad holds actors with resources that are neither owned nor released, and resource types
// without a release method (LS240, ANL-340 to ANL-360, AT-ANL-23).
package bad

import (
	"io"
	"net"
	"os"
	"time"

	"github.com/hmdsefi/lockstep"
)

// Msg is the message type of every actor here.
type Msg int

// Session has no Stopped.
type Session struct {
	conn net.Conn // want `^LS240: field conn \(net\.Conn\) of actor Session is neither passed to ctx\.Own nor released in Stopped$`
}

// Receive handles a message.
func (s *Session) Receive(ctx *lockstep.Context[Msg], msg Msg) {}

// Poller has a Stopped that does not stop the ticker.
type Poller struct {
	t *time.Ticker // want `^LS240: field t \(\*time\.Ticker\) of actor Poller is neither passed to ctx\.Own nor released in Stopped$`
}

// Receive handles a message.
func (p *Poller) Receive(ctx *lockstep.Context[Msg], msg Msg) {}

// Stopped forgets the ticker.
func (p *Poller) Stopped(ctx *lockstep.Context[Msg], reason lockstep.StopReason) {
	p.t = nil
}

// Store only syncs its file.
type Store struct {
	f *os.File // want `^LS240: field f \(\*os\.File\) of actor Store is neither passed to ctx\.Own nor released in Stopped$`
}

// Receive handles a message.
func (s *Store) Receive(ctx *lockstep.Context[Msg], msg Msg) {}

// Stopped syncs the file and leaves it open.
func (s *Store) Stopped(ctx *lockstep.Context[Msg], reason lockstep.StopReason) {
	_ = s.f.Sync()
}

// Handle is a resource whose release method is Release.
//
//lockstep:resource
type Handle struct{}

// Release frees the handle.
func (h *Handle) Release() {}

// Pool never releases its handle.
type Pool struct {
	h *Handle // want `^LS240: field h \(\*Handle\) of actor Pool is neither passed to ctx\.Own nor released in Stopped$`
}

// Receive handles a message.
func (p *Pool) Receive(ctx *lockstep.Context[Msg], msg Msg) {}

// Log embeds a file and never closes it; the report is at the embedded type.
type Log struct {
	*os.File // want `^LS240: field File \(\*os\.File\) of actor Log is neither passed to ctx\.Own nor released in Stopped$`
}

// Receive handles a message.
func (l *Log) Receive(ctx *lockstep.Context[Msg], msg Msg) {}

// Lease is marked as a resource but has no release method.
//
//lockstep:resource
type Lease struct{} // want `^LS240: resource type Lease has no Close, Stop or Release method$`

// Sleeper has a Stop method of its own, so Sleeper is a well-formed resource type.
//
//lockstep:resource
type Sleeper struct{}

// Stop stops the sleeper.
func (Sleeper) Stop() {}

// Gate closes its connection in Receive, which is not Stopped.
type Gate struct {
	c io.Closer // want `^LS240: field c \(io\.Closer\) of actor Gate is neither passed to ctx\.Own nor released in Stopped$`
}

// Receive closes c.
func (g *Gate) Receive(ctx *lockstep.Context[Msg], msg Msg) {
	_ = g.c.Close()
}

// Mixed releases a but only owns an unrelated local, and closes b through another value.
type Mixed struct {
	a net.Conn
	b net.Conn // want `^LS240: field b \(net\.Conn\) of actor Mixed is neither passed to ctx\.Own nor released in Stopped$`
	c net.Conn // want `^LS240: field c \(net\.Conn\) of actor Mixed is neither passed to ctx\.Own nor released in Stopped$`
}

// Receive handles a message.
func (m *Mixed) Receive(ctx *lockstep.Context[Msg], msg Msg) {}

// Started owns a connection that is not stored in c.
func (m *Mixed) Started(ctx *lockstep.Context[Msg]) {
	other, _ := net.Dial("tcp", "x")
	ctx.Own(other)
}

// Stopped closes a, and closes b only on another Mixed.
func (m *Mixed) Stopped(ctx *lockstep.Context[Msg], reason lockstep.StopReason) {
	_ = m.a.Close()
	var peer *Mixed
	_ = peer.b.Close()
}

// Napper resets its timer in Stopped instead of stopping it.
type Napper struct {
	t *time.Timer // want `^LS240: field t \(\*time\.Timer\) of actor Napper is neither passed to ctx\.Own nor released in Stopped$`
}

// Receive handles a message.
func (n *Napper) Receive(ctx *lockstep.Context[Msg], msg Msg) {}

// Stopped resets the timer.
func (n *Napper) Stopped(ctx *lockstep.Context[Msg], reason lockstep.StopReason) {
	n.t.Reset(time.Second)
}

// Late owns its connection in a method no root reaches.
type Late struct {
	c net.Conn // want `^LS240: field c \(net\.Conn\) of actor Late is neither passed to ctx\.Own nor released in Stopped$`
}

// Receive handles a message.
func (l *Late) Receive(ctx *lockstep.Context[Msg], msg Msg) {}

// adopt is never called.
func (l *Late) adopt(ctx *lockstep.Context[Msg]) {
	ctx.Own(l.c)
}

// valCloser has a Close method on its pointer type only.
type valCloser struct{}

// Close closes the value.
func (*valCloser) Close() error { return nil }

// ByValue holds a valCloser by value.
type ByValue struct {
	v valCloser // want `^LS240: field v \(valCloser\) of actor ByValue is neither passed to ctx\.Own nor released in Stopped$`
}

// Receive handles a message.
func (b *ByValue) Receive(ctx *lockstep.Context[Msg], msg Msg) {}

// Anon holds a structurally typed closer.
type Anon struct {
	c interface{ Close() error } // want `^LS240: field c \(interface\{Close\(\) error\}\) of actor Anon is neither passed to ctx\.Own nor released in Stopped$`
}

// Receive handles a message.
func (a *Anon) Receive(ctx *lockstep.Context[Msg], msg Msg) {}

// Generic is a generic actor; its resource is not released.
type Generic[M any] struct {
	c io.Closer // want `^LS240: field c \(io\.Closer\) of actor Generic is neither passed to ctx\.Own nor released in Stopped$`
}

// Receive handles a message.
func (g *Generic[M]) Receive(ctx *lockstep.Context[M], msg M) {}

// Cell is a generic resource type without a release method.
//
//lockstep:resource
type Cell[T any] struct{} // want `^LS240: resource type Cell has no Close, Stop or Release method$`

// Boxed converts its closer in Stopped, which does not release it.
type Boxed struct {
	c io.Closer // want `^LS240: field c \(io\.Closer\) of actor Boxed is neither passed to ctx\.Own nor released in Stopped$`
}

// Receive handles a message.
func (b *Boxed) Receive(ctx *lockstep.Context[Msg], msg Msg) {}

// Stopped converts c.
func (b *Boxed) Stopped(ctx *lockstep.Context[Msg], reason lockstep.StopReason) {
	_ = any(b.c)
}
