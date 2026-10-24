// Package lockstep is a stub of github.com/hmdsefi/lockstep for the analyzer tests. It declares
// every object of the lockstep API table (ANL-020) and the types those declarations name, each as
// its owner declares it, with unexported parts reduced to what compiles and bodies that panic.
package lockstep

import (
	"context"
	"io"
	"math/rand/v2"
	"time"
)

// ===== System, Spawner and Sender (ACT) =====

// System is one runtime instance. Its fields are unexported.
type System struct{}

func (*System) spawner() {}
func (*System) sender()  {}

// Spawner is a parent for Spawn.
type Spawner interface {
	spawner() // unexported: no other implementations
}

// Sender is the origin of a send, an ask, a reply or a stop.
type Sender interface {
	sender() // unexported: no other implementations
}

// ===== Actors and Context (ACT, SUP, TMR) =====

// Actor is implemented by actor values. M is the actor's message type.
type Actor[M any] interface {
	// Receive handles one user message. It runs during a turn and must not block.
	Receive(ctx *Context[M], msg M)
}

// Context is an incarnation's handle to the runtime. Its fields are unexported.
type Context[M any] struct{}

func (*Context[M]) spawner() {}
func (*Context[M]) sender()  {}

// Self returns the actor's PID.
func (c *Context[M]) Self() PID[M] { panic("stub") }

// Now returns the engine clock.
func (c *Context[M]) Now() time.Time { panic("stub") }

// After delivers msg to the actor itself once, d from now on the engine clock.
func (c *Context[M]) After(d time.Duration, msg M) TimerID { panic("stub") }

// Every delivers msg to the actor itself every d.
func (c *Context[M]) Every(d time.Duration, msg M) TimerID { panic("stub") }

// Rand returns the random source of the current incarnation.
func (c *Context[M]) Rand() *rand.Rand { panic("stub") }

// Own registers r to be closed after the incarnation's Stopped hook returns.
func (c *Context[M]) Own(r io.Closer) { panic("stub") }

// ===== Spawning (ACT) =====

type spawnConfig struct{}

// SpawnOption configures one Spawn.
type SpawnOption func(*spawnConfig)

// Spawn creates a child of parent named name and returns its PID.
func Spawn[M any](parent Spawner, name string, factory func() Actor[M], opts ...SpawnOption) (PID[M], error) {
	panic("stub")
}

// ===== Addresses (ACT) =====

// Address is the untyped address of one actor: node, path and uid.
type Address struct {
	node string
	path string
	uid  uint64
}

// PID is the typed address of an actor whose message type is M.
type PID[M any] struct {
	addr Address
}

// ===== Sending and asking (MSG) =====

// Send delivers msg to the actor to.
func Send[M any](from Sender, to PID[M], msg M) error { panic("stub") }

// ReplyTo is the address an answer goes to.
type ReplyTo[R any] struct {
	addr Address
}

// Send answers the ask with v.
func (rt ReplyTo[R]) Send(from Sender, v R) { panic("stub") }

// Reply is the typed future of one ask. Its fields are unexported.
type Reply[R any] struct{}

// Await waits for the ask to resolve and returns its value or error.
func (r *Reply[R]) Await(ctx context.Context) (R, error) { panic("stub") }

type askConfig struct{}

// AskOption configures one Ask.
type AskOption func(*askConfig)

// Ask sends the message build returns to the actor to and returns the future of its answer.
func Ask[M, R any](from Sender, to PID[M], build func(ReplyTo[R]) M, opts ...AskOption) *Reply[R] {
	panic("stub")
}

// Pipe delivers the resolution of r to the actor of ctx as one of its own messages.
func Pipe[R, M any](ctx *Context[M], r *Reply[R], to func(R, error) M) { panic("stub") }

// Signal is a notification from lockstep itself.
type Signal interface {
	signal() // unexported: no other implementations
}

// ===== Timers (TMR) =====

// TimerID identifies a timer of one actor.
type TimerID uint64

// ===== Effects (EFF) =====

// Effect is an operation that waits.
type Effect[T any] interface {
	// Name returns the effect's stable name.
	Name() string

	// Run performs the operation and returns its result.
	Run(ctx context.Context) (T, error)
}

// EffectID identifies one Do call within a System.
type EffectID uint64

type effectConfig struct{}

// EffectOption configures one Do call.
type EffectOption func(*effectConfig)

// Do starts effect e for the actor of ctx and returns its ID.
func Do[T, M any](ctx *Context[M], e Effect[T], done func(T, error) M, opts ...EffectOption) EffectID {
	panic("stub")
}

// ===== Stop reasons (SUP) =====

// StopKind classifies a StopReason.
type StopKind uint8

// StopReason tells why an incarnation ended.
type StopReason struct {
	Kind StopKind
	Err  error
}
