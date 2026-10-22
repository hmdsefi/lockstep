// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep/internal/engine/enginetest"
	"github.com/hmdsefi/lockstep/internal/engine/local"
)

// newManualSystem returns a system built through the ACT-015 constructor on a manual engine for
// node "local" with entropy seed 1 (ACT §10, "manual system").
func newManualSystem(t *testing.T) (*System, *enginetest.Engine) {
	t.Helper()
	eng := enginetest.New("local", 1)
	sys, err := newSystem(eng.Engine(), nil)
	if err != nil {
		t.Fatalf("newSystem on a manual engine: %v", err)
	}
	return sys, eng
}

// newRecordingSystem returns a system built through the ACT-015 constructor on a local engine
// whose tracer is a new recorder (ACT §10, "recording tracer"). The test stops the engine.
func newRecordingSystem(t *testing.T) (*System, *enginetest.Recorder) {
	t.Helper()
	rec := enginetest.NewRecorder()
	le, err := local.New(local.Config{Node: "local", Tracer: rec})
	if err != nil {
		t.Fatalf("local.New: %v", err)
	}
	sys, err := newSystem(le.Engine(), nil)
	if err != nil {
		le.Stop()
		t.Fatalf("newSystem on a local engine: %v", err)
	}
	stopEngineAtEnd(t, sys)
	return sys, rec
}

// newLocalSystem returns New(opts...) and stops its engine when the test ends.
func newLocalSystem(t *testing.T, opts ...Option) *System {
	t.Helper()
	sys := New(opts...)
	stopEngineAtEnd(t, sys)
	return sys
}

// stopEngineAtEnd stops sys's engine when the test ends, so goleak finds no engine goroutine. Until
// System has a Shutdown method this is the only way to stop a local system.
func stopEngineAtEnd(t *testing.T, sys *System) {
	t.Helper()
	t.Cleanup(func() {
		if sys.eng.Stop != nil {
			sys.eng.Stop()
		}
	})
}

// probe is ACT §10's test actor. It answers a Query, sent by inProbe through Ask, with the result
// of the Query's function run inside its Receive, so tests can observe ctx from inside a turn. It
// passes every other message to receive, if set, and forwards every signal it gets to signals.
type probe[M any] struct {
	receive func(ctx *Context[M], msg M)
	signals chan Signal // buffered (probeSignals); a full channel blocks the probe's turn
}

// probeSignals is the capacity of a probe's signal channel.
const probeSignals = 64

// Query is the message inProbe sends a probe: run fn during the probe's turn and answer reply
// with its result.
type Query[M any] struct {
	fn    func(ctx *Context[M]) any
	reply ReplyTo[any]
}

// Receive answers a Query and hands every other message to p.receive.
func (p *probe[M]) Receive(ctx *Context[M], msg M) {
	if q, ok := any(msg).(Query[M]); ok {
		q.reply.Send(ctx, q.fn(ctx))
		return
	}
	if p.receive != nil {
		p.receive(ctx, msg)
	}
}

// Signal forwards sig to p.signals (SignalReceiver).
func (p *probe[M]) Signal(_ *Context[M], sig Signal) { p.signals <- sig }

// spawnProbe spawns a probe named name under parent; receive, which may be nil, gets the messages
// that are not a Query. Every incarnation of the actor is the same probe value.
func spawnProbe[M any](t *testing.T, parent Spawner, name string, receive func(*Context[M], M)) (PID[M], *probe[M]) {
	t.Helper()
	p := &probe[M]{receive: receive, signals: make(chan Signal, probeSignals)}
	pid, err := Spawn(parent, name, actorOf[M](p))
	if err != nil {
		t.Fatalf("Spawn probe %q: %v", name, err)
	}
	return pid, p
}

// inProbe runs fn during a turn of the probe pid and returns fn's result. It asks the probe from
// sys and awaits the answer: on a local engine for up to 10 s, on a manual engine by running it
// until idle. M must be able to hold a Query[M] (for example any).
func inProbe[M any](t *testing.T, sys *System, pid PID[M], fn func(ctx *Context[M]) any) any {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r := Ask(sys, pid, func(rt ReplyTo[any]) M {
		m, ok := any(Query[M]{fn: fn, reply: rt}).(M)
		if !ok {
			panic(fmt.Sprintf("inProbe: message type %T cannot hold a Query", m))
		}
		return m
	}, WithTimeout(10*time.Second))
	v, err := r.Await(ctx)
	if err != nil {
		t.Fatalf("inProbe(%v): %v", pid, err)
	}
	return v
}
