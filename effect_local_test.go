// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"bytes"
	"context"
	"errors"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// localActor is an any actor for the local engine tests: it runs the func(*Context[any]) it
// receives and forwards every other message to out, which the test reads.
type localActor struct{ out chan any }

func (a *localActor) Receive(ctx *Context[any], m any) {
	if f, ok := m.(func(*Context[any])); ok {
		f(ctx)
		return
	}
	a.out <- m
}

// spawnLocalActor spawns a localActor on sys; out is buffered so a turn never blocks on it.
func spawnLocalActor(t *testing.T, sys *System) (PID[any], <-chan any) {
	t.Helper()
	a := &localActor{out: make(chan any, 8)}
	pid, err := Spawn(sys, "q", actorOf[any](a))
	if err != nil {
		t.Fatal(err)
	}
	return pid, a.out
}

// recvMsg returns the next message of out, failing after 10 s.
func recvMsg(t *testing.T, out <-chan any) any {
	t.Helper()
	select {
	case m := <-out:
		return m
	case <-time.After(10 * time.Second):
		t.Fatal("no message within 10s")
		return nil
	}
}

// noMsg fails if out gets a message within d.
func noMsg(t *testing.T, out <-chan any, d time.Duration) {
	t.Helper()
	select {
	case m := <-out:
		t.Fatalf("unexpected message %v", m)
	case <-time.After(d):
	}
}

// goroutineID returns the ID of the calling goroutine.
func goroutineID() uint64 {
	var buf [64]byte
	b := bytes.TrimPrefix(buf[:runtime.Stack(buf[:], false)], []byte("goroutine "))
	id, err := strconv.ParseUint(string(b[:bytes.IndexByte(b, ' ')]), 10, 64)
	if err != nil {
		panic(err)
	}
	return id
}

// funcEffect is an effect whose Run is a func.
type funcEffect struct {
	name string
	run  func(ctx context.Context) (int64, error)
}

func (e funcEffect) Name() string { return e.name }

func (e funcEffect) Run(ctx context.Context) (int64, error) { return e.run(ctx) }

// AT-EFF-20: on the local engine a timed-out effect delivers ErrTimeout, its Run sees ctx.Done
// closed, and the result Run returns later is ignored.
func TestATEFF20LocalTimeout(t *testing.T) {
	sys, rec := newRecordingSystem(t)
	pid, out := spawnLocalActor(t, sys)
	sawDone := make(chan bool, 1)
	e := funcEffect{"prices.fetch", func(ctx context.Context) (int64, error) {
		<-ctx.Done()
		sawDone <- true
		return 0, nil
	}}
	send(t, sys, pid, any(func(ctx *Context[any]) {
		Do(ctx, e, func(v int64, err error) any { return fetched{v, err} }, WithEffectTimeout(50*time.Millisecond))
	}))
	got, ok := recvMsg(t, out).(fetched)
	if !ok || got.v != 0 || !errors.Is(got.err, ErrTimeout) {
		t.Fatalf("done got %+v, want the zero value and ErrTimeout", got)
	}
	select {
	case <-sawDone:
	case <-time.After(10 * time.Second):
		t.Fatal("Run never saw ctx.Done closed")
	}
	// Run has returned (0, nil) by now or soon: its result is ignored.
	noMsg(t, out, 100*time.Millisecond)
	dones := effectDones(rec.Records())
	if len(dones) != 1 || dones[0]["outcome"] != "timeout" {
		t.Errorf("done records = %v, want one outcome=timeout", dones)
	}
}

// AT-EFF-21: a panic in Run is delivered as an error with ENG-430's text, with outcome=error.
func TestATEFF21LocalPanic(t *testing.T) {
	sys, rec := newRecordingSystem(t)
	pid, out := spawnLocalActor(t, sys)
	e := funcEffect{"db.query", func(context.Context) (int64, error) { panic("db") }}
	send(t, sys, pid, any(func(ctx *Context[any]) {
		Do(ctx, e, func(v int64, err error) any { return fetched{v, err} })
	}))
	got, ok := recvMsg(t, out).(fetched)
	want := `lockstep: effect "db.query" panicked: db`
	if !ok || got.v != 0 || got.err == nil || got.err.Error() != want {
		t.Fatalf("done got %+v, want the zero value and the error %q", got, want)
	}
	dones := effectDones(rec.Records())
	if len(dones) != 1 || dones[0]["outcome"] != "error" || dones[0]["err"] != want {
		t.Errorf("done records = %v, want one outcome=error with err %q", dones, want)
	}
}

// AT-EFF-22: 64 running effects, 1024 queued; the next Do gets ErrOverloaded with
// outcome=overloaded and its Run never runs. The running Run calls are on goroutines of their own.
func TestATEFF22LocalOverload(t *testing.T) {
	const running, queued = 64, 1024
	sys, rec := newRecordingSystem(t)
	pid, out := spawnLocalActor(t, sys)

	testG := goroutineID()
	var started, overRan, onTestOrLoop atomic.Int32
	blocked := func(ctx context.Context) (int64, error) {
		started.Add(1)
		if goroutineID() == testG || sys.eng.Scheduler.InTurn() {
			onTestOrLoop.Add(1)
		}
		<-ctx.Done() // the shutdown at the end of the test cancels it
		return 0, nil
	}
	send(t, sys, pid, any(func(ctx *Context[any]) {
		for range running + queued {
			Do(ctx, funcEffect{"wait", blocked}, func(v int64, err error) any { return fetched{v, err} }, WithEffectTimeout(time.Hour))
		}
		Do(ctx, funcEffect{"over", func(context.Context) (int64, error) {
			overRan.Add(1)
			return 0, nil
		}}, func(v int64, err error) any { return fetched{v, err} })
	}))
	got, ok := recvMsg(t, out).(fetched)
	if !ok || !errors.Is(got.err, ErrOverloaded) || got.err != ErrOverloaded {
		t.Fatalf("done of the last effect got %+v, want ErrOverloaded", got)
	}
	deadline := time.Now().Add(10 * time.Second)
	for started.Load() < running && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	if n := started.Load(); n != running {
		t.Errorf("%d Run calls started, want exactly %d (the queued ones wait)", n, running)
	}
	if n := onTestOrLoop.Load(); n != 0 {
		t.Errorf("%d Run calls ran on the test's or the loop's goroutine", n)
	}
	if overRan.Load() != 0 {
		t.Error("Run of the overloaded effect was called")
	}
	var overloaded []map[string]string
	for _, d := range effectDones(rec.Records()) {
		if d["outcome"] == "overloaded" {
			overloaded = append(overloaded, d)
		}
	}
	if len(overloaded) != 1 || overloaded[0]["name"] != "over" || overloaded[0]["effect"] != strconv.Itoa(running+queued+1) ||
		overloaded[0]["err"] != ErrOverloaded.Error() {
		t.Errorf("overloaded records = %v, want one for the last effect", overloaded)
	}
}
