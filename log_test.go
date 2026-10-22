// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"bytes"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// lockedBuffer is a bytes.Buffer safe to write from an engine goroutine and read from the test.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// setDefaultLogger makes slog.Default() write JSON lines to the returned buffer until the test
// ends.
func setDefaultLogger(t *testing.T) *lockedBuffer {
	t.Helper()
	buf := &lockedBuffer{}
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{
		// Drop the time so the line is the same in every run.
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if len(groups) == 0 && a.Key == slog.TimeKey {
				return slog.Attr{}
			}
			return a
		},
	})))
	t.Cleanup(func() { slog.SetDefault(old) })
	return buf
}

// AT-ACT-12: ACT-130.
func TestATACT12Logf(t *testing.T) {
	t.Run("local engine", func(t *testing.T) {
		buf := setDefaultLogger(t)
		sys, rec := newRecordingSystem(t)
		done := make(chan struct{})
		pid, err := Spawn(sys, "a", actorOf[int](&fnActor[int]{
			started: func(ctx *Context[int]) {
				ctx.Logf("n=%d", 3)
				close(done)
			},
		}))
		if err != nil {
			t.Fatal(err)
		}
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("Started did not run within 5 s")
		}

		recs := recordsOf(rec.Records(), "lockstep.log")
		if len(recs) != 1 {
			t.Fatalf("%d lockstep.log records, want 1: %v", len(recs), recs)
		}
		if got, want := strings.Join(recs[0], " "), "addr="+pid.String()+" text=n=3"; got != want {
			t.Errorf("lockstep.log = %q, want %q", got, want)
		}

		uid := strconv.FormatUint(pid.Address().UID(), 16)
		want := `{"level":"INFO","msg":"n=3","node":"local","path":"/user/a","uid":"` + uid + `"}` + "\n"
		if got := buf.String(); got != want {
			t.Errorf("slog output %q, want %q", got, want)
		}
	})

	t.Run("simulation-style engine", func(t *testing.T) {
		// The manual engine has Blocking false, as the simulation engine does: the line reaches
		// only the trace.
		buf := setDefaultLogger(t)
		sys, eng := newManualSystem(t)
		pid, err := Spawn(sys, "a", actorOf[int](&fnActor[int]{
			started: func(ctx *Context[int]) { ctx.Logf("n=%d", 3) },
		}))
		if err != nil {
			t.Fatal(err)
		}
		eng.RunUntilIdle()
		recs := recordsOf(eng.Records(), "lockstep.log")
		if len(recs) != 1 || strings.Join(recs[0], " ") != "addr="+pid.String()+" text=n=3" {
			t.Errorf("lockstep.log records = %v, want one for %v", recs, pid)
		}
		if got := buf.String(); got != "" {
			t.Errorf("slog output %q, want none", got)
		}
	})

	t.Run("text is the raw string", func(t *testing.T) {
		sys, eng := newManualSystem(t)
		_, err := Spawn(sys, "a", actorOf[int](&fnActor[int]{
			started: func(ctx *Context[int]) { ctx.Logf("say %q and 100%%", "hi") },
		}))
		if err != nil {
			t.Fatal(err)
		}
		eng.RunUntilIdle()
		recs := recordsOf(eng.Records(), "lockstep.log")
		if len(recs) != 1 || recs[0][1] != `text=say "hi" and 100%` {
			t.Errorf("lockstep.log = %v, want the raw formatted text", recs)
		}
	})

	t.Run("outside the turn", func(t *testing.T) {
		sys, eng := newManualSystem(t)
		var ctx *Context[int]
		pid, err := Spawn(sys, "a", actorOf[int](&fnActor[int]{
			started: func(c *Context[int]) { ctx = c },
		}))
		if err != nil {
			t.Fatal(err)
		}
		eng.RunUntilIdle()
		v := mustPanic(t, func() { ctx.Logf("x") })
		if want := "lockstep: Context of " + pid.String() + " used outside its turn"; v != want {
			t.Errorf("Logf outside the turn panicked with %v, want %q", v, want)
		}
		if n := len(recordsOf(eng.Records(), "lockstep.log")); n != 0 {
			t.Errorf("%d lockstep.log records after a panicking Logf, want 0", n)
		}
	})
}
