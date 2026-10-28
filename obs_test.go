// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"log/slog"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/internal/engine/enginetest"
	"github.com/hmdsefi/lockstep/internal/hooks"
)

// recRecord is one record a recHandler received.
type recRecord struct {
	ctx   context.Context // the context the handler was called with (OBS-050)
	level slog.Level
	msg   string
	pc    uintptr
	// attrs are the logger's With attributes and then the record's own, in order, each value
	// resolved. A group's attributes are flattened into keys "group.key".
	attrs []slog.Attr
}

// keys returns the keys of r's attributes in order.
func (r recRecord) keys() []string {
	ks := make([]string, len(r.attrs))
	for i, a := range r.attrs {
		ks[i] = a.Key
	}
	return ks
}

// recStore is what every handler derived from one recHandler shares.
type recStore struct {
	level slog.LevelVar
	mu    sync.Mutex
	recs  []recRecord
}

// recHandler is the recording handler of OBS §10: it stores every record it handles, with its
// attributes flattened (including With attributes), and the context Handle was called with. Its
// level is settable at any time; the zero level is Info. Handlers derived with WithAttrs and
// WithGroup record into the same store. It is safe for concurrent use.
type recHandler struct {
	st     *recStore
	attrs  []slog.Attr // the With attributes, keys already qualified by their groups
	prefix string      // the open groups, each followed by "."
}

var _ slog.Handler = (*recHandler)(nil)

// newRecHandler returns a recHandler at level.
func newRecHandler(level slog.Level) *recHandler {
	h := &recHandler{st: &recStore{}}
	h.st.level.Set(level)
	return h
}

// newRecLogger returns a logger on a new recHandler at level, and the handler.
func newRecLogger(level slog.Level) (*slog.Logger, *recHandler) {
	h := newRecHandler(level)
	return slog.New(h), h
}

// setLevel changes the level of h and of every handler derived from it.
func (h *recHandler) setLevel(l slog.Level) { h.st.level.Set(l) }

// records returns a copy of the records stored so far, in the order they were handled.
func (h *recHandler) records() []recRecord {
	h.st.mu.Lock()
	defer h.st.mu.Unlock()
	return slices.Clone(h.st.recs)
}

// withMsg returns the stored records whose message is msg.
func (h *recHandler) withMsg(msg string) []recRecord {
	var out []recRecord
	for _, r := range h.records() {
		if r.msg == msg {
			out = append(out, r)
		}
	}
	return out
}

func (h *recHandler) Enabled(_ context.Context, l slog.Level) bool { return l >= h.st.level.Level() }

func (h *recHandler) Handle(ctx context.Context, r slog.Record) error {
	rec := recRecord{ctx: ctx, level: r.Level, msg: r.Message, pc: r.PC, attrs: slices.Clone(h.attrs)}
	r.Attrs(func(a slog.Attr) bool {
		rec.attrs = flattenAttr(rec.attrs, h.prefix, a)
		return true
	})
	h.st.mu.Lock()
	defer h.st.mu.Unlock()
	h.st.recs = append(h.st.recs, rec)
	return nil
}

func (h *recHandler) WithAttrs(as []slog.Attr) slog.Handler {
	if len(as) == 0 {
		return h
	}
	h2 := *h
	h2.attrs = slices.Clip(slices.Clone(h.attrs))
	for _, a := range as {
		h2.attrs = flattenAttr(h2.attrs, h.prefix, a)
	}
	return &h2
}

func (h *recHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	h2 := *h
	h2.prefix = h.prefix + name + "."
	return &h2
}

// flattenAttr appends a to dst with its value resolved and its key prefixed, a group's members
// appended one by one under "<group>.". It follows slog's rules: an attribute with an empty key
// and a non-group value, and an empty group, are dropped; a group with an empty key is inlined.
func flattenAttr(dst []slog.Attr, prefix string, a slog.Attr) []slog.Attr {
	a.Value = a.Value.Resolve()
	if a.Value.Kind() == slog.KindGroup {
		if a.Key != "" {
			prefix += a.Key + "."
		}
		for _, m := range a.Value.Group() {
			dst = flattenAttr(dst, prefix, m)
		}
		return dst
	}
	if a.Key == "" {
		return dst
	}
	return append(dst, slog.Attr{Key: prefix + a.Key, Value: a.Value})
}

// setDefault makes l slog's default logger until the test ends, then restores the previous one
// and the log package's writer and flags, which slog.SetDefault redirects.
func setDefault(t *testing.T, l *slog.Logger) {
	t.Helper()
	old, oldWriter, oldFlags := slog.Default(), log.Writer(), log.Flags()
	slog.SetDefault(l)
	t.Cleanup(func() {
		slog.SetDefault(old)
		log.SetOutput(oldWriter)
		log.SetFlags(oldFlags)
	})
}

// recv returns the next value from ch, failing the test if none arrives within 10 s. It is the
// external tests' copy of the package-internal helper of the same name, which they cannot see.
func recv[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(10 * time.Second):
		t.Fatalf("no %T within 10 s", *new(T))
		panic("unreachable")
	}
}

// newProdSystem returns lockstep.New(opts...) and shuts it down when the test ends.
func newProdSystem(t *testing.T, opts ...lockstep.Option) *lockstep.System {
	t.Helper()
	sys := lockstep.New(opts...)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := sys.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
	})
	return sys
}

// manualSystem builds a system on node local with opts on a manual engine through hooks.NewSystem,
// as simulation does: no turn runs until the test calls RunUntilIdle.
func manualSystem(t *testing.T, opts ...lockstep.Option) (*lockstep.System, *enginetest.Engine) {
	t.Helper()
	eng := enginetest.New("local", 1)
	b, err := hooks.NewSystem(eng.Engine(), opts)
	if err != nil {
		t.Fatalf("hooks.NewSystem: %v", err)
	}
	return systemOf(t, b), eng
}

// lockedWriter is a bytes.Buffer safe to write from a worker goroutine and read from the test.
type lockedWriter struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (w *lockedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func (w *lockedWriter) Bytes() []byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	return bytes.Clone(w.buf.Bytes())
}

// AT-OBS-03: OBS-030.
func TestATOBS03ActorLogger(t *testing.T) {
	t.Run("one logger per actor", func(t *testing.T) {
		l, h := newRecLogger(slog.LevelInfo)
		sys := newProdSystem(t, lockstep.WithNodeName("n1"), lockstep.WithLogger(l))
		type result struct {
			first, second *slog.Logger
			inc           int
		}
		results := make(chan result, 2)
		inc := 0
		pid, err := lockstep.Spawn(sys, "a", func() lockstep.Actor[int] {
			inc++
			n := inc
			return &simActor[int]{receive: func(ctx *lockstep.Context[int], _ int) {
				a, b := ctx.Logger(), ctx.Logger()
				if n == 1 {
					ctx.Logger().Info("hi", "k", 1)
					ctx.Fail(errors.New("restart me"))
				}
				results <- result{first: a, second: b, inc: n}
			}}
		})
		if err != nil {
			t.Fatal(err)
		}
		for range 2 {
			if err := lockstep.Send(sys, pid, 0); err != nil {
				t.Fatal(err)
			}
		}
		r1, r2 := recv(t, results), recv(t, results)
		if r1.inc != 1 || r2.inc != 2 {
			t.Fatalf("incarnations %d and %d, want 1 and 2", r1.inc, r2.inc)
		}
		if r1.first == nil || r1.first != r1.second {
			t.Errorf("two Logger calls in one turn gave %p and %p, want one non-nil logger", r1.first, r1.second)
		}
		if r2.first != r1.first || r2.second != r1.first {
			t.Errorf("Logger after a restart gave %p and %p, want %p", r2.first, r2.second, r1.first)
		}

		recs := h.withMsg("hi")
		if len(recs) != 1 {
			t.Fatalf("%d records hi, want 1: %v", len(recs), recs)
		}
		rec := recs[0]
		if got, want := rec.keys(), []string{"node", "path", "uid", "k"}; !slices.Equal(got, want) {
			t.Fatalf("hi has keys %v, want %v", got, want)
		}
		if rec.level != slog.LevelInfo {
			t.Errorf("hi at %v, want INFO", rec.level)
		}
		want := []slog.Attr{
			slog.String("node", "n1"),
			slog.String("path", "/user/a"),
			slog.Uint64("uid", pid.Address().UID()),
			slog.Int("k", 1),
		}
		for i, a := range rec.attrs {
			if !a.Equal(want[i]) || a.Value.Kind() != want[i].Value.Kind() {
				t.Errorf("attribute %d = %v (%v), want %v (%v)", i, a, a.Value.Kind(), want[i], want[i].Value.Kind())
			}
		}
	})

	t.Run("Spawn builds no logger", func(t *testing.T) {
		l, _ := newRecLogger(slog.LevelInfo)
		sys, eng := manualSystem(t, lockstep.WithLogger(l))
		var got *slog.Logger
		pid, err := lockstep.Spawn(sys, "a", func() lockstep.Actor[int] {
			return &simActor[int]{started: func(ctx *lockstep.Context[int]) { got = ctx.Logger() }}
		})
		if err != nil {
			t.Fatal(err)
		}
		if lockstep.ActorLoggerBuilt(sys, pid) {
			t.Error("Spawn built the actor logger")
		}
		eng.RunUntilIdle()
		if got == nil || !lockstep.ActorLoggerBuilt(sys, pid) {
			t.Errorf("after Logger: logger %p, built %v; want a logger, built", got, lockstep.ActorLoggerBuilt(sys, pid))
		}

		if lockstep.RaceEnabled {
			t.Skip("allocation counts differ under the race detector")
		}
		idle := func() lockstep.Actor[int] { return &simActor[int]{} }
		measure := func(opts ...lockstep.Option) float64 {
			// The manual engine runs no turn until it is told to, so only Spawn allocates.
			sys, _ := manualSystem(t, opts...)
			const runs = 200
			names := make([]string, runs+1) // AllocsPerRun calls f once more to warm up
			for i := range names {
				names[i] = "a" + strconv.Itoa(i)
			}
			i := 0
			return testing.AllocsPerRun(runs, func() {
				if _, err := lockstep.Spawn(sys, names[i], idle); err != nil {
					t.Fatal(err)
				}
				i++
			})
		}
		without := measure()
		with := measure(lockstep.WithLogger(l))
		if with != without {
			t.Errorf("Spawn allocates %v times with WithLogger, %v without", with, without)
		}
	})
}

// countingStringer counts the calls of its String method.
type countingStringer struct{ n atomic.Int64 }

func (s *countingStringer) String() string {
	s.n.Add(1)
	return "s"
}

// spawnStarted spawns /user/a on sys running started in its Started hook, and waits until the
// hook has returned.
func spawnStarted(t *testing.T, sys *lockstep.System, started func(ctx *lockstep.Context[int])) lockstep.PID[int] {
	t.Helper()
	done := make(chan struct{}, 1)
	pid, err := lockstep.Spawn(sys, "a", func() lockstep.Actor[int] {
		return &simActor[int]{started: func(ctx *lockstep.Context[int]) {
			started(ctx)
			done <- struct{}{}
		}}
	})
	if err != nil {
		t.Fatal(err)
	}
	recv(t, done)
	return pid
}

// AT-OBS-04: OBS-040, OBS-050.
func TestATOBS04Logf(t *testing.T) {
	t.Run("source is the caller", func(t *testing.T) {
		buf := &lockedWriter{}
		l := slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{AddSource: true}))
		sys := newProdSystem(t, lockstep.WithLogger(l))
		var file string
		var line int
		spawnStarted(t, sys, func(ctx *lockstep.Context[int]) {
			_, file, line, _ = runtime.Caller(0)
			ctx.Logf("n=%d", 7)
		})
		line++ // the line of the Logf call

		var got struct {
			Level  string `json:"level"`
			Msg    string `json:"msg"`
			Source struct {
				File string `json:"file"`
				Line int    `json:"line"`
			} `json:"source"`
		}
		out := bytes.TrimSpace(buf.Bytes())
		lines := bytes.Split(out, []byte("\n"))
		if len(out) == 0 || len(lines) != 1 {
			t.Fatalf("%d log lines, want 1: %s", len(lines), out)
		}
		if err := json.Unmarshal(lines[0], &got); err != nil {
			t.Fatalf("%v: %s", err, lines[0])
		}
		if got.Msg != "n=7" || got.Level != "INFO" {
			t.Errorf("msg %q at %q, want n=7 at INFO", got.Msg, got.Level)
		}
		if got.Source.File != file || got.Source.Line != line {
			t.Errorf("source %s:%d, want %s:%d", got.Source.File, got.Source.Line, file, line)
		}
	})

	t.Run("handler gets context.Background", func(t *testing.T) {
		l, h := newRecLogger(slog.LevelInfo)
		sys := newProdSystem(t, lockstep.WithLogger(l))
		spawnStarted(t, sys, func(ctx *lockstep.Context[int]) { ctx.Logf("x=%d", 1) })
		recs := h.withMsg("x=1")
		if len(recs) != 1 {
			t.Fatalf("%d records x=1, want 1", len(recs))
		}
		if recs[0].ctx != context.Background() {
			t.Errorf("Handle got context %v, want context.Background()", recs[0].ctx)
		}
		if recs[0].level != slog.LevelInfo {
			t.Errorf("record at %v, want INFO", recs[0].level)
		}
		if got, want := recs[0].keys(), []string{"node", "path", "uid"}; !slices.Equal(got, want) {
			t.Errorf("record keys %v, want %v", got, want)
		}
	})

	t.Run("no formatting below Info", func(t *testing.T) {
		l, h := newRecLogger(slog.LevelWarn)
		for _, tt := range []struct {
			level slog.Level
			calls int64
		}{{slog.LevelWarn, 0}, {slog.LevelInfo, 1}} {
			h.setLevel(tt.level)
			sys := newProdSystem(t, lockstep.WithLogger(l))
			s := &countingStringer{}
			spawnStarted(t, sys, func(ctx *lockstep.Context[int]) { ctx.Logf("%v", s) })
			if n := s.n.Load(); n != tt.calls {
				t.Errorf("logger at %v: String called %d times, want %d", tt.level, n, tt.calls)
			}
		}
	})
}

// OBS-010: the system logger is resolved once, when the system is built.
func TestLoggerResolvedAtNew(t *testing.T) {
	logBoth := func(n int) func(ctx *lockstep.Context[int]) {
		return func(ctx *lockstep.Context[int]) {
			ctx.Logf("n=%d", n)
			ctx.Logger().Info("direct")
		}
	}

	t.Run("New", func(t *testing.T) {
		a, ha := newRecLogger(slog.LevelInfo)
		b, hb := newRecLogger(slog.LevelInfo)
		setDefault(t, a)
		sys := newProdSystem(t)
		slog.SetDefault(b)
		spawnStarted(t, sys, logBoth(1))
		if n, m := len(ha.withMsg("n=1")), len(ha.withMsg("direct")); n != 1 || m != 1 {
			t.Errorf("the default logger at New got %d records n=1 and %d records direct, want 1 and 1", n, m)
		}
		if recs := hb.records(); len(recs) != 0 {
			t.Errorf("the default logger set after New got %d records, want 0", len(recs))
		}
	})

	t.Run("WithLogger(nil)", func(t *testing.T) {
		c, hc := newRecLogger(slog.LevelInfo)
		setDefault(t, c)
		sys := newProdSystem(t, lockstep.WithLogger(nil))
		spawnStarted(t, sys, logBoth(2))
		if n := len(hc.withMsg("n=2")); n != 1 {
			t.Errorf("WithLogger(nil): the default logger got %d records n=2, want 1", n)
		}
	})

	// ACT-130, OBS-155: on a non-blocking engine (simulation, the manual engine) the default system
	// logger discards, so nothing reaches slog.Default(); WithLogger's logger gets the records.
	t.Run("non-blocking engine", func(t *testing.T) {
		d, hd := newRecLogger(slog.LevelDebug)
		setDefault(t, d)
		sys, eng := manualSystem(t)
		spawnStartedManual(t, sys, eng, logBoth(3))
		if recs := hd.records(); len(recs) != 0 {
			t.Errorf("slog.Default() got %d records from a manual-engine actor, want 0", len(recs))
		}

		l, h := newRecLogger(slog.LevelInfo)
		sys, eng = manualSystem(t, lockstep.WithLogger(l))
		spawnStartedManual(t, sys, eng, logBoth(4))
		if n, m := len(h.withMsg("n=4")), len(h.withMsg("direct")); n != 1 || m != 1 {
			t.Errorf("WithLogger on a manual engine got %d records n=4 and %d records direct, want 1 and 1", n, m)
		}
		if recs := hd.records(); len(recs) != 0 {
			t.Errorf("slog.Default() got %d records, want 0", len(recs))
		}
	})
}

// spawnStartedManual spawns /user/a on the manual-engine system sys running started in its Started
// hook, and runs eng until it is idle.
func spawnStartedManual(t *testing.T, sys *lockstep.System, eng *enginetest.Engine, started func(ctx *lockstep.Context[int])) {
	t.Helper()
	ran := false
	_, err := lockstep.Spawn(sys, "a", func() lockstep.Actor[int] {
		return &simActor[int]{started: func(ctx *lockstep.Context[int]) {
			started(ctx)
			ran = true
		}}
	})
	if err != nil {
		t.Fatal(err)
	}
	eng.RunUntilIdle()
	if !ran {
		t.Fatal("Started did not run")
	}
}
