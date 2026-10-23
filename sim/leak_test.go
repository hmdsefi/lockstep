// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim_test

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/sim"
)

// failCloser is a resource whose Close fails.
type failCloser struct{}

func (failCloser) Close() error { return errors.New("close failed") }

// ownFailing spawns /user/a on n1, which owns a failCloser from Started, and returns its PID.
func ownFailing(sys *lockstep.System) lockstep.PID[Deposit] {
	return spawn(sys, "a", &actor[Deposit]{started: func(ctx *lockstep.Context[Deposit]) { ctx.Own(failCloser{}) }})
}

var (
	// The actor is stopped: its resource fails to close.
	_ = scenario("leak-close-failure", func(t *testing.T) {
		sim.Simulate(t, fullTrace(1), func(s *sim.Sim) {
			w, tt := s.World(), s.World().T()
			var a lockstep.PID[Deposit]
			s.Node("n1", func(sys *lockstep.System) { a = ownFailing(sys) })
			w.RunFor(time.Second)
			if err := lockstep.Stop(s.System("n1"), a.Address()); err != nil {
				tt.Fatalf("Stop: %v", err)
			}
			w.RunFor(time.Second)
		})
	})
	// The actor is lost to a node crash: nothing is closed, nothing is checked.
	_ = scenario("leak-close-crash", func(t *testing.T) {
		sim.Simulate(t, fullTrace(1), func(s *sim.Sim) {
			w := s.World()
			n1 := s.Node("n1", func(sys *lockstep.System) { ownFailing(sys) })
			w.RunFor(time.Second)
			n1.Crash()
			w.RunFor(time.Second)
		})
	})
)

// AT-SIM-25, SIM-290 (a): a resource that fails to close fails the seed with check lockstep/leak,
// unless the actor was lost to a node crash.
func TestATSIM25CloseFailure(t *testing.T) {
	t.Run("stopped", func(t *testing.T) {
		got := failingSeed(t, "leak-close-failure")
		if got.Kind != "invariant" || got.Check != sim.CheckLeak || got.Signature != "invariant:lockstep/leak" {
			t.Errorf("failure %+v, want invariant lockstep/leak", got)
		}
		want := regexp.MustCompile(`^lockstep: leak: lockstep://n1/user/a#[0-9a-f]+ \(incarnation 1\): resource 0 \(sim_test\.failCloser\) failed to close: close failed$`)
		if !want.MatchString(got.Message) {
			t.Errorf("message %q does not match %s", got.Message, want)
		}
	})
	t.Run("node crash", func(t *testing.T) {
		if out, code := runScenario(t, "leak-close-crash"); code != 0 {
			t.Errorf("a crashed actor failed the seed (exit %d)\n%s", code, out)
		}
	})
}

// SIM-290, SIM §8: the failure is emitted as one lockstep.leak record with the actor, the
// incarnation, what leaked and the failure's text, right after the lockstep.close it is about.
func TestSIM290LeakRecord(t *testing.T) {
	dir := t.TempDir()
	runScenario(t, "leak-close-failure", "FAULTLINE_ARTIFACTS="+dir)
	var trace string
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err == nil && d.Name() == "trace.jsonl" {
			b, rerr := os.ReadFile(path) //nolint:gosec // reads the trace the child wrote to a temp dir
			trace, err = string(b), rerr
		}
		return err
	})
	if err != nil || trace == "" {
		t.Fatalf("no trace.jsonl under %s: %v", dir, err)
	}
	var kinds []string
	var leak string
	for _, line := range strings.Split(trace, "\n") {
		for _, kind := range []string{"lockstep.close", "lockstep.leak", "lockstep.stop"} {
			if strings.Contains(line, `"kind":"`+kind+`"`) {
				kinds = append(kinds, kind)
				if kind == "lockstep.leak" {
					leak = line
				}
			}
		}
	}
	if got := strings.Join(kinds, " "); got != "lockstep.close lockstep.leak lockstep.stop" {
		t.Errorf("records %q, want close, leak, stop in that order", got)
	}
	for _, want := range []string{`"text":"leak lockstep://n1/user/a#`, `["inc","1"]`, `["what","resource"]`,
		`["error","lockstep: leak: lockstep://n1/user/a#`, `failed to close: close failed"]`} {
		if !strings.Contains(leak, want) {
			t.Errorf("leak record %s lacks %s", leak, want)
		}
	}
}
