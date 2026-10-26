// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim_test

import (
	"errors"
	"fmt"
	"io/fs"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/faultline/kernel/simdisk"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/internal/hooks"
	"github.com/hmdsefi/lockstep/persist/filestore"
	"github.com/hmdsefi/lockstep/sim"
)

// spawnInline spawns /user/a from sys; its first incarnation starts one inline internal effect
// with run from Started. done, if not nil, runs as the effect's Done.
func spawnInline(sys *lockstep.System, run func() (any, error), done func(ctx any, v any, err error)) lockstep.PID[PMsg] {
	if done == nil {
		done = func(any, any, error) {}
	}
	starts := 0
	return spawnControlled(sys, "a", &controlled[PMsg]{started: func(ctx *lockstep.Context[PMsg]) {
		starts++
		if starts == 1 {
			hooks.Control(ctx).StartInternal(inline(run, done))
		}
	}})
}

// isPathError reports whether err is a *fs.PathError with op, path and the sentinel target.
func isPathError(err error, op, path string, target error) bool {
	var pe *fs.PathError
	return errors.As(err, &pe) && pe.Op == op && pe.Path == path && pe.Err == target //nolint:errorlint // the exact sentinel
}

// mustFile panics unless err is nil; it returns f. It runs in kernel callbacks, where a panic
// fails the seed.
func mustFile(f filestore.File, err error) filestore.File {
	if err != nil {
		panic(err)
	}
	return f
}

// must panics if err is not nil.
func must(err error) {
	if err != nil {
		panic(err)
	}
}

// PER-500, AT-PER-44: Volume returns one FS per node over its simdisk volume, mapping x to /x and
// . to /; Info comes from simdisk, simdisk's errors pass through, a handle crashed away reports
// its size as closed, the Volume's own name check uses the FS method name, and an unknown node
// panics.
func TestPER500VolumeMapping(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		boots := 0
		var kept filestore.File
		var invalidErr, backslashErr, missingErr, sizeErr error
		var root, sub []filestore.Info
		size := int64(-2)
		n1 := s.Node("n1", func(*lockstep.System) {
			boots++
			fsys := s.Volume("n1")
			if boots > 1 {
				size, sizeErr = kept.Size()
				return
			}
			must(fsys.MkdirAll("d"))
			kept = mustFile(fsys.Create("d/x"))
			if _, err := kept.WriteAt([]byte("hello"), 0); err != nil {
				panic(err)
			}
			_, invalidErr = fsys.Create("../x")
			_, backslashErr = fsys.Create(`d\x`)
			_, missingErr = fsys.Open("missing")
			var err error
			if root, err = fsys.ReadDir("."); err != nil {
				panic(err)
			}
			if sub, err = fsys.ReadDir("d"); err != nil {
				panic(err)
			}
		})
		if s.Volume("n1") != s.Volume("n1") {
			tt.Error("two Volume calls for n1 returned different values")
		}
		expectPanic(tt, `sim: Volume: unknown node "nope"`, func() { s.Volume("nope") })
		w.RunFor(time.Millisecond)

		vol := w.Disk.Volume(n1)
		if b, err := vol.ReadFile("/d/x"); err != nil || string(b) != "hello" {
			tt.Errorf("volume /d/x = %q, %v; want hello", b, err)
		}
		info := func(i simdisk.Info) filestore.Info { return filestore.Info{Name: i.Name, Size: i.Size, IsDir: i.IsDir} }
		vroot, err := vol.ReadDir("/")
		if err != nil {
			tt.Fatal(err)
		}
		if want := []filestore.Info{info(vroot[0])}; len(vroot) != 1 || !slices.Equal(root, want) {
			tt.Errorf("ReadDir(.) = %v, want %v from the volume root", root, vroot)
		}
		st, err := vol.Stat("/d/x")
		if err != nil {
			tt.Fatal(err)
		}
		if want := []filestore.Info{info(st)}; !slices.Equal(sub, want) || sub[0].Size != 5 {
			tt.Errorf("ReadDir(d) = %v, want %v", sub, want)
		}
		if !isPathError(invalidErr, "Create", "../x", fs.ErrInvalid) {
			tt.Errorf("Create(../x) error %#v, want a *fs.PathError Create ../x fs.ErrInvalid", invalidErr)
		}
		if !isPathError(backslashErr, "Create", `d\x`, fs.ErrInvalid) {
			tt.Errorf("Create(d\\x) error %#v, want a *fs.PathError Create d\\x fs.ErrInvalid", backslashErr)
		}
		if !isPathError(missingErr, "open", "/missing", fs.ErrNotExist) {
			tt.Errorf("Open(missing) error %#v, want simdisk's *fs.PathError open /missing fs.ErrNotExist", missingErr)
		}

		// The handle of incarnation 1 is stale after the crash; the restarted node's setup asks
		// for its size.
		n1.Crash()
		n1.Restart()
		w.RunFor(time.Millisecond)
		if boots != 2 || size != 0 || !isPathError(sizeErr, "Size", "/d/x", fs.ErrClosed) {
			tt.Errorf("after %d boots, Size of the crashed-away handle = %d, %#v; want 0 and a *fs.PathError Size /d/x fs.ErrClosed",
				boots, size, sizeErr)
		}
	})
}

// SIM-040: the FS of a Volume and its files panic once the run of their Sim has ended, as the
// Sim's own methods do.
func TestVolumeAfterRunPanics(t *testing.T) {
	var fsys filestore.FS
	var f filestore.File
	sim.Simulate(t, fullTrace(1), func(s *sim.Sim) {
		s.Node("n1", func(*lockstep.System) {
			fsys = s.Volume("n1")
			f = mustFile(fsys.Create("a"))
			must(f.Close())
		})
		s.World().RunFor(time.Millisecond)
	})
	const want = "lockstep: sim: Sim used after its run ended"
	expectPanic(t, want, func() { _, _ = fsys.Open("a") })
	expectPanic(t, want, func() { _, _ = f.Size() })
}

// The markers the misuse scenarios print when the refused call returned the expected error.
const (
	otherNodeRefused = "VOLUME OF n2 REFUSED"
	outsideRefused   = "VOLUME OUTSIDE A NODE EVENT REFUSED"
)

var (
	// An inline run on n1 uses n2's volume.
	_ = scenario("per510-other-node", func(t *testing.T) {
		sim.Simulate(t, fullTrace(1), func(s *sim.Sim) {
			w := s.World()
			n2 := s.Node("n2", noSetup)
			s.Node("n1", func(sys *lockstep.System) {
				spawnInline(sys, func() (any, error) {
					_, err := s.Volume("n2").Create("x")
					_, rerr := w.Disk.Volume(n2).ReadFile("/x")
					if isPathError(err, "Create", "x", fs.ErrPermission) && errors.Is(rerr, fs.ErrNotExist) {
						fmt.Println(otherNodeRefused)
					}
					return nil, nil
				}, nil)
			})
			w.RunFor(time.Second)
		})
	})
	// A world event uses n1's volume.
	_ = scenario("per510-outside", func(t *testing.T) {
		sim.Simulate(t, fullTrace(1), func(s *sim.Sim) {
			w := s.World()
			s.Node("n1", noSetup)
			w.Sim.After(time.Millisecond, "test/volume", func() {
				if _, err := s.Volume("n1").Open("FORMAT"); isPathError(err, "Open", "FORMAT", fs.ErrPermission) {
					fmt.Println(outsideRefused)
				}
			})
			w.RunFor(time.Second)
		})
	})
)

// PER-510, AT-PER-45: a volume used from another node's event, or outside any node event, fails
// the seed with check lockstep/misuse after a lockstep.misuse record, and the call returns a
// *fs.PathError with fs.ErrPermission without touching the volume.
func TestPER510VolumeMisuse(t *testing.T) {
	for _, tc := range []struct {
		name, scenario, marker, message string
		node                            kernel.NodeID
	}{
		{"other node", "per510-other-node", otherNodeRefused, `sim: volume of node "n2" used from node "n1"`, 2},
		{"outside a node event", "per510-outside", outsideRefused, `sim: volume of node "n1" used outside a node event`, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, out := failingSeedArtifact(t, tc.scenario)
			f := a.Report.Failure
			got := failure{Kind: f.Kind, Check: f.Check, Signature: f.Signature, Message: f.Message}
			want := failure{Kind: "invariant", Check: sim.CheckMisuse, Signature: "invariant:lockstep/misuse", Message: tc.message}
			if got != want {
				t.Errorf("failure %+v, want %+v", got, want)
			}
			if !strings.Contains(out, tc.marker) {
				t.Errorf("output lacks %q:\n%s", tc.marker, out)
			}
			recs := misuseRecords(t, a)
			if len(recs) != 1 || recs[0].Node != tc.node || attr(recs[0], "error") != tc.message {
				t.Errorf("lockstep.misuse records %+v, want one of node %d with the failure's text", recs, tc.node)
			}
		})
	}
}

// writeSync is an inline run that writes and syncs a file through n1's volume and asks for its
// size, which samples no latency.
func writeSync(s *sim.Sim) func() (any, error) {
	return func() (any, error) {
		fsys := s.Volume("n1")
		f := mustFile(fsys.Create("a"))
		if _, err := f.WriteAt([]byte("x"), 0); err != nil {
			return nil, err
		}
		if _, err := f.Size(); err != nil {
			return nil, err
		}
		if err := f.Sync(); err != nil {
			return nil, err
		}
		return nil, f.Close()
	}
}

// PER-530, PER-520: an inline run's completion event comes the run's sampled disk latency after
// the run: WriteAt 1ms plus Sync 2ms, with Size adding nothing. Without latency it comes at the
// run's time, and Volume calls outside inline runs add nothing.
func TestPER530InlineLatency(t *testing.T) {
	slow := fullTrace(3)
	slow.Disk.Latency = simdisk.LatencyConfig{
		Write: simdisk.Latency{Base: time.Millisecond},
		Sync:  simdisk.Latency{Base: 2 * time.Millisecond},
	}
	for _, tc := range []struct {
		name  string
		opts  sim.Options
		setup bool // n1's setup writes and syncs a file through the volume
		run   func(s *sim.Sim) func() (any, error)
		want  time.Duration
	}{
		{"write and sync", slow, false, writeSync, 3 * time.Millisecond},
		{"zero latency", fullTrace(3), false, writeSync, 0},
		{"setup calls", slow, true, func(*sim.Sim) func() (any, error) { return nil }, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sim.Simulate(t, tc.opts, func(s *sim.Sim) {
				w, tt := s.World(), s.World().T()
				var runAt, doneAt kernel.Time
				var doneErr error
				n1 := s.Node("n1", func(sys *lockstep.System) {
					if tc.setup {
						if _, err := writeSync(s)(); err != nil {
							panic(err)
						}
					}
					run := tc.run(s)
					spawnInline(sys, func() (any, error) {
						runAt = w.Sim.Now()
						if run == nil {
							return nil, nil
						}
						return run()
					}, func(_ any, _ any, err error) { doneAt, doneErr = w.Sim.Now(), err })
				})
				w.RunFor(time.Second)
				if doneErr != nil {
					tt.Fatalf("Done got %v", doneErr)
				}
				var effects []kernel.Time
				for _, r := range w.Sim.Records() {
					if r.Kind == "kernel.event" && r.Node == n1.ID() && r.Text == "lockstep/effect" {
						effects = append(effects, r.At)
					}
				}
				if len(effects) != 2 || effects[0] != runAt || effects[1].Sub(runAt) != tc.want || doneAt != effects[1] {
					tt.Errorf("lockstep/effect events at %v, Run at %v, Done at %v; want the completion %v after the run",
						effects, runAt, doneAt, tc.want)
				}
			})
		})
	}
}

// leakOpen is an inline run that creates /extra and /a through n1's volume and keeps them open.
func leakOpen(s *sim.Sim) func() (any, error) {
	return func() (any, error) {
		fsys := s.Volume("n1")
		mustFile(fsys.Create("extra"))
		mustFile(fsys.Create("a"))
		return nil, nil
	}
}

var (
	// The incarnation that opened the files fails.
	_ = scenario("per540-restart", func(t *testing.T) {
		sim.Simulate(t, fullTrace(1), func(s *sim.Sim) {
			w := s.World()
			var a lockstep.PID[PMsg]
			s.Node("n1", func(sys *lockstep.System) { a = spawnInline(sys, leakOpen(s), nil) })
			w.RunFor(time.Millisecond)
			must(s.InjectFail(a.Address(), errors.New("boom")))
			w.RunFor(time.Second)
		})
	})
	// The actor opens a file in its own turn and is stopped.
	_ = scenario("per540-stop", func(t *testing.T) {
		sim.Simulate(t, fullTrace(1), func(s *sim.Sim) {
			w := s.World()
			var a lockstep.PID[PMsg]
			s.Node("n1", func(sys *lockstep.System) {
				a = spawnControlled(sys, "a", &controlled[PMsg]{started: func(*lockstep.Context[PMsg]) {
					mustFile(s.Volume("n1").Create("a"))
				}})
			})
			w.RunFor(time.Millisecond)
			must(s.StopActor(a.Address()))
			w.RunFor(time.Second)
		})
	})
)

// leakFile matches the leak failure of file /a of n1's /user/a in incarnation 1.
var leakFile = regexp.MustCompile(`^lockstep: leak: lockstep://n1/user/a#[0-9a-f]+ \(incarnation 1\): file /a on node n1 still open$`)

// PER-540, SIM-290 (c), AT-PER-48: a file still open when the incarnation that opened it ends by
// restart or stop fails the seed with check lockstep/leak, files in path order, after the
// lockstep.leak records of every such file.
func TestPER540FileLeak(t *testing.T) {
	t.Run("restart", func(t *testing.T) {
		a, _ := failingSeedArtifact(t, "per540-restart")
		f := a.Report.Failure
		if f.Kind != "invariant" || f.Check != sim.CheckLeak || f.Signature != "invariant:lockstep/leak" || !leakFile.MatchString(f.Message) {
			t.Errorf("failure %+v, want invariant lockstep/leak matching %s", f, leakFile)
		}
		if a.Trace == nil {
			t.Fatal("the failing seed left no trace")
		}
		var kinds, errs []string
		for _, r := range a.Trace.Records {
			switch r.Kind {
			case "lockstep.restart":
				kinds = append(kinds, r.Kind)
			case "lockstep.leak":
				kinds = append(kinds, r.Kind+" "+attr(r, "what")+" "+attr(r, "inc"))
				errs = append(errs, attr(r, "error"))
			}
		}
		if want := []string{"lockstep.restart", "lockstep.leak file 1", "lockstep.leak file 1"}; !slices.Equal(kinds, want) {
			t.Errorf("records %q, want %q", kinds, want)
		}
		if len(errs) != 2 || errs[0] != f.Message || !strings.HasSuffix(errs[1], ": file /extra on node n1 still open") {
			t.Errorf("lockstep.leak errors %q, want the failure for /a, then /extra", errs)
		}
	})
	t.Run("stop", func(t *testing.T) {
		f := failingSeed(t, "per540-stop")
		if f.Kind != "invariant" || f.Check != sim.CheckLeak || !leakFile.MatchString(f.Message) {
			t.Errorf("failure %+v, want invariant lockstep/leak matching %s", f, leakFile)
		}
	})
	t.Run("crash", func(t *testing.T) {
		sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
			w := s.World()
			n1 := s.Node("n1", func(sys *lockstep.System) { spawnInline(sys, leakOpen(s), nil) })
			w.RunFor(time.Millisecond)
			n1.Crash()
			w.RunFor(time.Millisecond)
			n1.Restart()
			w.RunFor(time.Second)
		})
	})
	t.Run("setup", func(t *testing.T) {
		sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
			w := s.World()
			var a lockstep.PID[PMsg]
			s.Node("n1", func(sys *lockstep.System) {
				mustFile(s.Volume("n1").Create("setup"))
				a = spawnControlled(sys, "a", &controlled[PMsg]{})
			})
			w.RunFor(time.Millisecond)
			must(s.InjectFail(a.Address(), errors.New("boom")))
			w.RunFor(time.Millisecond)
			must(s.StopActor(a.Address()))
			w.RunFor(time.Second)
		})
	})
	t.Run("close error", func(t *testing.T) {
		sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
			w, tt := s.World(), s.World().T()
			var a lockstep.PID[PMsg]
			var closeErr error
			s.Node("n1", func(sys *lockstep.System) {
				a = spawnInline(sys, func() (any, error) {
					f := mustFile(s.Volume("n1").Create("a"))
					must(sim.CloseUnderlying(f))
					closeErr = f.Close()
					return nil, nil
				}, nil)
			})
			w.RunFor(time.Millisecond)
			if !errors.Is(closeErr, fs.ErrClosed) {
				tt.Fatalf("Close of a closed handle returned %v, want fs.ErrClosed", closeErr)
			}
			must(s.InjectFail(a.Address(), errors.New("boom")))
			w.RunFor(time.Second)
		})
	})
}
