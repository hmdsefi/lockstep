// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io/fs"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/faultline/kernel/simdisk"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/persist"
	"github.com/hmdsefi/lockstep/persist/filestore"
	"github.com/hmdsefi/lockstep/sim"
)

// The counter on the simulated disk meets disk faults, misuse of the volume and leaked files.

// crashRestart crashes n1 a millisecond from now, restarts it a millisecond later, and runs until
// the restarted counter has had time to recover.
func (r *counterRun) crashRestart(n1 *kernel.Node) {
	w := r.s.World()
	now := w.Sim.Now()
	w.Sim.At(now.Add(time.Millisecond), "test/crash", n1.Crash)
	w.Sim.At(now.Add(2*time.Millisecond), "test/restart", n1.Restart)
	w.RunFor(10 * time.Millisecond)
}

// lastRecovery returns the Recovery of the counter's latest Recovered call.
func (r *counterRun) lastRecovery(tt testing.TB) persist.Recovery {
	tt.Helper()
	if len(r.recs) == 0 {
		tt.Fatal("the counter has not recovered")
	}
	return r.recs[len(r.recs)-1]
}

// segSize returns the size of the counter's first journal segment, as the volume shows it.
func segSize(tt testing.TB, s *sim.Sim) int {
	tt.Helper()
	b, err := vol(s).ReadFile(vseg(1))
	if err != nil {
		tt.Fatalf("ReadFile(%s): %v", vseg(1), err)
	}
	return len(b)
}

// failedCommit runs the counter of AT-PER-33 up to and including the failed commit of Add{2}: Add{1}
// commits, the next sync fails, Add{2} fails the counter, and the restarted counter has recovered.
func failedCommit(tt testing.TB, s *sim.Sim) (*counterRun, *kernel.Node) {
	tt.Helper()
	w := s.World()
	r := newCounterRun(s)
	n1 := r.node()
	w.RunFor(time.Millisecond)
	r.ask(tt, addCmd(1))
	r.mustHaveRecovered(tt)
	vol(s).FailSyncs(1)
	if _, err := sim.Ask(s, "n1", r.pid, addCmd(2)); !errors.Is(err, lockstep.ErrTimeout) {
		tt.Fatalf("Ask(Add{2}): %v, want ErrTimeout", err)
	}
	w.RunFor(50 * time.Millisecond)
	if len(r.errs) != 1 {
		tt.Fatalf("errs %v after the failed commit, want one error", r.errs)
	}
	return r, n1
}

// fullDisk limits n1's volume to ten bytes above its usage and asks AddMany{50}, whose record does
// not fit, then runs until the counter has failed.
func fullDisk(tt testing.TB, r *counterRun) {
	tt.Helper()
	v := vol(r.s)
	v.SetCapacity(v.Usage() + 10)
	if _, err := sim.Ask(r.s, "n1", r.pid, addManyCmd(50)); !errors.Is(err, lockstep.ErrTimeout) {
		tt.Fatalf("Ask(AddMany{50}): %v, want ErrTimeout", err)
	}
	r.s.World().RunFor(50 * time.Millisecond)
}

// AT-PER-29, PER-320: the first recovery writes the FORMAT file and creates journal and snapshots
// durably; a FORMAT file of another version fails recovery with filestore.ErrFormat, at the first
// boot and at every restart.
func TestATPER29FormatFile(t *testing.T) {
	t.Run("first recovery", func(t *testing.T) {
		sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
			w, tt := s.World(), s.World().T()
			r := newCounterRun(s)
			r.node()
			w.RunFor(time.Millisecond)
			r.mustHaveRecovered(tt)
			v := vol(s)
			if b, err := v.ReadDurable("/FORMAT"); err != nil || string(b) != "lockstep-filestore 1\n" {
				tt.Errorf("durable /FORMAT = %q, %v; want %q", b, err, "lockstep-filestore 1\n")
			}
			for _, dir := range []string{"/journal", "/snapshots"} {
				if _, err := v.ReadDir(dir); err != nil {
					tt.Errorf("ReadDir(%s): %v", dir, err)
				}
			}
			// The directories are durable: a crash loses neither.
			n1 := w.Sim.Lookup("n1")
			n1.Crash()
			w.RunFor(time.Millisecond)
			for _, dir := range []string{"/FORMAT", "/journal", "/snapshots"} {
				if _, err := v.Stat(dir); err != nil {
					tt.Errorf("Stat(%s) after a crash: %v", dir, err)
				}
			}
		})
	})
	t.Run("unknown version", func(t *testing.T) {
		sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
			w, tt := s.World(), s.World().T()
			r := newCounterRun(s)
			r.directive = r.restartUpTo(2)
			n1 := r.node()
			if err := vol(s).WriteFileDurable("/FORMAT", []byte("lockstep-filestore 2\n")); err != nil {
				tt.Fatalf("WriteFileDurable: %v", err)
			}
			w.RunFor(time.Second)
			r.crashRestart(n1)
			w.RunFor(time.Second)
			if len(r.errs) < 3 {
				tt.Fatalf("%d errors after the first boot, a restart of the actor and a restart of the node; want at least 3: %v",
					len(r.errs), r.errs)
			}
			for i, err := range r.errs {
				if !errors.Is(err, filestore.ErrFormat) {
					tt.Errorf("errs[%d] = %v, want filestore.ErrFormat", i, err)
				}
			}
			if len(r.recs) != 0 {
				tt.Errorf("Recovered ran %d times, want never", len(r.recs))
			}
			if b, _ := vol(s).ReadDurable("/FORMAT"); string(b) != "lockstep-filestore 2\n" {
				tt.Errorf("durable /FORMAT = %q, want it untouched", b)
			}
		})
	})
}

// AT-PER-33, PER-360: a failed sync leaves the segment at its committed length whichever model the
// disk applies to the failed sync's data; the next batch commits as the next seq, a restart
// recovers it without a repair, and a write that does not fit leaves the segment as it was.
func TestATPER33FailedSyncRepair(t *testing.T) {
	for _, model := range []simdisk.FailedSyncModel{simdisk.FailedSyncDropDirty, simdisk.FailedSyncKeepDirty} {
		t.Run(model.String(), func(t *testing.T) {
			opts := fullTrace(3)
			opts.Disk.FailedSync = model
			sim.Simulate(t, opts, func(s *sim.Sim) {
				tt := s.World().T()
				r, n1 := failedCommit(tt, s)
				if err := r.errs[0]; !strings.HasPrefix(err.Error(), "persist: c-1: commit seq 2..2: ") || !errors.Is(err, simdisk.ErrIO) {
					tt.Errorf("error %q (errors.Is ErrIO: %v), want the commit error of seq 2..2 wrapping simdisk.ErrIO",
						err, errors.Is(err, simdisk.ErrIO))
				}
				if ops := failOps(s); !slices.Equal(ops, []string{"commit"}) {
					tt.Errorf("lockstep.persist.fail ops %v, want [commit]", ops)
				}
				if n := segSize(tt, s); n != 31 {
					tt.Errorf("segment holds %d bytes after the failed commit, want 31", n)
				}
				if got := r.ask(tt, addCmd(3)); got != 4 {
					tt.Errorf("Add{3} replied %d, want 4", got)
				}
				if got := r.thenSeqs[len(r.thenSeqs)-1]; got != 2 {
					tt.Errorf("Add{3} committed as seq %d, want 2", got)
				}
				r.crashRestart(n1)
				if got, want := r.lastRecovery(tt), (persist.Recovery{Events: 2, LastSeq: 2}); got != want {
					tt.Errorf("recovery after the restart %+v, want %+v", got, want)
				}
				if got := r.ask(tt, getCmd); got != 4 {
					tt.Errorf("Get replied %d, want 4", got)
				}

				before := segSize(tt, s)
				if before != 62 {
					tt.Fatalf("segment holds %d bytes before the full disk, want 62", before)
				}
				fullDisk(tt, r)
				if len(r.errs) != 2 || !errors.Is(r.errs[1], simdisk.ErrNoSpace) {
					tt.Fatalf("errs %v, want a second error wrapping simdisk.ErrNoSpace", r.errs)
				}
				if n := segSize(tt, s); n != before {
					tt.Errorf("segment holds %d bytes after the full disk, want %d", n, before)
				}
			})
		})
	}
}

// corruptedRun runs three committed Add{1}, crashes n1 and applies damage to the volume while n1 is
// down; it restarts n1 with the parent stopping the counter at its first error, and returns the run.
func corruptedRun(tt testing.TB, s *sim.Sim, segmentSize int64, damage func(v *simdisk.Volume) error) *counterRun {
	tt.Helper()
	w := s.World()
	r := newCounterRun(s)
	r.directive = r.restartUpTo(1)
	if segmentSize > 0 {
		r.config = func(cfg *persist.Config[Added, CounterState]) {
			store := filestore.New(s.Volume("n1"), filestore.WithSegmentSize(segmentSize))
			cfg.Journal, cfg.Snapshots = store, store
		}
	}
	n1 := r.node()
	w.RunFor(time.Millisecond)
	for range 3 {
		r.ask(tt, addCmd(1))
	}
	now := w.Sim.Now()
	w.Sim.At(now.Add(time.Millisecond), "test/crash", n1.Crash)
	w.RunFor(2 * time.Millisecond)
	if err := damage(vol(s)); err != nil {
		tt.Fatalf("damage: %v", err)
	}
	w.Sim.At(w.Sim.Now().Add(time.Millisecond), "test/restart", n1.Restart)
	w.RunFor(50 * time.Millisecond)
	return r
}

// AT-PER-36, PER-370: a bad record in the middle of the journal, and a bad record of a segment that
// is not the last, are corruption; a bad last record is a torn tail that recovery repairs.
func TestATPER36Corruption(t *testing.T) {
	const corruptText = "filestore: corrupt journal/id-c-1/00000000000000000001.seg at offset 0: checksum mismatch: "
	t.Run("first record", func(t *testing.T) {
		sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
			tt := s.World().T()
			r := corruptedRun(tt, s, 0, func(v *simdisk.Volume) error { return v.Corrupt(vseg(1), 10, 1) })
			if len(r.errs) != 1 || !errors.Is(r.errs[0], filestore.ErrCorrupt) {
				tt.Fatalf("errs %v, want one error wrapping filestore.ErrCorrupt", r.errs)
			}
			if got := r.errs[0].Error(); !strings.Contains(got, corruptText) {
				tt.Errorf("error %q lacks %q", got, corruptText)
			}
			if len(r.recs) != 1 {
				tt.Errorf("Recovered ran %d times, want only the first boot's", len(r.recs))
			}
		})
	})
	t.Run("last record", func(t *testing.T) {
		sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
			tt := s.World().T()
			r := corruptedRun(tt, s, 0, func(v *simdisk.Volume) error { return v.Corrupt(vseg(1), 72, 1) })
			if len(r.errs) != 0 {
				tt.Fatalf("errs %v, want none", r.errs)
			}
			if got, want := r.lastRecovery(tt), (persist.Recovery{Events: 2, LastSeq: 2, Repaired: 31}); got != want {
				tt.Errorf("recovery %+v, want %+v", got, want)
			}
			if got := r.ask(tt, getCmd); got != 2 {
				tt.Errorf("Get replied %d, want 2", got)
			}
		})
	})
	t.Run("not the last segment", func(t *testing.T) {
		sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
			tt := s.World().T()
			r := corruptedRun(tt, s, 1, func(v *simdisk.Volume) error { return v.Corrupt(vseg(1), 10, 1) })
			if len(r.errs) != 1 || !errors.Is(r.errs[0], filestore.ErrCorrupt) {
				tt.Fatalf("errs %v, want one error wrapping filestore.ErrCorrupt", r.errs)
			}
		})
	})
}

// pathErrorOf returns the *fs.PathError of err's chain, or nil.
func pathErrorOf(err error) *fs.PathError {
	var pe *fs.PathError
	if errors.As(err, &pe) {
		return pe
	}
	return nil
}

// AT-PER-43, PER-440: in the failed commit of AT-PER-15 and the full disk of AT-PER-33, the persist
// error wraps the store's error, which wraps simdisk's *fs.PathError for the segment, which wraps
// simdisk.ErrIO or simdisk.ErrNoSpace.
func TestATPER43ErrorChain(t *testing.T) {
	t.Run("failed sync", func(t *testing.T) {
		sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
			tt := s.World().T()
			r, _ := failedCommit(tt, s)
			err := r.errs[0]
			if !strings.HasPrefix(err.Error(), "persist: c-1: commit seq 2..2: filestore: ") || !errors.Is(err, simdisk.ErrIO) {
				tt.Errorf("error %q (errors.Is ErrIO: %v), want persist and filestore texts around an I/O error",
					err, errors.Is(err, simdisk.ErrIO))
			}
			if errors.Is(err, simdisk.ErrNoSpace) {
				tt.Error("the failed sync matches simdisk.ErrNoSpace")
			}
			if pe := pathErrorOf(err); pe == nil || pe.Path != vseg(1) {
				tt.Errorf("error chain holds %#v, want a *fs.PathError for %s", pe, vseg(1))
			}
		})
	})
	t.Run("full disk", func(t *testing.T) {
		sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
			w, tt := s.World(), s.World().T()
			r := newCounterRun(s)
			r.node()
			w.RunFor(time.Millisecond)
			r.ask(tt, addCmd(1))
			fullDisk(tt, r)
			if len(r.errs) != 1 {
				tt.Fatalf("errs %v, want one error", r.errs)
			}
			err := r.errs[0]
			if !strings.HasPrefix(err.Error(), "persist: c-1: write seq 2..51: filestore: ") || !errors.Is(err, simdisk.ErrNoSpace) {
				tt.Errorf("error %q (errors.Is ErrNoSpace: %v), want persist and filestore texts around a no-space error",
					err, errors.Is(err, simdisk.ErrNoSpace))
			}
			if errors.Is(err, simdisk.ErrIO) {
				tt.Error("the full disk matches simdisk.ErrIO")
			}
			if pe := pathErrorOf(err); pe == nil || pe.Path != vseg(1) {
				tt.Errorf("error chain holds %#v, want a *fs.PathError for %s", pe, vseg(1))
			}
		})
	})
}

// segmentOf returns the bytes of a journal segment holding the single batch of seq first with the
// given JSON events, in the on-disk format: length, CRC-32C of the payload, first seq, count and
// each event with its length.
func segmentOf(first uint64, events ...string) []byte {
	var payload []byte
	payload = binary.LittleEndian.AppendUint64(payload, first)
	payload = binary.LittleEndian.AppendUint32(payload, uint32(len(events))) //nolint:gosec // a handful of events
	for _, e := range events {
		payload = binary.LittleEndian.AppendUint32(payload, uint32(len(e))) //nolint:gosec // short event
		payload = append(payload, e...)
	}
	var out []byte
	out = binary.LittleEndian.AppendUint32(out, uint32(len(payload))) //nolint:gosec // short record
	out = binary.LittleEndian.AppendUint32(out, crc32.Checksum(payload, crc32.MakeTable(crc32.Castagnoli)))
	return append(out, payload...)
}

// AT-PER-44, PER-500: a node's Volume is one object for the whole run, a file the store wrote in
// one incarnation is there for the next, and an unknown node panics.
func TestATPER44VolumeAcrossIncarnations(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		r := newCounterRun(s)
		n1 := r.node()
		if s.Volume("n1") != s.Volume("n1") {
			tt.Error("Volume(n1) returned two different values after the node was added")
		}
		w.RunFor(time.Millisecond)
		r.ask(tt, addCmd(1))
		want := segmentOf(1, `{"N":1}`)
		if got, err := vol(s).ReadFile(vseg(1)); err != nil || !bytes.Equal(got, want) {
			tt.Errorf("segment = %x, %v; want %x", got, err, want)
		}
		r.crashRestart(n1)
		if got, want := r.lastRecovery(tt), (persist.Recovery{Events: 1, LastSeq: 1}); got != want {
			tt.Errorf("recovery in incarnation 2 %+v, want %+v", got, want)
		}
		if got := r.ask(tt, getCmd); got != 1 {
			tt.Errorf("Get replied %d, want 1", got)
		}
		if s.Volume("n1") != s.Volume("n1") {
			tt.Error("Volume(n1) returned two different values after a restart")
		}

		const wantPanic = `sim: Volume: unknown node "nope"`
		func() {
			defer func() {
				if got := recover(); got != wantPanic {
					tt.Errorf("Volume(nope) panicked with %v, want %q", got, wantPanic)
				}
			}()
			s.Volume("nope")
			tt.Error("Volume(nope) did not panic")
		}()
	})
}

var (
	// n1's counter is built on n2's volume.
	_ = scenario("atper45-other-node", func(t *testing.T) {
		sim.Simulate(t, fullTrace(1), func(s *sim.Sim) {
			w := s.World()
			s.Node("n2", noSetup)
			r := newCounterRun(s)
			r.directive = r.restartUpTo(1)
			r.config = func(cfg *persist.Config[Added, CounterState]) {
				store := filestore.New(s.Volume("n2"))
				cfg.Journal, cfg.Snapshots = store, store
			}
			r.node()
			w.RunFor(time.Second)
		})
	})
	// A world event opens a file on n1's volume while the counter runs.
	_ = scenario("atper45-outside", func(t *testing.T) {
		sim.Simulate(t, fullTrace(1), func(s *sim.Sim) {
			w := s.World()
			r := newCounterRun(s)
			r.node()
			w.Sim.After(5*time.Millisecond, "test/volume", func() {
				if _, err := s.Volume("n1").Open("FORMAT"); !errors.Is(err, fs.ErrPermission) {
					panic(fmt.Sprintf("Open returned %v, want fs.ErrPermission", err))
				}
			})
			w.RunFor(time.Second)
		})
	})
)

// AT-PER-45, PER-510: a counter whose store sits on another node's volume, and a world event that
// uses a node's volume, fail the seed with check lockstep/misuse after a lockstep.misuse record.
func TestATPER45VolumeMisuse(t *testing.T) {
	for _, tc := range []struct {
		name, scenario, message string
	}{
		{"other node", "atper45-other-node", `sim: volume of node "n2" used from node "n1"`},
		{"outside a node event", "atper45-outside", `sim: volume of node "n1" used outside a node event`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, _ := failingSeedArtifact(t, tc.scenario)
			f := a.Report.Failure
			got := failure{Kind: f.Kind, Check: f.Check, Signature: f.Signature, Message: f.Message}
			want := failure{Kind: "invariant", Check: sim.CheckMisuse, Signature: "invariant:lockstep/misuse", Message: tc.message}
			if got != want {
				t.Errorf("failure %+v, want %+v", got, want)
			}
			recs := misuseRecords(t, a)
			if len(recs) == 0 || attr(recs[0], "error") != tc.message {
				t.Errorf("lockstep.misuse records %+v, want the first to carry %q", recs, tc.message)
			}
		})
	}
}

// leakJournal is a Journal around Journal whose Write also creates the file extra on n1's volume
// and keeps it open.
type leakJournal struct {
	persist.Journal
	s *sim.Sim
}

func (j *leakJournal) Write(ctx context.Context, id string, events []persist.Event) error {
	if _, err := j.s.Volume("n1").Create("extra"); err != nil {
		return err
	}
	return j.Journal.Write(ctx, id, events)
}

// leakRun runs the counter, with a leakJournal if leaky and with a file created and left open in
// its setup if setupFile, and adds 1. It checks that the files exist, which shows that they were
// opened: neither is ever closed. end then ends the run: it gets the run and the node, and may
// fail, stop or crash the counter.
func leakRun(s *sim.Sim, leaky, setupFile bool, end func(r *counterRun, n1 *kernel.Node)) {
	w := s.World()
	r := newCounterRun(s)
	r.config = func(cfg *persist.Config[Added, CounterState]) {
		if setupFile {
			if _, err := s.Volume("n1").Create("setup"); err != nil {
				panic(err)
			}
		}
		if leaky {
			cfg.Journal = &leakJournal{Journal: cfg.Journal, s: s}
		}
	}
	n1 := r.node()
	w.RunFor(time.Millisecond)
	if got := r.ask(w.T(), addCmd(1)); got != 1 {
		w.T().Fatalf("Add{1} replied %d, want 1", got)
	}
	for _, f := range []struct {
		path string
		open bool
	}{{"/extra", leaky}, {"/setup", setupFile}} {
		if _, err := vol(s).Stat(f.path); (err == nil) != f.open {
			w.T().Fatalf("Stat(%s) before the end = %v; want the file to exist: %v", f.path, err, f.open)
		}
	}
	end(r, n1)
	w.RunFor(time.Second)
}

// leakWant is the leak failure of file /extra for the counter of r in its first incarnation.
func leakWant(r *counterRun) string {
	return "lockstep: leak: " + r.pid.String() + " (incarnation 1): file /extra on node n1 still open"
}

// leakMarker is the prefix of the line a leak scenario prints with the failure it expects.
const leakMarker = "LEAK WANT "

// injectLeak is the scenario of AT-PER-48: the counter's incarnation fails with the file open.
func injectLeak(t *testing.T) {
	sim.Simulate(t, fullTrace(1), func(s *sim.Sim) {
		leakRun(s, true, false, func(r *counterRun, _ *kernel.Node) {
			fmt.Println(leakMarker + leakWant(r))
			if err := s.InjectFail(r.pid.Address(), errors.New("injected")); err != nil {
				panic(err)
			}
		})
	})
}

var (
	_ = scenario("atper48-inject", injectLeak)
	_ = scenario("atsim35-inject", injectLeak)
)

// leakExpectation returns the failure the scenario printed in out.
func leakExpectation(t *testing.T, out string) string {
	t.Helper()
	for line := range strings.SplitSeq(out, "\n") {
		if want, ok := strings.CutPrefix(line, leakMarker); ok {
			return want
		}
	}
	t.Fatalf("the scenario printed no %q line:\n%s", leakMarker, out)
	return ""
}

// checkLeakRecords checks that the trace holds a lockstep.leak record with what=file and the
// message as its error text, before the failure.
func checkLeakRecords(t *testing.T, recs []kernel.Record, message string) {
	t.Helper()
	leaks := ofKind(recs, "lockstep.leak")
	if len(leaks) != 1 || attr(leaks[0], "what") != "file" || attr(leaks[0], "error") != message {
		t.Errorf("lockstep.leak records %+v, want one with what=file and error %q", leaks, message)
	}
}

// AT-PER-48, PER-540: a file a journal wrapper opens in Write and keeps open fails the seed with
// check lockstep/leak when the incarnation fails; a crash of the node, or a file opened in setup,
// fails nothing.
func TestATPER48FileLeak(t *testing.T) {
	t.Run("inject fail", func(t *testing.T) {
		a, out := failingSeedArtifact(t, "atper48-inject")
		want := leakExpectation(t, out)
		f := a.Report.Failure
		if f.Kind != "invariant" || f.Check != sim.CheckLeak || f.Message != want {
			t.Errorf("failure %+v, want invariant lockstep/leak with message %q", f, want)
		}
		if a.Trace == nil {
			t.Fatal("the failing seed left no trace")
		}
		checkLeakRecords(t, a.Trace.Records, want)
	})
	t.Run("crash", func(t *testing.T) {
		sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
			leakRun(s, true, false, func(_ *counterRun, n1 *kernel.Node) {
				n1.Crash()
				s.World().RunFor(time.Millisecond)
				n1.Restart()
			})
		})
	})
	t.Run("setup", func(t *testing.T) {
		sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
			leakRun(s, false, true, func(r *counterRun, _ *kernel.Node) {
				if err := s.InjectFail(r.pid.Address(), errors.New("injected")); err != nil {
					panic(err)
				}
			})
		})
	})
}

// AT-SIM-35, SIM-290 (c): the leaked file of AT-PER-48 fails the seed as an invariant with check
// lockstep/leak and signature invariant:lockstep/leak.
func TestATSIM35FileLeak(t *testing.T) {
	a, out := failingSeedArtifact(t, "atsim35-inject")
	want := leakExpectation(t, out)
	f := a.Report.Failure
	got := failure{Kind: f.Kind, Check: f.Check, Signature: f.Signature, Message: f.Message}
	wantF := failure{Kind: "invariant", Check: sim.CheckLeak, Signature: "invariant:lockstep/leak", Message: want}
	if got != wantF {
		t.Errorf("failure %+v, want %+v", got, wantF)
	}
	if a.Trace == nil {
		t.Fatal("the failing seed left no trace")
	}
	checkLeakRecords(t, a.Trace.Records, want)
}
