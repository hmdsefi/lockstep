// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"slices"
	"testing"
	"time"

	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/faultline/kernel/fault"
	"github.com/hmdsefi/faultline/kernel/simdisk"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/internal/hooks"
	"github.com/hmdsefi/lockstep/sim"
)

// Crash durability and inline timing of persistent actors on the simulated disk (PER-150,
// PER-160, PER-350, PER-370, PER-520, PER-530, PER §6).

// seedRange returns the seeds from 0 to n-1, for SimulateSeeds.
func seedRange(n int) []uint64 {
	out := make([]uint64, n)
	for i := range out {
		out[i] = uint64(i)
	}
	return out
}

// crashCapture is what the crash scheduled by crashAtRecord saw: whether it ran, and the visible
// and the durable content of each of its paths, read just before and just after the crash.
type crashCapture struct {
	fired            bool
	visible, durable [][]byte
}

// crashAtRecord is the crashAt fixture of PER §10. It registers an OnEvent observer that passes
// every record emitted from now on to match, once and in order, so match may keep state ("after
// the first …"). The first time match returns true it schedules, d later, an event that captures
// vol.ReadFile of paths, crashes n1, captures vol.ReadDurable of paths and schedules n1's restart
// 1 ms later. A path that does not exist is captured as nil; any other read error fails the test.
func crashAtRecord(s *sim.Sim, n1 *kernel.Node, match func(kernel.Record) bool, d time.Duration, paths ...string) *crashCapture {
	w := s.World()
	c := &crashCapture{}
	scanned, armed := len(w.Sim.Records()), false
	w.Sim.OnEvent(func() {
		if armed {
			return
		}
		recs := w.Sim.Records()
		for ; scanned < len(recs) && !armed; scanned++ {
			armed = match(recs[scanned])
		}
		if !armed {
			return
		}
		w.Sim.After(d, "test/crash", func() {
			v := vol(s)
			capture := func(op, p string, b []byte, err error) []byte {
				if err != nil && !errors.Is(err, fs.ErrNotExist) {
					w.T().Errorf("%s(%s) at the crash: %v", op, p, err)
				}
				return b
			}
			for _, p := range paths {
				b, err := v.ReadFile(p)
				c.visible = append(c.visible, capture("ReadFile", p, b, err))
			}
			n1.Crash()
			for _, p := range paths {
				b, err := v.ReadDurable(p)
				c.durable = append(c.durable, capture("ReadDurable", p, b, err))
			}
			c.fired = true
			w.Sim.After(time.Millisecond, "test/restart", n1.Restart)
		})
	})
	return c
}

// diskRecord matches the simdisk records of kind for the volume path p.
func diskRecord(kind, p string) func(kernel.Record) bool {
	return func(r kernel.Record) bool { return r.Kind == kind && attr(r, "path") == p }
}

// afterFirst matches the first record that then matches after the first record of kind first.
func afterFirst(first string, then func(kernel.Record) bool) func(kernel.Record) bool {
	seen := false
	return func(r kernel.Record) bool {
		if seen && then(r) {
			return true
		}
		seen = seen || r.Kind == first
		return false
	}
}

// AT-PER-16, PER-150: under random crashes of n1, every Add a client got a reply for is in the
// sum that a Get asked in the recovery window replies. Values are distinct bits, so the sum shows
// exactly which events were recovered. Without disk latency every batch is written and committed
// at the instant its command arrives, so a crash never finds one in flight; the second run adds
// simdisk's default latency model, under which crashes cut writes and commits.
func TestATPER16AckedSurvive(t *testing.T) {
	t.Run("zero-latency", func(t *testing.T) {
		crashes, lost, _ := atper16Sweep(t, simdisk.LatencyConfig{})
		t.Logf("%d crashes of n1; %d seeds left an Add unacknowledged", crashes, lost)
		if crashes == 0 || lost == 0 {
			t.Errorf("%d crashes and %d seeds with an unacknowledged Add; want both above 0", crashes, lost)
		}
	})
	t.Run("default-latency", func(t *testing.T) {
		crashes, lost, cut := atper16Sweep(t, simdisk.DefaultLatency())
		t.Logf("%d crashes of n1; %d seeds left an Add unacknowledged; %d batches cut while writing, %d while committing",
			crashes, lost, cut.write, cut.commit)
		if crashes == 0 || cut.write == 0 || cut.commit == 0 {
			t.Errorf("%d crashes, %d batches cut while writing and %d while committing; want all above 0",
				crashes, cut.write, cut.commit)
		}
	})
}

// cutBatches counts the batches a crash cut: write those whose write started and never finished,
// commit those whose commit started and never finished.
type cutBatches struct{ write, commit int }

// atper16Sweep runs the world of AT-PER-16 for seeds 0 to 49 with disk latency lat. It returns the
// number of crashes of n1, of seeds in which an Add got no reply, and of batches cut in flight.
func atper16Sweep(t *testing.T, lat simdisk.LatencyConfig) (crashes, lost int, cut cutBatches) {
	opts := fullTrace(0)
	opts.Duration = 250 * time.Millisecond
	opts.Disk.Crash = simdisk.CrashAny
	opts.Disk.Latency = lat
	sim.SimulateSeeds(t, opts, func(s *sim.Sim) {
		w := s.World()
		r := newCounterRun(s)
		r.node()
		s.Client("c", noSetup)
		w.Plan(&fault.Random{
			Rules: []fault.Rule{{
				Kind: fault.KindCrash, Every: 20 * time.Millisecond,
				MinFor: 2 * time.Millisecond, MaxFor: 2 * time.Millisecond,
			}},
			MaxDown: 1,                      // the default, (servers-1)/2, lets no single server go down
			Quiet:   100 * time.Millisecond, // the recovery window starts at 150 ms
		})
		// The client asks from its own node, which no fault touches; an Add sent while n1 is down,
		// or lost in a crash, never gets a reply.
		adds := make([]*lockstep.Reply[int64], 60)
		for i := range adds {
			w.Sim.At(vt(time.Duration(i+1)*2*time.Millisecond), "test/add", func() {
				adds[i] = lockstep.Ask(s.System("c"), r.pid, addCmd(int64(1)<<i))
			})
		}
		var get *lockstep.Reply[int64]
		w.Sim.At(w.RecoveryStart().Add(20*time.Millisecond), "test/get", func() {
			get = lockstep.Ask(s.System("c"), r.pid, getCmd)
		})
		w.Final("atper16/acked", func() error {
			if get == nil || !hooks.AskResolved(get) {
				return errors.New("the Get asked in the recovery window got no reply")
			}
			v, err := get.Await(context.Background())
			if err != nil {
				return fmt.Errorf("the Get asked in the recovery window failed: %w", err)
			}
			if acked := ackedBits(adds); v&acked != acked {
				return fmt.Errorf("Get replied %#x, which lacks the acknowledged values %#x", v, acked&^v)
			}
			return nil
		})
		w.RunFor(w.End().Sub(w.Sim.Now()))
		recs := w.Sim.Records()
		crashes += len(ofKind(recs, "kernel.crash"))
		cut.write += len(effectStarts(recs, "lockstep.persist.write")) - len(ofKind(recs, "lockstep.persist.write"))
		cut.commit += len(effectStarts(recs, "lockstep.persist.commit")) - len(ofKind(recs, "lockstep.persist.commit"))
		if ackedBits(adds) != 1<<60-1 {
			lost++
		}
	}, seedRange(50)...)
	return crashes, lost, cut
}

// ackedBits returns the bitwise OR of 1<<i for every ask adds[i] that resolved with a value.
func ackedBits(adds []*lockstep.Reply[int64]) int64 {
	var acked int64
	for i, a := range adds {
		if a == nil || !hooks.AskResolved(a) {
			continue
		}
		if _, err := a.Await(context.Background()); err == nil {
			acked |= int64(1) << i
		}
	}
	return acked
}

// AT-PER-17, PER-150: a crash between the write and the commit of AddMany{3} keeps the batch
// whole or loses it whole: the recovered sum is 1 or 4, both occur, and AddMany gets no reply.
func TestATPER17BatchAtomic(t *testing.T) {
	opts := fullTrace(0)
	opts.Disk.Crash = simdisk.CrashKeepPrefix
	opts.Disk.Latency.Write.Base = time.Millisecond
	opts.Disk.Latency.Sync.Base = time.Millisecond
	sums := map[int64]int{}
	sim.SimulateSeeds(t, opts, func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		r := newCounterRun(s)
		n1 := r.node()
		c := crashAtRecord(s, n1, afterFirst("lockstep.persist.commit", diskRecord("disk.write", vseg(1))), 500*time.Microsecond)
		w.RunFor(20 * time.Millisecond) // the store's first operation syncs, at 1 ms each
		r.ask(tt, addCmd(1))
		if v, err := sim.Ask(s, "n1", r.pid, addManyCmd(3)); !errors.Is(err, lockstep.ErrStopped) {
			tt.Fatalf("AddMany{3} replied %d, %v; want no reply (an error wrapping ErrStopped)", v, err)
		}
		if !c.fired {
			tt.Fatal("the crash did not run")
		}
		w.RunFor(20 * time.Millisecond)
		got := r.ask(tt, getCmd)
		if got != 1 && got != 4 {
			tt.Errorf("Get after the restart replied %d, want 1 or 4", got)
		}
		sums[got]++
		if len(r.errs) != 0 {
			tt.Errorf("errs %v, want none", r.errs)
		}
	}, seedRange(20)...)
	t.Logf("recovered sums by count: %v", sums)
	if sums[1] == 0 || sums[4] == 0 {
		t.Errorf("recovered sums %v; want both 1 and 4 across the seeds", sums)
	}
}

// AT-PER-18, PER-160: a failure while the write of Add{1} is in flight cancels the batch: the
// failed incarnation starts no commit, and the next one's Recover commits the written batch (a
// sync of the segment in its recover run) and recovers it.
func TestATPER18RecoverCommitsWritten(t *testing.T) {
	opts := fullTrace(3)
	opts.Disk.Latency.Write.Base = 5 * time.Millisecond
	sim.Simulate(t, opts, func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		r := newCounterRun(s)
		r.node()
		w.RunFor(20 * time.Millisecond) // the store's first operation writes FORMAT, at 5 ms
		if len(r.recs) != 1 {
			tt.Fatal("the counter has not recovered after 20 ms")
		}
		start, sentAt := len(w.Sim.Records()), w.Sim.Now()
		//lockstep:allow LS220 the injected failure cuts the command off; the test reads the records
		lockstep.Ask(s.System("n1"), r.pid, addCmd(1))
		w.RunFor(time.Millisecond)
		if err := s.InjectFail(r.pid.Address(), errors.New("injected")); err != nil {
			tt.Fatalf("InjectFail: %v", err)
		}
		w.RunFor(20 * time.Millisecond)

		recs := w.Sim.Records()[start:]
		ofCounter := func(kind string) func(kernel.Record) bool {
			return func(rec kernel.Record) bool { return rec.Kind == kind && attr(rec, "actor") == r.pid.String() }
		}
		deliver := indexOf(recs, func(rec kernel.Record) bool {
			return rec.Kind == "lockstep.deliver" && attr(rec, "to") == r.pid.String()
		})
		if deliver < 0 || recs[deliver].At != sentAt {
			tt.Fatalf("the Add was not delivered at %v, when it was sent", sentAt)
		}
		restart := indexOf(recs, ofCounter("lockstep.restart"))
		if restart < 0 || recs[restart].At != sentAt.Add(time.Millisecond) {
			tt.Fatalf("the counter did not restart 1 ms after the Add's delivery")
		}
		failed := attr(recs[restart], "inc")
		for _, st := range effectStarts(recs, "lockstep.persist.commit") {
			if attr(st, "actor") == r.pid.String() && attr(st, "inc") == failed {
				tt.Errorf("the failed incarnation %s started a commit at %v", failed, st.At)
			}
		}
		recov := restart + indexOf(recs[restart:], func(rec kernel.Record) bool {
			return ofCounter("lockstep.effect.start")(rec) && attr(rec, "name") == "lockstep.persist.recover"
		})
		if recov < restart {
			tt.Fatal("no recover effect start of the new incarnation")
		}
		run := recov + indexOf(recs[recov:], func(rec kernel.Record) bool {
			return rec.Kind == "kernel.event" && rec.Text == "lockstep/effect"
		})
		end := run + 1 + indexOf(recs[run+1:], isKind("kernel.event"))
		if run < recov || end <= run {
			tt.Fatal("no run event of the new incarnation's recover effect")
		}
		if indexOf(recs[run:end], diskRecord("disk.sync", vseg(1))) < 0 {
			tt.Errorf("no disk.sync of %s in the recover run at %v", vseg(1), recs[run].At)
		}
		if n := len(r.recs); n != 2 || r.recs[1].LastSeq != 1 {
			tt.Errorf("recs %+v, want two with LastSeq 1 in the second", r.recs)
		}
	})
}

// AT-PER-32, PER-350: a crash 500 µs after the first sync of /journal during the first Add{1}
// loses no event: the restart recovers LastSeq 1. Inline runs execute every sync of a commit at
// the run's instant (the latency only delays the completion), so the crash always finds the batch
// and its directory entries durable: this checks that a synced but unacknowledged batch is
// recovered whole, not a crash between the directory syncs.
func TestATPER32CrashDuringDirSync(t *testing.T) {
	opts := fullTrace(0)
	opts.Disk.Latency.Sync.Base = time.Millisecond
	sim.SimulateSeeds(t, opts, func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		r := newCounterRun(s)
		n1 := r.node()
		w.RunFor(20 * time.Millisecond) // the store's first operation syncs, at 1 ms each
		c := crashAtRecord(s, n1, diskRecord("disk.sync_dir", "/journal"), 500*time.Microsecond)
		if v, err := sim.Ask(s, "n1", r.pid, addCmd(1)); !errors.Is(err, lockstep.ErrStopped) {
			tt.Fatalf("Add{1} replied %d, %v; want no reply (an error wrapping ErrStopped)", v, err)
		}
		if !c.fired {
			tt.Fatal("the crash did not run")
		}
		w.RunFor(20 * time.Millisecond)
		if n := len(r.recs); n != 2 || r.recs[1].LastSeq != 1 {
			tt.Errorf("recs %+v, want two with LastSeq 1 in the second", r.recs)
		}
		if got := r.ask(tt, getCmd); got != 1 {
			tt.Errorf("Get after the restart replied %d, want 1", got)
		}
	}, seedRange(20)...)
}

// AT-PER-35, PER-370: a torn write of AddMany{100} (a 1120-byte record at offset 31, over three
// 512-byte sectors) is repaired at recovery: the whole record survived (Get 101, Repaired 0) or
// the segment is cut back to the first record (Get 1, Repaired the bytes that survived beyond it).
// Every recovery succeeds, and the next Add{1} commits.
func TestATPER35TornTail(t *testing.T) {
	opts := fullTrace(0)
	opts.Disk.Crash = simdisk.CrashTorn
	opts.Disk.Latency.Write.Base = time.Millisecond
	opts.Disk.Latency.Sync.Base = time.Millisecond
	whole, torn := 0, 0
	sim.SimulateSeeds(t, opts, func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		r := newCounterRun(s)
		n1 := r.node()
		c := crashAtRecord(s, n1, afterFirst("lockstep.persist.commit", diskRecord("disk.write", vseg(1))),
			500*time.Microsecond, vseg(1))
		w.RunFor(20 * time.Millisecond) // the store's first operation syncs, at 1 ms each
		r.ask(tt, addCmd(1))
		if v, err := sim.Ask(s, "n1", r.pid, addManyCmd(100)); !errors.Is(err, lockstep.ErrStopped) {
			tt.Fatalf("AddMany{100} replied %d, %v; want no reply (an error wrapping ErrStopped)", v, err)
		}
		if !c.fired {
			tt.Fatal("the crash did not run")
		}
		w.RunFor(20 * time.Millisecond)
		if len(r.errs) != 0 || len(r.recs) != 2 {
			tt.Fatalf("errs %v, recs %+v; want no error and two recoveries", r.errs, r.recs)
		}
		visible, durable := c.visible[0], c.durable[0]
		if len(visible) != 31+1120 {
			tt.Errorf("visible capture of %d bytes, want %d", len(visible), 31+1120)
		}
		wantGet, wantRepaired := int64(101), int64(0)
		if !bytes.Equal(durable, visible) {
			wantGet, wantRepaired = 1, int64(len(durable)-31)
			torn++
		} else {
			whole++
		}
		if got := r.ask(tt, getCmd); got != wantGet {
			tt.Errorf("Get after the restart replied %d, want %d (durable capture of %d bytes)", got, wantGet, len(durable))
		}
		if got := r.recs[1].Repaired; got != wantRepaired {
			tt.Errorf("Repaired %d, want %d (durable capture of %d bytes)", got, wantRepaired, len(durable))
		}
		if got := r.ask(tt, addCmd(1)); got != wantGet+1 {
			tt.Errorf("Add{1} after the repair replied %d, want %d", got, wantGet+1)
		}
		// The repair cut the segment back to its last valid record, so the new record follows it
		// with nothing behind.
		wantSize := 31 + 31 // after a torn write
		if wantGet == 101 {
			wantSize = 31 + 1120 + 31
		}
		if b, err := vol(s).ReadFile(vseg(1)); err != nil || len(b) != wantSize {
			tt.Errorf("%s holds %d bytes (%v) after the Add, want %d", vseg(1), len(b), err, wantSize)
		}
		if n, err := vol(s).DurableSize(vseg(1)); err != nil || n != int64(wantSize) {
			tt.Errorf("%s has %d durable bytes (%v) after the Add, want %d", vseg(1), n, err, wantSize)
		}
	}, seedRange(50)...)
	t.Logf("%d seeds kept the whole record, %d tore it", whole, torn)
	if whole == 0 || torn == 0 {
		t.Errorf("%d seeds kept the whole record and %d tore it; want both", whole, torn)
	}
}

// AT-PER-46, PER-520: with 1 ms writes and 2 ms syncs, the second Add{1}, delivered at t, runs
// its write at t, emits lockstep.persist.write at t+1ms, runs its commit at t+1ms (where the
// segment's durable size becomes 62) and emits lockstep.persist.commit and the reply at t+3ms.
// No lockstep/no-effect-handler failure is stored: inline effects never need a handler.
func TestATPER46InlineTiming(t *testing.T) {
	opts := fullTrace(3)
	opts.Disk.Latency.Write.Base = time.Millisecond
	opts.Disk.Latency.Sync.Base = 2 * time.Millisecond
	sim.Simulate(t, opts, func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		r := newCounterRun(s)
		r.node()
		w.RunFor(20 * time.Millisecond) // the store's first operation syncs, at 2 ms each
		r.ask(tt, addCmd(1))
		durableAt := kernel.Time(-1) // when vol.DurableSize(vseg(1)) became 62
		w.Sim.OnEvent(func() {
			if n, err := vol(s).DurableSize(vseg(1)); durableAt < 0 && err == nil && n == 62 {
				durableAt = w.Sim.Now()
			}
		})
		start := len(w.Sim.Records())
		if got := r.ask(tt, addCmd(1)); got != 2 {
			tt.Errorf("the second Add{1} replied %d, want 2", got)
		}
		recs := w.Sim.Records()[start:]
		find := func(what string, match func(kernel.Record) bool) kernel.Record {
			tt.Helper()
			i := indexOf(recs, match)
			if i < 0 {
				tt.Fatalf("no %s", what)
			}
			return recs[i]
		}
		// runOf returns the run event of the inline effect name: the first lockstep/effect event
		// after its start record.
		runOf := func(name string) kernel.Record {
			tt.Helper()
			st := indexOf(recs, func(rec kernel.Record) bool {
				return rec.Kind == "lockstep.effect.start" && attr(rec, "name") == name
			})
			i := indexOf(recs[max(st, 0):], func(rec kernel.Record) bool {
				return rec.Kind == "kernel.event" && rec.Text == "lockstep/effect"
			})
			if st < 0 || i < 0 {
				tt.Fatalf("no run of %s", name)
			}
			return recs[st+i]
		}
		t0 := find("delivery of the Add", func(rec kernel.Record) bool {
			return rec.Kind == "lockstep.deliver" && attr(rec, "to") == r.pid.String()
		}).At
		at := func(what string, got kernel.Time, d time.Duration) {
			tt.Helper()
			if want := t0.Add(d); got != want {
				tt.Errorf("%s at %v, want %v (t+%v)", what, got, want, d)
			}
		}
		at("write run", runOf("lockstep.persist.write").At, 0)
		at("disk.write", find("disk.write", diskRecord("disk.write", vseg(1))).At, 0)
		at("lockstep.persist.write", find("lockstep.persist.write", isKind("lockstep.persist.write")).At, time.Millisecond)
		at("commit run", runOf("lockstep.persist.commit").At, time.Millisecond)
		at("durable size 62", durableAt, time.Millisecond)
		at("lockstep.persist.commit", find("lockstep.persist.commit", isKind("lockstep.persist.commit")).At, 3*time.Millisecond)
		at("lockstep.reply", find("lockstep.reply", isKind("lockstep.reply")).At, 3*time.Millisecond)
		if err := s.Stored(sim.CheckNoEffectHandler); err != nil {
			tt.Errorf("lockstep/no-effect-handler failure stored: %v", err)
		}
	})
}

// atper47Run is the workload of AT-PER-47: three Adds, a crash and restart of n1, and a Get.
func atper47Run(s *sim.Sim) {
	w, tt := s.World(), s.World().T()
	r := newCounterRun(s)
	n1 := r.node()
	w.RunFor(50 * time.Millisecond)
	for n := range int64(3) {
		r.ask(tt, addCmd(n+1))
	}
	now := w.Sim.Now()
	w.Sim.At(now.Add(time.Millisecond), "test/crash", n1.Crash)
	w.Sim.At(now.Add(2*time.Millisecond), "test/restart", n1.Restart)
	w.RunFor(50 * time.Millisecond)
	if got := r.ask(tt, getCmd); got != 6 {
		tt.Errorf("Get after the restart replied %d, want 6", got)
	}
}

// AT-PER-47, PER-530, PER §6: with simdisk's default latency model, two runs of seed 7 give the
// same trace hash, and the latency delays some replies. With zero latency, each reply's record
// has the At of its command's delivery.
func TestATPER47LatencyDeterminism(t *testing.T) {
	opts := fullTrace(0)
	opts.Disk.Latency = simdisk.DefaultLatency()
	var hashes []uint64
	delayed := 0
	for i := range 2 {
		t.Run(fmt.Sprintf("latency-run-%d", i+1), func(t *testing.T) {
			sim.SimulateSeeds(t, opts, func(s *sim.Sim) {
				atper47Run(s)
				w := s.World()
				hashes = append(hashes, w.Sim.TraceHash())
				recs := w.Sim.Records()
				for _, d := range commandReplies(recs) {
					if d[1].At != d[0].At {
						delayed++
					}
				}
			}, 7)
		})
	}
	if len(hashes) < 2 || slices.ContainsFunc(hashes, func(h uint64) bool { return h != hashes[0] }) {
		t.Errorf("trace hashes of seed 7: %#x, want two equal ones", hashes)
	}
	if delayed == 0 {
		t.Error("no reply came later than its delivery: the latency model was not applied")
	}

	t.Run("zero-latency", func(t *testing.T) {
		sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
			atper47Run(s)
			tt := s.World().T()
			pairs := commandReplies(s.World().Sim.Records())
			if len(pairs) != 4 {
				tt.Fatalf("%d command replies, want 4 (three Adds and a Get)", len(pairs))
			}
			for i, p := range pairs {
				if p[1].At != p[0].At {
					tt.Errorf("reply %d at %v, its command delivered at %v; want equal", i+1, p[1].At, p[0].At)
				}
			}
		})
	})
}

// commandReplies pairs each lockstep.reply record with the lockstep.deliver record of its ask's
// request: the first delivery after the lockstep.ask record of the same ask.
func commandReplies(recs []kernel.Record) [][2]kernel.Record {
	var out [][2]kernel.Record
	for i, rec := range recs {
		if rec.Kind != "lockstep.reply" {
			continue
		}
		ask := indexOf(recs[:i], func(a kernel.Record) bool {
			return a.Kind == "lockstep.ask" && attr(a, "ask") == attr(rec, "ask")
		})
		if ask < 0 {
			continue
		}
		to := attr(recs[ask], "to")
		d := indexOf(recs[ask:i], func(x kernel.Record) bool {
			return x.Kind == "lockstep.deliver" && attr(x, "to") == to
		})
		if d >= 0 {
			out = append(out, [2]kernel.Record{recs[ask+d], rec})
		}
	}
	return out
}
