// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package raftsim

import (
	"encoding/binary"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/hmdsefi/faultline/kernel"

	"github.com/hmdsefi/lockstep/examples/raft/kv"
	"github.com/hmdsefi/lockstep/examples/raft/raft"
)

// fakeLog is a LogView over literal entries, with chain hashes computed as RFT-290 defines them.
type fakeLog struct {
	entries []raft.Entry
	chains  []uint64
}

func newLog(es ...raft.Entry) *fakeLog {
	l := &fakeLog{entries: es}
	h := raft.ChainSeed
	for _, e := range es {
		h = testChain(h, e)
		l.chains = append(l.chains, h)
	}
	return l
}

func (l *fakeLog) LastIndex() uint64 { return uint64(len(l.entries)) }

func (l *fakeLog) Term(i uint64) uint64 {
	if i == 0 || i > l.LastIndex() {
		return 0
	}
	return l.entries[i-1].Term
}

func (l *fakeLog) Entry(i uint64) raft.Entry { return l.entries[i-1] }

func (l *fakeLog) Chain(i uint64) uint64 {
	if i == 0 {
		return raft.ChainSeed
	}
	return l.chains[i-1]
}

// testChain continues FNV-1a 64 from prev over be64(Term), be32(Client), be64(Seq),
// be32(len(Cmd)) and Cmd.
func testChain(prev uint64, e raft.Entry) uint64 {
	b := binary.BigEndian.AppendUint64(nil, e.Term)
	b = binary.BigEndian.AppendUint32(b, e.Client)
	b = binary.BigEndian.AppendUint64(b, e.Seq)
	b = binary.BigEndian.AppendUint32(b, uint32(len(e.Cmd))) //nolint:gosec // G115: test commands are short
	b = append(b, e.Cmd...)
	h := prev
	for _, c := range b {
		h ^= uint64(c)
		h *= 1099511628211
	}
	return h
}

func noop(term uint64) raft.Entry { return raft.Entry{Term: term} }

func put(term uint64, client uint32, seq uint64, key, value string) raft.Entry {
	return raft.Entry{Term: term, Client: client, Seq: seq, Cmd: kv.EncodePut(key, value)}
}

func hex(h uint64) string { return fmt.Sprintf("0x%016x", h) }

// rig is an oracle over servers n1..n3 and two clients, with a settable clock and the emitted
// records.
type rig struct {
	o    *Oracle
	now  kernel.Time
	recs []kernel.Record
}

func newRig() *rig {
	r := &rig{}
	r.o = newOracle([]string{"n1", "n2", "n3"}, 2,
		func() kernel.Time { return r.now },
		func(rec kernel.Record) { r.recs = append(r.recs, rec) })
	return r
}

// start boots node and recovers it with the zero state, as on a first boot.
func (r *rig) start(nodes ...string) {
	for _, n := range nodes {
		r.o.boot(n)
		r.o.Recovered(n, raft.HardState{}, newLog())
	}
}

// commit reports log as committed by n1 in term, from the canonical length to the log's end.
func (r *rig) commit(term uint64, log *fakeLog) {
	r.o.Committed("n1", term, uint64(len(r.o.canon)), log.LastIndex(), log)
}

// wantErr fails t unless check's violation has the text want; want "" means no violation.
func wantErr(t *testing.T, o *Oracle, check, want string) {
	t.Helper()
	err := o.Err(check)
	switch {
	case want == "" && err != nil:
		t.Errorf("Err(%q) = %q, want nil", check, err)
	case want != "" && err == nil:
		t.Errorf("Err(%q) = nil, want %q", check, want)
	case want != "" && err.Error() != want:
		t.Errorf("Err(%q) = %q, want %q", check, err, want)
	}
}

// allChecks lists the checks the oracle stores during a run.
var allChecks = []string{
	CheckHarness, CheckElectionSafety, CheckLogMatching, CheckLeaderCompleteness,
	CheckStateMachineSafety, CheckDurableState,
}

// wantOnly fails t unless check has the text want and every other check has no violation.
func wantOnly(t *testing.T, o *Oracle, check, want string) {
	t.Helper()
	for _, c := range allChecks {
		if c == check {
			wantErr(t, o, c, want)
		} else {
			wantErr(t, o, c, "")
		}
	}
}

func TestATRFT36ElectionSafety(t *testing.T) {
	r := newRig()
	r.start("n1", "n2")
	r.now = kernel.Time(time.Second)
	r.o.BecameLeader("n1", 3, newLog())
	r.o.BecameLeader("n1", 3, newLog()) // the same server again is not a second leader
	wantOnly(t, r.o, CheckElectionSafety, "")
	r.now = kernel.Time(2500 * time.Millisecond)
	r.o.BecameLeader("n2", 3, newLog(noop(1), noop(2)))
	wantOnly(t, r.o, CheckElectionSafety,
		"term 3 has two leaders: n1 (since 1.000000000s) and n2 (at 2.500000000s)")

	want := []kernel.Record{
		{Node: 1, Kind: "raft.leader", Text: "leader of term 3",
			Attrs: []kernel.Attr{{Key: "term", Value: "3"}, {Key: "last", Value: "0"}}},
		{Node: 1, Kind: "raft.leader", Text: "leader of term 3",
			Attrs: []kernel.Attr{{Key: "term", Value: "3"}, {Key: "last", Value: "0"}}},
		{Node: 2, Kind: "raft.leader", Text: "leader of term 3",
			Attrs: []kernel.Attr{{Key: "term", Value: "3"}, {Key: "last", Value: "2"}}},
	}
	if len(r.recs) != len(want) {
		t.Fatalf("records = %+v, want %+v", r.recs, want)
	}
	for i := range want {
		got := r.recs[i]
		if got.Node != want[i].Node || got.Kind != want[i].Kind || got.Text != want[i].Text ||
			!slices.Equal(got.Attrs, want[i].Attrs) {
			t.Errorf("record %d = %+v, want %+v", i, got, want[i])
		}
	}
}

func TestATRFT37LogMatching(t *testing.T) {
	a := newLog(noop(1), put(1, 1, 1, "k0", "a"), noop(1))
	b := newLog(noop(1), put(1, 1, 1, "k0", "b"))
	want := fmt.Sprintf("index 2 term 1: n1 has prefix hash %s but n2 has %s", hex(a.Chain(2)), hex(b.Chain(2)))

	t.Run("LogWritten", func(t *testing.T) {
		r := newRig()
		r.start("n1", "n2")
		r.o.LogWritten("n1", 1, a)
		r.o.LogWritten("n2", 1, newLog(noop(1))) // a common prefix agrees
		wantOnly(t, r.o, CheckLogMatching, "")
		r.o.LogWritten("n2", 2, b)
		wantOnly(t, r.o, CheckLogMatching, want)
	})
	t.Run("Recovered", func(t *testing.T) {
		r := newRig()
		r.o.boot("n1")
		r.o.Recovered("n1", raft.HardState{}, a)
		r.o.boot("n2")
		r.o.Recovered("n2", raft.HardState{}, b)
		wantErr(t, r.o, CheckLogMatching, want)
	})
}

func TestATRFT38LeaderCompleteness(t *testing.T) {
	canon := newLog(noop(1), noop(2))
	setup := func() *rig {
		r := newRig()
		r.start("n1", "n3")
		r.commit(1, newLog(noop(1)))
		r.commit(2, canon)
		return r
	}

	t.Run("committed in the same term", func(t *testing.T) {
		r := setup()
		r.o.BecameLeader("n3", 2, newLog(noop(1)))
		wantOnly(t, r.o, CheckLeaderCompleteness, "")
	})
	t.Run("missing", func(t *testing.T) {
		r := setup()
		r.o.BecameLeader("n3", 3, newLog(noop(1)))
		wantOnly(t, r.o, CheckLeaderCompleteness,
			"n3 became leader of term 3 with log ending at 1, missing committed index 2")
	})
	t.Run("complete", func(t *testing.T) {
		r := setup()
		r.o.BecameLeader("n3", 3, newLog(noop(1), noop(2), noop(2)))
		wantOnly(t, r.o, CheckLeaderCompleteness, "")
	})
	t.Run("different entry", func(t *testing.T) {
		r := setup()
		r.o.BecameLeader("n3", 3, newLog(noop(1), put(2, 1, 1, "k0", "x")))
		wantOnly(t, r.o, CheckLeaderCompleteness,
			"n3 became leader of term 3 with a different entry at committed index 2")
	})
	t.Run("empty canonical log", func(t *testing.T) {
		r := newRig()
		r.start("n3")
		r.o.BecameLeader("n3", 5, newLog())
		wantOnly(t, r.o, CheckLeaderCompleteness, "")
	})
}

func TestATRFT39StateMachineSafety(t *testing.T) {
	t.Run("different chains", func(t *testing.T) {
		r := newRig()
		r.start("n1", "n2")
		a := newLog(noop(1), noop(1), put(1, 1, 1, "k0", "a"))
		b := newLog(noop(1), noop(1), put(1, 1, 1, "k0", "b"))
		r.o.Committed("n1", 1, 0, 3, a)
		r.o.Committed("n2", 1, 0, 2, b) // the common prefix agrees
		wantOnly(t, r.o, CheckStateMachineSafety, "")
		r.o.Committed("n2", 2, 2, 3, b)
		wantOnly(t, r.o, CheckStateMachineSafety, fmt.Sprintf(
			"index 3 committed as term=1 hash=%s by n1 but as term=1 hash=%s by n2",
			hex(a.Chain(3)), hex(b.Chain(3))))
	})
	t.Run("duplicate command", func(t *testing.T) {
		r := newRig()
		r.start("n1")
		log := newLog(noop(1), put(1, 2, 1, "k1", "c2-1"), noop(1),
			put(1, 1, 5, "k0", "c1-5"), noop(2), put(2, 1, 5, "k0", "c1-5"))
		r.o.Committed("n1", 1, 0, 4, log)
		wantOnly(t, r.o, CheckStateMachineSafety, "")
		r.o.Committed("n1", 2, 4, 6, log)
		wantOnly(t, r.o, CheckStateMachineSafety, "command c1#5 committed at index 4 and again at index 6")
	})
	t.Run("duplicate probe command", func(t *testing.T) {
		r := newRig()
		r.start("n1")
		r.commit(1, newLog(noop(1), put(1, 3, 2, "probe", "probe-2"), put(1, 3, 2, "probe", "probe-2")))
		wantOnly(t, r.o, CheckStateMachineSafety, "command probe#2 committed at index 2 and again at index 3")
	})
	t.Run("applied beyond", func(t *testing.T) {
		r := newRig()
		r.start("n1")
		log := newLog(noop(1), put(1, 1, 1, "k0", "c1-1"), noop(1))
		r.commit(1, newLog(log.entries[:2]...))
		r.o.Applied("n1", 1, log.Entry(1), raft.ChainSeed)
		r.o.Applied("n1", 2, log.Entry(2), 7)
		wantOnly(t, r.o, CheckStateMachineSafety, "")
		r.o.Applied("n1", 3, log.Entry(3), 7)
		wantOnly(t, r.o, CheckStateMachineSafety, "n1 applied index 3 beyond the committed log (ends at 2)")
	})
	t.Run("applied another term", func(t *testing.T) {
		r := newRig()
		r.start("n1")
		r.commit(1, newLog(noop(1)))
		r.o.Applied("n1", 1, noop(2), raft.ChainSeed)
		wantOnly(t, r.o, CheckStateMachineSafety, "n1 applied index 1 term=2 but the committed entry has term=1")
	})
	t.Run("applied another command", func(t *testing.T) {
		r := newRig()
		r.start("n1")
		r.commit(1, newLog(noop(1), put(1, 1, 1, "k0", "c1-1")))
		r.o.Applied("n1", 1, noop(1), raft.ChainSeed)
		r.o.Applied("n1", 2, put(1, 1, 1, "k0", "other"), 7)
		wantOnly(t, r.o, CheckStateMachineSafety,
			"n1 applied index 2 term=1 but the committed entry has a different command")
	})
}

func TestATRFT40DurableState(t *testing.T) {
	seed := hex(raft.ChainSeed)

	t.Run("lost vote", func(t *testing.T) {
		r := newRig()
		r.start("n1")
		r.o.PersistStarted("n1", raft.HardState{Term: 3, Vote: "n2"}, 0, raft.ChainSeed)
		r.o.Durable("n1", raft.HardState{Term: 3, Vote: "n2"}, 0, raft.ChainSeed)
		r.o.boot("n1")
		r.o.Recovered("n1", raft.HardState{Term: 3}, newLog())
		wantOnly(t, r.o, CheckDurableState, "n1 recovered term=3 vote=none last=0 hash="+seed+
			", but persisted term=3 vote=n2 last=0 hash="+seed)
	})
	t.Run("unfinished persist", func(t *testing.T) {
		r := newRig()
		r.start("n1")
		hs := raft.HardState{Term: 2, Vote: "n1"}
		log := newLog(noop(1), noop(2))
		r.o.PersistStarted("n1", raft.HardState{Term: 1, Vote: "n1"}, 1, log.Chain(1))
		r.o.Durable("n1", raft.HardState{Term: 1, Vote: "n1"}, 1, log.Chain(1))
		r.o.PersistStarted("n1", hs, 2, log.Chain(2))
		r.o.boot("n1")
		r.o.Recovered("n1", hs, log)
		wantOnly(t, r.o, CheckDurableState, "")
		// The recovered state is now the durable one.
		r.o.boot("n1")
		r.o.Recovered("n1", hs, log)
		wantOnly(t, r.o, CheckDurableState, "")
	})
	t.Run("persist lost by a crash", func(t *testing.T) {
		r := newRig()
		r.start("n1")
		r.o.PersistStarted("n1", raft.HardState{Term: 1, Vote: "n1"}, 0, raft.ChainSeed)
		r.o.boot("n1")
		r.o.Recovered("n1", raft.HardState{}, newLog())
		wantOnly(t, r.o, CheckDurableState, "")
	})
	t.Run("first boot", func(t *testing.T) {
		r := newRig()
		r.start("n1")
		wantOnly(t, r.o, CheckDurableState, "")
	})
	t.Run("first boot with state", func(t *testing.T) {
		r := newRig()
		r.o.boot("n1")
		r.o.Recovered("n1", raft.HardState{Term: 1}, newLog())
		wantOnly(t, r.o, CheckDurableState, "n1 recovered term=1 vote=none last=0 hash="+seed+
			", but persisted term=0 vote=none last=0 hash="+seed)
	})
}

func TestATRFT41Harness(t *testing.T) {
	t.Run("before Recovered", func(t *testing.T) {
		r := newRig()
		r.o.boot("n1")
		r.o.Applied("n1", 1, noop(1), raft.ChainSeed)
		wantErr(t, r.o, CheckHarness, "n1: Applied before Recovered")
	})
	t.Run("before Recovered after a reboot", func(t *testing.T) {
		r := newRig()
		r.start("n1")
		r.o.boot("n1")
		r.o.PersistStarted("n1", raft.HardState{Term: 1}, 0, raft.ChainSeed)
		wantErr(t, r.o, CheckHarness, "n1: PersistStarted before Recovered")
	})
	t.Run("applied out of order", func(t *testing.T) {
		r := newRig()
		r.start("n1")
		r.commit(1, newLog(noop(1), noop(1), noop(1)))
		r.o.Applied("n1", 1, noop(1), raft.ChainSeed)
		r.o.Applied("n1", 3, noop(1), raft.ChainSeed)
		wantOnly(t, r.o, CheckHarness, "n1: applied index 3 after 1")
	})
	t.Run("applied restarts with an incarnation", func(t *testing.T) {
		r := newRig()
		r.start("n1")
		r.commit(1, newLog(noop(1), noop(1)))
		r.o.Applied("n1", 1, noop(1), raft.ChainSeed)
		r.o.Applied("n1", 2, noop(1), raft.ChainSeed)
		r.o.boot("n1")
		r.o.Recovered("n1", raft.HardState{}, newLog())
		r.o.Applied("n1", 1, noop(1), raft.ChainSeed)
		wantOnly(t, r.o, CheckHarness, "")
	})
	t.Run("ok reply beyond the canonical log", func(t *testing.T) {
		r := newRig()
		r.start("n1")
		r.commit(1, newLog(noop(1), put(1, 1, 4, "k0", "c1-4")))
		r.o.Replied("n1", 1, 4, raft.Result{Status: raft.OK, Index: 2})
		r.o.Replied("n1", 2, 1, raft.Result{Status: raft.Unknown, Index: 9})
		wantOnly(t, r.o, CheckHarness, "")
		r.o.Replied("n1", 1, 5, raft.Result{Status: raft.OK, Index: 5})
		wantOnly(t, r.o, CheckHarness, "n1: replied ok for c1#5 at index 5, but index 5 is not canonical")
	})
	t.Run("ok write at index 0", func(t *testing.T) {
		r := newRig()
		r.start("n1")
		r.o.Replied("n1", 1, 6, raft.Result{Status: raft.OK, Output: "", Index: 0})
		wantOnly(t, r.o, CheckHarness, "n1: replied ok for c1#6 at index 0, but index 0 is not canonical")
	})
	t.Run("ok read at index 0", func(t *testing.T) {
		r := newRig()
		r.start("n1")
		r.o.Replied("n1", 1, 6, raft.Result{Status: raft.OK, Output: "0", Index: 0})
		r.o.Replied("n1", 2, 1, raft.Result{Status: raft.OK, Output: "1v", Index: 0})
		r.o.Replied("n1", 2, 2, raft.Result{Status: raft.OK, Output: "!", Index: 0})
		wantOnly(t, r.o, CheckHarness, "")
	})
	t.Run("committed beyond the log", func(t *testing.T) {
		r := newRig()
		r.start("n1")
		r.o.Committed("n1", 1, 0, 3, newLog(noop(1)))
		wantOnly(t, r.o, CheckHarness, "n1: committed index 3 beyond its log (ends at 1)")
		if len(r.o.canon) != 1 {
			t.Errorf("canonical length = %d, want 1", len(r.o.canon))
		}
	})
	t.Run("committed after a gap", func(t *testing.T) {
		r := newRig()
		r.start("n1")
		log := newLog(noop(1), put(1, 1, 1, "k0", "c1-1"), noop(1), put(1, 2, 1, "k1", "c2-1"))
		r.o.Committed("n1", 1, 0, 1, log)
		// from 3 lies past the canonical end (1): indexes 2 and 3 are taken from log too.
		r.o.Committed("n1", 1, 3, 4, log)
		for _, c := range allChecks {
			wantErr(t, r.o, c, "")
		}
		if len(r.o.canon) != 4 {
			t.Fatalf("canonical length = %d, want 4", len(r.o.canon))
		}
		for i := uint64(1); i <= 4; i++ {
			if c := r.o.canon[i-1]; c.entry != log.Entry(i) || c.chain != log.Chain(i) || c.term != 1 || c.by != "n1" {
				t.Errorf("canonical %d = %+v, want entry %+v chain %s term 1 by n1", i, c, log.Entry(i), hex(log.Chain(i)))
			}
		}
		if got := r.o.commands[command{client: 2, seq: 1}]; got != 4 {
			t.Errorf("commands[c2#1] = %d, want 4", got)
		}
	})
	t.Run("unknown server", func(t *testing.T) {
		r := newRig()
		r.o.Durable("n9", raft.HardState{}, 0, raft.ChainSeed)
		wantErr(t, r.o, CheckHarness, "n9: Durable from a server not in the cluster")
	})
}

// TestOracleReference checks the state the final checks read: the reference KV hashes per
// canonical index, each key's values in index order, and the first OK reply index per command.
func TestOracleReference(t *testing.T) {
	r := newRig()
	r.start("n1")
	log := newLog(noop(1), put(1, 1, 1, "k0", "c1-1"), put(1, 2, 1, "k1", "c2-1"),
		raft.Entry{Term: 1, Client: 1, Seq: 2, Cmd: kv.EncodeGet("k0")}, put(1, 1, 3, "k0", "c1-3"))
	r.commit(1, log)

	ref := kv.New()
	var hashes []uint64
	for i := uint64(1); i <= log.LastIndex(); i++ {
		if cmd := log.Entry(i).Cmd; cmd != "" {
			ref.Apply(i, cmd)
		}
		hashes = append(hashes, ref.Hash())
	}
	if !slices.Equal(r.o.refHashes, hashes) {
		t.Errorf("refHashes = %v, want %v", r.o.refHashes, hashes)
	}
	wantK0 := []keyValue{{index: 2, value: "c1-1"}, {index: 5, value: "c1-3"}}
	if got := r.o.values["k0"]; !slices.Equal(got, wantK0) {
		t.Errorf("values[k0] = %v, want %v", got, wantK0)
	}
	wantK1 := []keyValue{{index: 3, value: "c2-1"}}
	if got := r.o.values["k1"]; !slices.Equal(got, wantK1) {
		t.Errorf("values[k1] = %v, want %v", got, wantK1)
	}

	r.o.Replied("n1", 1, 2, raft.Result{Status: raft.OK, Output: "1c1-1", Index: 4})
	r.o.Replied("n1", 1, 2, raft.Result{Status: raft.OK, Index: 5})
	r.o.Replied("n1", 2, 1, raft.Result{Status: raft.Unknown, Index: 3})
	if got, ok := r.o.okIndex[command{client: 1, seq: 2}]; !ok || got != 4 {
		t.Errorf("okIndex[c1#2] = %d, %t, want 4, true", got, ok)
	}
	if _, ok := r.o.okIndex[command{client: 2, seq: 1}]; ok {
		t.Errorf("okIndex[c2#1] is set for an Unknown reply")
	}
	for _, c := range allChecks {
		wantErr(t, r.o, c, "")
	}
	if err := r.o.Err("no such check"); err != nil {
		t.Errorf("Err(unknown) = %v, want nil", err)
	}
}
