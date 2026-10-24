// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package raftsim

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/hmdsefi/faultline/check/history"
	"github.com/hmdsefi/faultline/kernel"

	"github.com/hmdsefi/lockstep/examples/raft/kv"
	"github.com/hmdsefi/lockstep/examples/raft/raft"
)

func ptr(s string) *string { return &s }

// wop builds a write op as faultline's recorder stores it: the output is null, and a pending op has
// no return index.
func wop(id int64, process string, status history.Status, key, value string, call, ret int64) history.Op {
	return mkOp(id, process, "write", status, map[string]string{"key": key, "value": value}, nil, call, ret)
}

// rop builds a read op; out nil is a read of an absent key.
func rop(id int64, process string, status history.Status, key string, out *string, call, ret int64) history.Op {
	return mkOp(id, process, "read", status, map[string]string{"key": key}, out, call, ret)
}

func mkOp(id int64, process, f string, status history.Status, in map[string]string, out any, call, ret int64) history.Op {
	input, _ := json.Marshal(in)
	output, _ := json.Marshal(out)
	op := history.Op{
		ID: id, Process: process, F: f, Status: status, CallIndex: call, ReturnIndex: ret,
		Input: json.RawMessage(input), Output: json.RawMessage(output),
	}
	if status == history.Pending {
		op.Output, op.ReturnIndex = json.RawMessage("null"), 0
	}
	return op
}

// fill returns n puts of client 3 (the probe of a two-client rig) to key "f", seqs 1..n.
func fill(n int) []raft.Entry {
	var es []raft.Entry
	for i := 1; i <= n; i++ {
		es = append(es, put(1, 3, uint64(i), "f", "x")) //nolint:gosec // G115: i is small
	}
	return es
}

// finalRig returns a rig whose canonical log is entries, all committed in term 1 by n1.
func finalRig(entries ...raft.Entry) *rig {
	r := newRig()
	r.start("n1", "n2", "n3")
	r.commit(1, newLog(entries...))
	return r
}

// served reports an OK reply of n1 for (client, seq) at index.
func (r *rig) served(client uint32, seq, index uint64) {
	r.o.Replied("n1", client, seq, raft.Result{Status: raft.OK, Output: "1", Index: index})
}

func TestATRFT43Linearizable(t *testing.T) {
	tests := []struct {
		name  string
		rig   func() *rig
		ops   []history.Op
		want  string // "" means nil
		check string // the check name, for wantErr
	}{
		{
			name: "correct log reads",
			rig: func() *rig {
				r := finalRig(noop(1), put(1, 1, 1, "k0", "a"), put(1, 2, 1, "k1", "b"))
				r.served(1, 2, 3)
				r.served(2, 2, 3)
				return r
			},
			ops: []history.Op{
				wop(1, "c1", history.OK, "k0", "a", 1, 3),
				wop(2, "c2", history.OK, "k1", "b", 2, 4),
				rop(3, "c1", history.OK, "k0", ptr("a"), 5, 6),
				rop(4, "c2", history.OK, "k2", nil, 7, 8),
			},
		},
		{
			name: "read at 10 after a write committed at 12 completed",
			rig: func() *rig {
				r := finalRig(append(append([]raft.Entry{noop(1)}, fill(10)...), put(1, 1, 1, "k0", "x"))...)
				r.served(2, 1, 10)
				return r
			},
			ops: []history.Op{
				wop(1, "c1", history.OK, "k0", "x", 1, 2),
				rop(2, "c2", history.OK, "k0", nil, 3, 4),
			},
			want: "write c1#1 (k0=x) at index 12 completed before read c2#1 (k0) at after index 10 was invoked",
		},
		{
			name: "ok write never committed",
			rig:  func() *rig { return finalRig(noop(1)) },
			ops:  []history.Op{wop(1, "c1", history.OK, "k0", "a", 1, 2)},
			want: "write c1#1 (k0=a) was acknowledged but never committed",
		},
		{
			name: "read returns a value the log does not hold",
			rig: func() *rig {
				r := finalRig(noop(1), put(1, 1, 1, "k0", "a"))
				r.served(2, 1, 2)
				return r
			},
			ops: []history.Op{
				wop(1, "c1", history.OK, "k0", "a", 1, 2),
				rop(2, "c2", history.OK, "k0", ptr("zzz"), 3, 4),
			},
			want: `read c2#1 (k0) returned "zzz" but the committed log at index 2 holds "a"`,
		},
		{
			name: "read returns null for a present key",
			rig: func() *rig {
				r := finalRig(noop(1), put(1, 1, 1, "k0", "a"))
				r.served(2, 1, 2)
				return r
			},
			ops: []history.Op{
				wop(1, "c1", history.OK, "k0", "a", 1, 2),
				rop(2, "c2", history.OK, "k0", nil, 3, 4),
			},
			want: `read c2#1 (k0) returned null but the committed log at index 2 holds "a"`,
		},
		{
			name: "read returns a value for an absent key",
			rig: func() *rig {
				r := finalRig(noop(1), put(1, 1, 1, "k0", "a"))
				r.served(2, 1, 1)
				return r
			},
			ops: []history.Op{
				wop(1, "c1", history.Info, "k0", "a", 1, 2),
				rop(2, "c2", history.OK, "k0", ptr("a"), 3, 4),
			},
			want: `read c2#1 (k0) returned "a" but the committed log at index 1 holds null`,
		},
		{
			name: "info write committed at 8, read before its invocation served after 8",
			rig: func() *rig {
				es := append([]raft.Entry{noop(1)}, fill(6)...)
				r := finalRig(append(es, put(1, 1, 1, "k0", "v"), put(1, 3, 7, "f", "y"))...)
				r.served(2, 1, 9)
				return r
			},
			ops: []history.Op{
				rop(1, "c2", history.OK, "k0", ptr("v"), 1, 2),
				wop(2, "c1", history.Info, "k0", "v", 3, 4),
			},
			want: "read c2#1 (k0) at after index 9 completed before write c1#1 (k0=v) at index 8 was invoked",
		},
		{
			name: "read served beyond the canonical log",
			rig: func() *rig {
				r := finalRig(noop(1), put(1, 1, 1, "k0", "a"), put(1, 1, 2, "k0", "b"))
				r.served(2, 1, 5)
				return r
			},
			ops:  []history.Op{rop(1, "c2", history.OK, "k0", ptr("b"), 1, 2)},
			want: "read c2#1 (k0) was served at index 5, beyond the committed log (ends at 3)",
		},
		{
			name: "seqs count fail ops, fail ops are skipped",
			rig: func() *rig {
				r := finalRig(noop(1), put(1, 1, 2, "k0", "a"))
				r.served(1, 3, 2)
				return r
			},
			ops: []history.Op{
				wop(1, "c1", history.Fail, "k0", "z", 1, 2),
				wop(2, "c1", history.OK, "k0", "a", 3, 4),
				rop(3, "c1", history.OK, "k0", ptr("a"), 5, 6),
			},
		},
		{
			name: "pending ops are kept like info ops",
			rig: func() *rig {
				return finalRig(noop(1), put(1, 2, 1, "k0", "b"))
			},
			ops: []history.Op{
				rop(1, "c1", history.Pending, "k0", nil, 1, 0), // no point: never an OK read
				wop(2, "c1", history.Pending, "k0", "x", 2, 0), // never committed: skipped
				wop(3, "c2", history.Info, "k0", "b", 3, 4),
			},
		},
		{
			name: "a pending write committed is b in precedence",
			rig: func() *rig {
				return finalRig(noop(1), put(1, 2, 1, "k0", "b"), put(1, 1, 1, "k0", "a"))
			},
			ops: []history.Op{
				wop(1, "c1", history.OK, "k0", "a", 1, 2),
				wop(2, "c2", history.Pending, "k0", "b", 3, 0),
			},
			want: "write c1#1 (k0=a) at index 3 completed before write c2#1 (k0=b) at index 2 was invoked",
		},
		{
			name: "a return and a call at one event index do not precede",
			rig: func() *rig {
				r := finalRig(noop(1), put(1, 1, 1, "k0", "a"))
				r.served(2, 1, 1)
				return r
			},
			ops: []history.Op{
				wop(1, "c1", history.OK, "k0", "a", 1, 5),
				rop(2, "c2", history.OK, "k0", nil, 5, 6),
			},
		},
		{
			name: "equal largest points: the lower id is named",
			rig: func() *rig {
				r := finalRig(noop(1), put(1, 1, 1, "k0", "a"), put(1, 1, 2, "k0", "b"))
				r.served(2, 1, 3)
				r.served(2, 2, 3)
				r.served(1, 1, 2)
				return r
			},
			ops: []history.Op{
				rop(1, "c2", history.OK, "k0", ptr("b"), 1, 2),
				rop(2, "c2", history.OK, "k0", ptr("b"), 3, 4),
				rop(3, "c1", history.OK, "k0", ptr("a"), 5, 6),
			},
			want: "read c2#1 (k0) at after index 3 completed before read c1#1 (k0) at after index 2 was invoked",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.rig().o.linearizable(tt.ops)
			switch {
			case tt.want == "" && err != nil:
				t.Errorf("linearizable = %q, want nil", err)
			case tt.want != "" && err == nil:
				t.Errorf("linearizable = nil, want %q", tt.want)
			case tt.want != "" && err.Error() != tt.want:
				t.Errorf("linearizable = %q, want %q", err, tt.want)
			}
		})
	}
}

func TestATRFT44Progress(t *testing.T) {
	const within = 3 * time.Second
	recovery := kernel.Time(0).Add(10 * time.Second)
	probe := func(id int64, status history.Status, ret kernel.Time) history.Op {
		op := wop(id, "probe", status, "probe", "probe-1", id*2-1, id*2)
		op.Return = ret
		return op
	}
	tests := []struct {
		name string
		ops  []history.Op
		want string
	}{
		{
			name: "acknowledged shortly after the recovery start",
			ops:  []history.Op{probe(1, history.OK, recovery.Add(time.Second))},
		},
		{
			name: "acknowledged exactly at the deadline",
			ops:  []history.Op{probe(1, history.OK, recovery.Add(within))},
		},
		{
			name: "the first ok op is late",
			ops: []history.Op{
				probe(1, history.Info, recovery.Add(time.Second)),
				probe(2, history.OK, recovery.Add(4*time.Second)),
			},
			want: "probe write acknowledged at 14.000000000s, after the deadline 13.000000000s",
		},
		{
			name: "no probe op",
			want: "probe write not acknowledged within 3s of recovery start 10.000000000s",
		},
		{
			name: "only unacknowledged probe ops and other processes",
			ops: []history.Op{
				probe(1, history.Info, recovery.Add(time.Second)),
				probe(2, history.Pending, 0),
				wop(3, "c1", history.OK, "k0", "a", 5, 6),
			},
			want: "probe write not acknowledged within 3s of recovery start 10.000000000s",
		},
	}
	o := newRig().o
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := o.progress(tt.ops, recovery, within)
			switch {
			case tt.want == "" && err != nil:
				t.Errorf("progress = %q, want nil", err)
			case tt.want != "" && err == nil:
				t.Errorf("progress = nil, want %q", tt.want)
			case tt.want != "" && err.Error() != tt.want:
				t.Errorf("progress = %q, want %q", err, tt.want)
			}
		})
	}
}

// applyUpTo reports server's Applied calls for indexes 1..upTo of log, with the reference state
// hashes.
func applyUpTo(r *rig, server string, log *fakeLog, upTo uint64) {
	store := kv.New()
	for i := uint64(1); i <= upTo; i++ {
		store.Apply(i, log.Entry(i).Cmd)
		r.o.Applied(server, i, log.Entry(i), store.Hash())
	}
}

func TestATRFT45ReplicasAgree(t *testing.T) {
	log := newLog(noop(1), put(1, 1, 1, "k0", "a"), put(1, 2, 1, "k1", "b"), put(1, 1, 2, "k0", "c"), put(1, 2, 2, "k1", "d"))
	// ready returns a rig with the five entries committed, an OK reply at index 5 (A = 5), and
	// every server recovered.
	ready := func(ack bool) *rig {
		r := newRig()
		r.start("n1", "n2", "n3")
		r.commit(1, log)
		if ack {
			r.served(2, 2, 5)
		}
		return r
	}
	tests := []struct {
		name  string
		noAck bool
		setup func(r *rig)
		up    []string
		want  string
	}{
		{
			name:  "all agree",
			setup: func(r *rig) { applyUpTo(r, "n1", log, 5); applyUpTo(r, "n2", log, 5); applyUpTo(r, "n3", log, 5) },
			up:    []string{"n1", "n2", "n3"},
		},
		{
			name:  "applied below the acknowledged index",
			setup: func(r *rig) { applyUpTo(r, "n1", log, 5); applyUpTo(r, "n2", log, 3); applyUpTo(r, "n3", log, 5) },
			up:    []string{"n1", "n2", "n3"},
			want:  "n2 applied up to 3 at the end, below acknowledged index 5",
		},
		{
			name: "wrong state hash",
			setup: func(r *rig) {
				applyUpTo(r, "n1", log, 5)
				applyUpTo(r, "n2", log, 4)
				r.o.Applied("n2", 5, log.Entry(5), 0x1234)
				applyUpTo(r, "n3", log, 5)
			},
			up:   []string{"n1", "n2", "n3"},
			want: "n2 at index 5 has state hash 0x0000000000001234, reference has " + refHashText(log, 5),
		},
		{
			name: "up but never recovered in the current incarnation",
			setup: func(r *rig) {
				applyUpTo(r, "n1", log, 5)
				applyUpTo(r, "n2", log, 5)
				applyUpTo(r, "n3", log, 5)
				r.o.boot("n3") // restarted, has not loaded its state
			},
			up:   []string{"n1", "n2", "n3"},
			want: "n3 is up but never loaded its state",
		},
		{
			name:  "servers are visited in the given order",
			setup: func(r *rig) { applyUpTo(r, "n1", log, 2); applyUpTo(r, "n2", log, 3); applyUpTo(r, "n3", log, 1) },
			up:    []string{"n2", "n3", "n1"},
			want:  "n2 applied up to 3 at the end, below acknowledged index 5",
		},
		{
			name:  "a server that is not up is not checked",
			setup: func(r *rig) { applyUpTo(r, "n1", log, 5); applyUpTo(r, "n3", log, 5) },
			up:    []string{"n1", "n3"},
		},
		{
			name:  "no acknowledged index: the empty state matches",
			noAck: true,
			setup: func(*rig) {},
			up:    []string{"n1", "n2", "n3"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := ready(!tt.noAck)
			tt.setup(r)
			err := r.o.replicasAgree(tt.up)
			switch {
			case tt.want == "" && err != nil:
				t.Errorf("replicasAgree = %q, want nil", err)
			case tt.want != "" && err == nil:
				t.Errorf("replicasAgree = nil, want %q", tt.want)
			case tt.want != "" && err.Error() != tt.want:
				t.Errorf("replicasAgree = %q, want %q", err, tt.want)
			}
		})
	}
}

// refHashText returns the reference state hash after the first i entries of log, as text.
func refHashText(log *fakeLog, i uint64) string {
	store := kv.New()
	for j := uint64(1); j <= i; j++ {
		store.Apply(j, log.Entry(j).Cmd)
	}
	return hex(store.Hash())
}
