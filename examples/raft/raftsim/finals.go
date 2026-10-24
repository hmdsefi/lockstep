// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package raftsim

import (
	"cmp"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/hmdsefi/faultline/check/history"
	"github.com/hmdsefi/faultline/kernel"

	"github.com/hmdsefi/lockstep/examples/raft/raft"
)

// probeProcess is the process name of the probe client (RFT-370).
const probeProcess = "probe"

// logPosition is the position of an operation in the canonical log (RFT §3): a write at index i is
// (i, 0), a read served at index r is (r, 1). Positions compare lexicographically.
type logPosition struct {
	index uint64
	read  bool
}

func (p logPosition) less(q logPosition) bool {
	if p.index != q.index {
		return p.index < q.index
	}
	return !p.read && q.read
}

// String writes a write's position as "index <i>" and a read's as "after index <r>".
func (p logPosition) String() string {
	if p.read {
		return "after index " + strconv.FormatUint(p.index, 10)
	}
	return "index " + strconv.FormatUint(p.index, 10)
}

// finalOp is one operation of the history that the linearizable final looks at.
type finalOp struct {
	op   history.Op
	desc string // "write <p>#<s> (<k>=<v>)" or "read <p>#<s> (<k>)"
	key  string
	out  *string // an OK read's output; nil for an absent key
	pos  logPosition
	has  bool // pos is set
}

// ok reports whether the op completed OK. Fail ops are not looked at; Info and Pending ops are
// kept for precedence as the later op, never as the earlier one.
func (f *finalOp) ok() bool { return f.op.Status == history.OK }

// linearizable checks that the canonical log order is a linearization of the history (RFT-510):
// every acknowledged write is committed, every acknowledged read returns the value the committed
// log holds at the index it was served at, and no operation is placed before one that completed
// before it was invoked. It returns the first violation. ops are the history's operations in ID
// order.
func (o *Oracle) linearizable(ops []history.Op) error {
	fops, err := o.finalOps(ops)
	if err != nil {
		return err
	}

	// Step 1, continued: an acknowledged write has to be committed.
	for _, f := range fops {
		if f.ok() && !f.has && f.op.F == "write" {
			return fmt.Errorf("%s was acknowledged but never committed", f.desc)
		}
	}

	// Step 2: acknowledged reads against the committed log.
	for _, f := range fops {
		if !f.has || !f.ok() || !f.pos.read {
			continue
		}
		r := f.pos.index
		if r > uint64(len(o.canon)) {
			return fmt.Errorf("%s was served at index %d, beyond the committed log (ends at %d)",
				f.desc, r, len(o.canon))
		}
		if want := o.valueAt(f.key, r); !sameValue(f.out, want) {
			return fmt.Errorf("%s returned %s but the committed log at index %d holds %s",
				f.desc, jsonText(f.out), r, jsonText(want))
		}
	}

	// Step 3: precedence.
	return precedence(fops)
}

// finalOps decodes ops, skipping Fail ops, and gives each op its description and, if it has one,
// its point (RFT-510 step 1). An op without a point has has false.
func (o *Oracle) finalOps(ops []history.Op) ([]*finalOp, error) {
	ops = slices.Clone(ops)
	slices.SortStableFunc(ops, func(a, b history.Op) int { return cmp.Compare(a.ID, b.ID) })

	seqs := make(map[string]uint64) // per process, the ordinal of its latest op
	var fops []*finalOp
	for _, op := range ops {
		seqs[op.Process]++
		seq := seqs[op.Process] // counts Fail ops: it is the client's seq (RFT-350)
		if op.Status == history.Fail {
			continue
		}
		var in map[string]string
		if err := op.DecodeInput(&in); err != nil {
			return nil, err
		}
		f := &finalOp{op: op, key: in["key"]}
		cmd, isClient := o.processCommand(op.Process, seq)
		name := op.Process + "#" + strconv.FormatUint(seq, 10)

		switch op.F {
		case "write":
			f.desc = fmt.Sprintf("write %s (%s=%s)", name, in["key"], in["value"])
			i, committed := o.commands[cmd]
			if isClient && committed {
				f.pos, f.has = logPosition{index: i}, true
			}
		case "read":
			f.desc = fmt.Sprintf("read %s (%s)", name, in["key"])
			if f.ok() {
				if err := op.DecodeOutput(&f.out); err != nil {
					return nil, err
				}
				if r, served := o.okIndex[cmd]; isClient && served {
					f.pos, f.has = logPosition{index: r, read: true}, true
				}
			}
		default:
			return nil, fmt.Errorf("op %d: unexpected operation %q", op.ID, op.F)
		}
		fops = append(fops, f)
	}
	return fops, nil
}

// processCommand returns the command of the seq-th operation of process, and false if process is
// neither a workload client (c<k>) nor the probe.
func (o *Oracle) processCommand(process string, seq uint64) (command, bool) {
	if process == probeProcess {
		return command{client: uint32(o.clients + 1), seq: seq}, true //nolint:gosec // G115: at most 9 clients
	}
	n, err := strconv.ParseUint(strings.TrimPrefix(process, "c"), 10, 32)
	if err != nil || !strings.HasPrefix(process, "c") || n < 1 || n > uint64(o.clients) { //nolint:gosec // G115: clients is in [1, 8]
		return command{}, false
	}
	return command{client: uint32(n), seq: seq}, true
}

// valueAt returns the value of key after applying the canonical entries 1..r, nil if absent.
func (o *Oracle) valueAt(key string, r uint64) *string {
	puts := o.values[key]
	n := 0 // the number of puts at an index of at most r
	for n < len(puts) && puts[n].index <= r {
		n++
	}
	if n == 0 {
		return nil
	}
	return &puts[n-1].value
}

// event is a call or a return in the precedence sweep.
type event struct {
	index int64
	ret   bool // a return; at equal index returns sort after calls
	f     *finalOp
}

// precedence checks RFT-510 step 3: for ops a (OK) and b with a.ReturnIndex < b.CallIndex,
// point(a) <= point(b). It sweeps the call and return events in event-index order, keeping the OK
// op with the largest point returned so far (ties: lower ID), and reports the first op b below it.
func precedence(fops []*finalOp) error {
	var events []event
	for _, f := range fops {
		if !f.has {
			continue
		}
		events = append(events, event{index: f.op.CallIndex, f: f})
		if f.ok() && f.op.ReturnIndex > 0 {
			events = append(events, event{index: f.op.ReturnIndex, ret: true, f: f})
		}
	}
	slices.SortStableFunc(events, func(a, b event) int {
		if c := cmp.Compare(a.index, b.index); c != 0 {
			return c
		}
		switch {
		case a.ret == b.ret:
			return 0
		case a.ret:
			return 1
		}
		return -1
	})

	var best *finalOp
	for _, e := range events {
		f := e.f
		switch {
		case !e.ret && best != nil && f.pos.less(best.pos):
			return fmt.Errorf("%s at %s completed before %s at %s was invoked", best.desc, best.pos, f.desc, f.pos)
		case e.ret && (best == nil || best.pos.less(f.pos) || (best.pos == f.pos && f.op.ID < best.op.ID)):
			best = f
		}
	}
	return nil
}

// progress checks that the probe's first OK op returned at most within after recovery (RFT-520).
// ops are the history's operations in ID order; recovery is the recovery start.
func (o *Oracle) progress(ops []history.Op, recovery kernel.Time, within time.Duration) error {
	var first *history.Op
	for i := range ops {
		if ops[i].Process == probeProcess && ops[i].Status == history.OK && (first == nil || ops[i].ID < first.ID) {
			first = &ops[i]
		}
	}
	if first == nil {
		return fmt.Errorf("probe write not acknowledged within %s of recovery start %s", within, recovery)
	}
	if first.Return.Sub(recovery) > within {
		return fmt.Errorf("probe write acknowledged at %s, after the deadline %s", first.Return, recovery.Add(within))
	}
	return nil
}

// replicasAgree checks that every server in up, the servers that are up at the end in NodeID
// order, recovered in its current incarnation, applied at least up to the largest acknowledged
// index A, and holds the reference state at the index it applied up to (RFT-530). A is the
// largest point index of any OK op of ops, the history's operations in ID order: an OK reply the
// client never saw does not count.
func (o *Oracle) replicasAgree(ops []history.Op, up []string) error {
	fops, err := o.finalOps(ops)
	if err != nil {
		return err
	}
	a := uint64(0)
	for _, f := range fops {
		if f.ok() && f.has {
			a = max(a, f.pos.index)
		}
	}
	for _, name := range up {
		s := o.byName[name]
		if s == nil {
			return fmt.Errorf("%s is up but not in the cluster", name)
		}
		if !s.recovered {
			return fmt.Errorf("%s is up but never loaded its state", name)
		}
		if s.applied < a {
			return fmt.Errorf("%s applied up to %d at the end, below acknowledged index %d", name, s.applied, a)
		}
		if s.applied > uint64(len(o.canon)) {
			continue // Applied already reported it beyond the committed log
		}
		ref := raft.ChainSeed
		if s.applied > 0 {
			ref = o.refHashes[s.applied-1]
		}
		if s.stateHash != ref {
			return fmt.Errorf("%s at index %d has state hash %s, reference has %s",
				name, s.applied, hashText(s.stateHash), hashText(ref))
		}
	}
	return nil
}

// sameValue reports whether two read values are equal; nil is an absent key.
func sameValue(a, b *string) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

// jsonText writes a read value as a JSON string, or null for an absent key.
func jsonText(v *string) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "null" // unreachable: a *string always marshals
	}
	return string(b)
}
