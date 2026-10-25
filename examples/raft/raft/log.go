// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package raft

import "slices"

// fnvPrime is the FNV-1a 64 prime. The offset basis is ChainSeed.
const fnvPrime uint64 = 1099511628211

// raftLog is a server's log: the entries and, in a parallel slice, the chain hash of every prefix
// (RFT-290). Index 1 is entries[0]. The zero value is an empty log. It implements LogView.
//
// raftLog is reached from actor code: it reads no clock, starts no goroutine and ranges over no
// map. Its read methods never allocate.
type raftLog struct {
	entries []Entry
	chains  []uint64 // chains[i-1] == Chain(i)
}

var _ LogView = (*raftLog)(nil)

// LastIndex returns the index of the last entry, 0 for an empty log.
func (l *raftLog) LastIndex() uint64 { return uint64(len(l.entries)) }

// Term returns the term of entry i, 0 for i == 0 or i > LastIndex().
func (l *raftLog) Term(i uint64) uint64 {
	if i == 0 || i > l.LastIndex() {
		return 0
	}
	return l.entries[i-1].Term
}

// Entry returns entry i. It panics unless 1 <= i <= LastIndex().
func (l *raftLog) Entry(i uint64) Entry {
	if i == 0 || i > l.LastIndex() {
		panic("raft: log entry index out of range")
	}
	return l.entries[i-1]
}

// Chain returns the chain hash of [1, i]; Chain(0) is ChainSeed. It panics if i > LastIndex().
func (l *raftLog) Chain(i uint64) uint64 {
	if i == 0 {
		return ChainSeed
	}
	if i > l.LastIndex() {
		panic("raft: log chain index out of range")
	}
	return l.chains[i-1]
}

// append adds entries at the end and extends the chain.
func (l *raftLog) append(es ...Entry) {
	for _, e := range es {
		l.chains = append(l.chains, chainNext(l.Chain(l.LastIndex()), e))
		l.entries = append(l.entries, e)
	}
}

// truncate drops every entry after last, keeping [1, last]. It does nothing if last >=
// LastIndex().
func (l *raftLog) truncate(last uint64) {
	if last >= l.LastIndex() {
		return
	}
	clear(l.entries[last:]) // release the command strings
	l.entries = l.entries[:last]
	l.chains = l.chains[:last]
}

// slice returns a copy of entries [from, to]. It is empty if from > to and panics if the range is
// not empty and does not lie within [1, LastIndex()]. The result never shares memory with the log.
func (l *raftLog) slice(from, to uint64) []Entry {
	if from > to {
		return nil
	}
	if from == 0 || to > l.LastIndex() {
		panic("raft: log slice out of range")
	}
	return slices.Clone(l.entries[from-1 : to])
}

// chainNext returns the chain hash of an entry given the chain hash of the prefix before it:
// FNV-1a 64 continued from prev over be64(Term), be32(Client), be64(Seq), be32(len(Cmd)) and Cmd.
func chainNext(prev uint64, e Entry) uint64 {
	h := prev
	h = fnvUint64(h, e.Term)
	h = fnvUint32(h, e.Client)
	h = fnvUint64(h, e.Seq)
	h = fnvUint32(h, uint32(len(e.Cmd))) //nolint:gosec // G115: a command is far shorter than 4 GiB
	for i := 0; i < len(e.Cmd); i++ {
		h ^= uint64(e.Cmd[i])
		h *= fnvPrime
	}
	return h
}

// fnvUint64 feeds x to the FNV-1a state h as eight big-endian bytes.
func fnvUint64(h, x uint64) uint64 { return fnvLow(h, x, 8) }

// fnvUint32 feeds x to the FNV-1a state h as four big-endian bytes.
func fnvUint32(h uint64, x uint32) uint64 { return fnvLow(h, uint64(x), 4) }

// fnvLow feeds the low n bytes of x to the FNV-1a state h, most significant first.
func fnvLow(h, x uint64, n int) uint64 {
	for shift := 8 * (n - 1); shift >= 0; shift -= 8 {
		h ^= x >> uint(shift) & 0xff
		h *= fnvPrime
	}
	return h
}
