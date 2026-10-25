// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package raftsim runs the Raft demo under lockstep/sim with faults, invariants and final checks.
// Import it only from tests.
package raftsim

import (
	"fmt"
	"strconv"

	"github.com/hmdsefi/faultline/kernel"

	"github.com/hmdsefi/lockstep/examples/raft/kv"
	"github.com/hmdsefi/lockstep/examples/raft/raft"
)

// Check names: invariant and final names, and the "check" field of faultline's results.
const (
	CheckHarness            = "harness"
	CheckElectionSafety     = "election safety"
	CheckLogMatching        = "log matching"
	CheckLeaderCompleteness = "leader completeness"
	CheckStateMachineSafety = "state machine safety"
	CheckDurableState       = "durable state"
	CheckLinearizable       = "linearizable"
	CheckProgress           = "progress"
	CheckReplicasAgree      = "replicas agree"
)

// Oracle observes every server and holds the first violation of each check.
//
// It runs inside simulation turns, so it follows the demo's determinism rules: its maps are
// accessed by key only, and violation and record texts hold no pointers and no map-ordered content.
type Oracle struct {
	clients int                 // workload clients; client ID clients+1 is the probe
	now     func() kernel.Time  // virtual time, for violation texts
	emit    func(kernel.Record) // record sink; Node is already the server's NodeID (see newOracle)

	servers []*server          // in the order newOracle got them
	byName  map[string]*server // servers by node name
	errs    map[string]error   // first violation per check

	leaders  map[uint64][]leader    // per term, the servers that became its leader, first first
	highest  uint64                 // the highest term any server became leader of
	prefixes map[logPoint]logPrefix // RFT-450: first chain hash seen per (index, term)

	// The canonical log: canon[i-1] is index i.
	canon    []canonical
	commands map[command]uint64 // canonical index of each client command

	// The reference state machine replays canonical entries in index order.
	ref       *kv.Store
	refHashes []uint64              // refHashes[i-1]: state hash after applying canonical 1..i
	values    map[string][]keyValue // per key, the values written, in index order
	okIndex   map[command]uint64    // the Index of the first OK reply per command
}

// server is the oracle's view of one server: its durable state, which outlives incarnations,
// and the state of its current incarnation.
type server struct {
	name string
	id   kernel.NodeID

	d, p       durableState // RFT-480: last Durable or Recovered; last PersistStarted
	hasD, hasP bool

	recovered bool   // the current incarnation called Recovered
	applied   uint64 // last applied index in the current incarnation
	stateHash uint64 // state hash of the last Applied call (or of the empty store)
}

// durableState is the (term, vote, last, chain) tuple RFT-480 compares.
type durableState struct {
	term  uint64
	vote  string
	last  uint64
	chain uint64
}

// leader is one BecameLeader report.
type leader struct {
	node string
	at   kernel.Time
}

// logPoint is a log index and the term of its entry.
type logPoint struct{ index, term uint64 }

// logPrefix is the first chain hash seen for a logPoint and the server that reported it.
type logPrefix struct {
	chain uint64
	node  string
}

// canonical is one committed entry: the entry, its chain hash, its commit term (the term of the
// first server that reported it committed) and that server.
type canonical struct {
	entry raft.Entry
	chain uint64
	term  uint64
	by    string
}

// command identifies a client command.
type command struct {
	client uint32
	seq    uint64
}

// keyValue is a value written to a key by the canonical entry at index.
type keyValue struct {
	index uint64
	value string
}

var _ raft.Observer = (*Oracle)(nil)

// newOracle returns an oracle for the named servers and clients workload clients (process names
// c1..c<clients>, then probe). now reads virtual time and emit receives the oracle's records.
//
// A record's Node is the NodeID of the server it is about, taken as its position in servers plus
// one: Setup adds the servers first, in this order, and lockstep/sim adds no nodes of its own, so
// servers[i] is node i+1.
func newOracle(servers []string, clients int, now func() kernel.Time, emit func(kernel.Record)) *Oracle {
	o := &Oracle{
		clients:  clients,
		now:      now,
		emit:     emit,
		byName:   make(map[string]*server, len(servers)),
		errs:     make(map[string]error),
		leaders:  make(map[uint64][]leader),
		prefixes: make(map[logPoint]logPrefix),
		commands: make(map[command]uint64),
		ref:      kv.New(),
		values:   make(map[string][]keyValue),
		okIndex:  make(map[command]uint64),
	}
	for i, name := range servers {
		s := &server{name: name, id: kernel.NodeID(i + 1)} //nolint:gosec // G115: at most 5 servers
		o.servers = append(o.servers, s)
		o.byName[name] = s
	}
	return o
}

// boot starts a new incarnation of server: it has not recovered and has applied nothing.
func (o *Oracle) boot(server string) {
	s := o.byName[server]
	if s == nil {
		panic("raftsim: boot of unknown server " + strconv.Quote(server))
	}
	s.recovered = false
	s.applied = 0
	s.stateHash = raft.ChainSeed
}

// Err returns the first violation of check, or nil.
func (o *Oracle) Err(check string) error {
	return o.errs[check]
}

// fail stores a violation of check unless check already has one.
func (o *Oracle) fail(check, format string, args ...any) {
	if o.errs[check] == nil {
		o.errs[check] = fmt.Errorf(format, args...)
	}
}

// observe returns the server behind an Observer call. An unknown server is a harness violation
// and returns nil; any method but Recovered before the incarnation's Recovered is one too.
func (o *Oracle) observe(node, method string) *server {
	s := o.byName[node]
	if s == nil {
		o.fail(CheckHarness, "%s: %s from a server not in the cluster", node, method)
		return nil
	}
	if method != "Recovered" && !s.recovered {
		o.fail(CheckHarness, "%s: %s before Recovered", node, method)
	}
	return s
}

// Recovered checks the loaded state against the durable state (RFT-480) and the log against the
// prefixes seen so far (RFT-450), and starts the incarnation's checks.
func (o *Oracle) Recovered(node string, hs raft.HardState, log raft.LogView) {
	s := o.observe(node, "Recovered")
	if s == nil {
		return
	}
	s.recovered = true
	s.applied = 0
	s.stateHash = raft.ChainSeed

	last := log.LastIndex()
	got := durableState{term: hs.Term, vote: hs.Vote, last: last, chain: log.Chain(last)}
	want := durableState{chain: raft.ChainSeed}
	if s.hasD {
		want = s.d
	}
	if got != want && (!s.hasP || got != s.p) {
		o.fail(CheckDurableState, "%s recovered %s, but persisted %s", node, got, want)
	}
	s.d, s.hasD = got, true
	s.hasP = false

	o.matchPrefixes(node, 1, log)
}

// PersistStarted records the state a persist covers until its Durable call (RFT-480).
func (o *Oracle) PersistStarted(node string, hs raft.HardState, last, chain uint64) {
	s := o.observe(node, "PersistStarted")
	if s == nil {
		return
	}
	s.p = durableState{term: hs.Term, vote: hs.Vote, last: last, chain: chain}
	s.hasP = true
}

// Durable records the state a completed persist covers (RFT-480).
func (o *Oracle) Durable(node string, hs raft.HardState, last, chain uint64) {
	s := o.observe(node, "Durable")
	if s == nil {
		return
	}
	s.d = durableState{term: hs.Term, vote: hs.Vote, last: last, chain: chain}
	s.hasD = true
	s.hasP = false
}

// LogWritten checks the written part of the log against the prefixes seen so far (RFT-450).
func (o *Oracle) LogWritten(node string, from uint64, log raft.LogView) {
	if o.observe(node, "LogWritten") == nil {
		return
	}
	o.matchPrefixes(node, max(from, 1), log)
}

// matchPrefixes checks the chain hash of every index from from to the end of log against the
// first one seen for the same index and term (RFT-450).
func (o *Oracle) matchPrefixes(node string, from uint64, log raft.LogView) {
	for i := from; i <= log.LastIndex(); i++ {
		key := logPoint{index: i, term: log.Term(i)}
		h := log.Chain(i)
		first, ok := o.prefixes[key]
		if !ok {
			o.prefixes[key] = logPrefix{chain: h, node: node}
			continue
		}
		if first.chain != h {
			o.fail(CheckLogMatching, "index %d term %d: %s has prefix hash %s but %s has %s",
				i, key.term, first.node, hashText(first.chain), node, hashText(h))
		}
	}
}

// BecameLeader emits the raft.leader record and checks election safety (RFT-440) and leader
// completeness (RFT-460).
func (o *Oracle) BecameLeader(node string, term uint64, log raft.LogView) {
	s := o.observe(node, "BecameLeader")
	if s == nil {
		return
	}
	last := log.LastIndex()
	o.emit(kernel.Record{
		Node: s.id,
		Kind: "raft.leader",
		Text: "leader of term " + strconv.FormatUint(term, 10),
		Attrs: []kernel.Attr{
			{Key: "term", Value: strconv.FormatUint(term, 10)},
			{Key: "last", Value: strconv.FormatUint(last, 10)},
		},
	})

	now := o.now()
	seen := o.leaders[term]
	if len(seen) > 0 && seen[0].node != node {
		o.fail(CheckElectionSafety, "term %d has two leaders: %s (since %s) and %s (at %s)",
			term, seen[0].node, seen[0].at, node, now)
	}
	known := false
	for _, l := range seen {
		known = known || l.node == node
	}
	if !known {
		o.leaders[term] = append(seen, leader{node: node, at: now})
	}
	o.highest = max(o.highest, term)

	m := uint64(0) // the largest canonical index committed in a term below term
	for i := len(o.canon); i >= 1; i-- {
		if o.canon[i-1].term < term {
			m = uint64(i)
			break
		}
	}
	switch {
	case m == 0:
	case last < m:
		o.fail(CheckLeaderCompleteness, "%s became leader of term %d with log ending at %d, missing committed index %d",
			node, term, last, m)
	case log.Chain(m) != o.canon[m-1].chain:
		o.fail(CheckLeaderCompleteness, "%s became leader of term %d with a different entry at committed index %d",
			node, term, m)
	}
}

// latestLeaders returns the servers that became leader of the highest term seen, first first;
// none before the first BecameLeader.
func (o *Oracle) latestLeaders() []string {
	var names []string
	for _, l := range o.leaders[o.highest] {
		names = append(names, l.node)
	}
	return names
}

// Committed checks the newly committed indexes against the canonical log and extends it
// (RFT-470).
func (o *Oracle) Committed(node string, term, from, to uint64, log raft.LogView) {
	if o.observe(node, "Committed") == nil {
		return
	}
	if last := log.LastIndex(); to > last {
		o.fail(CheckHarness, "%s: committed index %d beyond its log (ends at %d)", node, to, last)
		to = last
	}
	// Indexes up to from were committed earlier, so they are canonical already; start at the
	// canonical end if they are not, so the canonical log never has a gap.
	start := min(from, uint64(len(o.canon))) + 1
	for i := start; i <= to; i++ {
		e, h := log.Entry(i), log.Chain(i)
		if i <= uint64(len(o.canon)) {
			c := o.canon[i-1]
			if c.chain != h {
				o.fail(CheckStateMachineSafety, "index %d committed as term=%d hash=%s by %s but as term=%d hash=%s by %s",
					i, c.entry.Term, hashText(c.chain), c.by, e.Term, hashText(h), node)
			}
			continue
		}
		o.appendCanonical(node, term, e, h)
	}
}

// appendCanonical appends a newly committed entry to the canonical log, checks that its command
// is not canonical already, and applies it to the reference state machine.
func (o *Oracle) appendCanonical(node string, term uint64, e raft.Entry, chain uint64) {
	o.canon = append(o.canon, canonical{entry: e, chain: chain, term: term, by: node})
	i := uint64(len(o.canon))
	if e.Client != 0 {
		cmd := command{client: e.Client, seq: e.Seq}
		if j, ok := o.commands[cmd]; ok {
			o.fail(CheckStateMachineSafety, "command %s committed at index %d and again at index %d",
				o.commandText(e.Client, e.Seq), j, i)
		} else {
			o.commands[cmd] = i
		}
	}
	if e.Cmd != "" && o.ref.Apply(i, e.Cmd) == "" { // only a put has an empty output
		if key, value, ok := decodePut(e.Cmd); ok {
			o.values[key] = append(o.values[key], keyValue{index: i, value: value})
		}
	}
	o.refHashes = append(o.refHashes, o.ref.Hash())
}

// Applied checks the incarnation's apply order (RFT-490) and the applied entry against the
// canonical log (RFT-470).
func (o *Oracle) Applied(node string, index uint64, e raft.Entry, stateHash uint64) {
	s := o.observe(node, "Applied")
	if s == nil {
		return
	}
	if index != s.applied+1 {
		o.fail(CheckHarness, "%s: applied index %d after %d", node, index, s.applied)
	}
	s.applied = index
	s.stateHash = stateHash

	switch {
	case index == 0:
	case index > uint64(len(o.canon)):
		o.fail(CheckStateMachineSafety, "%s applied index %d beyond the committed log (ends at %d)",
			node, index, len(o.canon))
	case e.Term != o.canon[index-1].entry.Term:
		o.fail(CheckStateMachineSafety, "%s applied index %d term=%d but the committed entry has term=%d",
			node, index, e.Term, o.canon[index-1].entry.Term)
	case e != o.canon[index-1].entry:
		o.fail(CheckStateMachineSafety, "%s applied index %d term=%d but the committed entry has a different command",
			node, index, e.Term)
	}
}

// Replied checks that an OK reply names a canonical index (RFT-490) and records that index for
// the final checks.
//
// Index 0 is not canonical for a write (output "", the only command with an empty output) but is
// for a read: a leader that answers reads from local state before applying anything replies with
// its applied index 0, which the linearizable final judges as a read of the empty state.
func (o *Oracle) Replied(node string, client uint32, seq uint64, r raft.Result) {
	if o.observe(node, "Replied") == nil || r.Status != raft.OK {
		return
	}
	if r.Index > uint64(len(o.canon)) || (r.Index == 0 && r.Output == "") {
		o.fail(CheckHarness, "%s: replied ok for %s at index %d, but index %d is not canonical",
			node, o.commandText(client, seq), r.Index, r.Index)
	}
	cmd := command{client: client, seq: seq}
	if _, ok := o.okIndex[cmd]; !ok {
		o.okIndex[cmd] = r.Index
	}
}

// commandText names a command as "<process>#<seq>": c<k> for a workload client, probe for the
// probe.
func (o *Oracle) commandText(client uint32, seq uint64) string {
	p := "c" + strconv.FormatUint(uint64(client), 10)
	if int(client) == o.clients+1 {
		p = "probe"
	}
	return p + "#" + strconv.FormatUint(seq, 10)
}

// String formats d as "term=<t> vote=<v> last=<i> hash=<h>", with vote "none" for an empty one.
func (d durableState) String() string {
	vote := d.vote
	if vote == "" {
		vote = "none"
	}
	return fmt.Sprintf("term=%d vote=%s last=%d hash=%s", d.term, vote, d.last, hashText(d.chain))
}

// hashText formats a hash as 0x and sixteen hex digits.
func hashText(h uint64) string {
	return fmt.Sprintf("0x%016x", h)
}

// decodePut splits a KV put command "p<len>:<key><value>" (RFT-310).
func decodePut(cmd string) (key, value string, ok bool) {
	if len(cmd) < 1 || cmd[0] != 'p' {
		return "", "", false
	}
	n, rest, found := cutDecimal(cmd[1:])
	if !found || n > len(rest) {
		return "", "", false
	}
	return rest[:n], rest[n:], true
}

// cutDecimal splits s at its first ':' and parses the part before it as a plain decimal number.
func cutDecimal(s string) (n int, rest string, ok bool) {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == ':' && i > 0 {
			n, err := strconv.Atoi(s[:i])
			return n, s[i+1:], err == nil
		}
		if c < '0' || c > '9' {
			return 0, "", false
		}
	}
	return 0, "", false
}
