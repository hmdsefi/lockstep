// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"fmt"
	"log/slog"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/mailbox"
)

// WithLogger sets the logger the system writes its log events to and that Context.Logger and
// Context.Logf build on. The default, and the value used when l is nil, is slog.Default() as it
// is when New runs. To discard logs, pass slog.New(slog.DiscardHandler).
func WithLogger(l *slog.Logger) Option {
	return func(c *systemConfig) { c.log = l }
}

// systemLogger returns the system logger of a system built on eng with cfg (OBS-010): WithLogger's
// value if non-nil; otherwise slog.Default() at the time of the call on a blocking engine (New's
// production engine, the local engine), and a logger that discards on any other (the simulation
// and manual engines), so an actor there writes nothing outside the simulation (ACT-130). sim
// passes WithLogger with its kernel-log handler, so in simulation the actor loggers write to the
// node's kernel log (OBS-155, OBS-160). newSystem calls it once, so a later slog.SetDefault does
// not change a running system's logger.
func systemLogger(cfg *systemConfig, eng engine.Engine) *slog.Logger {
	switch {
	case cfg.log != nil:
		return cfg.log
	case eng.Blocking:
		return slog.Default()
	default:
		return slog.New(slog.DiscardHandler)
	}
}

// WithMetrics sets the metrics backend. New registers every counter and gauge function with m
// before it returns. nil, the default, disables metrics: nothing is registered or counted.
func WithMetrics(m Metrics) Option {
	return func(c *systemConfig) { c.metrics = m }
}

// Metrics is the backend lockstep reports to. Implementations must be safe for concurrent use.
// lockstep calls Counter and GaugeFunc once per series: from New, in a fixed order, and may call
// them later, from any goroutine, when a package enabled on the system after New registers series
// of its own (late registration). Duplicate registration (two systems with the same node name on
// one backend) is the implementation's to handle.
//
// lockstep does not recover panics from Metrics, Counter or a gauge function.
type Metrics interface {
	// Counter returns the counter for one series. lockstep keeps the result and calls its Add
	// method from any goroutine, including inside actor turns.
	Counter(name, help string, labels ...MetricLabel) Counter

	// GaugeFunc registers fn for one series. The backend calls fn whenever it collects, from any
	// goroutine. fn returns quickly and never blocks a Send. Once the system has shut down, fn
	// returns 0 and no longer refers to the system, except the goroutine count's, which keeps
	// reporting.
	GaugeFunc(name, help string, fn func() int64, labels ...MetricLabel)
}

// Counter is one counter series.
type Counter interface {
	// Add increases the counter by n. lockstep passes n >= 1.
	Add(n uint64)
}

// MetricLabel is one label of a series.
type MetricLabel struct {
	Name  string
	Value string
}

// obsCounters holds the counters of OBS-120 that New registered: a field for each series without
// a label and an array for each labeled series, indexed by the label's value as the event knows it
// (a MailboxPolicy, a StopKind, a dropReason, a deadReason), so an event adds to its counter with
// no lookup and no allocation (OBS-090). A system without metrics has none: System.obs is nil, and
// every event path skips counting after one nil check (OBS-110). The count methods below are the
// event paths' only use of it.
type obsCounters struct {
	spawned     Counter
	stopped     [numFinalStops]Counter // by StopKind - 1
	restarts    Counter
	mailboxFull [numPolicies]Counter          // by MailboxPolicy
	shed        [numPolicies]Counter          // by MailboxPolicy
	drops       [len(dropReasons)]Counter     // by dropReason
	deadLetters [len(deadReasons)]Counter     // by deadReason
	transitions [len(transitionNames)]Counter // by transition
	stuck       Counter
}

const (
	// numPolicies is the number of MailboxPolicy values, FailSend to BlockSender.
	numPolicies = int(BlockSender) + 1
	// numFinalStops is the number of StopKind values an actor's final stop has, StopNormal to
	// StopShutdown (1 to 4): StopRestarting is an incarnation's end, not a stop.
	numFinalStops = int(StopShutdown)
)

// dropReason is why a message or result was discarded, the reason of the message dropped log event
// and of lockstep_drops_total (OBS-145): an index into dropReasons.
type dropReason uint8

const (
	dropNewest          dropReason = iota // a DropNewest mailbox was full: the message being sent is discarded
	dropOldest                            // a DropOldest mailbox was full: its oldest message is discarded
	dropLateReply                         // a reply arrived after its ask had resolved
	dropCancelledEffect                   // the result of an effect that a stop or restart cancelled
)

// dropReasons lists the spelling of every drop reason, in the order of the constants.
var dropReasons = [...]string{
	dropNewest:          "drop-newest",
	dropOldest:          "drop-oldest",
	dropLateReply:       "late-reply",
	dropCancelledEffect: "cancelled-effect",
}

// String returns the reason's spelling.
func (r dropReason) String() string { return dropReasons[r] }

// transition is a change of admission control's combined state, the label to of
// lockstep_admission_transitions_total: an index into transitionNames.
type transition uint8

const (
	toRefusing  transition = iota // admission control started to shed
	toAdmitting                   // admission control stopped shedding
)

// transitionNames lists the spelling of every transition, in the order of the constants.
var transitionNames = [...]string{toRefusing: "refusing", toAdmitting: "admitting"}

// String returns the transition's spelling.
func (t transition) String() string { return transitionNames[t] }

// registerMetrics registers the counters of OBS-120 and then the gauge functions of OBS-130 with m,
// in the order of the tables and, within a labeled series, of its label values, each with the node
// label first (OBS-080), and keeps the counters in s.obs. New calls it once, after newSystem and
// before startOverload: the sampler's first sample, taken in New, can already change admission, and
// the change must be counted. The gauge functions return 0 until New has armed them (armGauges). A
// panic in m propagates, and s.obs, s.live, s.gauges and s.metrics stay nil.
func (s *System) registerMetrics(m Metrics) {
	node := MetricLabel{Name: "node", Value: s.node}
	o := new(obsCounters)
	o.spawned = m.Counter("lockstep_actors_spawned_total", "Actors spawned.", node)
	registerLabeled(m, node, o.stopped[:], "lockstep_actors_stopped_total", "Actors stopped for good.",
		"reason", StopNormal, StopKind.String)
	o.restarts = m.Counter("lockstep_restarts_total", "Actor restarts.", node)
	registerLabeled(m, node, o.mailboxFull[:], "lockstep_mailbox_full_total",
		"Sends that found the target mailbox full.", "policy", FailSend, MailboxPolicy.String)
	registerLabeled(m, node, o.shed[:], "lockstep_shed_total", "User messages shed by admission control.",
		"policy", FailSend, MailboxPolicy.String)
	registerLabeled(m, node, o.drops[:], "lockstep_drops_total", "Messages and results discarded.",
		"reason", dropNewest, dropReason.String)
	registerLabeled(m, node, o.deadLetters[:], "lockstep_deadletters_total", "Messages sent to dead letters.",
		"reason", reasonStopped, deadReason.String)
	registerLabeled(m, node, o.transitions[:], "lockstep_admission_transitions_total",
		"Admission control state changes.", "to", toRefusing, transition.String)
	o.stuck = m.Counter("lockstep_stuck_turns_total", "Turns reported by the watchdog.", node)

	g := new(gaugeState)
	m.GaugeFunc("lockstep_actors", "Live actors.", g.actors, node)
	m.GaugeFunc("lockstep_mailbox_depth_total", "User messages queued in all mailboxes.", g.depthTotal, node)
	m.GaugeFunc("lockstep_mailbox_depth_max", "User messages queued in the fullest mailbox.", g.depthMax, node)
	m.GaugeFunc("lockstep_admission_enabled", "1 if memory admission control is on.", g.admissionEnabled, node)
	m.GaugeFunc("lockstep_admission_refusing", "1 while admission control refuses work.", g.admissionRefusing, node)
	m.GaugeFunc("lockstep_goroutines", "Goroutines in the process.", goroutines, node)
	s.obs, s.gauges, s.live, s.metrics = o, g, new(liveIndex), m
}

// armGauges lets the gauge functions read s. New calls it once, after startOverload, which sets the
// sampler the admission gauge reads: a backend may call a gauge function on another goroutine as
// soon as it holds it, even while New is still running, and until now the functions return 0.
func (s *System) armGauges() {
	if s.gauges != nil {
		s.gauges.sys.Store(s)
	}
}

// gaugeState is what the gauge functions read the system through (OBS-100): the functions capture
// the state and never the system, and Shutdown clears the pointer when the system is stopped, so a
// backend that keeps the functions neither keeps the system alive nor reads a stopped system, and
// each function then returns 0. lockstep_goroutines needs no system and does not use it.
type gaugeState struct{ sys atomic.Pointer[System] }

// actors is lockstep_actors: the live actors as the actor cap counts them.
func (g *gaugeState) actors() int64 {
	if s := g.sys.Load(); s != nil {
		return int64(s.liveActors())
	}
	return 0
}

// depthTotal is lockstep_mailbox_depth_total: the user messages queued in the mailboxes of all live
// actors. It takes the live index's read lock, which only Spawn and a final stop take for writing.
func (g *gaugeState) depthTotal() int64 {
	if s := g.sys.Load(); s != nil {
		total, _ := s.live.depth()
		return total
	}
	return 0
}

// depthMax is lockstep_mailbox_depth_max: the user messages queued in the fullest mailbox of a live
// actor, 0 with no actor.
func (g *gaugeState) depthMax() int64 {
	if s := g.sys.Load(); s != nil {
		_, fullest := s.live.depth()
		return fullest
	}
	return 0
}

// admissionEnabled is lockstep_admission_enabled: 1 when memory admission has thresholds in force.
func (g *gaugeState) admissionEnabled() int64 {
	if s := g.sys.Load(); s != nil {
		if sp := s.prodOvl.sampler; sp != nil && sp.Enabled() {
			return 1
		}
	}
	return 0
}

// admissionRefusing is lockstep_admission_refusing: 1 while admission control sheds, from any
// source.
func (g *gaugeState) admissionRefusing() int64 {
	if s := g.sys.Load(); s != nil && s.shedding() {
		return 1
	}
	return 0
}

// goroutines is lockstep_goroutines, the one gauge that keeps reporting after Shutdown.
func goroutines() int64 { return int64(runtime.NumGoroutine()) }

// liveIndex lists the user mailboxes of a system's live actors for the depth gauges (OBS-130). The
// registry cannot serve them: it has one lock, which every Send takes, and no iteration. The index
// has a lock of its own, taken for writing only by Spawn and by an actor's final stop, which OBS-100
// lets a gauge delay, and for reading by the gauges: never by a Send or a turn. Only a system with
// metrics has one (System.live).
type liveIndex struct {
	mu    sync.RWMutex
	boxes []liveBox // in no particular order
}

// liveBox is the entry of one live actor in a liveIndex.
type liveBox struct {
	q    *mailbox.Queue[entry]
	slot *liveSlot
}

// liveSlot is a cell's place in a liveIndex: one more than its position in boxes, or 0 while the
// cell is not listed. liveIndex.mu guards it.
type liveSlot struct{ at int }

// add lists the user mailbox q of a cell whose place is slot.
func (x *liveIndex) add(q *mailbox.Queue[entry], slot *liveSlot) {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.boxes = append(x.boxes, liveBox{q: q, slot: slot})
	slot.at = len(x.boxes)
}

// remove takes the cell whose place is slot out of the list, if it is in it: the last entry moves
// into its position, so the cost of a removal does not depend on the number of actors.
func (x *liveIndex) remove(slot *liveSlot) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if slot.at == 0 {
		return
	}
	i, last := slot.at-1, len(x.boxes)-1
	moved := x.boxes[last]
	x.boxes[i] = moved
	moved.slot.at = i + 1
	x.boxes[last] = liveBox{} // no reference to the queue stays in the spare capacity
	x.boxes = x.boxes[:last]
	slot.at = 0
}

// depth returns the number of user messages queued in all listed mailboxes and the number queued in
// the fullest one. It reads each mailbox's count without the mailbox's lock, so its cost is linear
// in the number of live actors and no Send or turn waits for it.
func (x *liveIndex) depth() (total, fullest int64) {
	x.mu.RLock()
	defer x.mu.RUnlock()
	for i := range x.boxes {
		n := int64(x.boxes[i].q.SentLenAtomic())
		total += n
		fullest = max(fullest, n)
	}
	return total, fullest
}

// seriesKind is the kind of a late series: a counter or a gauge.
type seriesKind uint8

const (
	kindCounter seriesKind = iota
	kindGauge
)

// String returns the kind's name.
func (k seriesKind) String() string {
	if k == kindGauge {
		return "gauge"
	}
	return "counter"
}

// lateSeries is the table of OBS-150: every series a package enabled on a system after New may
// register, with its kind. Late registration panics for any other name; a new late series is added
// to the table first (OBS owns the names), then here.
var lateSeries = map[string]seriesKind{
	"lockstep_remote_frames_sent_total":     kindCounter,
	"lockstep_remote_frames_received_total": kindCounter,
	"lockstep_remote_bytes_sent_total":      kindCounter,
	"lockstep_remote_bytes_received_total":  kindCounter,
	"lockstep_remote_reconnects_total":      kindCounter,
	"lockstep_remote_gaps_total":            kindCounter,
	"lockstep_remote_sessions":              kindGauge,
}

// lateRegistry holds the series registered on a system after New (OBS-150). It is a field of the
// System, so it lives exactly as long as the system, and the late gauge functions a backend keeps
// do not refer to it.
type lateRegistry struct {
	// mu makes a registration atomic with the check for an earlier one of the same series and with
	// the check that Shutdown has not started, and orders both with dropGauges. It is held while the
	// backend registers the series; no event path takes it.
	mu sync.Mutex
	// series holds, by seriesKey, the Add function of each late counter and nil for each late gauge.
	series map[string]func(n uint64)
	// gauges are the late gauges, whose functions dropGauges drops.
	gauges []*lateGauge
}

// lateGauge is a late gauge (OBS-150). Its read method is the function lockstep registers: it reads
// through the gauge state of the system's other gauges and calls fn, the function its caller
// passed. dropGauges clears fn at Shutdown, so a backend that keeps read keeps neither the system
// nor fn.
type lateGauge struct {
	state *gaugeState
	fn    atomic.Pointer[func() int64]
}

// read returns fn's value, or 0 once the system has shut down.
func (g *lateGauge) read() int64 {
	if g.state.sys.Load() == nil {
		return 0
	}
	if fn := g.fn.Load(); fn != nil {
		return (*fn)()
	}
	return 0
}

// dropGauges makes every late gauge return 0 and drop its function. completeShutdown calls it.
func (l *lateRegistry) dropGauges() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, g := range l.gauges {
		g.fn.Store(nil)
	}
	l.gauges = nil
}

// add records the late series key as registered, with the Add function of a counter or nil for a
// gauge. The caller holds l.mu.
func (l *lateRegistry) add(key string, add func(n uint64)) {
	if l.series == nil {
		l.series = make(map[string]func(n uint64))
	}
	l.series[key] = add
}

// metricCounterHook is hooks.MetricCounter (OBS-150): it registers the counter series name with
// labels on the backend of sys, once, and returns its Add function; it returns nil with metrics
// off or once Shutdown has started.
func metricCounterHook(sys any, name, help string, labels ...string) func(n uint64) {
	s := hookSystem("MetricCounter", sys)
	checkLate(name, kindCounter, labels)
	if s.metrics == nil {
		return nil
	}
	key := seriesKey(name, labels)
	l := &s.late
	l.mu.Lock()
	defer l.mu.Unlock()
	if s.closing.Load() {
		return nil
	}
	if add, ok := l.series[key]; ok {
		return add
	}
	add := s.metrics.Counter(name, help, s.lateLabels(labels)...).Add
	l.add(key, add)
	return add
}

// metricGaugeHook is hooks.MetricGauge (OBS-150): it registers fn as the gauge series name with
// labels on the backend of sys, once, through a lateGauge; it does nothing with metrics off or once
// Shutdown has started.
func metricGaugeHook(sys any, name, help string, fn func() int64, labels ...string) {
	s := hookSystem("MetricGauge", sys)
	checkLate(name, kindGauge, labels)
	if s.metrics == nil {
		return
	}
	key := seriesKey(name, labels)
	l := &s.late
	l.mu.Lock()
	defer l.mu.Unlock()
	if s.closing.Load() {
		return
	}
	if _, ok := l.series[key]; ok {
		return
	}
	g := &lateGauge{state: s.gauges}
	g.fn.Store(&fn)
	s.metrics.GaugeFunc(name, help, g.read, s.lateLabels(labels)...)
	l.add(key, nil)
	l.gauges = append(l.gauges, g)
}

// checkLate panics unless name is a series of OBS-150's table of kind kind and labels is a list of
// name, value pairs (OBS §7).
func checkLate(name string, kind seriesKind, labels []string) {
	got, ok := lateSeries[name]
	switch {
	case !ok:
		panic("lockstep: metrics: " + name + " is not a late series")
	case got != kind:
		panic(fmt.Sprintf("lockstep: metrics: %s is a late %s, not a %s", name, got, kind))
	case len(labels)%2 != 0:
		panic("lockstep: metrics: " + name + ": odd label list")
	}
}

// seriesKey returns the key of the late series name with labels: each string prefixed with its
// length, so two different lists never share a key.
func seriesKey(name string, labels []string) string {
	var b strings.Builder
	for _, p := range append([]string{name}, labels...) {
		b.WriteString(strconv.Itoa(len(p)))
		b.WriteByte(':')
		b.WriteString(p)
	}
	return b.String()
}

// lateLabels returns the labels of a late series of s: node, then the name, value pairs of labels
// in order (OBS-150).
func (s *System) lateLabels(labels []string) []MetricLabel {
	out := make([]MetricLabel, 0, 1+len(labels)/2)
	out = append(out, MetricLabel{Name: "node", Value: s.node})
	for i := 0; i < len(labels); i += 2 {
		out = append(out, MetricLabel{Name: labels[i], Value: labels[i+1]})
	}
	return out
}

// registerLabeled registers one counter series of name for each element of dst, in order: element i
// carries the label label with the value of the i-th label value counted from first, and is
// stored in dst[i].
func registerLabeled[K ~uint8](m Metrics, node MetricLabel, dst []Counter, name, help, label string,
	first K, value func(K) string,
) {
	for i, k := 0, first; i < len(dst); i, k = i+1, k+1 {
		dst[i] = m.Counter(name, help, node, MetricLabel{Name: label, Value: value(k)})
	}
}

// The count methods add 1 to the counter of one event, after one nil check when the system has no
// metrics (OBS-110). The event helpers of obs_log.go call them first, so a disabled log level does
// not skip counting, and none of them holds a lockstep lock: Counter.Add is user code (OBS §7).

// countSpawned counts a successful Spawn.
func (s *System) countSpawned() {
	if o := s.obs; o != nil {
		o.spawned.Add(1)
	}
}

// countStopped counts the final stop of an actor with the stop kind kind.
func (s *System) countStopped(kind StopKind) {
	if o := s.obs; o != nil {
		// Only StopNormal to StopShutdown end an actor for good.
		if i := int(kind) - 1; uint(i) < uint(len(o.stopped)) {
			o.stopped[i].Add(1)
		}
	}
}

// countRestart counts an actor's incarnation that ended for a restart.
func (s *System) countRestart() {
	if o := s.obs; o != nil {
		o.restarts.Add(1)
	}
}

// countMailboxFull counts a send that found the mailbox of c full.
func (s *System) countMailboxFull(c actorCell) {
	if o := s.obs; o != nil {
		o.mailboxFull[c.policy()].Add(1)
	}
}

// countShed counts a message to c that admission control shed, or that was refused because c is
// stuck.
func (s *System) countShed(c actorCell) {
	if o := s.obs; o != nil {
		o.shed[c.policy()].Add(1)
	}
}

// countDrop counts a message or result discarded for reason.
func (s *System) countDrop(reason dropReason) {
	if o := s.obs; o != nil {
		o.drops[reason].Add(1)
	}
}

// countDeadLetter counts a message that went to dead letters for reason.
func (s *System) countDeadLetter(reason deadReason) {
	if o := s.obs; o != nil {
		o.deadLetters[reason].Add(1)
	}
}

// countTransition counts a change of admission control's combined state: to refusing when
// shedding, to admitting otherwise.
func (s *System) countTransition(shedding bool) {
	if o := s.obs; o != nil {
		to := toAdmitting
		if shedding {
			to = toRefusing
		}
		o.transitions[to].Add(1)
	}
}

// countStuck counts a turn the watchdog reported.
func (s *System) countStuck() {
	if o := s.obs; o != nil {
		o.stuck.Add(1)
	}
}
