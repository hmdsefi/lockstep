// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"log/slog"

	"github.com/hmdsefi/lockstep/internal/engine"
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
// and manual engines), so an actor there writes nothing outside the simulation (ACT-130,
// OBS-155). newSystem calls it once, so a later slog.SetDefault does not change a running
// system's logger.
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
	// goroutine. fn returns quickly and never blocks a Send.
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

// registerMetrics registers the counters of OBS-120 with m, in the order of the table and, within
// a labeled series, of its label values, each with the node label first (OBS-080), and keeps them
// in s.obs. New calls it once, after newSystem and before startOverload: the sampler's first
// sample, taken in New, can already change admission, and the change must be counted. A panic in
// m propagates, and s.obs stays nil.
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
	s.obs = o
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
