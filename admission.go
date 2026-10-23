// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/hmdsefi/lockstep/internal/engine"
)

// admissionState is a system's view of its engine's admission (OVL-150): whether it sheds, and the
// change that started the current shedding period. The engine's Admission decides each spawn and
// each user message itself (OVL-100, OVL-110); this state serves the waiting outside senders,
// whose admit must read it under a mailbox's lock (MBX-075).
type admissionState struct {
	shedding atomic.Bool // a source is on: set and cleared by the change handler only

	mu     sync.Mutex             // guards period; a leaf
	period engine.AdmissionChange // the change that started the current shedding period; zero while admitting
}

// bindAdmission connects the system to its engine's admission, once, while the system is built
// (ENG-125, OVL §4.3): the engine reports every source change to admissionChanged and reads the
// system's usage through usage.
func (s *System) bindAdmission() { s.eng.Admission.Bind(s.admissionChanged, s.usage) }

// admissionChanged handles one source change (OVL-150). It records event=on or event=off, then
// updates the shedding state, and on a change to admitting calls Recheck on every user mailbox
// that outside senders wait on (MBX-075). The record comes first, so a waiter that the change
// releases records its delivery after it. The state is updated before the mailboxes are listed:
// a sender that starts waiting meanwhile either sees the system admitting or is listed (its
// mailbox is listed before it offers). It runs in engine context or on any goroutine, never
// concurrently with itself (§4.3), and takes no lock that Spawn or Send hold while they ask
// admission; a released waiter schedules its target itself (ENG-350).
func (s *System) admissionChanged(c engine.AdmissionChange) {
	s.recordAdmissionChange(c)
	s.adm.mu.Lock()
	switch {
	case !c.Shedding:
		s.adm.period = engine.AdmissionChange{}
	case !s.adm.shedding.Load():
		s.adm.period = c
	}
	s.adm.shedding.Store(c.Shedding)
	s.adm.mu.Unlock()
	if !c.Shedding {
		s.waiting.recheck()
	}
}

// shedding reports whether the system sheds: a source is on (OVL-150).
func (s *System) shedding() bool { return s.adm.shedding.Load() }

// admits reports whether the system admits a waiting outside sender's message now (MBX-075): no
// overload source is on (OVL-150). Phase 1 marks no actor stuck (OVL-520). It is the admit of
// every waiter on every mailbox, called with a mailbox's lock held, and takes no lock.
func (s *System) admits() bool { return !s.shedding() }

// usage is the usage function the system binds to admission (§4.3): its live-actor count and the
// sum of its actors' estimates, which is 0 while estimates are off (OVL-400).
func (s *System) usage() engine.Usage {
	return engine.Usage{Actors: s.liveActors(), Bytes: s.memBytes.Load()}
}

// admitSend asks admission whether to admit a user message of Send or the request of an Ask to the
// actor at target, once per message (OVL-110), and returns nil or admission's error. A message to
// a system actor is never shed and admission is not asked.
func (s *System) admitSend(target Address) error {
	if !strings.HasPrefix(target.path, "/user/") {
		return nil
	}
	return s.eng.Admission.AdmitSend()
}

// overloadError is the error of work refused while the system sheds: a normal spawn (OVL-100) or a
// message shed under FailSend, or BlockSender acting as FailSend (OVL-120). It matches
// ErrOverloaded, ErrMailboxFull for a message, and admission's own error.
type overloadError struct {
	text   string
	parent string // a refused spawn's parent path; empty for a message
	reason string // the reason of OVL §8: the source that turned shedding on
	errs   []error
}

func (e *overloadError) Error() string { return e.text }

func (e *overloadError) Unwrap() []error { return e.errs }

// spawnOverloaded returns the error of a normal spawn under the parent at parentPath that
// admission refused with err (OVL-100).
func spawnOverloaded(parentPath string, err error) *overloadError {
	reason := overloadReason(err)
	return &overloadError{
		text:   "lockstep: spawn in " + parentPath + ": overloaded (" + reason + ")",
		parent: parentPath,
		reason: reason,
		errs:   []error{ErrOverloaded, err},
	}
}

// sendOverloaded returns the error of a message to to that admission refused with err and the
// mailbox's policy refused as if full (OVL-120). The ask of a shed request fails with it too, also
// when the message was dropped (MBX-092).
func sendOverloaded(to Address, err error) error {
	reason := overloadReason(err)
	return &overloadError{
		text:   "lockstep: send to " + to.String() + ": overloaded (" + reason + "), mailbox treated as full",
		reason: reason,
		errs:   []error{ErrOverloaded, ErrMailboxFull, err},
	}
}

// overloadReason returns the reason admission's error err names: its source's String (memory,
// budget or pressure). An engine whose Admission breaks §4.3 by returning another error is named by
// that error's text.
func overloadReason(err error) string {
	var ae *engine.AdmissionError
	if errors.As(err, &ae) {
		return ae.Source.String()
	}
	return err.Error()
}

// recordRefuse emits lockstep.overload with event=refuse (OVL §8) for the spawn refused with e.
func (s *System) recordRefuse(e *overloadError) {
	tr := s.eng.Tracer
	if !tr.Enabled() {
		return
	}
	tr.Record("lockstep.overload",
		engine.Attr{Key: "event", Value: "refuse"},
		engine.Attr{Key: "parent", Value: e.parent},
		engine.Attr{Key: "reason", Value: e.reason},
	)
}

// recordShed emits lockstep.overload with event=shed (OVL §8) for the message of r that c's
// mailbox treated as full because admission refused it with err. dropped is new, oldest, or none
// for a BlockSender wait that starts.
func (s *System) recordShed(c actorCell, r routeRequest, err error, dropped string) {
	tr := s.eng.Tracer
	if !tr.Enabled() {
		return
	}
	_, _, policy := c.mailboxState()
	tr.Record("lockstep.overload",
		engine.Attr{Key: "event", Value: "shed"},
		engine.Attr{Key: "from", Value: r.from.String()},
		engine.Attr{Key: "to", Value: r.to.String()},
		engine.Attr{Key: "reason", Value: overloadReason(err)},
		engine.Attr{Key: "policy", Value: policy.String()},
		engine.Attr{Key: "dropped", Value: dropped},
	)
}

// recordAdmissionChange emits lockstep.overload with event=on or event=off (OVL §8) for c.
func (s *System) recordAdmissionChange(c engine.AdmissionChange) {
	tr := s.eng.Tracer
	if !tr.Enabled() {
		return
	}
	event := "off"
	if c.On {
		event = "on"
	}
	tr.Record("lockstep.overload",
		engine.Attr{Key: "event", Value: event},
		engine.Attr{Key: "reason", Value: c.Source.String()},
		engine.Attr{Key: "live", Value: strconv.FormatUint(c.Live, 10)},
		engine.Attr{Key: "high", Value: strconv.FormatUint(c.High, 10)},
		engine.Attr{Key: "low", Value: strconv.FormatUint(c.Low, 10)},
		engine.Attr{Key: "shedding", Value: strconv.FormatBool(c.Shedding)},
	)
}
