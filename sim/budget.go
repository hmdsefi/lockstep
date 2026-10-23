// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim

import (
	"fmt"
	"slices"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/internal/engine"
)

// Budget sets count-based admission budgets for every lockstep node. A node system starts
// shedding (source budget) when a count reaches its budget and stops when every count is at or
// below ResumePercent percent of its budget. The zero value sets no budget.
type Budget struct {
	// Actors is the budget of live actors per node; 0 sets no actor budget.
	Actors int
	// Bytes is the budget of the system's memory estimate (the lockstep.Sizer and
	// lockstep.MemSizer estimates); 0 sets no byte budget.
	Bytes int64
	// ResumePercent is the resume mark in percent of each budget, 1 to 99; 0 means 80.
	ResumePercent int
}

// defaultResumePercent is the resume mark of a Budget whose ResumePercent is 0 (SFT-130).
const defaultResumePercent = 80

// problem returns what makes b invalid (SFT-130), or "" if it is valid.
func (b Budget) problem() string {
	switch {
	case b.Actors < 0:
		return fmt.Sprintf("Actors %d is negative", b.Actors)
	case b.Bytes < 0:
		return fmt.Sprintf("Bytes %d is negative", b.Bytes)
	case b.ResumePercent < 0 || b.ResumePercent > 99:
		return fmt.Sprintf("ResumePercent %d is outside 0..99", b.ResumePercent)
	}
	return ""
}

// validateBudget returns SIM-020's error for an invalid b (SFT-130), or nil.
func validateBudget(b Budget) error {
	if p := b.problem(); p != "" {
		return fmt.Errorf("lockstep: sim: Options.Budget: %s", p)
	}
	return nil
}

// set reports whether b sets any budget.
func (b Budget) set() bool { return b.Actors > 0 || b.Bytes > 0 }

// resume returns b's resume mark in percent: ResumePercent, or 80 for 0 (SFT-130).
func (b Budget) resume() int64 {
	if b.ResumePercent == 0 {
		return defaultResumePercent
	}
	return int64(b.ResumePercent)
}

// reached reports whether u reaches a budget of b (SFT-140).
func (b Budget) reached(u engine.Usage) bool {
	return b.Actors > 0 && u.Actors >= b.Actors || b.Bytes > 0 && u.Bytes >= b.Bytes
}

// resumed reports whether every count of u is at or below its resume mark, a count without a
// budget being always at it (SFT-140).
func (b Budget) resumed(u engine.Usage) bool {
	p := b.resume()
	return (b.Actors == 0 || int64(u.Actors)*100 <= int64(b.Actors)*p) &&
		(b.Bytes == 0 || u.Bytes*100 <= b.Bytes*p)
}

// systemOptions returns the options of a node system at boot: Options.System, followed by
// lockstep.WithMemoryEstimates() when a byte budget is set, so that the usage has the system's
// estimate (SIM-070, SFT-140, OVL-400).
func (s *Sim) systemOptions() []lockstep.Option {
	opts := slices.Clip(s.system)
	if s.budget.Bytes > 0 {
		opts = append(opts, lockstep.WithMemoryEstimates())
	}
	return opts
}

// changesUsage reports whether a record of kind can change the usage of its system: the records
// after which SFT-140 re-reads it.
func changesUsage(kind string) bool {
	switch kind {
	case "lockstep.spawn", "lockstep.stop", "lockstep.deliver", "lockstep.turn":
		return true
	}
	return false
}

// checkBudget re-reads the usage of e's system after a record that can change it and turns the
// budget source of e's gate on or off when SFT-140's rules say so: on when a count reaches its
// budget, off once every count is back at or below its resume mark. It calls Gate.Set only on a
// change, from the record's event (SFT-140, SFT-160).
func (e *simEngine) checkBudget() {
	b := e.s.budget
	if !b.set() {
		return
	}
	u := e.gate.Usage()
	var exceeded bool
	if e.budgetExceeded {
		exceeded = !b.resumed(u)
	} else {
		exceeded = b.reached(u)
	}
	if exceeded == e.budgetExceeded {
		return
	}
	e.budgetExceeded = exceeded
	// The estimate is a sum of non-negative sizes (OVL-410, OVL-420) and validate rejects a
	// negative byte budget (SFT-130), so the conversions keep every value.
	e.gate.Set(engine.SourceBudget, exceeded, uint64(u.Bytes), uint64(b.Bytes), uint64(b.Bytes*b.resume()/100)) //nolint:gosec // G115: non-negative values
}
