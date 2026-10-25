// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package toys

import (
	"fmt"
	"time"

	"github.com/hmdsefi/lockstep"
)

// GroupMsg is the message type of the group.
type GroupMsg interface{ groupMsg() }

// StopFirst tells the group to stop its first worker.
type StopFirst struct{}

// Report is a worker's report to its group.
type Report struct{ Worker int }

// workerMsg is the message type of the workers.
type workerMsg interface{ workerMsg() }

// work is the worker's timer message: report to the group.
type work struct{}

func (StopFirst) groupMsg() {}
func (Report) groupMsg()    {}
func (work) workerMsg()     {}

// Describe returns "stop-first".
func (StopFirst) Describe() string { return "stop-first" }

// Describe returns "report <Worker>".
func (m Report) Describe() string { return fmt.Sprintf("report %d", m.Worker) }

// Describe returns "work".
func (work) Describe() string { return "work" }

// GroupProbe is what the groups and their workers report to the test. One probe may serve the
// groups of several nodes.
type GroupProbe struct {
	Started    int // actors whose Started ran, groups and workers
	Stopped    int // actors whose Stopped ran
	Terminated int // Terminated signals the groups received
	Reports    int // reports the groups received
}

// SpawnGroup spawns the actor "group" under the user guardian of sys. When it starts, the group
// spawns size-1 workers as its children: worker i is named "w<i>" for odd i and gets a generated
// name for even i. It watches the first two workers, w1 and the first generated one. Each worker
// reports to the group once, after a delay of 1 ms plus a draw from its random stream between 0
// and 19 ms. On StopFirst the group stops w1 with lockstep.Stop. Every actor counts its start and
// its stop in p, and the group counts the Terminated signals and the reports it receives
// (AT-ACT-33). It returns an error for a size below 3, before it spawns anything.
func SpawnGroup(sys *lockstep.System, size int, p *GroupProbe) (lockstep.PID[GroupMsg], error) {
	if size < 3 {
		return lockstep.PID[GroupMsg]{}, invalid("size", size)
	}
	return lockstep.Spawn(sys, "group", func() lockstep.Actor[GroupMsg] { return &group{p: p, size: size} })
}

type group struct {
	p       *GroupProbe
	size    int
	workers []lockstep.Address
}

func (a *group) Started(ctx *lockstep.Context[GroupMsg]) {
	a.p.Started++
	self := ctx.Self()
	for i := 1; i < a.size; i++ {
		name := ""
		if i%2 == 1 {
			name = fmt.Sprintf("w%d", i)
		}
		pid, err := lockstep.Spawn(ctx, name, func() lockstep.Actor[workerMsg] {
			return &worker{p: a.p, id: i, group: self}
		})
		if err != nil {
			panic(err)
		}
		a.workers = append(a.workers, pid.Address())
	}
	for _, w := range a.workers[:2] {
		if err := lockstep.Watch(ctx, w); err != nil {
			panic(err)
		}
	}
}

func (a *group) Receive(ctx *lockstep.Context[GroupMsg], msg GroupMsg) {
	switch msg.(type) {
	case StopFirst:
		if err := lockstep.Stop(ctx, a.workers[0]); err != nil {
			panic(err)
		}
	case Report:
		a.p.Reports++
	}
}

func (a *group) Signal(_ *lockstep.Context[GroupMsg], sig lockstep.Signal) {
	if _, ok := sig.(lockstep.Terminated); ok {
		a.p.Terminated++
	}
}

func (a *group) Stopped(*lockstep.Context[GroupMsg], lockstep.StopReason) { a.p.Stopped++ }

type worker struct {
	p     *GroupProbe
	id    int
	group lockstep.PID[GroupMsg]
}

func (a *worker) Started(ctx *lockstep.Context[workerMsg]) {
	a.p.Started++
	ctx.After(time.Duration(1+ctx.Rand().IntN(20))*time.Millisecond, workerMsg(work{}))
}

func (a *worker) Receive(ctx *lockstep.Context[workerMsg], msg workerMsg) {
	if _, ok := msg.(work); ok {
		_ = lockstep.Send(ctx, a.group, GroupMsg(Report{Worker: a.id}))
	}
}

func (a *worker) Stopped(*lockstep.Context[workerMsg], lockstep.StopReason) { a.p.Stopped++ }
