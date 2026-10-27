// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package client_test

import (
	"time"

	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/examples/raft/client"
	"github.com/hmdsefi/lockstep/examples/raft/kv"
	"github.com/hmdsefi/lockstep/examples/raft/raft"
	"github.com/hmdsefi/lockstep/sim"
)

// op is one recorded operation.
type op struct {
	process   string
	f         string
	input     any
	invokedAt kernel.Time
	completed bool
	outcome   client.Outcome
	output    any
	doneAt    kernel.Time
}

// recorder is a client.Recorder that keeps every operation with its virtual times. Like faultline's
// history.Recorder it refuses an Invoke while the process has an operation pending.
type recorder struct {
	now func() kernel.Time
	ops []*op
}

func (r *recorder) Invoke(process, f string, input any) int64 {
	for _, o := range r.ops {
		if o.process == process && !o.completed {
			panic("recorder: " + process + " invoked while an operation is pending")
		}
	}
	r.ops = append(r.ops, &op{process: process, f: f, input: input, invokedAt: r.now()})
	return int64(len(r.ops))
}

func (r *recorder) Complete(id int64, outcome client.Outcome, output any) {
	o := r.ops[id-1]
	if o.completed {
		panic("recorder: operation completed twice")
	}
	o.completed, o.outcome, o.output, o.doneAt = true, outcome, output, r.now()
}

// attempt is one Propose that reached a stub server.
type attempt struct {
	node string
	at   time.Time
	msg  raft.Propose
}

// attempts collects the Proposes of every stub server, in arrival order.
type attempts struct{ list []attempt }

// nodes returns the node of each attempt, in order.
func (a *attempts) nodes() []string {
	out := make([]string, len(a.list))
	for i, at := range a.list {
		out[i] = at.node
	}
	return out
}

// script is a list of answers handed out in order; the last one repeats. Stubs can share one script.
type script struct {
	answers []raft.Result
	next    int
}

func (s *script) take() raft.Result {
	r := s.answers[min(s.next, len(s.answers)-1)]
	s.next++
	return r
}

// stubSpec describes the stub server of one node.
type stubSpec struct {
	node   string
	script *script   // the answers; nil with silent
	silent bool      // never answer
	store  *kv.Store // when set, answer OK with the command applied to it
}

// stubServer is a raft.Msg actor that answers Propose from a script, or never. It is actor code:
// it records into the test-owned attempts and answers through ReplyTo.
type stubServer struct {
	spec stubSpec
	log  *attempts
	next uint64
}

func (s *stubServer) Receive(ctx *lockstep.Context[raft.Msg], msg raft.Msg) {
	switch m := msg.(type) {
	case raft.Propose:
		s.log.list = append(s.log.list, attempt{node: s.spec.node, at: ctx.Now(), msg: m})
		if s.spec.silent {
			return
		}
		if s.spec.store != nil {
			s.next++
			m.ReplyTo.Send(ctx, raft.Result{Status: raft.OK, Output: s.spec.store.Apply(s.next, m.Cmd), Index: s.next})
			return
		}
		m.ReplyTo.Send(ctx, s.spec.script.take())
	case raft.RequestVote, raft.VoteReply, raft.AppendEntries, raft.AppendReply:
		// The stub is a lone server: no peer sends it election or replication traffic.
	}
}

// addStubs adds one server node per spec, each running a stubServer at /user/raft.
func addStubs(s *sim.Sim, log *attempts, specs ...stubSpec) {
	for _, spec := range specs {
		s.Node(spec.node, func(sys *lockstep.System) {
			_, err := lockstep.Spawn(sys, "raft", func() lockstep.Actor[raft.Msg] {
				return &stubServer{spec: spec, log: log}
			})
			if err != nil {
				panic(err)
			}
		})
	}
}

// addClient adds client node name running a client.Client built from cfg at /user/client.
func addClient(s *sim.Sim, name string, cfg client.Config) {
	factory, err := client.New(cfg)
	if err != nil {
		s.World().T().Fatalf("client.New: %v", err)
	}
	s.Client(name, func(sys *lockstep.System) {
		_, err := lockstep.Spawn(sys, "client", factory)
		if err != nil {
			panic(err)
		}
	})
}
