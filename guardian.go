// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"fmt"
	"strings"
	"sync"

	"github.com/hmdsefi/lockstep/internal/engine"
)

// guardians is the engine.ActorRef that runs the two guardians' part of supervision and shutdown.
// The guardians have no cell (ACT-030), so their children's notices come here:
//
//   - failed, ended and stopped: each guardian supervises its children as a supervisor in Running
//     does (SUP-300), through the same supervisor code as a cell: the user guardian with the
//     WithGuardianStrategy strategy, the system guardian with OneForOne(nil);
//   - cleanupFailed: never decided nor counted (SUP-110), and guardians implement no Signal, so it
//     changes nothing. A guardian cannot fail either: it applies an escalation, from its decider,
//     a decider failure or restart intensity, as a stop (SUP-300);
//   - the shutdown's start: the sweep stops the user guardian's children, then, after every one of
//     them has completed its stop, the system guardian's, each guardian's one subtree at a time in
//     reverse spawn order, with reason Shutdown (ACT-050, SUP-350). Once the sweep has started,
//     the guardians are stopping: failed and ended notices are discarded, as a stopping supervisor
//     discards them (SUP-170), and the sweep stops those children, suspended or restarting.
//
// Its queue takes messages from any goroutine; the engine schedules it like a cell (ENG-350), so
// a notice is handled after the turns already scheduled. Its turns run no actor code but the
// deciders, record no lockstep.turn and never block.
type guardians struct {
	sys  *System
	slot engine.Slot

	mu sync.Mutex
	q  fifo[sysMsg] // guarded by mu

	// turn-owned
	user, system supervisor
	phase        shutdownPhase
	wait         Address // the guardian's child whose final stop the sweep waits for, or zero
}

// sweepStart starts the shutdown's sweep (startShutdown).
type sweepStart struct{}

func (sweepStart) isSysMsg() {}

// shutdownPhase is the guardian whose children the sweep stops (ACT-050).
type shutdownPhase uint8

const (
	sweepIdle   shutdownPhase = iota // not started
	sweepUser                        // stopping the user guardian's children
	sweepSystem                      // stopping the system guardian's children
	sweepDone                        // every guardian child has stopped
)

// init sets up g for sys, the user guardian supervising with userStrategy (SUP-300).
func (g *guardians) init(sys *System, userStrategy Strategy) {
	g.sys = sys
	g.user = supervisor{strategy: userStrategy, guardian: true}
	g.system = supervisor{strategy: OneForOne(nil), guardian: true}
}

// Address returns the user guardian's address (engine.ActorRef).
func (g *guardians) Address() engine.Address { return g.sys.user.engine() }

// Slot returns the guardians' scheduling state (engine.ActorRef).
func (g *guardians) Slot() *engine.Slot { return &g.slot }

// push queues m and schedules g (ENG-350). from is the cell whose turn pushes, or nil.
func (g *guardians) push(m sysMsg, from engine.ActorRef) {
	g.mu.Lock()
	g.q.push(m)
	g.mu.Unlock()
	g.sys.ready(g, from)
}

func (g *guardians) pop() (sysMsg, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.q.pop()
}

// supervisorOf returns the supervision state and the address of the guardian whose child is
// child: the system guardian for a path under /system, otherwise the user guardian.
func (g *guardians) supervisorOf(child Address) (*supervisor, Address) {
	if strings.HasPrefix(child.path, g.sys.system.path+"/") {
		return &g.system, g.sys.system
	}
	return &g.user, g.sys.user
}

// RunTurn handles every queued message (engine.ActorRef). After the last guardian child has
// stopped in a shutdown it completes the shutdown.
func (g *guardians) RunTurn(int) bool {
	for {
		m, ok := g.pop()
		if !ok {
			return false
		}
		switch m := m.(type) {
		case sweepStart:
			if g.phase != sweepIdle {
				continue
			}
			g.phase = sweepUser
			g.user.abandonGroup()
			g.system.abandonGroup()
		case stopped:
			sv, self := g.supervisorOf(m.child)
			sv.childStopped(g.sys, m.child, g)
			g.replay(sv, self)
			if g.phase != sweepUser && g.phase != sweepSystem || m.child != g.wait {
				continue // a child the sweep does not wait for: it has left the registry
			}
			g.wait = Address{}
		case failed:
			if g.phase == sweepIdle {
				sv, self := g.supervisorOf(m.child)
				sv.childFailed(g.sys, self, m, g)
			}
			continue
		case ended:
			if g.phase == sweepIdle {
				sv, self := g.supervisorOf(m.child)
				sv.childEnded(g.sys, m, g)
				g.replay(sv, self)
			}
			continue
		case cleanupFailed:
			continue
		default:
			// Only children's notices and the sweep's start reach the guardians; anything else is
			// a bug in the runtime.
			panic(fmt.Sprintf("lockstep: guardians got an unexpected system message %T", m))
		}
		if g.next() {
			g.phase = sweepDone
			g.sys.completeShutdown()
		}
	}
}

// replay decides the failed notices that sv held during a group operation, now that it has pushed
// its last command (SUP-260), unless the shutdown has started.
func (g *guardians) replay(sv *supervisor, self Address) {
	for g.phase == sweepIdle {
		n, ok := sv.nextHeld()
		if !ok {
			return
		}
		sv.childFailed(g.sys, self, n, g)
	}
}

// next asks the most recently spawned child of the current phase's guardian to stop with reason
// Shutdown and waits for its notice; with no child left it moves from the user guardian to the
// system guardian, and after that reports true. A child whose stop has started already, for any
// reason, keeps its reason and still notifies (ACT-710).
func (g *guardians) next() bool {
	s := g.sys
	for {
		parent := s.user
		if g.phase == sweepSystem {
			parent = s.system
		}
		kids := s.reg.Children(parent.path)
		if len(kids) == 0 {
			if g.phase == sweepUser {
				g.phase = sweepSystem
				continue
			}
			return true
		}
		k := kids[len(kids)-1]
		child, ok := s.lookupCell(k)
		if !ok {
			continue // it completed its stop after Children returned
		}
		g.wait = Address{node: k.Node, path: k.Path, uid: k.UID}
		child.requestStop(Shutdown, g)
		return false
	}
}
