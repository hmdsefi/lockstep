// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package cluster

import (
	"slices"
	"strconv"
	"strings"
)

// MemberState is the state of a member in the member table.
type MemberState uint8

// The member states. Alive and Suspect members are up; Dead and Left members are down.
const (
	Alive   MemberState = iota + 1 // "alive": the member answers probes
	Suspect                        // "suspect": the member did not answer and may be dead
	Dead                           // "dead": the member was suspected for too long
	Left                           // "left": the member left the cluster gracefully
)

var stateNames = [...]string{
	Alive:   "alive",
	Suspect: "suspect",
	Dead:    "dead",
	Left:    "left",
}

// String returns the state's name, or "MemberState(<n>)" for a state that is not defined.
func (s MemberState) String() string {
	if s >= Alive && s <= Left {
		return stateNames[s]
	}
	return "MemberState(" + strconv.Itoa(int(s)) + ")"
}

// Up reports whether a member in state s is up: Alive or Suspect.
func (s MemberState) Up() bool { return s == Alive || s == Suspect }

// Member is one entry of the member table.
type Member struct {
	// Node is the member's node name.
	Node string
	// ID is the incarnation of the member's process (its System.Incarnation()), so a restarted
	// process has a new ID.
	ID uint32
	// Incarnation is raised only by the member itself, to refute a suspicion about it.
	Incarnation uint64
	// State is the member's state.
	State MemberState
}

// View is an immutable snapshot of the member table. Callers must not modify it.
type View struct {
	// Version is 1 for the first view of a node and grows by one with every published change.
	Version uint64
	// Self is this node's own entry.
	Self Member
	// Members holds every entry of the table, Self included, sorted by Node.
	Members []Member
}

// Member returns the entry of node, and whether the view has one.
func (v *View) Member(node string) (Member, bool) {
	i, ok := slices.BinarySearchFunc(v.Members, node, compareNode)
	if !ok {
		return Member{}, false
	}
	return v.Members[i], true
}

// Up returns the members that are up, Self included, sorted by Node, in a new slice.
func (v *View) Up() []Member {
	up := make([]Member, 0, len(v.Members))
	for _, m := range v.Members {
		if m.State.Up() {
			up = append(up, m)
		}
	}
	return up
}

// compareNode orders a member by its node name against name, for binary searches.
func compareNode(m Member, name string) int { return strings.Compare(m.Node, name) }
