// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package cluster keeps track of which nodes belong to a cluster and whether they are up.
//
// Membership is gossip based: every node probes the others at a fixed interval, asks other members
// to probe a node it cannot reach directly, suspects a node that answers neither, and declares it
// dead when the suspicion is not refuted in time. News about members travels piggybacked on the
// probe traffic, and nodes exchange their whole member tables now and then so that partitions
// heal.
//
// Each member is known by its node name and its node ID, the incarnation of the process that runs
// the node, so a restarted process is a new member under the old name. A member is in one of four
// states: Alive and Suspect members are up; Dead and Left members are down. A member raises its own
// incarnation number to refute a suspicion about itself, and nothing brings back a member that
// left under the same ID.
//
// A node is configured with a Config. DefaultConfig returns every default filled in, and
// Config.Validate fills in the defaults of a partly set Config and checks it; every validation
// error wraps ErrInvalidConfig. The members a node knows are published as a View: an immutable
// snapshot of the member table, sorted by node name.
package cluster
