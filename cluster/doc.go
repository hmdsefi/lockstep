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
//
// Join makes a System a member: it starts the system actor /system/cluster, which joins through
// the seeds, and returns a Membership whose View method returns the latest view without
// messaging the actor:
//
//	m, err := cluster.Join(sys, cluster.Config{Seeds: []string{"n1", "n2"}})
//	if err != nil {
//		return err
//	}
//	for _, member := range m.View().Up() {
//		fmt.Println(member.Node)
//	}
//
// Members send each other their whole member tables when a node joins, every SyncInterval to one
// member chosen at random, and to every dead member and every seed that is not up, so that the
// two sides of a healed partition learn of each other.
//
// Every ProbeInterval a node probes one other up member, taking them in a new random order each
// round, so that it probes every one of them at least once in any two rounds. A probe without an
// answer after ProbeTimeout is sent again through up to IndirectProbes other Alive members; a
// probe still without an answer at the next interval makes the member Suspect, and the member is
// Dead SuspicionTimeout later unless it refuted the suspicion meanwhile. At every interval a node
// also tells each member it suspects, other than the one it probes, that it suspects it, so a
// member that was only cut off, for less than SuspicionTimeout - ProbeInterval, refutes within
// about one interval of the heal, before it is declared Dead. In a cluster of N nodes a crashed
// node is Dead in the view of every other node within about
// 2*(N-1)*ProbeInterval + ProbeTimeout + SuspicionTimeout + ProbeInterval.
//
// A local actor that implements lockstep.SignalReceiver can subscribe to the membership with
// Membership.Subscribe: it receives lockstep.NodeUp when another member comes up and
// lockstep.NodeDown when one goes down, in the order of the published views. When a member goes
// down, every death watch that a local actor holds on an actor of that node ends with a failure
// wrapping ErrNodeDown, and a new watch of an actor there ends at once, until the node comes up
// again.
//
// Membership.Leave, or shutting the System down, makes a node leave gracefully: the other members
// see it as Left, not Dead, and their subscribers get NodeDown with Left set. When the membership
// actor fails and restarts, it keeps the member table of the last published view and its
// subscribers.
//
// A node can also host grains: virtual actors addressed by a kind and a key, which exist whenever
// a message is sent to them. Config.Kinds lists the kinds a node hosts, each made by NewKind with
// the factory of its activations. Join starts a host per kind under /system/grains; the owner of a
// key is chosen among the up members that host its kind (Owner), and the owner's host activates
// the key on its first message:
//
//	g := cluster.GrainOf[CounterMsg](m, "counter", "user-42")
//	if err := g.Send(sys, Add{N: 1}); err != nil {
//		return err
//	}
//
// A message for a grain that another node owns is encoded with Config.Registry and Config.Codec
// and sent to that node's host, so its type must be registered under the same wire name on every
// node that sends to or hosts the kind. While the views of the nodes disagree about the owner, a
// host forwards the message to the owner it sees, at most Config.MaxHops times.
//
// Membership.Host describes the host of a kind on this node.
package cluster
