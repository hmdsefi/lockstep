// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package sim runs lockstep actor systems as seeded faultline simulations inside go test.
//
// Simulate builds on faultline.Run in event mode: it runs body once for every run of every seed,
// with a new Sim on that run's faultline World. A failing seed replays exactly with faultline's
// replay command.
//
// Import sim only from _test.go files, so production builds never link faultline:
//
//	func TestCluster(t *testing.T) {
//		sim.Simulate(t, sim.Options{}, func(s *sim.Sim) {
//			s.World().RunFor(time.Second)
//		})
//	}
//
// Lockstep nodes talk as production nodes do: the system of each node boot runs the remoting
// protocol, and its frames travel as simnet datagrams, so every message between nodes is encoded
// with the codec of Options.Remote and its type must be registered. Loss, delay and partitions on
// a link apply to the datagrams; duplicates are not made, and each link keeps its datagrams in
// order.
//
// Run the tests with LOCKSTEP_CHECK_DETERMINISM=1 to run every seed twice and compare the traces.
package sim
