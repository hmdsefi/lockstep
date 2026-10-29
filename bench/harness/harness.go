// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package harness defines what the benchmarks need from an actor library: the interfaces each
// library's adapter implements, the message types every workload sends, and Recorder, which
// collects latency samples.
//
// Adapters import this package and their own library only, so no adapter depends on another.
package harness

// Lib is one actor library under test.
type Lib interface {
	// Name returns the lib= value: "lockstep", "protoactor", "hollywood" or "goakt".
	Name() string
	// Start builds and starts a system with the library's defaults, except where the adapter
	// documents otherwise.
	Start() (System, error)
}

// System is one running system of a library.
type System interface {
	// PingPong spawns pairs ping-pong pairs and returns a runner.
	PingPong(pairs int) (Workload, error)
	// FanOut spawns one producer and consumers consumers and returns a runner.
	FanOut(consumers int) (Workload, error)
	// PrepareIdle allocates the storage SpawnIdle needs for n actors, so the
	// measurement does not include it.
	PrepareIdle(n int)
	// SpawnIdle spawns the n prepared actors and returns when each has processed its Hello.
	SpawnIdle() error
	// Echo spawns one echo actor that records into rec.
	Echo(rec *Recorder) (Echo, error)
	// Close stops every actor and the system and returns when they are stopped.
	Close() error
}

// Workload runs a spawned workload.
type Workload interface {
	// Run delivers n messages and returns when all are delivered, with the number of
	// messages the actors counted.
	Run(n int) (delivered int, err error)
}

// Echo is a running echo actor.
type Echo interface {
	// Send sends Stamp{At: rec.Now()} from outside the system and returns after the echo
	// actor has recorded rec.Now()-At.
	Send() error
	// Ask makes one request-reply round trip from outside the system with a 5 s timeout.
	Ask() error
}
