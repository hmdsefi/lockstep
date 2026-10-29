// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package bench runs the same workloads on lockstep, protoactor-go, hollywood and GoAkt, and
// measures how many simulation seeds lockstep runs per second.
//
// The workloads measure message throughput, send and ask latency, memory per idle actor and
// allocations per message. Package harness defines the interface each library's adapter
// implements, and package libs lists the adapters.
package bench
