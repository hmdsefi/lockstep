// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package harness

// PingStart starts a ping-pong pair: it is sent from outside the system to the pair's first
// actor, and the pair stops once it has delivered Quota messages, counting every Ping and Pong.
type PingStart struct{ Quota int }

// Ping is the message the first actor of a ping-pong pair sends to the second.
type Ping struct{ Seq int64 }

// Pong is the second actor's answer to a Ping, with the same Seq.
type Pong struct{ Seq int64 }

// FanStart tells the fan-out producer to send N items, numbered 0 to N-1.
type FanStart struct{ N int }

// Item is one message from the fan-out producer to a consumer.
type Item struct{ Seq int64 }

// Ack is a consumer's acknowledgement to the fan-out producer for the items it has received;
// it restores the credit that lets the producer send that consumer more items.
type Ack struct{}

// Hello is the one message each idle actor receives after it is spawned.
type Hello struct{}

// Stamp carries the time it was sent, from [Recorder.Now]; the echo actor records how long it
// took to arrive.
type Stamp struct{ At int64 }

// EchoReq is the request the echo actor answers with int64(1).
type EchoReq struct{}
