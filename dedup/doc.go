// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package dedup holds the state that exactly-once protocols keep in an actor: a window of request
// keys and tombstones (Window), the reply to each handled request (Replies), and the highest
// version seen per entity (Versions).
//
// lockstep delivers each message at most once, in send order per sender and receiver, and never
// duplicates one. Duplicates and stale messages come from the application: a request retried after
// its Ask timed out although the first copy was handled, or two messages about one entity that took
// different routes and arrive out of order. The receiving actor drops them with this package's
// state:
//
//	if r, ok := a.replies.Get(ctx.Now(), req.Key); ok {
//		req.ReplyTo.Send(ctx, r) // a retry: answer with the first reply
//		return
//	}
//	r := a.apply(req)
//	a.replies.Put(ctx.Now(), req.Key, r)
//	req.ReplyTo.Send(ctx, r)
//
// # Time
//
// The values read no clock and draw no randomness. Every method that ages entries takes now,
// which an actor passes as ctx.Now(). Given the same calls, a value holds the same entries and
// encodes to the same bytes in production and in simulation.
//
// A value never lets time go backwards: it keeps the latest now given to a method that writes,
// and treats an earlier now as that latest one. ctx.Now can step back, when a clock fault moves a
// simulated node's clock or the operating system steps the wall clock; entries then age more
// slowly until the clock catches up. A step forward ages every entry by the step at once.
//
// # Bounds
//
// Config bounds every value by age (TTL) and count (Capacity). An entry is visible while it is
// younger than TTL; one exactly TTL old is gone. A write that adds an entry to a full value first
// removes the oldest entry. Methods that write remove expired entries; methods that only read
// change nothing. A value keeps its bounds for its whole life, also across encoding; to use new
// bounds, start a new value.
//
// A protocol built on a value is correct only within its bounds. For requests that may be
// retried, TTL must be longer than the retry horizon: the sum of the caller's timeouts and backoff
// waits before its last attempt, plus the longest time a request can take from send to handling.
// Capacity must hold every key written within one TTL. A key removed early, by age or by
// capacity, lets a late duplicate through as new. A forgotten entity in Versions accepts any
// version, so a stale update that arrives after its entity was removed is applied.
//
// # Persistence
//
// A value is plain actor state: a restart that rebuilds the actor loses it. A persistent actor
// (package persist) keeps it in its state through a pointer field; the values implement
// json.Marshaler and json.Unmarshaler, so persist.JSON stores them in snapshots. Write to the
// value in Apply, with a time stored in the event, and only read it in Handle; recovery then
// rebuilds exactly the value the actor had.
//
// A value is not safe for concurrent use.
package dedup
