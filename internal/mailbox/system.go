// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package mailbox

import (
	"sync"
	"sync/atomic"
)

// SystemQueue is an unbounded FIFO, many producers, one consumer. Its size is bounded by its
// producers, not by a capacity (MBX-080).
type SystemQueue[T any] struct {
	mu     sync.Mutex
	buf    []T // buf[head:] holds the values in order
	head   int
	closed bool
	n      atomic.Int64 // len(buf) - head, stored under mu after every change, for Empty
}

// NewSystemQueue returns an open, empty system queue.
func NewSystemQueue[T any]() *SystemQueue[T] { return &SystemQueue[T]{} }

// Push appends v. It returns false, and drops v, if the queue is closed.
func (s *SystemQueue[T]) Push(v T) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	// Reuse the space Pop freed instead of growing the slice for ever.
	if s.head > 0 && len(s.buf) == cap(s.buf) && s.head >= len(s.buf)/2 {
		n := copy(s.buf, s.buf[s.head:])
		clear(s.buf[n:])
		s.buf, s.head = s.buf[:n], 0
	}
	s.buf = append(s.buf, v)
	s.n.Store(int64(len(s.buf) - s.head))
	return true
}

// Pop removes and returns the oldest value. ok is false if the queue is empty.
func (s *SystemQueue[T]) Pop() (v T, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.head == len(s.buf) {
		return v, false
	}
	v = s.buf[s.head]
	var zero T
	s.buf[s.head] = zero
	s.head++
	if s.head == len(s.buf) {
		s.buf, s.head = s.buf[:0], 0
	}
	s.n.Store(int64(len(s.buf) - s.head))
	return v, true
}

// Empty reports whether the queue holds no value, without taking the queue's lock: the answer
// Len() == 0 would give at some moment during the call. A consumer that holds a lock every
// producer takes before Push gets the exact answer.
func (s *SystemQueue[T]) Empty() bool { return s.n.Load() == 0 }

// Len returns the number of values.
func (s *SystemQueue[T]) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.buf) - s.head
}

// Close closes the queue and returns the remaining values in order. Later pushes return false.
// Close on a closed queue returns nil.
func (s *SystemQueue[T]) Close() []T {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	rest := make([]T, len(s.buf)-s.head)
	copy(rest, s.buf[s.head:])
	s.buf, s.head = nil, 0
	s.n.Store(0)
	return rest
}
