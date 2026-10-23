// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package mailbox

import (
	"reflect"
	"testing"
)

func TestSystemQueueFIFO(t *testing.T) {
	s := NewSystemQueue[int]()
	if _, ok := s.Pop(); ok {
		t.Fatal("Pop on an empty queue returned a value")
	}
	for i := 1; i <= 3; i++ {
		if !s.Push(i) {
			t.Fatalf("Push(%d) = false", i)
		}
	}
	if s.Len() != 3 {
		t.Fatalf("Len = %d, want 3", s.Len())
	}
	for want := 1; want <= 3; want++ {
		v, ok := s.Pop()
		if !ok || v != want {
			t.Fatalf("Pop = %d, %v, want %d, true", v, ok, want)
		}
	}
	if s.Len() != 0 {
		t.Fatalf("Len = %d, want 0", s.Len())
	}
}

// TestSystemQueueUnbounded pushes far more entries than any mailbox capacity, interleaving pops so
// the buffer shifts, and expects every value back in order.
func TestSystemQueueUnbounded(t *testing.T) {
	s := NewSystemQueue[int]()
	const total = 5000
	next := 0
	for i := 0; i < total; i++ {
		if !s.Push(i) {
			t.Fatalf("Push(%d) = false", i)
		}
		if i%3 == 2 {
			v, ok := s.Pop()
			if !ok || v != next {
				t.Fatalf("Pop = %d, %v, want %d, true", v, ok, next)
			}
			next++
		}
	}
	if s.Len() != total-next {
		t.Fatalf("Len = %d, want %d", s.Len(), total-next)
	}
	rest := s.Close()
	if len(rest) != total-next {
		t.Fatalf("Close returned %d values, want %d", len(rest), total-next)
	}
	for i, v := range rest {
		if v != next+i {
			t.Fatalf("Close[%d] = %d, want %d", i, v, next+i)
		}
	}
}

func TestSystemQueueCloseEmpty(t *testing.T) {
	s := NewSystemQueue[int]()
	if got := s.Close(); len(got) != 0 {
		t.Fatalf("Close of an empty queue = %v, want no values", got)
	}
	if s.Push(1) {
		t.Fatal("Push after Close returned true")
	}
	if _, ok := s.Pop(); ok {
		t.Fatal("Pop after Close returned a value")
	}
	if got := s.Close(); !reflect.DeepEqual(got, []int(nil)) {
		t.Fatalf("second Close = %v, want nil", got)
	}
}
