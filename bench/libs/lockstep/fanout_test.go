// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// creditProbe counts a fan-out's items and acks per consumer and, after every send, records the
// most items any consumer had outstanding (sent but not yet received). It holds consumer 0's
// first item until the producer stops for lack of consumer 0's credit, so the run reaches the
// credit limit however fast the consumers are, and a producer that never stops sends consumer 0
// more than its credit while the item is held.
type creditProbe struct {
	sentN, receivedN, ackedN []atomic.Int64
	maxOutstanding           atomic.Int64
	release                  chan struct{} // closed when the producer first stops for consumer 0
	releaseOnce              sync.Once
}

func newCreditProbe(consumers int) *creditProbe {
	return &creditProbe{
		sentN:     make([]atomic.Int64, consumers),
		receivedN: make([]atomic.Int64, consumers),
		ackedN:    make([]atomic.Int64, consumers),
		release:   make(chan struct{}),
	}
}

func (p *creditProbe) sent(consumer int) {
	p.sentN[consumer].Add(1)
	for c := range p.sentN {
		out := p.sentN[c].Load() - p.receivedN[c].Load()
		for {
			cur := p.maxOutstanding.Load()
			if out <= cur || p.maxOutstanding.CompareAndSwap(cur, out) {
				break
			}
		}
	}
}

func (p *creditProbe) stopped(consumer int) {
	if consumer == 0 {
		p.releaseOnce.Do(func() { close(p.release) })
	}
}

func (p *creditProbe) received(consumer int) {
	if consumer == 0 && p.receivedN[0].Load() == 0 {
		// The timeout, shorter than lockstep's 5 s watchdog, ends the hold when the producer
		// never stops; the checks then report it.
		select {
		case <-p.release:
		case <-time.After(2 * time.Second):
		}
	}
	p.receivedN[consumer].Add(1)
}

func (p *creditProbe) acked(consumer int) { p.ackedN[consumer].Add(1) }

// TestATBEN04FanOutCredits covers AT-BEN-04: with 3 consumers and n = 1,537, no consumer ever has
// more than 512 items outstanding, each consumer sends one ack per 256 items it receives, and all
// items arrive. Consumer 0 gets 513 items. The probe holds its first item, so the producer spends
// all 512 credits of consumer 0 and must stop at the 513th until consumer 0 acks; consumer 0's
// last ack window then holds one item.
func TestATBEN04FanOutCredits(t *testing.T) {
	const consumers, n = 3, 1537
	s := start(t)
	probe := newCreditProbe(consumers)
	s.fanProbe = probe
	w, err := s.FanOut(consumers)
	if err != nil {
		t.Fatal(err)
	}
	delivered, err := w.Run(n)
	if err != nil {
		t.Fatal(err)
	}
	if delivered != n {
		t.Errorf("delivered %d of %d", delivered, n)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if got := probe.maxOutstanding.Load(); got != credits {
		t.Errorf("at most %d items were outstanding at one consumer, want %d: more breaks the credit limit, fewer means the run never reached it", got, credits)
	}
	total := int64(0)
	for c := range consumers {
		sent, received, acked := probe.sentN[c].Load(), probe.receivedN[c].Load(), probe.ackedN[c].Load()
		want := int64(n / consumers)
		if c < n%consumers {
			want++
		}
		if sent != want || received != want {
			t.Errorf("consumer %d: sent %d, received %d, want %d", c, sent, received, want)
		}
		if acked != received/ackEvery {
			t.Errorf("consumer %d: %d acks for %d items, want %d", c, acked, received, received/ackEvery)
		}
		total += received
	}
	if total != n {
		t.Errorf("consumers received %d items, want %d", total, n)
	}
}
