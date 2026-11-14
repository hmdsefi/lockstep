// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package remote_test

import (
	"encoding/json"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/internal/hooks"
	"github.com/hmdsefi/lockstep/internal/remoting"
	"github.com/hmdsefi/lockstep/remote"
)

// metricsBackend is a lockstep.Metrics that keeps every series it is given, in registration order.
type metricsBackend struct {
	mu       sync.Mutex
	names    []string
	labels   map[string][]lockstep.MetricLabel
	counters map[string]*atomic.Uint64
	gauges   map[string]func() int64
}

func newMetricsBackend() *metricsBackend {
	return &metricsBackend{
		labels:   make(map[string][]lockstep.MetricLabel),
		counters: make(map[string]*atomic.Uint64),
		gauges:   make(map[string]func() int64),
	}
}

type adder struct{ n *atomic.Uint64 }

func (a adder) Add(n uint64) { a.n.Add(n) }

func (m *metricsBackend) Counter(name, _ string, labels ...lockstep.MetricLabel) lockstep.Counter {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.names = append(m.names, name)
	m.labels[name] = labels
	c := new(atomic.Uint64)
	m.counters[name] = c
	return adder{c}
}

func (m *metricsBackend) GaugeFunc(name, _ string, fn func() int64, labels ...lockstep.MetricLabel) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.names = append(m.names, name)
	m.labels[name] = labels
	m.gauges[name] = fn
}

// remoteSeries returns the names of the remote series registered so far, in order.
func (m *metricsBackend) remoteSeries() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for _, n := range m.names {
		if strings.HasPrefix(n, "lockstep_remote_") {
			out = append(out, n)
		}
	}
	return out
}

// value returns the counter's count, or the gauge's value.
func (m *metricsBackend) value(name string) int64 {
	m.mu.Lock()
	c, g := m.counters[name], m.gauges[name]
	m.mu.Unlock()
	if c != nil {
		return int64(c.Load()) //nolint:gosec // G115: small test counts
	}
	return g()
}

// reached waits until series name reads want, or fails the test.
func (m *metricsBackend) reached(t *testing.T, name string, want int64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for m.value(name) != want {
		if time.Now().After(deadline) {
			t.Fatalf("%s = %d, want %d", name, m.value(name), want)
		}
		time.Sleep(time.Millisecond)
	}
}

// frameBytes is the size of f on the wire: L + 4.
func frameBytes(f *remoting.Frame) int64 { return int64(len(remoting.AppendFrame(nil, f))) }

// TestATREM42Metrics checks the seven series of REM-395 against the frames a raw peer sees and
// writes, and that a system without metrics counts nothing.
func TestATREM42Metrics(t *testing.T) {
	t.Run("counts", func(t *testing.T) {
		m := newMetricsBackend()
		reg := newRegistry()
		rLn := rawListener(t)
		log := newCapture()
		sys := lockstep.New(lockstep.WithNodeName("a"), lockstep.WithLogger(slog.New(log)), lockstep.WithMetrics(m))
		t.Cleanup(func() { shutdown(t, sys) })
		a := &node{sys: sys, log: log}
		before := len(m.remoteSeries())
		// No pings: every frame the test counts is one it knows of.
		a.enable(t, remote.Config{
			Listen: "127.0.0.1:0", Registry: reg, Heartbeat: 30 * time.Minute, IdleTimeout: time.Hour,
			Peers: map[string]string{"r": rLn.Addr().String()},
		})

		names := []string{
			"lockstep_remote_frames_sent_total", "lockstep_remote_frames_received_total",
			"lockstep_remote_bytes_sent_total", "lockstep_remote_bytes_received_total",
			"lockstep_remote_reconnects_total", "lockstep_remote_gaps_total", "lockstep_remote_sessions",
		}
		if got := m.remoteSeries(); before != 0 || strings.Join(got, ",") != strings.Join(names, ",") {
			t.Fatalf("Enable registered %v (%d before it), want %v in this order", got, before, names)
		}
		for _, n := range names {
			if got := m.labels[n]; len(got) != 1 || got[0] != (lockstep.MetricLabel{Name: "node", Value: "a"}) {
				t.Errorf("%s has labels %v, want only node=a", n, got)
			}
			if v := m.value(n); v != 0 {
				t.Errorf("%s = %d before any traffic, want 0", n, v)
			}
		}

		var sentFrames, sentBytes, recvFrames, recvBytes, reconnects, gaps, sessions int64
		check := func() {
			t.Helper()
			for name, want := range map[string]int64{
				names[0]: sentFrames, names[1]: recvFrames, names[2]: sentBytes, names[3]: recvBytes,
				names[4]: reconnects, names[5]: gaps, names[6]: sessions,
			} {
				m.reached(t, name, want)
			}
		}

		notes := make(chan Note, 4)
		spawn[Note](t, a.sys, "notes", forward[Note]{notes})

		// r dials a: HELLO in, WELCOME out; then MSG 1 and MSG 3, which leave a gap.
		in, err := net.Dial("tcp", a.addr)
		if err != nil {
			t.Fatal(err)
		}
		defer in.Close()
		hello := helloFrame(remoting.KindHello, "r", "a", "")
		writeFrame(t, in, hello)
		welcome := mustRead(t, in, remoting.KindWelcome)
		recvFrames, recvBytes = 1, frameBytes(hello)
		sentFrames, sentBytes = 1, frameBytes(&welcome)
		sessions = 1
		check()

		to := lockstep.PIDAt[Note]("a", "/user/notes").Address().String()
		from := lockstep.PIDAt[Note]("r", "/user/s").Address().String()
		for _, seq := range []int{1, 3} {
			body, err := json.Marshal(Note{N: seq})
			if err != nil {
				t.Fatal(err)
			}
			msg := &remoting.Frame{Kind: remoting.KindMsg, Seq: uint64(seq), Sender: from, Recipient: to, Type: "remotetest.Note", Payload: body} //nolint:gosec // G115: small test sequence numbers
			writeFrame(t, in, msg)
			recvFrames++
			recvBytes += frameBytes(msg)
			if got := recv(t, notes); got.N != seq {
				t.Fatalf("received %+v, want N=%d", got, seq)
			}
		}
		gaps = 1
		check()

		// a dials r for a message: HELLO and MSG out, WELCOME in.
		send := func() {
			t.Helper()
			if err := lockstep.Send(a.sys, lockstep.PIDAt[Note]("r", "/user/x"), Note{N: 1}); err != nil {
				t.Fatal(err)
			}
		}
		session := func() {
			t.Helper()
			out := acceptWithin(t, rLn, 10*time.Second)
			if out == nil {
				t.Fatal("a did not dial r")
			}
			h := mustRead(t, out, remoting.KindHello)
			w := helloFrame(remoting.KindWelcome, "r", "a", "")
			writeFrame(t, out, w)
			msg := mustRead(t, out, remoting.KindMsg)
			sentFrames += 2
			sentBytes += frameBytes(&h) + frameBytes(&msg)
			recvFrames++
			recvBytes += frameBytes(w)
			sessions++
			check()
			// Closing the session ends it on a's side too.
			_ = out.Close()
			sessions--
			check()
		}
		send()
		session()
		if m.value(names[4]) != 0 {
			t.Fatalf("the first session to r counted a reconnect")
		}

		// The second session to r is a reconnect.
		send()
		reconnects = 1
		session()

		_ = in.Close()
		sessions--
		check()
	})

	t.Run("two systems", func(t *testing.T) {
		reg := newRegistry()
		start := func(name string, peers map[string]string) (*node, *metricsBackend) {
			m := newMetricsBackend()
			log := newCapture()
			sys := lockstep.New(lockstep.WithNodeName(name), lockstep.WithLogger(slog.New(log)), lockstep.WithMetrics(m))
			t.Cleanup(func() { shutdown(t, sys) })
			n := &node{sys: sys, log: log}
			n.enable(t, remote.Config{
				Listen: "127.0.0.1:0", Registry: reg, Peers: peers,
				Heartbeat: 30 * time.Minute, IdleTimeout: time.Hour,
			})
			return n, m
		}
		b, mb := start("b", nil)
		a, ma := start("a", map[string]string{"b": b.addr})
		notes := make(chan Note, 8)
		spawn[Note](t, b.sys, "notes", forward[Note]{notes})
		const n = 5
		for i := range n {
			if err := lockstep.Send(a.sys, lockstep.PIDAt[Note]("b", "/user/notes"), Note{N: i}); err != nil {
				t.Fatal(err)
			}
			if got := recv(t, notes); got.N != i {
				t.Fatalf("received %+v, want N=%d", got, i)
			}
		}
		const (
			sent, recvd = "lockstep_remote_frames_sent_total", "lockstep_remote_frames_received_total"
			bsent, brcv = "lockstep_remote_bytes_sent_total", "lockstep_remote_bytes_received_total"
		)
		// a wrote HELLO and the messages; b wrote WELCOME; each side received what the other wrote.
		ma.reached(t, sent, n+1)
		mb.reached(t, recvd, n+1)
		mb.reached(t, sent, 1)
		ma.reached(t, recvd, 1)
		ma.reached(t, bsent, mb.value(brcv))
		ma.reached(t, brcv, mb.value(bsent))
		if ma.value(bsent) <= 4*(n+1) {
			t.Errorf("a counted %d bytes for %d frames, want more than 4 bytes each", ma.value(bsent), n+1)
		}
		for _, m := range []*metricsBackend{ma, mb} {
			m.reached(t, "lockstep_remote_sessions", 1)
			m.reached(t, "lockstep_remote_reconnects_total", 0)
			m.reached(t, "lockstep_remote_gaps_total", 0)
		}
	})

	t.Run("off", func(t *testing.T) {
		reg := newRegistry()
		sys := lockstep.New(lockstep.WithNodeName("z"))
		t.Cleanup(func() { shutdown(t, sys) })
		if add := hooks.MetricCounter(sys, remoting.MetricFramesSent, remoting.HelpFramesSent); add != nil {
			t.Error("MetricCounter returned a function on a system with metrics off")
		}
		b := startNode(t, "b", reg, nil)
		a := startNode(t, "a", reg, func(c *remote.Config) { c.Peers = map[string]string{"b": b.addr} })
		notes := make(chan Note, 1)
		spawn[Note](t, b.sys, "notes", forward[Note]{notes})
		if err := lockstep.Send(a.sys, lockstep.PIDAt[Note]("b", "/user/notes"), Note{N: 5}); err != nil {
			t.Fatal(err)
		}
		if got := recv(t, notes); got.N != 5 {
			t.Errorf("received %+v, want N=5", got)
		}
		for _, n := range []*node{a, b} {
			if hooks.MetricCounter(n.sys, remoting.MetricGaps, remoting.HelpGaps) != nil {
				t.Errorf("%s: MetricCounter returned a function after Enable with metrics off", n.sys.Node())
			}
		}
	})
}
