// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim

import (
	"encoding/binary"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/faultline/kernel/simnet"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/internal/remoting"
	"github.com/hmdsefi/lockstep/remote"
)

// rawPeer is a kernel node that is not a lockstep node: its simnet handler keeps every datagram
// it receives, split into connection ID and parsed frame, and can answer a HELLO with a WELCOME.
type rawPeer struct {
	n       *kernel.Node
	welcome bool // answer every HELLO with a WELCOME
	got     []rawDatagram
}

// rawDatagram is one datagram a rawPeer received.
type rawDatagram struct {
	from  kernel.NodeID
	id    uint64
	frame remoting.Frame
	err   error // ParseFrame's error
	size  int   // the frame's bytes, length prefix included
}

// addRawPeer adds the kernel node name, which runs no lockstep system.
func addRawPeer(s *Sim, name string, welcome bool) *rawPeer {
	w := s.World()
	p := &rawPeer{welcome: welcome}
	p.n = w.AddServer(name, func(n *kernel.Node) {
		w.Net.Handle(n, func(from kernel.NodeID, payload any) {
			dg, ok := payload.(datagram)
			if !ok || len(dg) < idLen {
				w.T().Errorf("%s received %s, want a datagram", name, kernel.Describe(payload))
				return
			}
			id := binary.BigEndian.Uint64(dg)
			f, err := remoting.ParseFrame(dg[idLen:], remoting.MaxFrameLimit)
			p.got = append(p.got, rawDatagram{from: from, id: id, frame: f, err: err, size: len(dg) - idLen})
			if p.welcome && err == nil && f.Kind == remoting.KindHello {
				h, _ := remoting.ParseHello(f.Payload)
				p.send(s, from, id|dirBit, &remoting.Frame{Kind: remoting.KindWelcome, Payload: remoting.AppendHello(nil, &remoting.Hello{
					Sender: name, Receiver: h.Sender, Incarnation: 1, FrameLimit: remoting.MinFrameLimit, Codec: "json",
				})})
			}
		})
	})
	return p
}

// send sends f to node to as one datagram with connection ID id.
func (p *rawPeer) send(s *Sim, to kernel.NodeID, id uint64, f *remoting.Frame) {
	dg := binary.BigEndian.AppendUint64(nil, id)
	s.World().Net.SendRaw(p.n, to, datagram(remoting.AppendFrame(dg, f)), simnet.RawOptions{FIFO: true})
}

// kinds returns the frame kind and connection ID of every datagram p received, as "KIND 0x<id>".
func (p *rawPeer) kinds() []string {
	var out []string
	for _, d := range p.got {
		out = append(out, d.frame.Kind.String()+" 0x"+strconv.FormatUint(d.id, 16))
	}
	return out
}

// recAttr returns the value of r's first attribute key, or "".
func recAttr(r kernel.Record, key string) string {
	for _, a := range r.Attrs {
		if a.Key == key {
			return a.Value
		}
	}
	return ""
}

// recordsOf returns the records of kind on node n, each as its attributes key=value joined by
// spaces.
func recordsOf(w *faultline.World, kind string, n kernel.NodeID) []string {
	var out []string
	for _, r := range w.Sim.Records() {
		if r.Kind != kind || r.Node != n {
			continue
		}
		var b []byte
		for i, a := range r.Attrs {
			if i > 0 {
				b = append(b, ' ')
			}
			b = append(b, a.Key+"="+a.Value...)
		}
		out = append(out, string(b))
	}
	return out
}

// pingSender spawns, on sys, an actor /user/s that sends a pairPing to to for every pairPing it
// receives.
func pingSender(to lockstep.PID[pairPing]) func(sys *lockstep.System) {
	return func(sys *lockstep.System) {
		if _, err := lockstep.Spawn(sys, "s", func() lockstep.Actor[pairPing] { return pairSender{to: to} }); err != nil {
			panic(err)
		}
	}
}

// AT-REM-36 (defined here: the spec gives it one line), REM-100, REM-135, REM-310: the driver
// finds peers with w.Sim.Lookup, so a kernel node that is not a lockstep node still receives
// datagrams: n1 dials raw, which answers the HELLO with a WELCOME of its own, and then gets n1's
// MSG frame, each as one datagram whose frame parses.
func TestATREM36NonLockstepNodeReceives(t *testing.T) {
	Simulate(t, Options{Options: faultline.Options{Seeds: 3, Trace: kernel.TraceConfig{Level: kernel.TraceFull}}}, func(s *Sim) {
		w, tt := s.World(), s.World().T()
		raw := addRawPeer(s, "raw", true)
		to := lockstep.PIDAt[pairPing]("raw", "/user/x")
		n1 := s.Node("n1", pingSender(to))
		w.RunFor(time.Millisecond)
		if err := lockstep.Send(s.System("n1"), lockstep.PIDAt[pairPing]("n1", "/user/s"), pairPing{}); err != nil {
			tt.Fatal(err)
		}
		w.RunFor(time.Second)

		if want := []string{"HELLO 0x100000001", "MSG 0x100000001"}; !slices.Equal(raw.kinds(), want) {
			tt.Fatalf("raw received %v, want %v", raw.kinds(), want)
		}
		for _, d := range raw.got {
			if d.from != n1.ID() || d.err != nil {
				tt.Errorf("raw received a datagram from node %d with frame error %v, want one from n1 that parses", d.from, d.err)
			}
		}
		if m := raw.got[1].frame; m.Seq != 1 || m.Recipient != to.Address().String() || m.Type != "simtest.PairPing" {
			tt.Errorf("MSG seq %d to %q type %q, want seq 1 to %s type simtest.PairPing", m.Seq, m.Recipient, m.Type, to)
		}
		ups := recordsOf(w, "lockstep.remote.up", n1.ID())
		if want := []string{"peer=raw conn=4294967297 dir=out peer-inc=1 max-frame=4096"}; !slices.Equal(ups, want) {
			tt.Errorf("n1's up records %q, want %q", ups, want)
		}
	})
}

// AT-REM-37 (defined here), REM-310, REM-315: a datagram is the 8-byte big-endian connection ID
// followed by exactly one frame whose length field equals the rest. IDs are
// uint64(incarnation)<<32 | k, k counting each incarnation's dials from 1, with bit 31 set on
// the datagrams the acceptor writes; A's and B's dials to each other may share an ID.
func TestATREM37DatagramLayout(t *testing.T) {
	opts := Options{
		Options: faultline.Options{Seeds: 3, Trace: kernel.TraceConfig{Level: kernel.TraceFull}},
		// raw never answers: the dial times out after 100 ms and n1 dials again 10 ms later.
		Remote: remote.Config{DialTimeout: 100 * time.Millisecond, RedialMin: 10 * time.Millisecond, RedialMax: 10 * time.Millisecond},
	}
	Simulate(t, opts, func(s *Sim) {
		w, tt := s.World(), s.World().T()
		raw := addRawPeer(s, "raw", false)
		n1 := s.Node("n1", pingSender(lockstep.PIDAt[pairPing]("raw", "/user/x")))
		w.RunFor(time.Millisecond)
		send := func() {
			tt.Helper()
			if err := lockstep.Send(s.System("n1"), lockstep.PIDAt[pairPing]("n1", "/user/s"), pairPing{}); err != nil {
				tt.Fatal(err)
			}
		}
		send()
		w.RunFor(150 * time.Millisecond) // the first dial, its timeout and the second dial
		n1.Crash()
		n1.Restart()
		send()
		w.RunFor(50 * time.Millisecond)

		want := []string{"HELLO 0x100000001", "HELLO 0x100000002", "HELLO 0x200000001"}
		if !slices.Equal(raw.kinds(), want) {
			tt.Fatalf("raw received %v, want %v", raw.kinds(), want)
		}
		for i, d := range raw.got {
			h, err := remoting.ParseHello(d.frame.Payload)
			if d.err != nil || err != nil || h.Sender != "n1" || h.Receiver != "raw" || h.Incarnation != uint32(d.id>>32) {
				tt.Errorf("datagram %d: frame error %v, hello %+v (%v); want n1's HELLO to raw of incarnation %d", i, d.err, h, err, d.id>>32)
			}
		}

		// Two lockstep nodes that dial each other: both connections have ID 1<<32|1, told apart
		// by direction; each WELCOME carries bit 31.
		s.Node("n2", noopSetup)
		s.Node("n3", noopSetup)
		w.RunFor(time.Millisecond)
		n2, n3 := s.lookup("n2"), s.lookup("n3")
		for _, c := range [][2]string{{"n2", "n3"}, {"n3", "n2"}} {
			if err := s.lookup(c[0]).eng.core.Connect(c[1]); err != nil {
				tt.Fatal(err)
			}
		}
		w.RunFor(50 * time.Millisecond)
		for _, n := range []*node{n2, n3} {
			peer := map[*node]string{n2: "n3", n3: "n2"}[n]
			got := recordsOf(w, "lockstep.remote.up", n.n.ID())
			slices.Sort(got)
			want := []string{
				"peer=" + peer + " conn=4294967297 dir=in peer-inc=1 max-frame=1048576",
				"peer=" + peer + " conn=4294967297 dir=out peer-inc=1 max-frame=1048576",
			}
			if !slices.Equal(got, want) {
				tt.Errorf("%s's up records %q, want %q", n.n.Name(), got, want)
			}
		}
		var payloads []string
		for _, r := range w.Sim.Records() {
			if r.Kind == "net.send_raw" && (r.Node == n2.n.ID() || r.Node == n3.n.ID()) {
				payloads = append(payloads, recAttr(r, "payload"))
			}
		}
		wantPayloads := []string{
			"datagram id=0x100000001 HELLO seq=0 bytes=40",
			"datagram id=0x100000001 HELLO seq=0 bytes=40",
			"datagram id=0x180000001 WELCOME seq=0 bytes=40",
			"datagram id=0x180000001 WELCOME seq=0 bytes=40",
		}
		slices.Sort(payloads)
		if !slices.Equal(payloads, wantPayloads) {
			tt.Errorf("datagrams between n2 and n3: %q, want %q", payloads, wantPayloads)
		}
	})
}

// noopSetup spawns nothing.
func noopSetup(*lockstep.System) {}

// AT-REM-37 (defined here), REM-325: a datagram whose connection ID is unknown is discarded with
// reason unknown-connection and answered with CLOSE unknown-connection on that ID, with bit 31
// flipped to the answering side's direction; an unknown CLOSE is not answered.
func TestATREM37UnknownConnection(t *testing.T) {
	Simulate(t, Options{Options: faultline.Options{Seeds: 3, Trace: kernel.TraceConfig{Level: kernel.TraceFull}}}, func(s *Sim) {
		w, tt := s.World(), s.World().T()
		raw := addRawPeer(s, "raw", false)
		n1 := s.Node("n1", noopSetup)
		w.RunFor(time.Millisecond)
		raw.send(s, n1.ID(), 0x500000007, &remoting.Frame{Kind: remoting.KindPing})        // from a dialer
		raw.send(s, n1.ID(), 0x580000009, &remoting.Frame{Kind: remoting.KindPong})        // from an acceptor
		raw.send(s, n1.ID(), 0x50000000b, &remoting.Frame{Kind: remoting.KindMsg, Seq: 4}) // a data frame
		raw.send(s, n1.ID(), 0x50000000c, &remoting.Frame{Kind: remoting.KindClose, Payload: []byte("idle")})
		w.RunFor(time.Second)

		discards := recordsOf(w, "lockstep.remote.discard", n1.ID())
		want := []string{
			"peer=raw conn=21474836487 kind=PING seq=0 reason=unknown-connection",
			"peer=raw conn=21474836489 kind=PONG seq=0 reason=unknown-connection",
			"peer=raw conn=21474836491 kind=MSG seq=4 reason=unknown-connection",
			"peer=raw conn=21474836492 kind=CLOSE seq=0 reason=unknown-connection",
		}
		if !slices.Equal(discards, want) {
			tt.Errorf("n1's discard records\n%q\nwant\n%q", discards, want)
		}
		got := raw.kinds() // the answers have their own link delays
		slices.Sort(got)
		if want := []string{"CLOSE 0x500000009", "CLOSE 0x580000007", "CLOSE 0x58000000b"}; !slices.Equal(got, want) {
			tt.Errorf("raw received %v, want %v", got, want)
		}
		for _, d := range raw.got {
			if d.err != nil || string(d.frame.Payload) != remoting.CloseUnknownConnection {
				tt.Errorf("raw received %s %q (%v), want CLOSE unknown-connection", d.frame.Kind, d.frame.Payload, d.err)
			}
		}
		if err := s.slots[slot(checkMisuse)]; err != nil {
			tt.Errorf("misuse stored: %v", err)
		}
	})
}
