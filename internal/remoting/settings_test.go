// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package remoting

import (
	"errors"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/codec"
)

// otherCodec is codec.JSON under another name, to see the name a core announces.
type otherCodec struct{ codec.JSON }

func (otherCodec) Name() string { return "other" }

// TestSetSettings checks REM-040's core side: only a simulated core's settings can be replaced,
// once; invalid settings change nothing; the new frame limit and codec are announced in later
// handshakes; the heartbeat tick is armed again with the new Heartbeat; a shut-down core refuses
// with an error wrapping lockstep.ErrStopped.
func TestSetSettings(t *testing.T) {
	t.Run("not simulated", func(t *testing.T) {
		h := newHarness(t, nil)
		if h.core.Settable() {
			t.Error("Settable() = true for a core that is not simulated")
		}
		if err := h.core.SetSettings(testSettings()); err == nil {
			t.Error("SetSettings on a core that is not simulated returned nil")
		}
	})
	t.Run("simulated", func(t *testing.T) {
		h := newHarness(t, func(c *Config) { c.Simulated = true })
		if !h.core.Settable() {
			t.Fatal("Settable() = false for a new simulated core")
		}
		bad := testSettings()
		bad.MaxFrame = 100
		if err := h.core.SetSettings(bad); err == nil || err.Error() != "remoting: invalid config: MaxFrame 100 out of range [4096, 67108864]" {
			t.Errorf("SetSettings with MaxFrame 100: %v", err)
		}
		if !h.core.Settable() || h.core.set.MaxFrame != 1<<20 {
			t.Fatal("invalid settings changed the core")
		}

		up := h.dialUp()
		written := len(up.frames)
		// With a session up, the codec and the registry stay.
		for _, mod := range []func(*Settings){
			func(s *Settings) { s.Codec = otherCodec{} },
			func(s *Settings) { s.Registry = codec.NewRegistry() },
		} {
			s := testSettings()
			mod(&s)
			if err := h.core.SetSettings(s); !errors.Is(err, ErrCodecInUse) {
				t.Errorf("SetSettings changing the codec or registry while connected: %v, want ErrCodecInUse", err)
			}
		}
		if !h.core.Settable() || h.core.codecName != "json" {
			t.Fatal("a refused codec change changed the core")
		}
		set := testSettings()
		set.MaxFrame, set.Heartbeat = 8192, 2*time.Second
		h.eng.Advance(500 * time.Millisecond)
		if err := h.core.SetSettings(set); err != nil {
			t.Fatalf("SetSettings: %v", err)
		}
		if h.core.Settable() {
			t.Error("Settable() = true after SetSettings")
		}
		if err := h.core.SetSettings(set); err == nil {
			t.Error("a second SetSettings returned nil")
		}
		if len(up.frames) != written {
			t.Errorf("the up session wrote %v after SetSettings, want nothing", up.kinds()[written:])
		}
		if got := h.recs(); len(got) != 0 {
			t.Errorf("SetSettings recorded %q, want nothing", got)
		}

		// The tick armed at Start (due at 1s) is replaced by one due 2s after SetSettings.
		ticks := count(h.eng.Labels(), labelTick)
		h.eng.Advance(1900 * time.Millisecond)
		if n := count(h.eng.Labels(), labelTick); n != ticks {
			t.Errorf("%d ticks armed before the new Heartbeat passed, want %d", n-ticks, 0)
		}
		h.eng.Advance(100 * time.Millisecond)
		if n := count(h.eng.Labels(), labelTick); n != ticks+1 {
			t.Errorf("%d ticks armed at the new Heartbeat, want 1", n-ticks)
		}

		// A new handshake announces the new limit.
		l := &fakeLink{t: t, id: 9}
		h.core.Accepted(l)
		h.core.Frame(l, hello("c", "a", 3, "json", ""))
		if len(l.frames) != 1 || l.frames[0].Kind != KindWelcome {
			t.Fatalf("frames on the accepted link: %q, want WELCOME", l.kinds())
		}
		w, err := ParseHello(l.frames[0].Payload)
		if err != nil || w.FrameLimit != 8192 || w.Codec != "json" {
			t.Errorf("WELCOME %+v, %v; want frame limit 8192 and codec json", w, err)
		}
	})
	t.Run("codec before any connection", func(t *testing.T) {
		h := newHarness(t, func(c *Config) { c.Simulated = true })
		set := testSettings()
		set.Codec = otherCodec{}
		if err := h.core.SetSettings(set); err != nil {
			t.Fatalf("SetSettings with another codec and no connection: %v", err)
		}
		if err := h.core.Connect("b"); err != nil {
			t.Fatal(err)
		}
		l := h.drv.links[0]
		if len(l.frames) != 1 || l.frames[0].Kind != KindHello {
			t.Fatalf("frames on the dialed link: %q, want HELLO", l.kinds())
		}
		if hl, err := ParseHello(l.frames[0].Payload); err != nil || hl.Codec != "other" {
			t.Errorf("HELLO %+v, %v; want codec other", hl, err)
		}
	})
	t.Run("after shutdown", func(t *testing.T) {
		h := newHarness(t, func(c *Config) { c.Simulated = true })
		h.core.Shutdown()
		if err := h.core.SetSettings(testSettings()); !errors.Is(err, lockstep.ErrStopped) {
			t.Errorf("SetSettings after Shutdown: %v, want an error wrapping lockstep.ErrStopped", err)
		}
	})
}

// TestStoppedMarker checks the table's stopped markers (REM-040): a simulated core's Shutdown and
// MarkStopped leave one, which blocks Register and which Unregister removes; a production core's
// Shutdown leaves none.
func TestStoppedMarker(t *testing.T) {
	for _, simulated := range []bool{true, false} {
		h := newHarness(t, func(c *Config) { c.Simulated = simulated })
		sys := h.core.sys
		if !Register(h.core) {
			t.Fatal("Register of a new core reported false")
		}
		h.core.Shutdown()
		if CoreOf(sys) != nil || Stopped(sys) != simulated {
			t.Errorf("simulated %t: after Shutdown CoreOf = %p, Stopped = %t; want nil, %t", simulated, CoreOf(sys), Stopped(sys), simulated)
		}
		if Register(h.core) == simulated {
			t.Errorf("simulated %t: Register after Shutdown reported %t", simulated, !simulated)
		}
		Unregister(sys)
		if CoreOf(sys) != nil || Stopped(sys) {
			t.Errorf("simulated %t: Unregister left an entry", simulated)
		}
	}
	h := newHarness(t, func(c *Config) { c.Simulated = true })
	sys := h.core.sys
	Register(h.core)
	MarkStopped(sys)
	if CoreOf(sys) != nil || !Stopped(sys) {
		t.Error("MarkStopped did not replace the core with a marker")
	}
	Unregister(sys)
	if Stopped(sys) {
		t.Error("Unregister left the marker")
	}
}
