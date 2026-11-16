// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package cluster

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/sim"
)

// AT-CLU-19, CLU-600, CLU-020: n1, n2 and n3 seed each other and converge; then n3 calls Leave,
// which returns at once, and a second Leave returns ErrLeft. n3 sets its own entry Left at its
// incarnation, sends a leave to n1 and n2, records leave members=2, publishes the view that shows
// it and stops its membership actor. n1 and n2 merge the leave (or one of them learns it from the
// other first, which only a node that merged the leave can tell): n3 is Left (not Dead) and their
// subscribers get NodeDown{n3, ID, Left: true}; n3's own subscriber gets nothing. Afterwards Leave
// and Subscribe on n3 return ErrLeft and Join returns ErrAlreadyJoined, and no node probes n3 any
// more.
func TestATCLU19Leave(t *testing.T) {
	runLeave(t, func(tt testing.TB, s *sim.Sim, c *simCluster) {
		if err := c.ms["n3"].Leave(); err != nil {
			tt.Fatalf("Leave: %v", err)
		}
		if err := c.ms["n3"].Leave(); !errors.Is(err, ErrLeft) {
			tt.Errorf("second Leave = %v, want ErrLeft", err)
		}
	})
}

// AT-CLU-20, CLU-610: as AT-CLU-19, but n3's System shuts down instead of calling Leave: the
// membership actor's Stopped leaves the same way before the remoting core closes, so n1 and n2 see
// n3 Left and signal NodeDown{Left: true}.
func TestATCLU20ShutdownLeaves(t *testing.T) {
	runLeave(t, func(tt testing.TB, s *sim.Sim, _ *simCluster) {
		if err := s.System("n3").Shutdown(context.Background()); err != nil {
			tt.Fatalf("Shutdown: %v", err)
		}
	})
}

// runLeave runs AT-CLU-19 and AT-CLU-20: leave makes n3 leave at 3s.
func runLeave(t *testing.T, leave func(tt testing.TB, s *sim.Sim, c *simCluster)) {
	names := []string{"n1", "n2", "n3"}
	const leaveAt = 3 * time.Second
	sim.Simulate(t, simOpts(5, 8*time.Second), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		c := newSimCluster(s)
		subs := map[string]*subLog{} // read by key
		for _, n := range names {
			subs[n] = c.subNode(n, simConfig(names...), true)
		}
		w.RunFor(leaveAt)
		id3, sys3 := c.id("n3"), s.System("n3")
		for _, n := range names {
			if v := c.view(n); len(v.Up()) != 3 {
				tt.Fatalf("%s's view before the leave: %v, want the three up", n, v.Members)
			}
		}
		leave(tt, s, c)
		w.RunFor(4 * time.Second)

		var got []string
		for _, r := range clusterRecs(c, "n3") {
			if r.At >= vt(leaveAt) {
				if k := strings.TrimPrefix(r.Kind, recordPrefix); k == "member" || k == "leave" || k == "view" || strings.HasPrefix(k, "node.") {
					got = append(got, k+" "+attrs(r))
				}
			}
		}
		want := []string{
			"member node=n3 id=" + uitoa(id3) + " inc=0 from=alive to=left source=self",
			"leave members=2",
			"view version=" + strconv.FormatUint(c.view("n3").Version, 10) + " members=3 up=2",
		}
		if !slices.Equal(got, want) {
			tt.Errorf("n3's records from the leave: %q, want %q", got, want)
		}
		if self := c.view("n3").Self; self != (Member{Node: "n3", ID: id3, State: Left}) {
			tt.Errorf("n3's own entry %v, want Left at incarnation 0", self)
		}
		if _, err := lockstep.Lookup[memberMsg](sys3, actorPath); !errors.Is(err, lockstep.ErrNotFound) && !errors.Is(err, lockstep.ErrStopped) {
			tt.Errorf("Lookup of n3's membership actor after the leave: %v, want it gone", err)
		}
		if got := subs["n3"].sigs()[2:]; len(got) != 0 {
			tt.Errorf("n3's subscriber got %q at the leave, want nothing about itself", got)
		}

		byLeave := 0
		for _, n := range names[:2] {
			var left []kernel.Record
			for _, r := range c.recs(n, "member") {
				if attr(r, "node") == "n3" && r.At >= vt(leaveAt) {
					left = append(left, r)
				}
			}
			if len(left) != 1 || !strings.HasPrefix(attrs(left[0]), "node=n3 id="+uitoa(id3)+" inc=0 from=alive to=left source=") {
				tt.Errorf("%s's records of n3 after the leave %s, want one Left", n, describe(left))
			} else if attr(left[0], "source") == wireLeave[len(recordPrefix):] {
				byLeave++
			}
			downs := c.recs(n, "node.down")
			if len(downs) != 1 || attrs(downs[0]) != "node=n3 id="+uitoa(id3)+" left=true" || len(left) == 1 && downs[0].At != left[0].At {
				tt.Errorf("%s's node.down records %s, want n3 with left=true when it merged the leave", n, describe(downs))
			}
			if sg := subs[n].sigs(); len(sg) != 3 || sg[2] != "down n3 "+uitoa(id3)+" left=true" {
				tt.Errorf("%s's subscriber got %q, want NodeUp of the two others, then NodeDown{n3, Left: true}", n, sg)
			}
			if m, _ := c.view(n).Member("n3"); m.State != Left {
				tt.Errorf("%s's view has n3 %v, want Left", n, m)
			}
			for _, r := range c.recs(n, "probe") {
				if attr(r, "target") == "n3" && len(left) == 1 && r.At > left[0].At {
					tt.Errorf("%s probed n3 at %s, after it left", n, r.At)
				}
			}
			expectAlternation(tt, subs[n])
		}
		if byLeave == 0 {
			tt.Error("neither n1 nor n2 learnt of n3's leave from its leave message")
		}

		m3 := c.ms["n3"]
		if err := m3.Leave(); !errors.Is(err, ErrLeft) {
			tt.Errorf("Leave after the leave = %v, want ErrLeft", err)
		}
		if err := m3.Subscribe(subs["n3"].pid.Address()); !errors.Is(err, ErrLeft) {
			tt.Errorf("Subscribe after the leave = %v, want ErrLeft", err)
		}
		if m, err := Join(sys3, simConfig(names...)); m != nil || !errors.Is(err, ErrAlreadyJoined) {
			tt.Errorf("Join after the leave = %v, %v, want nil, ErrAlreadyJoined", m, err)
		}
	})
}
