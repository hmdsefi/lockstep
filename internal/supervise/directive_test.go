// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package supervise

import "testing"

func TestNormalize(t *testing.T) {
	sources := []Source{InFactory, InStarted, InReceive, InSignal, InStopped, InDecider, InEscalate}
	for _, src := range sources {
		for _, d := range []Directive{Restart, Stop, Escalate} {
			if eff, ok := Normalize(d, src); !ok || eff != d {
				t.Errorf("Normalize(%d, %v) = %d, %v; want %d, true", d, src, eff, ok, d)
			}
		}
		want := Resume
		if src == InFactory || src == InStarted {
			want = Restart
		}
		if eff, ok := Normalize(Resume, src); !ok || eff != want {
			t.Errorf("Normalize(Resume, %v) = %d, %v; want %d, true", src, eff, ok, want)
		}
		for _, bad := range []Directive{0, 5, 9, 255} {
			if _, ok := Normalize(bad, src); ok {
				t.Errorf("Normalize(Directive(%d), %v) ok, want not ok", bad, src)
			}
		}
	}
}

func TestDirectiveAndScopeValues(t *testing.T) {
	for i, d := range []Directive{Restart, Stop, Escalate, Resume} {
		if int(d) != i+1 {
			t.Errorf("directive %d has value %d", i, d)
		}
	}
	if OneForOne != 1 || AllForOne != 2 {
		t.Errorf("OneForOne, AllForOne = %d, %d; want 1, 2", OneForOne, AllForOne)
	}
}
