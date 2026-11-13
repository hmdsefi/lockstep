// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep_test

import (
	"context"
	"encoding"
	"strconv"
	"strings"
	"testing"

	"github.com/hmdsefi/lockstep"
)

type textAsk struct{ rt lockstep.ReplyTo[int] }

type textAskActor struct{}

func (textAskActor) Receive(ctx *lockstep.Context[textAsk], m textAsk) { m.rt.Send(ctx, 1) }

var (
	_ encoding.TextMarshaler   = lockstep.Address{}
	_ encoding.TextUnmarshaler = (*lockstep.Address)(nil)
	_ encoding.TextMarshaler   = lockstep.PID[int]{}
	_ encoding.TextUnmarshaler = (*lockstep.PID[int])(nil)
	_ encoding.TextMarshaler   = lockstep.ReplyTo[int]{}
	_ encoding.TextUnmarshaler = (*lockstep.ReplyTo[int])(nil)
)

// AT-COD-10: COD-120.
func TestATCOD10TextForms(t *testing.T) {
	sys := newProdSystem(t, lockstep.WithNodeName("n1"))
	pid, err := lockstep.Spawn(sys, "worker", func() lockstep.Actor[textAsk] { return textAskActor{} })
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	byPath := lockstep.PIDAt[textAsk]("n1", "/user/worker")
	var rts []lockstep.ReplyTo[int]
	for i := 0; i < 12; i++ {
		r := lockstep.Ask(sys, pid, func(rt lockstep.ReplyTo[int]) textAsk {
			rts = append(rts, rt)
			return textAsk{rt: rt}
		})
		if _, err := r.Await(context.Background()); err != nil {
			t.Fatalf("Ask: %v", err)
		}
	}

	t.Run("address", func(t *testing.T) {
		if pid.Address().UID() == 0 || byPath.Address().UID() != 0 {
			t.Fatalf("uids: spawned %d, PIDAt %d", pid.Address().UID(), byPath.Address().UID())
		}
		for _, a := range []lockstep.Address{pid.Address(), byPath.Address()} {
			b, err := a.MarshalText()
			if err != nil || string(b) != a.String() {
				t.Fatalf("MarshalText(%v) = %q, %v", a, b, err)
			}
			var back lockstep.Address
			if err := back.UnmarshalText(b); err != nil || back != a {
				t.Errorf("round trip of %q = %v, %v", b, back, err)
			}
		}
		if s := byPath.Address().String(); s != "lockstep://n1/user/worker#0" {
			t.Errorf("PIDAt text = %q", s)
		}
		b, err := lockstep.Address{}.MarshalText()
		if err != nil || len(b) != 0 {
			t.Errorf("zero Address marshals to %q, %v", b, err)
		}
		a := pid.Address()
		if err := a.UnmarshalText(nil); err != nil || !a.IsZero() {
			t.Errorf("empty text gives %v, %v", a, err)
		}
	})

	t.Run("pid", func(t *testing.T) {
		for _, p := range []lockstep.PID[textAsk]{pid, byPath} {
			b, err := p.MarshalText()
			if err != nil || string(b) != p.Address().String() {
				t.Fatalf("PID MarshalText = %q, %v", b, err)
			}
			var back lockstep.PID[textAsk]
			if err := back.UnmarshalText(b); err != nil || back != p {
				t.Errorf("round trip = %v, %v", back, err)
			}
			var other lockstep.PID[string] // M is not checked
			if err := other.UnmarshalText(b); err != nil || other.Address() != p.Address() {
				t.Errorf("other M: %v, %v", other, err)
			}
		}
		b, err := lockstep.PID[int]{}.MarshalText()
		if err != nil || len(b) != 0 {
			t.Errorf("zero PID marshals to %q, %v", b, err)
		}
		p := pid
		if err := p.UnmarshalText([]byte{}); err != nil || !p.IsZero() {
			t.Errorf("empty text gives %v, %v", p, err)
		}
	})

	t.Run("reply", func(t *testing.T) {
		if len(rts) != 12 {
			t.Fatalf("captured %d reply addresses", len(rts))
		}
		for i, rt := range rts {
			b, err := rt.MarshalText()
			if err != nil {
				t.Fatalf("MarshalText: %v", err)
			}
			s := string(b)
			if want := "lockstep-reply://n1/temp/" + strconv.Itoa(i+1) + "#"; !strings.HasPrefix(s, want) || strings.HasSuffix(s, "#0") {
				t.Errorf("reply text %q, want prefix %q and a non-zero uid", s, want)
			}
			var back lockstep.ReplyTo[int]
			if err := back.UnmarshalText(b); err != nil || back != rt {
				t.Errorf("round trip of %q = %v, %v", s, back, err)
			}
			var other lockstep.ReplyTo[string] // R is not checked
			if err := other.UnmarshalText(b); err != nil {
				t.Errorf("other R: %v", err)
			}
		}
		b, err := lockstep.ReplyTo[int]{}.MarshalText()
		if err != nil || len(b) != 0 {
			t.Errorf("zero ReplyTo marshals to %q, %v", b, err)
		}
		rt := rts[0]
		if err := rt.UnmarshalText(nil); err != nil || rt != (lockstep.ReplyTo[int]{}) {
			t.Errorf("empty text gives %v, %v", rt, err)
		}
	})

	t.Run("address rejections", func(t *testing.T) {
		for _, in := range []string{
			"lockstep-reply://n1/user/a#1",
			"http://n1/user/a#1",
			"Lockstep://n1/user/a#1",
			"lockstep://n1/user/a#1F",
			"lockstep://n1/user/a#01",
			"lockstep://n1/user/a#00",
			"lockstep://n1/user/a#10000000000000000",
			"lockstep://n1/user/a#",
			"lockstep://n1/user/a#g",
			"lockstep://n1/user/a#-1",
			"lockstep://n1/user/a#+1",
			"lockstep:///user/a#1",
			"lockstep://-n/user/a#1",
			"lockstep://n 1/user/a#1",
			"lockstep://n:1/user/a#1",
			"lockstep://n1#1",
			"lockstep://n1",
			"lockstep://",
			"lockstep://n1/#1",
			"lockstep://n1/user//a#1",
			"lockstep://n1/user/a/#1",
			"lockstep://n1/user/$01#1",
			"lockstep://n1/user/a",
			"lockstep://n1/user/a #1",
			"lockstep://n1/user/a#1 ",
			"lockstep://n1/user/a#1#2",
			"lockstep://n1/user/a#1\n",
			"<zero>",
			" ",
			"lockstep://" + strings.Repeat("n", 65) + "/user/a#1",
		} {
			want := "lockstep: invalid address " + strconv.Quote(in)
			a := lockstep.PIDAt[int]("keep", "/user/k").Address()
			if err := a.UnmarshalText([]byte(in)); err == nil || err.Error() != want {
				t.Errorf("Address %q: err = %v, want %s", in, err, want)
			}
			if a != lockstep.PIDAt[int]("keep", "/user/k").Address() {
				t.Errorf("Address %q: receiver changed to %v", in, a)
			}
			p := lockstep.PIDAt[int]("keep", "/user/k")
			if err := p.UnmarshalText([]byte(in)); err == nil || err.Error() != want {
				t.Errorf("PID %q: err = %v, want %s", in, err, want)
			}
			if p != lockstep.PIDAt[int]("keep", "/user/k") {
				t.Errorf("PID %q: receiver changed", in)
			}
		}
	})

	t.Run("reply rejections", func(t *testing.T) {
		for _, in := range []string{
			"lockstep://n1/temp/1#1",
			"http://n1/temp/1#1",
			"lockstep-reply://n1/temp/1#0",
			"lockstep-reply://n1/temp/1#01",
			"lockstep-reply://n1/temp/1#A",
			"lockstep-reply://n1/temp/1#10000000000000000",
			"lockstep-reply://n1/temp/1",
			"lockstep-reply://n1/temp/1#",
			"lockstep-reply://n1/temp/0#1",
			"lockstep-reply://n1/temp/01#1",
			"lockstep-reply://n1/temp/#1",
			"lockstep-reply://n1/temp/1/#1",
			"lockstep-reply://n1/temp/1x#1",
			"lockstep-reply://n1/temp/+1#1",
			"lockstep-reply://n1/temp/18446744073709551616#1",
			"lockstep-reply://n1/temp/$1#1",
			"lockstep-reply://n1/user/a#1",
			"lockstep-reply://n1/temp#1",
			"lockstep-reply://n1/Temp/1#1",
			"lockstep-reply://n1/temp/1/2#1",
			"lockstep-reply://n1/temp/1#1 ",
			"lockstep-reply://n1/temp/1 #1",
			"lockstep-reply://n1/temp/1#1#1",
			"lockstep-reply:///temp/1#1",
			"lockstep-reply://_n/temp/1#1",
			"lockstep-reply://n1#1",
			"lockstep-reply://",
			"<zero>",
		} {
			rt := rts[0]
			want := "lockstep: invalid reply address " + strconv.Quote(in)
			if err := rt.UnmarshalText([]byte(in)); err == nil || err.Error() != want {
				t.Errorf("ReplyTo %q: err = %v, want %s", in, err, want)
			}
			if rt != rts[0] {
				t.Errorf("ReplyTo %q: receiver changed", in)
			}
		}
	})

	t.Run("extremes", func(t *testing.T) {
		in := "lockstep://n-1.x_y/user/a%2B/$7/b#ffffffffffffffff"
		var a lockstep.Address
		if err := a.UnmarshalText([]byte(in)); err != nil || a.UID() != 1<<64-1 {
			t.Fatalf("%q: %v, %v", in, a, err)
		}
		if b, _ := a.MarshalText(); string(b) != in {
			t.Errorf("marshals to %q", b)
		}
		in = "lockstep://n1/temp/5#0"
		if err := a.UnmarshalText([]byte(in)); err != nil || a.Path() != "/temp/5" || a.UID() != 0 {
			t.Errorf("%q: %v, %v", in, a, err)
		}
		for _, ok := range []string{
			"lockstep-reply://n1/temp/1#1",
			"lockstep-reply://n1/temp/18446744073709551615#ffffffffffffffff",
		} {
			var rt lockstep.ReplyTo[int]
			if err := rt.UnmarshalText([]byte(ok)); err != nil {
				t.Errorf("%q: %v", ok, err)
			}
			if b, err := rt.MarshalText(); err != nil || string(b) != ok {
				t.Errorf("%q marshals to %q, %v", ok, b, err)
			}
		}
	})
}
