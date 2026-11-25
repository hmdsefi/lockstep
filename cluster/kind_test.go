// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package cluster

import (
	"errors"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep"
)

// nopGrain is an actor of M that ignores its messages.
type nopGrain[M any] struct{}

func (nopGrain[M]) Receive(*lockstep.Context[M], M) {}

// nopKind returns a valid Kind of name whose activations ignore their messages of type M.
func nopKind[M any](name string, opts ...KindOption) Kind {
	return NewKind(name, func(string) lockstep.Actor[M] { return nopGrain[M]{} }, opts...)
}

// AT-VAC-04, VAC-010: NewKind's defaults, the order of its options and what it never does.
func TestATVAC04Defaults(t *testing.T) {
	if DefaultIdleTimeout != 10*time.Minute || DefaultHandoffTimeout != 5*time.Second || DefaultBuffer != 1024 {
		t.Fatalf("defaults %v, %v, %d, want 10m, 5s, 1024", DefaultIdleTimeout, DefaultHandoffTimeout, DefaultBuffer)
	}

	t.Run("unset options", func(t *testing.T) {
		k := nopKind[int]("counter")
		if k.Name() != "counter" || k.name != "counter" {
			t.Errorf("Name = %q, want counter", k.Name())
		}
		if k.idle != DefaultIdleTimeout || k.handoff != DefaultHandoffTimeout || k.buffer != DefaultBuffer ||
			k.activation != nil || k.host != nil {
			t.Errorf("defaults not applied: %+v", k.kindConfig)
		}
		if k.spawn == nil || k.accepts == nil || k.send == nil || k.typ != reflect.TypeFor[int]() {
			t.Errorf("closures or type missing: spawn=%v accepts=%v send=%v typ=%v", k.spawn != nil, k.accepts != nil, k.send != nil, k.typ)
		}
	})

	t.Run("options", func(t *testing.T) {
		act := []lockstep.SpawnOption{lockstep.WithMailbox(8, lockstep.FailSend)}
		host := []lockstep.SpawnOption{lockstep.WithActorThroughput(3), lockstep.WithMailbox(9, lockstep.FailSend)}
		k := nopKind[int]("counter", WithIdleTimeout(time.Second), WithHandoffTimeout(2*time.Second), WithBuffer(7),
			WithActivationOptions(act...), WithHostOptions(host...))
		if k.idle != time.Second || k.handoff != 2*time.Second || k.buffer != 7 ||
			len(k.activation) != 1 || len(k.host) != 2 {
			t.Errorf("options not applied: %+v", k.kindConfig)
		}
		// The kind keeps its own copy of the option slices.
		host[0], act[0] = nil, nil
		if k.host[0] == nil || k.activation[0] == nil {
			t.Error("the kind shares the caller's option slices")
		}
	})

	t.Run("order", func(t *testing.T) {
		a, b := lockstep.WithMailbox(1, lockstep.FailSend), lockstep.WithMailbox(2, lockstep.FailSend)
		k := nopKind[int]("counter",
			WithIdleTimeout(time.Second), WithHandoffTimeout(time.Second), WithBuffer(1), WithActivationOptions(a), WithHostOptions(a),
			WithIdleTimeout(3*time.Second), WithHandoffTimeout(4*time.Second), WithBuffer(5), WithActivationOptions(b, b), WithHostOptions(),
			nil)
		if k.idle != 3*time.Second || k.handoff != 4*time.Second || k.buffer != 5 || len(k.activation) != 2 || len(k.host) != 0 {
			t.Errorf("a later option must replace an earlier one: %+v", k.kindConfig)
		}
		// Options are not checked: an invalid value is kept for Join to report.
		k = nopKind[int]("counter", WithIdleTimeout(-1), WithHandoffTimeout(0), WithBuffer(-3))
		if k.idle != -1 || k.handoff != 0 || k.buffer != -3 {
			t.Errorf("NewKind changed invalid options: %+v", k.kindConfig)
		}
	})

	t.Run("never panics", func(t *testing.T) {
		for _, k := range []Kind{
			NewKind[int]("", nil),
			NewKind[int]("x", nil, nil, nil),
			NewKind[any]("\xff", func(string) lockstep.Actor[any] { return nil }),
			NewKind[int](strings.Repeat("a", 1000), nil, WithBuffer(-1)),
		} {
			_ = k.Name()
		}
		var zero Kind
		if zero.Name() != "" || zero.spawn != nil {
			t.Errorf("zero Kind = %+v", zero)
		}
		if k := NewKind[int]("x", nil); k.spawn != nil {
			t.Error("a nil factory must leave spawn nil")
		}
	})
}

// validKind returns a valid Kind of name.
func validKind(name string) Kind { return nopKind[int](name) }

// AT-VAC-04, VAC-010: every row of the kinds validation, with its text, in order, with the index of
// the kind; Validate neither changes Kinds nor reports a kind before a field of CLU-010.
func TestATVAC04Rows(t *testing.T) {
	long65 := strings.Repeat("k", 65)
	badOpt := lockstep.WithMailbox(0, lockstep.FailSend)
	_, mailboxErr := lockstep.Spawn(newSystem(t), "", func() lockstep.Actor[int] { return nopGrain[int]{} }, badOpt)
	if !errors.Is(mailboxErr, lockstep.ErrInvalidOption) {
		t.Fatalf("setup: Spawn with an invalid option returned %v", mailboxErr)
	}
	badThroughput := lockstep.WithActorThroughput(0)
	_, throughputErr := lockstep.Spawn(newSystem(t, lockstep.WithNodeName("n2")), "", func() lockstep.Actor[int] { return nopGrain[int]{} }, badThroughput)

	rows := []struct {
		name  string
		kinds []Kind
		want  string
	}{
		// Row 1: the name.
		{"name empty", []Kind{nopKind[int]("")}, `Kinds[0]: name "" is not a valid kind name`},
		{"name upper", []Kind{nopKind[int]("Counter")}, `Kinds[0]: name "Counter" is not a valid kind name`},
		{"name space", []Kind{nopKind[int]("a b")}, `Kinds[0]: name "a b" is not a valid kind name`},
		{"name slash", []Kind{nopKind[int]("a/b")}, `Kinds[0]: name "a/b" is not a valid kind name`},
		{"name percent", []Kind{nopKind[int]("a%41")}, `Kinds[0]: name "a%41" is not a valid kind name`},
		{"name non-ASCII", []Kind{nopKind[int]("café")}, "Kinds[0]: name \"caf\u00e9\" is not a valid kind name"},
		{"name long", []Kind{nopKind[int](long65)}, `Kinds[0]: name "` + long65 + `" is not a valid kind name`},
		{"name dot", []Kind{nopKind[int](".")}, `Kinds[0]: name "." is not a valid kind name`},
		{"name dot dot", []Kind{nopKind[int]("..")}, `Kinds[0]: name ".." is not a valid kind name`},
		{"zero kind", []Kind{{}}, `Kinds[0]: name "" is not a valid kind name`},
		{"name second", []Kind{validKind("a"), nopKind[int]("B")}, `Kinds[1]: name "B" is not a valid kind name`},
		// Row 2: the second occurrence of a name.
		{"duplicate", []Kind{validKind("a"), validKind("b"), validKind("a")}, `Kinds[2]: duplicate kind "a"`},
		{"duplicate adjacent", []Kind{validKind("a"), validKind("a")}, `Kinds[1]: duplicate kind "a"`},
		{"duplicate before factory", []Kind{validKind("a"), NewKind[int]("a", nil)}, `Kinds[1]: duplicate kind "a"`},
		// Row 3: the factory.
		{"nil factory", []Kind{NewKind[int]("a", nil)}, `Kinds[0]: nil factory`},
		{"nil factory second", []Kind{validKind("a"), NewKind[string]("b", nil)}, `Kinds[1]: nil factory`},
		{"nil factory before timeout", []Kind{NewKind[int]("a", nil, WithIdleTimeout(0))}, `Kinds[0]: nil factory`},
		// Rows 4 to 6: the options, explicit zero or negative values.
		{"idle zero", []Kind{nopKind[int]("a", WithIdleTimeout(0))}, `Kinds[0]: IdleTimeout 0s is not positive`},
		{"idle negative", []Kind{nopKind[int]("a", WithIdleTimeout(-time.Minute))}, `Kinds[0]: IdleTimeout -1m0s is not positive`},
		{"handoff zero", []Kind{nopKind[int]("a", WithHandoffTimeout(0))}, `Kinds[0]: HandoffTimeout 0s is not positive`},
		{"handoff negative", []Kind{nopKind[int]("a", WithHandoffTimeout(-1500*time.Millisecond))}, `Kinds[0]: HandoffTimeout -1.5s is not positive`},
		{"buffer zero", []Kind{nopKind[int]("a", WithBuffer(0))}, `Kinds[0]: Buffer 0 is not positive`},
		{"buffer negative", []Kind{nopKind[int]("a", WithBuffer(-4))}, `Kinds[0]: Buffer -4 is not positive`},
		{"idle before handoff", []Kind{nopKind[int]("a", WithIdleTimeout(0), WithHandoffTimeout(0), WithBuffer(0))}, `Kinds[0]: IdleTimeout 0s is not positive`},
		{"handoff before buffer", []Kind{nopKind[int]("a", WithHandoffTimeout(0), WithBuffer(0))}, `Kinds[0]: HandoffTimeout 0s is not positive`},
		// Row 7: the spawn options, the activation's first.
		{"activation options", []Kind{nopKind[int]("a", WithActivationOptions(badOpt))}, `Kinds[0]: ActivationOptions: ` + mailboxErr.Error()},
		{"host options", []Kind{nopKind[int]("a", WithHostOptions(badThroughput))}, `Kinds[0]: HostOptions: ` + throughputErr.Error()},
		{"activation before host", []Kind{nopKind[int]("a", WithHostOptions(badThroughput), WithActivationOptions(badOpt))}, `Kinds[0]: ActivationOptions: ` + mailboxErr.Error()},
		{"buffer before options", []Kind{nopKind[int]("a", WithBuffer(0), WithActivationOptions(badOpt))}, `Kinds[0]: Buffer 0 is not positive`},
		{"nil spawn option", []Kind{nopKind[int]("a", WithHostOptions(nil))}, `Kinds[0]: HostOptions: ` + "lockstep: invalid option: option 0 is lockstep.SpawnOption, want a non-nil SpawnOption"},
		// The first invalid kind is the one reported, whatever follows.
		{"first kind wins", []Kind{nopKind[int]("a", WithBuffer(0)), nopKind[int]("B")}, `Kinds[0]: Buffer 0 is not positive`},
	}
	check := func(t *testing.T, err error, want string) {
		t.Helper()
		if !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("error %v does not wrap ErrInvalidConfig", err)
		}
		if got, want := err.Error(), "cluster: invalid config: "+want; got != want {
			t.Fatalf("error %q, want %q", got, want)
		}
	}
	for _, r := range rows {
		t.Run(r.name, func(t *testing.T) {
			c := Config{Kinds: r.kinds}
			err := c.Validate()
			check(t, err, r.want)
			if strings.Contains(r.want, "ActivationOptions") || strings.Contains(r.want, "HostOptions") {
				if !errors.Is(err, lockstep.ErrInvalidOption) {
					t.Errorf("error %v does not wrap ErrInvalidOption", err)
				}
			}
			// Join reports the same error and joins nothing.
			sys := newSystem(t)
			m, err := Join(sys, Config{Kinds: r.kinds})
			if m != nil {
				t.Error("Join returned a Membership for invalid kinds")
			}
			check(t, err, r.want)
			if m, err := Join(sys, Config{}); err != nil || m == nil {
				t.Errorf("Join after the refused one = %v, %v: the refused Join must not mark the system", m, err)
			}
		})
	}

	t.Run("literal texts", func(t *testing.T) {
		for _, tc := range []struct {
			kinds []Kind
			want  string
		}{
			{[]Kind{nopKind[int]("Bad")}, `cluster: invalid config: Kinds[0]: name "Bad" is not a valid kind name`},
			{[]Kind{validKind("a"), validKind("a")}, `cluster: invalid config: Kinds[1]: duplicate kind "a"`},
			{[]Kind{NewKind[int]("a", nil)}, `cluster: invalid config: Kinds[0]: nil factory`},
			{[]Kind{nopKind[int]("a", WithIdleTimeout(0))}, `cluster: invalid config: Kinds[0]: IdleTimeout 0s is not positive`},
			{[]Kind{nopKind[int]("a", WithHandoffTimeout(-time.Second))}, `cluster: invalid config: Kinds[0]: HandoffTimeout -1s is not positive`},
			{[]Kind{nopKind[int]("a", WithBuffer(-1))}, `cluster: invalid config: Kinds[0]: Buffer -1 is not positive`},
			{[]Kind{nopKind[int]("a", WithActivationOptions(badOpt))}, `cluster: invalid config: Kinds[0]: ActivationOptions: lockstep: invalid option: mailbox capacity must be at least 1, got 0`},
		} {
			c := Config{Kinds: tc.kinds}
			if err := c.Validate(); err == nil || err.Error() != tc.want {
				t.Errorf("Validate = %v, want %q", err, tc.want)
			}
		}
	})

	t.Run("valid names", func(t *testing.T) {
		for _, n := range []string{"a", "0", "_", "-", "a.b", "..a", "a..", ".a", "counter", "session.v2", "a_b-c.d9", strings.Repeat("k", 64)} {
			c := Config{Kinds: []Kind{validKind(n)}}
			if err := c.Validate(); err != nil {
				t.Errorf("Validate with kind %q = %v", n, err)
			}
		}
	})

	t.Run("valid kinds", func(t *testing.T) {
		opts := []lockstep.SpawnOption{lockstep.WithMailbox(4, lockstep.FailSend), lockstep.WithActorThroughput(2)}
		kinds := []Kind{
			validKind("z"),
			nopKind[string]("a", WithIdleTimeout(time.Nanosecond), WithHandoffTimeout(time.Nanosecond), WithBuffer(1),
				WithActivationOptions(opts...), WithHostOptions(opts...)),
		}
		c := Config{Kinds: kinds}
		if err := c.Validate(); err != nil {
			t.Fatalf("Validate = %v", err)
		}
		if len(c.Kinds) != 2 || c.Kinds[0].Name() != "z" || c.Kinds[1].Name() != "a" {
			t.Errorf("Validate changed Kinds: %v", c.Kinds)
		}
		var none Config
		if err := none.Validate(); err != nil || none.Kinds != nil {
			t.Errorf("Validate of a client-only Config = %v, Kinds %v", err, none.Kinds)
		}
	})

	t.Run("after the other fields", func(t *testing.T) {
		c := Config{ProbeInterval: -time.Second, Kinds: []Kind{nopKind[int]("B")}}
		check(t, c.Validate(), "ProbeInterval: -1s is not positive")
		c = Config{DeadRetention: -time.Second, Kinds: []Kind{nopKind[int]("B")}}
		check(t, c.Validate(), "DeadRetention: -1s is not positive")
	})
}

// AT-VAC-04: the closures of a Kind spawn an activation of M with the key, say which values are an
// M and send an M to an activation; nothing else gets through.
func TestATVAC04Closures(t *testing.T) {
	sys := newSystem(t)
	type msg struct{ n int }
	got := make(chan string, 4)
	var keys []string
	k := NewKind("counter", func(key string) lockstep.Actor[msg] {
		keys = append(keys, key)
		return actorFunc[msg](func(_ *lockstep.Context[msg], m msg) { got <- key + ":" + strconv.Itoa(m.n) })
	})
	if !k.accepts(msg{1}) || k.accepts(&msg{1}) || k.accepts("x") || k.accepts(nil) {
		t.Error("accepts must say yes only to an M")
	}
	if k.typ != reflect.TypeFor[msg]() {
		t.Errorf("typ = %v", k.typ)
	}
	addr, pid, err := k.spawn(sys, "a%2Fb", "a/b", nil)
	if err != nil {
		t.Fatal(err)
	}
	typed, ok := pid.(lockstep.PID[msg])
	if !ok {
		t.Fatalf("spawn returned a %T, want a lockstep.PID of the kind's message type", pid)
	}
	if addr.Path() != "/user/a%2Fb" || addr != typed.Address() {
		t.Errorf("activation at %v with pid %v, want /user/a%%2Fb", addr, pid)
	}
	if err := k.send(sys, pid, msg{7}); err != nil {
		t.Fatal(err)
	}
	select {
	case s := <-got:
		if s != "a/b:7" {
			t.Errorf("activation got %q, want a/b:7", s)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the activation received nothing")
	}
	if !slices.Equal(keys, []string{"a/b"}) {
		t.Errorf("factory keys %v, want [a/b]", keys)
	}
	if err := k.send(sys, pid, "wrong"); err == nil {
		t.Error("send of a value that is not an M succeeded")
	}
	if err := k.send(sys, "wrong", msg{1}); err == nil {
		t.Error("send to a value that is not a PID succeeded")
	}
	// The name is taken by the live activation; a spawn error comes back as Spawn's.
	if _, _, err := k.spawn(sys, "a%2Fb", "a/b", nil); !errors.Is(err, lockstep.ErrNameTaken) {
		t.Errorf("second spawn = %v, want ErrNameTaken", err)
	}
	// The spawn options reach Spawn.
	if _, _, err := k.spawn(sys, "other", "other", []lockstep.SpawnOption{lockstep.WithMailbox(0, lockstep.FailSend)}); !errors.Is(err, lockstep.ErrInvalidOption) {
		t.Errorf("spawn with an invalid option = %v, want ErrInvalidOption", err)
	}
}

// actorFunc is an actor made of a function.
type actorFunc[M any] func(*lockstep.Context[M], M)

func (f actorFunc[M]) Receive(ctx *lockstep.Context[M], m M) { f(ctx, m) }

// AT-VAC-04, CLU-020: Join puts the sorted kind names in Self.Kinds of view 1, nil for none, takes
// its own copy of Config.Kinds and publishes the kinds in the view's Members entry too.
func TestSelfKinds(t *testing.T) {
	t.Run("sorted", func(t *testing.T) {
		sys := newSystem(t)
		kinds := []Kind{validKind("zeta"), validKind("alpha"), validKind("mid.1")}
		m, err := Join(sys, Config{Kinds: kinds})
		if err != nil {
			t.Fatal(err)
		}
		want := []string{"alpha", "mid.1", "zeta"}
		v := m.View()
		if v.Version != 1 || !slices.Equal(v.Self.Kinds, want) || len(v.Members) != 1 || !slices.Equal(v.Members[0].Kinds, want) {
			t.Errorf("view %+v, want version 1 with Kinds %v in Self and Members", v, want)
		}
		if got := v.Up(); len(got) != 1 || !slices.Equal(got[0].Kinds, want) {
			t.Errorf("Up() = %+v, want Kinds %v", got, want)
		}
		// The caller's slice keeps its order and a change to it does not reach the Membership.
		if kinds[0].Name() != "zeta" || kinds[1].Name() != "alpha" {
			t.Errorf("Join reordered the caller's Kinds: %v", kinds)
		}
		kinds[0] = validKind("changed")
		if m.cfg.Kinds[0].Name() != "zeta" {
			t.Errorf("Membership shares the caller's Kinds slice: %v", m.cfg.Kinds)
		}
	})

	t.Run("none", func(t *testing.T) {
		for _, kinds := range [][]Kind{nil, {}} {
			sys := lockstep.New(lockstep.WithNodeName("n1"), quiet)
			m, err := Join(sys, Config{Kinds: kinds})
			if err != nil {
				t.Fatal(err)
			}
			v := m.View()
			if v.Self.Kinds != nil || v.Members[0].Kinds != nil {
				t.Errorf("Kinds of %#v: Self %#v, Members %#v, want nil", kinds, v.Self.Kinds, v.Members[0].Kinds)
			}
			shutdown(t, sys)
		}
	})

	t.Run("one", func(t *testing.T) {
		sys := newSystem(t)
		m, err := Join(sys, Config{Kinds: []Kind{validKind("only")}})
		if err != nil {
			t.Fatal(err)
		}
		if got := m.View().Self.Kinds; !slices.Equal(got, []string{"only"}) {
			t.Errorf("Self.Kinds = %v", got)
		}
	})
}

// kindNames and validKindList are the helpers of the table and the wire rule.
func TestKindNameHelpers(t *testing.T) {
	if kindNames(nil) != nil || kindNames([]Kind{}) != nil {
		t.Error("kindNames of no kinds must be nil")
	}
	if got := kindNames([]Kind{validKind("b"), validKind("a")}); !slices.Equal(got, []string{"a", "b"}) {
		t.Errorf("kindNames = %v", got)
	}
	for _, tc := range []struct {
		names []string
		want  bool
	}{
		{nil, true},
		{[]string{}, true},
		{[]string{"a"}, true},
		{[]string{"a", "b", "c.d"}, true},
		{[]string{"b", "a"}, false},
		{[]string{"a", "a"}, false},
		{[]string{"a", "B"}, false},
		{[]string{""}, false},
		{[]string{"a", ""}, false},
		{[]string{"a", "b c"}, false},
		{[]string{strings.Repeat("a", 65)}, false},
	} {
		if got := validKindList(tc.names); got != tc.want {
			t.Errorf("validKindList(%q) = %v, want %v", tc.names, got, tc.want)
		}
	}
}
