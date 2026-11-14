// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim_test

import (
	"math"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/codec"
	"github.com/hmdsefi/lockstep/remote"
	"github.com/hmdsefi/lockstep/sim"
)

// The codec acceptance tests that need the remoting transport (codecs.md gives AT-COD-13 to
// AT-COD-16 and AT-COD-19 no text; each is defined at its test).

// Builtins carries one ReplyTo of each of seven built-in types; the answering actor answers each.
type Builtins struct {
	Int64 lockstep.ReplyTo[int64]
	Str   lockstep.ReplyTo[string]
	Dur   lockstep.ReplyTo[time.Duration]
	At    lockstep.ReplyTo[time.Time]
	Addr  lockstep.ReplyTo[lockstep.Address]
	Bytes lockstep.ReplyTo[[]byte]
	Empty lockstep.ReplyTo[struct{}]
}

// builtinAt is the time.Time that answers Builtins.At: with a location, whose name does not
// survive the wire.
var builtinAt = time.Date(2026, 11, 14, 9, 30, 0, 123456789, time.FixedZone("X", 3600))

// AT-COD-13 (defined here), COD-040, COD-050, COD-150, REM-110, REM-160: values cross nodes by wire
// name. An actor on b that takes any message receives each value as its registered Go type, a
// value type and a pointer type alike, and a's send records name the wire names; answers of
// seven built-in types, none registered by a test, reach a's asks equal to the values sent, a
// time.Time without its location name.
func TestATCOD13WireNames(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		s.Node("a", noSetup)
		var got []any
		var builtins lockstep.PID[Builtins]
		s.Node("b", func(sys *lockstep.System) {
			spawn(sys, "any", &actor[any]{receive: func(_ *lockstep.Context[any], m any) { got = append(got, m) }})
			builtins = spawn(sys, "builtins", &actor[Builtins]{receive: func(ctx *lockstep.Context[Builtins], m Builtins) {
				m.Int64.Send(ctx, math.MinInt64)
				m.Str.Send(ctx, "héllo <&>")
				m.Dur.Send(ctx, 90*time.Minute)
				m.At.Send(ctx, builtinAt)
				m.Addr.Send(ctx, ctx.Self().Address())
				m.Bytes.Send(ctx, []byte{0, 1, 255})
				m.Empty.Send(ctx, struct{}{})
			}})
		})
		w.RunFor(time.Millisecond)

		sent := []any{Deposit{N: 3}, &box{N: 4}, Seq{N: 5}, int64(6)}
		for _, m := range sent {
			if err := sim.Send(s, "a", lockstep.PIDAt[any]("b", "/user/any"), m); err != nil {
				tt.Fatalf("Send %T: %v", m, err)
			}
		}
		w.RunFor(time.Second)
		if len(got) != len(sent) {
			tt.Fatalf("b received %v, want %d messages", got, len(sent))
		}
		for i, m := range got {
			if reflect.TypeOf(m) != reflect.TypeOf(sent[i]) || !reflect.DeepEqual(m, sent[i]) {
				tt.Errorf("message %d is %#v, want %#v", i, m, sent[i])
			}
		}
		var types []string
		for _, r := range ofKind(w.Sim.Records(), "lockstep.remote.send") {
			if attr(r, "kind") == "MSG" {
				types = append(types, attr(r, "type"))
			}
		}
		if want := []string{"simtest.Deposit", "simtest.BoxPtr", "simtest.Seq", "lockstep.int64"}; !slices.Equal(types, want) {
			tt.Errorf("MSG types %q, want %q", types, want)
		}

		sys := s.System("a")
		var m Builtins
		var (
			r1 *lockstep.Reply[int64]
			r2 *lockstep.Reply[string]
			r3 *lockstep.Reply[time.Duration]
			r4 *lockstep.Reply[time.Time]
			r5 *lockstep.Reply[lockstep.Address]
			r6 *lockstep.Reply[[]byte]
			r7 *lockstep.Reply[struct{}]
		)
		m.Int64, r1 = lockstep.NewReply[int64](sys)
		m.Str, r2 = lockstep.NewReply[string](sys)
		m.Dur, r3 = lockstep.NewReply[time.Duration](sys)
		m.At, r4 = lockstep.NewReply[time.Time](sys)
		m.Addr, r5 = lockstep.NewReply[lockstep.Address](sys)
		m.Bytes, r6 = lockstep.NewReply[[]byte](sys)
		m.Empty, r7 = lockstep.NewReply[struct{}](sys)
		if err := sim.Send(s, "a", lockstep.PIDAt[Builtins]("b", "/user/builtins"), m); err != nil {
			tt.Fatal(err)
		}
		if v, err := await(s, r1); v != math.MinInt64 || err != nil {
			tt.Errorf("int64 answer %d, %v", v, err)
		}
		if v, err := await(s, r2); v != "héllo <&>" || err != nil {
			tt.Errorf("string answer %q, %v", v, err)
		}
		if v, err := await(s, r3); v != 90*time.Minute || err != nil {
			tt.Errorf("time.Duration answer %v, %v", v, err)
		}
		if v, err := await(s, r4); !v.Equal(builtinAt) || err != nil || v.Location().String() == "X" {
			tt.Errorf("time.Time answer %v, %v; want the same instant without the location name X", v, err)
		}
		if v, err := await(s, r5); v != builtins.Address() || err != nil {
			tt.Errorf("lockstep.Address answer %v, %v", v, err)
		}
		if v, err := await(s, r6); !slices.Equal(v, []byte{0, 1, 255}) || err != nil {
			tt.Errorf("[]byte answer %v, %v", v, err)
		}
		if _, err := await(s, r7); err != nil {
			tt.Errorf("struct{} answer: %v", err)
		}
	})
}

// AT-COD-14 (simulation half; defined here, the production half is remote's
// TestATCOD14UnsupportedTypeFailsEnable), REM-030, REM-300: a registry holding a type that the
// codec cannot encode fails Simulate before any seed, with the error of Registry.Check (scenario
// remote-codec of remote_test.go).
func TestATCOD14UnsupportedTypeFailsSimulate(t *testing.T) {
	r := codec.NewRegistry()
	codec.RegisterIn[chan int](r, "simtest.Chan")
	check := r.Check(codec.JSON{})
	if check == nil {
		t.Fatal("Check accepts chan int; the scenario needs a type JSON does not support")
	}
	out, code := runScenario(t, "remote-codec")
	if code == 0 {
		t.Fatalf("child exited 0, want a failure\n%s", out)
	}
	if want := "lockstep: sim: Options.Remote: " + check.Error(); !strings.Contains(out, want) {
		t.Errorf("output lacks %q:\n%s", want, out)
	}
	if strings.Contains(out, bodyRan) || strings.Contains(out, "seed=0x") {
		t.Errorf("a seed ran:\n%s", out)
	}
}

// GetSeq asks for a Seq.
type GetSeq struct{ ReplyTo lockstep.ReplyTo[Seq] }

// withoutSeq returns a registry with GetSeq and without Seq.
func withoutSeq() *codec.Registry {
	r := codec.NewRegistry()
	codec.RegisterIn[GetSeq](r, "simtest.GetSeq")
	return r
}

// The scenarios of TestATCOD15UnknownWireName and TestATCOD16EncodeFailure: each runs one seed,
// which fails.
var (
	_ = scenario("cod15-msg", func(t *testing.T) {
		sim.Simulate(t, fullTrace(1), func(s *sim.Sim) {
			s.Node("a", noSetup)
			s.Node("b", func(sys *lockstep.System) {
				if err := remote.Enable(sys, remote.Config{Registry: withoutSeq()}); err != nil {
					panic(err)
				}
				spawn(sys, "seq", &actor[Seq]{})
			})
			s.World().RunFor(time.Millisecond)
			if err := sim.Send(s, "a", lockstep.PIDAt[Seq]("b", "/user/seq"), Seq{N: 1}); err != nil {
				s.World().T().Fatal(err)
			}
			s.World().RunFor(time.Second)
		})
	})
	_ = scenario("cod15-reply", func(t *testing.T) {
		sim.Simulate(t, fullTrace(1), func(s *sim.Sim) {
			s.Node("a", func(sys *lockstep.System) {
				if err := remote.Enable(sys, remote.Config{Registry: withoutSeq()}); err != nil {
					panic(err)
				}
			})
			s.Node("b", func(sys *lockstep.System) {
				spawn(sys, "seqs", &actor[GetSeq]{receive: func(ctx *lockstep.Context[GetSeq], m GetSeq) { m.ReplyTo.Send(ctx, Seq{N: 7}) }})
			})
			s.World().RunFor(time.Millisecond)
			lockstep.Ask(s.System("a"), lockstep.PIDAt[GetSeq]("b", "/user/seqs"), func(rt lockstep.ReplyTo[Seq]) GetSeq { return GetSeq{ReplyTo: rt} })
			s.World().RunFor(time.Second)
		})
	})
	_ = scenario("cod16-reply", func(t *testing.T) {
		sim.Simulate(t, fullTrace(1), func(s *sim.Sim) {
			s.Node("a", noSetup)
			s.Node("b", func(sys *lockstep.System) {
				spawn(sys, "nan", &actor[GetRate]{receive: func(ctx *lockstep.Context[GetRate], m GetRate) { m.ReplyTo.Send(ctx, math.NaN()) }})
			})
			s.World().RunFor(time.Millisecond)
			connect(s, 300*time.Millisecond, "a", "b")
			lockstep.Ask(s.System("a"), lockstep.PIDAt[GetRate]("b", "/user/nan"), func(rt lockstep.ReplyTo[float64]) GetRate { return GetRate{ReplyTo: rt} })
			s.World().RunFor(time.Second)
		})
	})
)

// AT-COD-15 (defined here), COD-070, REM-230, REM-340: a frame whose wire name the receiving
// node's registry does not hold. b, whose registry lacks simtest.Seq, discards a's MSG of a Seq
// as decode; a, whose registry lacks it, discards b's REPLY of a Seq as decode and the pending
// ask fails with that error. Both fail the seed with check lockstep/misuse and a message naming
// the node, the kind, the peer, the wire name, and the registry's error.
func TestATCOD15UnknownWireName(t *testing.T) {
	cause := `codec: type not registered: wire name "simtest.Seq"`
	t.Run("MSG", func(t *testing.T) {
		recs := decodeSeed(t, "cod15-msg", `lockstep: remote: b: MSG from a: "simtest.Seq": `+cause)
		if got, want := discards(recs), []string{"discard MSG decode"}; !slices.Equal(got, want) {
			t.Errorf("records %q, want %q", got, want)
		}
	})
	t.Run("REPLY", func(t *testing.T) {
		msg := `lockstep: remote: a: REPLY from b: "simtest.Seq": ` + cause
		recs := decodeSeed(t, "cod15-reply", msg)
		if got, want := discards(recs), []string{"discard REPLY decode"}; !slices.Equal(got, want) {
			t.Errorf("records %q, want %q", got, want)
		}
		replies := ofKind(recs, "lockstep.reply")
		if len(replies) != 1 || attr(replies[0], "outcome") != "resolved" || attr(replies[0], "err") != msg {
			t.Errorf("lockstep.reply records %v, want one resolving the ask with %q", replies, msg)
		}
	})
}

// decodeSeed runs scenario name and checks that it fails with check lockstep/misuse and message.
// It returns the failing seed's trace records.
func decodeSeed(t *testing.T, name, message string) []kernel.Record {
	t.Helper()
	a, _ := failingSeedArtifact(t, name)
	want := failure{Kind: "invariant", Check: sim.CheckMisuse, Signature: "invariant:" + sim.CheckMisuse, Message: message}
	f := a.Report.Failure
	if got := (failure{Kind: f.Kind, Check: f.Check, Signature: f.Signature, Message: f.Message}); got != want {
		t.Errorf("failure %+v, want %+v", got, want)
	}
	if a.Trace == nil {
		t.Fatal("the failing seed left no trace")
	}
	return a.Trace.Records
}

// GetRate asks for a float64.
type GetRate struct{ ReplyTo lockstep.ReplyTo[float64] }

// nanError is the JSON codec's error for a NaN float64, through Registry.Encode.
const nanError = "codec: encode lockstep.float64: json: unsupported value: NaN"

// AT-COD-16 (defined here), COD-060, REM-110, REM-266: a registered value that fails to encode.
// Sent, it makes Send return the encode error, and nothing leaves the node. As an answer, it is
// sent as a FAIL of code other with the encode error's text, so the ask does not wait for its
// timeout, and the seed fails with check lockstep/misuse.
func TestATCOD16EncodeFailure(t *testing.T) {
	t.Run("Send", func(t *testing.T) {
		sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
			w, tt := s.World(), s.World().T()
			s.Node("a", noSetup)
			s.Node("b", func(sys *lockstep.System) { spawn(sys, "f", &actor[float64]{}) })
			w.RunFor(time.Millisecond)
			before := len(w.Sim.Records())
			to := lockstep.PIDAt[float64]("b", "/user/f")
			err := sim.Send(s, "a", to, math.NaN())
			if want := "lockstep: send to " + to.String() + ": " + nanError; err == nil || err.Error() != want {
				tt.Errorf("Send of NaN = %v, want %q", err, want)
			}
			w.RunFor(time.Second)
			for _, r := range w.Sim.Records()[before:] {
				switch r.Kind {
				case "lockstep.remote.send", "net.send_raw", "lockstep.deadletter":
					tt.Errorf("record %s %v after the failed Send, want none", r.Kind, r.Attrs)
				}
			}
		})
	})
	t.Run("Reply", func(t *testing.T) {
		a, _ := failingSeedArtifact(t, "cod16-reply")
		f := a.Report.Failure
		want := failure{
			Kind: "invariant", Check: sim.CheckMisuse, Signature: "invariant:" + sim.CheckMisuse,
			Message: "lockstep: remote: b: REPLY to a: lockstep.float64: " + nanError,
		}
		if got := (failure{Kind: f.Kind, Check: f.Check, Signature: f.Signature, Message: f.Message}); got != want {
			t.Errorf("failure %+v, want %+v", got, want)
		}
		if a.Trace == nil {
			t.Fatal("the failing seed left no trace")
		}
		var answers []string
		for _, r := range ofKind(a.Trace.Records, "lockstep.remote.send") {
			if k := attr(r, "kind"); k == "REPLY" || k == "FAIL" {
				answers = append(answers, k+" "+attr(r, "from"))
			}
		}
		if len(answers) != 1 || !strings.HasPrefix(answers[0], "FAIL lockstep://b/user/nan#") {
			t.Errorf("answers sent %q, want one FAIL from b's actor", answers)
		}
	})
}

// Carry holds identity values: Bank, a PID with its uid; Reply, an ask's ReplyTo; Seen, a map
// keyed by Address; None, the zero PID.
type Carry struct {
	Bank  lockstep.PID[BankMsg]
	Reply lockstep.ReplyTo[int]
	Seen  map[lockstep.Address]int
	None  lockstep.PID[BankMsg]
}

// AT-COD-19 (defined here), COD-120, COD-130: identity values decoded on another node are the
// values that were sent, and they work there. a sends the relay on b a Carry with the PID of c's
// bank, the ReplyTo of an ask of a, a map keyed by the bank's address and a zero PID; the relay
// receives values equal to the sent ones, deposits through the decoded PID, which reaches c's
// bank, and answers through the decoded ReplyTo, which resolves a's ask.
func TestATCOD19DecodedIdentities(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		s.Node("a", noSetup)
		var got []Carry
		s.Node("b", func(sys *lockstep.System) {
			spawn(sys, "relay", &actor[Carry]{receive: func(ctx *lockstep.Context[Carry], m Carry) {
				got = append(got, m)
				if err := lockstep.Send(ctx, m.Bank, BankMsg(Deposit{N: 9})); err != nil {
					panic(err)
				}
				m.Reply.Send(ctx, 42)
			}})
		})
		var cBank lockstep.PID[BankMsg]
		s.Node("c", func(sys *lockstep.System) { cBank = spawn(sys, "bank", bank()) })
		w.RunFor(time.Millisecond)

		rt, r := lockstep.NewReply[int](s.System("a"))
		sent := Carry{Bank: cBank, Reply: rt, Seen: map[lockstep.Address]int{cBank.Address(): 1}}
		if err := sim.Send(s, "a", lockstep.PIDAt[Carry]("b", "/user/relay"), sent); err != nil {
			tt.Fatal(err)
		}
		if v, err := await(s, r); v != 42 || err != nil {
			tt.Errorf("the ask answered through the decoded ReplyTo = %d, %v; want 42, nil", v, err)
		}
		if len(got) != 1 || !reflect.DeepEqual(got[0], sent) {
			tt.Fatalf("the relay received %+v, want %+v", got, sent)
		}
		if v, err := sim.Ask(s, "a", cBank, getBalance("")); v != 9 || err != nil {
			tt.Errorf("c's balance = %d, %v; want 9, nil: the decoded PID reached it", v, err)
		}
	})
}
