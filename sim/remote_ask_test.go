// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/faultline/kernel/fault"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/codec"
	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/hooks"
	"github.com/hmdsefi/lockstep/internal/remoting"
	"github.com/hmdsefi/lockstep/remote"
	"github.com/hmdsefi/lockstep/sim"
)

// The cross-node tests of this file and of remote_watch_test.go and codec_test.go run on the
// nodes a, b and c (REM §10). Their messages are registered in codec_register_test.go.

// BankMsg is a message of the bank actor: Deposit or GetBalance.
type BankMsg interface{ bankMsg() }

func (Deposit) bankMsg()    {}
func (GetBalance) bankMsg() {}

// GetBalance asks for the balance of Account; "" is the bank's own account.
type GetBalance struct {
	Account string
	ReplyTo lockstep.ReplyTo[int]
}

// Seq is a numbered message.
type Seq struct{ N int }

// Forward asks its receiver to ask Bank for the balance of Account, with ReplyTo as the
// GetBalance's ReplyTo: the answer goes straight to the original asker.
type Forward struct {
	Bank    lockstep.PID[BankMsg]
	Account string
	ReplyTo lockstep.ReplyTo[int]
}

// errFrozen is the failure of a GetBalance of the account "frozen": it matches no lockstep error.
var errFrozen = errors.New("bank: account frozen")

// bank returns an actor that adds every Deposit to its balance and answers GetBalance: with the
// balance for its own account, errFrozen for "frozen", an error wrapping lockstep.ErrNotFound for
// any other account, and nothing at all for "mute".
func bank() *actor[BankMsg] {
	balance := 0
	return &actor[BankMsg]{receive: func(ctx *lockstep.Context[BankMsg], m BankMsg) {
		switch m := m.(type) {
		case Deposit:
			balance += m.N
		case GetBalance:
			switch m.Account {
			case "":
				m.ReplyTo.Send(ctx, balance)
			case "frozen":
				m.ReplyTo.Fail(ctx, errFrozen)
			case "mute":
			default:
				m.ReplyTo.Fail(ctx, fmt.Errorf("bank: no account %q: %w", m.Account, lockstep.ErrNotFound))
			}
		}
	}}
}

// getBalance returns the build function of a GetBalance of account.
func getBalance(account string) func(lockstep.ReplyTo[int]) BankMsg {
	return func(rt lockstep.ReplyTo[int]) BankMsg { return GetBalance{Account: account, ReplyTo: rt} }
}

// remoteErr checks that err is a *remote.Error from node with code and message, which unwraps to
// is (nil for none). It reports the mismatch to tt.
func remoteErr(tt testing.TB, what string, err error, node string, code remote.Code, message string, is error) {
	tt.Helper()
	var re *remote.Error
	if !errors.As(err, &re) {
		tt.Errorf("%s: error %v (%T), want a *remote.Error", what, err, err)
		return
	}
	if re.Node != node || re.Code != code || re.Message != message {
		tt.Errorf("%s: %+v, want node %s, code %s, message %q", what, *re, node, code, message)
	}
	if got := errors.Unwrap(re); got != is { //nolint:errorlint // the sentinel itself
		tt.Errorf("%s: unwraps to %v, want %v", what, got, is)
	}
}

// AT-REM-10 (defined here: the spec gives it one line), REM-160, REM-260, REM-262, REM-264:
// asks across nodes. An ask from a to the bank on b is answered with the balance; a Fail on b
// reaches a as a *remote.Error from b with the failure's code and text (not-found, unwrapping to
// lockstep.ErrNotFound, and other, unwrapping to nothing); an ask b never answers fails with
// lockstep.ErrTimeout exactly at its timeout. No seed fails.
func TestATREM10AskAcrossNodes(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		s.Node("a", noSetup)
		s.Node("b", func(sys *lockstep.System) { spawn(sys, "bank", bank()) })
		w.RunFor(time.Millisecond)
		to := lockstep.PIDAt[BankMsg]("b", "/user/bank")
		if err := sim.Send(s, "a", to, BankMsg(Deposit{N: 40})); err != nil {
			tt.Fatalf("Send: %v", err)
		}
		if v, err := sim.Ask(s, "a", to, getBalance("")); v != 40 || err != nil {
			tt.Errorf("balance = %d, %v; want 40, nil", v, err)
		}

		_, err := sim.Ask(s, "a", to, getBalance("x"))
		remoteErr(tt, "the ask of a missing account", err, "b", remote.CodeNotFound, `bank: no account "x": lockstep: not found`, lockstep.ErrNotFound)
		if want := `remote: b: bank: no account "x": lockstep: not found`; err == nil || err.Error() != want {
			tt.Errorf("error text %v, want %q", err, want)
		}
		_, err = sim.Ask(s, "a", to, getBalance("frozen"))
		remoteErr(tt, "the ask of the frozen account", err, "b", remote.CodeOther, errFrozen.Error(), nil)

		start := w.Sim.Now()
		_, err = sim.Ask(s, "a", to, getBalance("mute"), lockstep.WithTimeout(250*time.Millisecond))
		if !errors.Is(err, lockstep.ErrTimeout) {
			tt.Errorf("the unanswered ask: %v, want ErrTimeout", err)
		}
		if d := w.Sim.Now().Sub(start); d != 250*time.Millisecond {
			tt.Errorf("the unanswered ask failed after %v, want 250ms", d)
		}
		if err := s.Stored(sim.CheckMisuse); err != nil {
			tt.Errorf("lockstep/misuse holds %v, want nothing", err)
		}
	})
}

// AT-REM-11 (defined here), REM-160, MSG-350: an ask to an actor that is not alive on another node
// fails with an error matching lockstep.ErrTerminated: the runtime of b makes the request a dead
// letter and fails it back as a FAIL of code terminated. Both an unregistered path and the uid of
// a stopped actor whose path is in use again fail so.
func TestATREM11AskMissingActor(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		s.Node("a", noSetup)
		var first lockstep.PID[BankMsg]
		s.Node("b", func(sys *lockstep.System) { first = spawn(sys, "bank", bank()) })
		w.RunFor(time.Millisecond)

		none := lockstep.PIDAt[BankMsg]("b", "/user/none")
		_, err := sim.Ask(s, "a", none, getBalance(""))
		remoteErr(tt, "the ask of a missing path", err, "b", remote.CodeTerminated, "lockstep: actor terminated: "+none.String(), lockstep.ErrTerminated)

		if err := lockstep.Stop(s.System("b"), first.Address()); err != nil {
			tt.Fatal(err)
		}
		w.RunFor(time.Millisecond)
		second := spawn(s.System("b"), "bank", bank())
		if second == first {
			tt.Fatal("the new bank has the old uid")
		}
		_, err = sim.Ask(s, "a", first, getBalance(""))
		remoteErr(tt, "the ask of the stopped bank's uid", err, "b", remote.CodeTerminated, "lockstep: actor terminated: "+first.String(), lockstep.ErrTerminated)
		if v, err := sim.Ask(s, "a", second, getBalance("")); v != 0 || err != nil {
			tt.Errorf("the ask of the new bank = %d, %v; want 0, nil", v, err)
		}
		var dead []string
		for _, r := range ofKind(w.Sim.Records(), "lockstep.deadletter") {
			dead = append(dead, nodeName(s, r.Node)+" "+attr(r, "to")+" "+attr(r, "reason"))
		}
		if want := []string{"b " + none.String() + " not-found", "b " + first.String() + " stopped"}; !slices.Equal(dead, want) {
			tt.Errorf("dead letters %q, want %q", dead, want)
		}
	})
}

// AT-REM-13 (defined here), REM-290: lookups across nodes, through lookup below. A lookup of the
// bank on b with its message type returns its PID, uid included, which a can ask; a lookup of a
// path nobody holds fails with a *remote.Error of code not-found, and one with another message
// type with code wrong-type; a lookup of the paused b fails with lockstep.ErrTimeout at its
// timeout, and the answer b sends when it resumes is a late reply that fails nothing. A lookup on a's own node is lockstep.Lookup.
func TestATREM13Lookup(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		s.Node("a", noSetup)
		var bankPID lockstep.PID[BankMsg]
		s.Node("b", func(sys *lockstep.System) { bankPID = spawn(sys, "bank", bank()) })
		w.RunFor(time.Millisecond)

		pid, err := lookup[BankMsg](s, "a", "b", "/user/bank")
		if err != nil || pid != bankPID {
			tt.Fatalf("lookup of the bank = %v, %v; want %v, nil", pid, err, bankPID)
		}
		if v, err := sim.Ask(s, "a", pid, getBalance("")); v != 0 || err != nil {
			tt.Errorf("ask through the looked-up PID = %d, %v; want 0, nil", v, err)
		}
		_, err = lookup[BankMsg](s, "a", "b", "/user/none")
		remoteErr(tt, "the lookup of a missing path", err, "b", remote.CodeNotFound, `lockstep: not found: path "/user/none"`, lockstep.ErrNotFound)
		_, err = lookup[Seq](s, "a", "b", "/user/bank")
		var re *remote.Error
		if !errors.As(err, &re) || re.Node != "b" || re.Code != remote.CodeWrongType || !errors.Is(err, lockstep.ErrWrongType) {
			tt.Errorf("the lookup with another type: %v, want a *remote.Error from b of code wrong-type", err)
		}

		w.Faults.Inject(fault.Event{Kind: fault.KindPause, Node: "b"})
		start := w.Sim.Now()
		_, err = lookup[BankMsg](s, "a", "b", "/user/bank", lockstep.WithTimeout(300*time.Millisecond))
		if !errors.Is(err, lockstep.ErrTimeout) || w.Sim.Now().Sub(start) != 300*time.Millisecond {
			tt.Errorf("the lookup of the paused node: %v after %v, want ErrTimeout after 300ms", err, w.Sim.Now().Sub(start))
		}
		resumed := len(w.Sim.Records())
		w.Faults.Inject(fault.Event{Kind: fault.KindResume, Node: "b"})
		w.RunFor(5 * time.Second)
		var late []string
		for _, r := range ofKind(w.Sim.Records()[resumed:], "lockstep.reply") {
			late = append(late, nodeName(s, r.Node)+" "+attr(r, "from")+" "+attr(r, "outcome"))
		}
		if want := []string{"a " + bankPID.String() + " late"}; !slices.Equal(late, want) {
			tt.Errorf("reply records after b resumed %q, want %q: b's answer is a late reply", late, want)
		}
		if err := s.Stored(sim.CheckMisuse); err != nil {
			tt.Errorf("lockstep/misuse holds %v, want nothing", err)
		}

		local, err := lookup[BankMsg](s, "b", "b", "/user/bank")
		if err != nil || local != bankPID {
			tt.Errorf("lookup on b's own node = %v, %v; want %v", local, err, bankPID)
		}
	})
}

// lookup is remote.Lookup[M] from the System of node for the simulation's test body. Like
// remote.Lookup it sends the LOOKUP through the system's remoting core with M's type key and a
// lockstep.NewReply as the answer's address, and builds the PID from the answered address's text
// form; but instead of Reply.Await, which fails the seed on a pending reply in simulation, it runs
// the world until the reply resolves, as sim.Ask does. On node's own node it is lockstep.Lookup.
func lookup[M any](s *sim.Sim, node, target, path string, opts ...lockstep.AskOption) (lockstep.PID[M], error) {
	sys := s.System(node)
	if target == node {
		return lockstep.Lookup[M](sys, path)
	}
	rt, r := lockstep.NewReply[lockstep.Address](sys, opts...)
	text, err := rt.MarshalText()
	if err != nil {
		panic(err)
	}
	var reply lockstep.Address
	if err := reply.UnmarshalText([]byte("lockstep://" + strings.TrimPrefix(string(text), "lockstep-reply://"))); err != nil {
		panic(err)
	}
	t := reflect.TypeFor[M]()
	key := t.String()
	if t.PkgPath() != "" {
		key = t.PkgPath() + "." + t.Name()
	}
	addr := engine.Address{Node: reply.Node(), Path: reply.Path(), UID: reply.UID()}
	if err := remoting.CoreOf(sys).Lookup(addr, target, path, key); err != nil {
		rt.Fail(sys, err)
	}
	a, err := await(s, r)
	if err != nil {
		return lockstep.PID[M]{}, err
	}
	var pid lockstep.PID[M]
	b, _ := a.MarshalText()
	if err := pid.UnmarshalText(b); err != nil {
		return lockstep.PID[M]{}, err
	}
	return pid, nil
}

// await runs the world event by event until r resolves, then returns its result, as sim.Ask does
// for its own reply. The run must not end first.
func await[R any](s *sim.Sim, r *lockstep.Reply[R]) (R, error) {
	w := s.World()
	for !hooks.AskResolved(r) {
		next, ok := w.Sim.NextAt()
		if !ok {
			w.T().Fatal("the reply is pending and no event is left")
		}
		w.RunFor(max(0, next.Sub(w.Sim.Now())))
	}
	return r.Await(context.Background()) // resolved: returns at once
}

// AT-REM-34 (defined here), ACT-410, REM-210: path PIDs (lockstep.PIDAt) work across nodes. a
// sends to and asks the bank on b, and the bank on c, through PIDs built from node and path
// alone; after b's bank stops and a new one starts at the same path, the same path PID reaches
// the new one, while a send to the old uid is a dead letter on b. a's remote send records name the
// path PID, uid 0, as the recipient of the messages sent through it.
func TestATREM34PathPIDs(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		s.Node("a", noSetup)
		var first lockstep.PID[BankMsg]
		s.Node("b", func(sys *lockstep.System) { first = spawn(sys, "bank", bank()) })
		s.Node("c", func(sys *lockstep.System) { spawn(sys, "bank", bank()) })
		w.RunFor(time.Millisecond)
		atB, atC := lockstep.PIDAt[BankMsg]("b", "/user/bank"), lockstep.PIDAt[BankMsg]("c", "/user/bank")

		for i, to := range []lockstep.PID[BankMsg]{atB, atC} {
			if err := sim.Send(s, "a", to, BankMsg(Deposit{N: 10 * (i + 1)})); err != nil {
				tt.Fatalf("Send to %v: %v", to, err)
			}
		}
		if v, err := sim.Ask(s, "a", atB, getBalance("")); v != 10 || err != nil {
			tt.Errorf("balance on b = %d, %v; want 10, nil", v, err)
		}
		if v, err := sim.Ask(s, "a", atC, getBalance("")); v != 20 || err != nil {
			tt.Errorf("balance on c = %d, %v; want 20, nil", v, err)
		}

		if err := lockstep.Stop(s.System("b"), first.Address()); err != nil {
			tt.Fatal(err)
		}
		w.RunFor(time.Millisecond)
		second := spawn(s.System("b"), "bank", bank())
		if err := sim.Send(s, "a", first, BankMsg(Deposit{N: 1})); err != nil {
			tt.Fatalf("Send to the old uid: %v", err)
		}
		if err := sim.Send(s, "a", atB, BankMsg(Deposit{N: 5})); err != nil {
			tt.Fatal(err)
		}
		if v, err := sim.Ask(s, "a", atB, getBalance("")); v != 5 || err != nil {
			tt.Errorf("balance of the new bank through the path PID = %d, %v; want 5, nil", v, err)
		}
		if v, err := sim.Ask(s, "a", second, getBalance("")); v != 5 || err != nil {
			tt.Errorf("balance of the new bank through its own PID = %d, %v; want 5, nil", v, err)
		}

		recs := w.Sim.Records()
		var dead []string
		for _, r := range ofKind(recs, "lockstep.deadletter") {
			dead = append(dead, nodeName(s, r.Node)+" "+attr(r, "to")+" "+attr(r, "reason"))
		}
		if want := []string{"b " + first.String() + " stopped"}; !slices.Equal(dead, want) {
			tt.Errorf("dead letters %q, want %q", dead, want)
		}
		var tos []string // to b, in order
		for _, r := range ofKind(recs, "lockstep.remote.send") {
			if to := attr(r, "to"); nodeName(s, r.Node) == "a" && attr(r, "kind") == "MSG" && strings.HasPrefix(to, "lockstep://b/") {
				tos = append(tos, to)
			}
		}
		if want := []string{atB.String(), first.String(), atB.String()}; !slices.Equal(tos, want) {
			tt.Errorf("a's MSG recipients on b %q, want %q", tos, want)
		}
	})
}

// AT-REM-35 (defined here), COD-130, MSG-400: PIDs and ReplyTos inside messages work across
// nodes. a asks the broker on b with a Forward that carries the PID of the bank on c, uid
// included, and the ask's ReplyTo; the broker passes the ReplyTo on to c's bank in a GetBalance,
// and c answers a directly: the REPLY travels from c to a, never through b, and a's reply record
// names c's bank as the replier. A failure from c reaches a as a *remote.Error from c.
func TestATREM35IdentitiesInMessages(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		s.Node("a", noSetup)
		s.Node("b", func(sys *lockstep.System) { spawn(sys, "broker", broker()) })
		var cBank lockstep.PID[BankMsg]
		s.Node("c", func(sys *lockstep.System) { cBank = spawn(sys, "bank", bank()) })
		w.RunFor(time.Millisecond)
		if err := sim.Send(s, "a", cBank, BankMsg(Deposit{N: 7})); err != nil {
			tt.Fatal(err)
		}
		brk := lockstep.PIDAt[Forward]("b", "/user/broker")
		forward := func(account string) func(lockstep.ReplyTo[int]) Forward {
			return func(rt lockstep.ReplyTo[int]) Forward { return Forward{Bank: cBank, Account: account, ReplyTo: rt} }
		}
		if v, err := sim.Ask(s, "a", brk, forward("")); v != 7 || err != nil {
			tt.Errorf("the forwarded ask = %d, %v; want 7, nil", v, err)
		}
		_, err := sim.Ask(s, "a", brk, forward("frozen"))
		remoteErr(tt, "the forwarded failure", err, "c", remote.CodeOther, errFrozen.Error(), nil)

		recs := w.Sim.Records()
		var answers []string
		for _, r := range ofKind(recs, "lockstep.remote.send") {
			if k := attr(r, "kind"); k == "REPLY" || k == "FAIL" {
				answers = append(answers, nodeName(s, r.Node)+" "+k+" "+attr(r, "from")+" "+strings.SplitN(attr(r, "to"), "/temp/", 2)[0])
			}
		}
		want := []string{"c REPLY " + cBank.String() + " lockstep://a", "c FAIL " + cBank.String() + " lockstep://a"}
		if !slices.Equal(answers, want) {
			tt.Errorf("answers sent %q, want %q", answers, want)
		}
		var froms []string
		for _, r := range ofKind(recs, "lockstep.reply") {
			if nodeName(s, r.Node) == "a" {
				froms = append(froms, attr(r, "from")+" "+attr(r, "outcome"))
			}
		}
		if len(froms) != 2 || !strings.HasPrefix(froms[0], cBank.String()+" ") || !strings.HasPrefix(froms[1], cBank.String()+" ") {
			tt.Errorf("a's reply records %q, want two from %v", froms, cBank)
		}
	})
}

// broker passes each Forward on to its Bank as a GetBalance with the Forward's ReplyTo.
func broker() *actor[Forward] {
	return &actor[Forward]{receive: func(ctx *lockstep.Context[Forward], m Forward) {
		if err := lockstep.Send(ctx, m.Bank, BankMsg(GetBalance{Account: m.Account, ReplyTo: m.ReplyTo})); err != nil {
			panic(err)
		}
	}}
}

// Unregistered is a message type that is never registered.
type Unregistered struct{ N int }

// AT-REM-41 (defined here), REM-110, COD-060: a value of an unregistered type cannot cross nodes.
// Send returns the encode error, wrapped by the runtime and matching codec.ErrNotRegistered, and
// nothing leaves a: no remote send record, no datagram, no dead letter. An Ask with such a request
// resolves at once with the same error. A later registered message still arrives.
func TestATREM41UnregisteredType(t *testing.T) {
	sim.Simulate(t, fullTrace(3), func(s *sim.Sim) {
		w, tt := s.World(), s.World().T()
		s.Node("a", noSetup)
		var got []Seq
		s.Node("b", func(sys *lockstep.System) {
			spawn(sys, "u", &actor[Unregistered]{})
			spawn(sys, "seq", &actor[Seq]{receive: func(_ *lockstep.Context[Seq], m Seq) { got = append(got, m) }})
		})
		w.RunFor(time.Millisecond)
		before := len(w.Sim.Records())

		to := lockstep.PIDAt[Unregistered]("b", "/user/u")
		want := "lockstep: send to " + to.String() + ": codec: type not registered: sim_test.Unregistered"
		err := sim.Send(s, "a", to, Unregistered{N: 1})
		if !errors.Is(err, codec.ErrNotRegistered) || err.Error() != want {
			tt.Errorf("Send = %v, want %q", err, want)
		}
		_, err = sim.Ask(s, "a", to, func(lockstep.ReplyTo[int]) Unregistered { return Unregistered{N: 2} })
		if !errors.Is(err, codec.ErrNotRegistered) || err.Error() != want {
			tt.Errorf("Ask = %v, want %q", err, want)
		}
		w.RunFor(time.Second)
		for _, r := range w.Sim.Records()[before:] {
			switch r.Kind {
			case "lockstep.remote.send", "net.send_raw", "net.send", "lockstep.deadletter", "lockstep.remote.dial":
				tt.Errorf("record %s %v after the failed Send, want none", r.Kind, r.Attrs)
			}
		}

		if err := sim.Send(s, "a", lockstep.PIDAt[Seq]("b", "/user/seq"), Seq{N: 2}); err != nil {
			tt.Fatal(err)
		}
		w.RunFor(time.Second)
		if !slices.Equal(got, []Seq{{N: 2}}) {
			tt.Errorf("seq got %v, want [{2}]", got)
		}
	})
}

// The scenarios of TestATREM27WrongTypeMessage and TestATREM28WrongTypeReply: each runs one seed,
// which fails.
var (
	_ = scenario("rem27-msg", func(t *testing.T) {
		sim.Simulate(t, fullTrace(1), func(s *sim.Sim) {
			wrongTypeNodes(s)
			if err := sim.Send(s, "a", lockstep.PIDAt[Deposit]("b", "/user/seq"), Deposit{N: 1}); err != nil {
				s.World().T().Fatal(err)
			}
			s.World().RunFor(time.Second)
		})
	})
	_ = scenario("rem27-ask", func(t *testing.T) {
		sim.Simulate(t, fullTrace(1), func(s *sim.Sim) {
			wrongTypeNodes(s)
			lockstep.Ask(s.System("a"), lockstep.PIDAt[BankMsg]("b", "/user/seq"), getBalance(""))
			s.World().RunFor(time.Second)
		})
	})
	_ = scenario("rem28-reply", func(t *testing.T) {
		sim.Simulate(t, fullTrace(1), func(s *sim.Sim) {
			wrongTypeNodes(s)
			lockstep.Ask(s.System("a"), lockstep.PIDAt[Ping]("b", "/user/liar"), buildPing(1))
			s.World().RunFor(time.Second)
		})
	})
)

// wrongTypeNodes boots a, and b with an actor taking Seq at /user/seq and a liar at /user/liar,
// and runs the world until both are up and connected both ways.
func wrongTypeNodes(s *sim.Sim) {
	s.Node("a", noSetup)
	s.Node("b", func(sys *lockstep.System) {
		spawn(sys, "seq", &actor[Seq]{receive: func(*lockstep.Context[Seq], Seq) { fmt.Println(seqReceived) }})
		spawn(sys, "liar", liar())
	})
	s.World().RunFor(time.Millisecond)
	// Both directions up, so that b writes the FAIL of a wrong-type request in the event that
	// fails the seed.
	connect(s, 300*time.Millisecond, "a", "b")
}

// seqReceived is what the actor at /user/seq of wrongTypeNodes prints for each message.
const seqReceived = "SEQ RECEIVED"

// liar answers each Ping with a string, through its ReplyTo converted to a ReplyTo[string] by the
// text form, which does not check the answer's type.
func liar() *actor[Ping] {
	return &actor[Ping]{receive: func(ctx *lockstep.Context[Ping], m Ping) {
		text, err := m.ReplyTo.MarshalText()
		if err != nil {
			panic(err)
		}
		var rt lockstep.ReplyTo[string]
		if err := rt.UnmarshalText(text); err != nil {
			panic(err)
		}
		rt.Send(ctx, "not an int")
	}}
}

// wrongTypeSeed runs scenario name and checks its failure: check lockstep/misuse, and a message
// that starts with prefix and holds lockstep.ErrWrongType's text. It returns the failing seed's
// trace records.
func wrongTypeSeed(t *testing.T, name, prefix string) []kernel.Record {
	t.Helper()
	a, out := failingSeedArtifact(t, name)
	f := a.Report.Failure
	if f.Kind != "invariant" || f.Check != sim.CheckMisuse || f.Signature != "invariant:"+sim.CheckMisuse ||
		!strings.HasPrefix(f.Message, prefix) || !strings.Contains(f.Message, lockstep.ErrWrongType.Error()) {
		t.Errorf("failure %+v, want invariant %s with a message starting %q and holding %q", f, sim.CheckMisuse, prefix, lockstep.ErrWrongType)
	}
	if strings.Contains(out, seqReceived) {
		t.Errorf("the actor taking Seq received a message:\n%s", out)
	}
	if a.Trace == nil {
		t.Fatal("the failing seed left no trace")
	}
	return a.Trace.Records
}

// discards returns the discard and dead-letter records among recs, in order, as
// "discard <kind> <reason>" and "deadletter <to> <reason>".
func discards(recs []kernel.Record) []string {
	var out []string
	for _, r := range recs {
		switch r.Kind {
		case "lockstep.remote.discard":
			out = append(out, "discard "+attr(r, "kind")+" "+attr(r, "reason"))
		case "lockstep.deadletter":
			out = append(out, "deadletter "+attr(r, "to")+" "+attr(r, "reason"))
		}
	}
	return out
}

// AT-REM-27 (defined here: the spec gives it one line), REM-230, REM-240, REM-340: a message or
// request whose value its live recipient does not take. b discards the frame wrong-type, makes it
// a dead letter wrong-type, fails a request back to its asker (a FAIL from the recipient to the
// ask address), and the seed fails with check lockstep/misuse and a message naming b, the frame's
// kind, a and the wire name. The recipient receives nothing.
func TestATREM27WrongTypeMessage(t *testing.T) {
	seq := lockstep.PIDAt[Seq]("b", "/user/seq").String()
	t.Run("MSG", func(t *testing.T) {
		recs := wrongTypeSeed(t, "rem27-msg", `lockstep: remote: b: MSG from a: "simtest.Deposit": `)
		if got, want := discards(recs), []string{"discard MSG wrong-type", "deadletter " + seq + " wrong-type"}; !slices.Equal(got, want) {
			t.Errorf("records %q, want %q", got, want)
		}
	})
	t.Run("ASK", func(t *testing.T) {
		recs := wrongTypeSeed(t, "rem27-ask", `lockstep: remote: b: ASK from a: "simtest.GetBalance": `)
		if got, want := discards(recs), []string{"discard ASK wrong-type", "deadletter " + seq + " wrong-type"}; !slices.Equal(got, want) {
			t.Errorf("records %q, want %q", got, want)
		}
		var fails []string
		for _, r := range ofKind(recs, "lockstep.remote.send") {
			if attr(r, "kind") == "FAIL" {
				fails = append(fails, attr(r, "from")+" "+strings.SplitN(attr(r, "to"), "/temp/", 2)[0])
			}
		}
		if want := []string{seq + " lockstep://a"}; !slices.Equal(fails, want) {
			t.Errorf("FAIL frames %q, want %q: the request fails back to its asker", fails, want)
		}
	})
}

// AT-REM-28 (defined here), REM-264, REM-268, REM-340: an answer that the pending ask's R does not
// take. b's liar answers a's ask for an int with a string; a discards the REPLY wrong-type, the
// ask resolves with an error wrapping lockstep.ErrWrongType (a's lockstep.reply record), and the
// seed fails with check lockstep/misuse and a message naming a, REPLY, b and the wire name.
func TestATREM28WrongTypeReply(t *testing.T) {
	prefix := `lockstep: remote: a: REPLY from b: "lockstep.string": `
	recs := wrongTypeSeed(t, "rem28-reply", prefix)
	if got, want := discards(recs), []string{"discard REPLY wrong-type"}; !slices.Equal(got, want) {
		t.Errorf("records %q, want %q", got, want)
	}
	replies := ofKind(recs, "lockstep.reply")
	if len(replies) != 1 || attr(replies[0], "outcome") != "resolved" || !strings.HasPrefix(attr(replies[0], "err"), prefix) ||
		!strings.HasPrefix(attr(replies[0], "from"), "lockstep://b/user/liar#") {
		t.Errorf("lockstep.reply records %v, want one from b's liar resolving the ask with the wrong-type error", replies)
	}
}
