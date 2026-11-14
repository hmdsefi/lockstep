// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package remote_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/remote"
)

// waitLog waits up to 10 s for a record of n with message msg whose attributes hold want, and
// returns the first one.
func waitLog(t *testing.T, n *node, msg string, want map[string]string) logRecord {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if recs := n.log.find(msg, want); len(recs) > 0 {
			return recs[0]
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s logged no %q record with %v", n.sys.Node(), msg, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// wantNotApplied checks that n logged "remoting: frame not applied" for a frame of kind whose
// error starts with prefix and holds lockstep.ErrWrongType's text: what fails a seed in
// simulation is a warning in production.
func wantNotApplied(t *testing.T, n *node, kind, prefix string) {
	t.Helper()
	rec := waitLog(t, n, "remoting: frame not applied", map[string]string{"kind": kind})
	if err := rec.attrs["err"]; !strings.HasPrefix(err, prefix) || !strings.Contains(err, lockstep.ErrWrongType.Error()) {
		t.Errorf("frame not applied with err %q, want one starting %q and holding %q", err, prefix, lockstep.ErrWrongType)
	}
}

// TestATREM27WrongTypeMessage is AT-REM-27's production variant over TCP: a message or request
// whose value the live recipient on b does not take is not delivered; b makes it a dead letter
// with reason wrong-type and logs it as a frame not applied, naming b, the kind, a and the wire
// name; a request fails back to its asker with an error matching lockstep.ErrWrongType, a
// *remote.Error from b of code wrong-type. A later message of the right type still arrives, first.
func TestATREM27WrongTypeMessage(t *testing.T) {
	reg := newRegistry()
	b := startNode(t, "b", reg, nil)
	a := startNode(t, "a", reg, func(c *remote.Config) { c.Peers = map[string]string{"b": b.addr} })
	seqs := make(chan Seq, 4)
	spawn[Seq](t, b.sys, "seq", forward[Seq]{seqs})
	at := lockstep.PIDAt[Seq]("b", "/user/seq")

	if err := lockstep.Send(a.sys, lockstep.PIDAt[Note]("b", "/user/seq"), Note{N: 1}); err != nil {
		t.Fatal(err)
	}
	wantNotApplied(t, b, "MSG", `lockstep: remote: b: MSG from a: "remotetest.Note": `)
	waitLog(t, b, "dead letter", map[string]string{"reason": "wrong-type", "to": at.String()})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := lockstep.Ask(a.sys, lockstep.PIDAt[Echo]("b", "/user/seq"), func(rt lockstep.ReplyTo[string]) Echo {
		return Echo{Text: "x", Reply: rt}
	}).Await(ctx)
	var re *remote.Error
	if !errors.As(err, &re) || re.Node != "b" || re.Code != remote.CodeWrongType || !errors.Is(err, lockstep.ErrWrongType) {
		t.Errorf("the wrong-type ask failed with %v, want a *remote.Error from b of code wrong-type", err)
	}
	wantNotApplied(t, b, "ASK", `lockstep: remote: b: ASK from a: "remotetest.Echo": `)
	if n := len(b.log.find("dead letter", map[string]string{"reason": "wrong-type"})); n != 2 {
		t.Errorf("%d wrong-type dead letters on b, want 2", n)
	}

	if err := lockstep.Send(a.sys, at, Seq{N: 2}); err != nil {
		t.Fatal(err)
	}
	if got := recv(t, seqs); got.N != 2 {
		t.Errorf("seq received %+v first, want {2}", got)
	}
}

// liar answers each Echo with an int, through its ReplyTo converted to a ReplyTo[int] by the text
// form, which does not check the answer's type.
type liar struct{}

func (liar) Receive(ctx *lockstep.Context[Echo], m Echo) {
	text, err := m.Reply.MarshalText()
	if err != nil {
		panic(err)
	}
	var rt lockstep.ReplyTo[int]
	if err := rt.UnmarshalText(text); err != nil {
		panic(err)
	}
	rt.Send(ctx, 5)
}

// TestATREM28WrongTypeReply is AT-REM-28's production variant over TCP: b's liar answers a's ask
// for a string with an int; the ask fails at once with an error matching lockstep.ErrWrongType
// that names a, REPLY, b and the wire name, and a logs the frame as not applied.
func TestATREM28WrongTypeReply(t *testing.T) {
	reg := newRegistry()
	b := startNode(t, "b", reg, nil)
	a := startNode(t, "a", reg, func(c *remote.Config) { c.Peers = map[string]string{"b": b.addr} })
	spawn[Echo](t, b.sys, "liar", liar{})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	prefix := `lockstep: remote: a: REPLY from b: "lockstep.int": `
	_, err := lockstep.Ask(a.sys, lockstep.PIDAt[Echo]("b", "/user/liar"), func(rt lockstep.ReplyTo[string]) Echo {
		return Echo{Text: "x", Reply: rt}
	}, lockstep.WithTimeout(time.Minute)).Await(ctx)
	if !errors.Is(err, lockstep.ErrWrongType) || !strings.HasPrefix(err.Error(), prefix) {
		t.Errorf("the ask failed with %v, want an error matching ErrWrongType starting %q", err, prefix)
	}
	wantNotApplied(t, a, "REPLY", prefix)
}
