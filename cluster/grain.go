// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package cluster

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/hmdsefi/lockstep"
)

// Grain addresses one grain: a virtual actor named by its kind and its key, which exists whenever
// a message is sent to it. The grain's owner is the up member that hosts the kind and has the
// highest Score for the key (see Owner); the owner's host for the kind activates the grain on the
// first message, by calling the kind's factory with the key, and delivers every message to that
// activation. A Grain is a small value that holds no state of the grain, so it can be built
// anywhere with GrainOf; M is the kind's message type. The zero Grain is not usable: its Send
// returns ErrNotJoined.
//
// While the owner of a grain does not change and nothing is dropped, messages from one sender
// reach the activation in the order they were sent. A message that a host cannot hold, deliver
// or forward is dropped and traced; Send does not report it.
type Grain[M any] struct {
	m    *Membership
	kind string
	key  string
}

// GrainOf returns the grain of kind and key in the cluster of m. It checks nothing: Send and Ask
// report a problem with the grain. GrainOf(nil, kind, key) returns a grain whose Send returns
// ErrNotJoined, as the zero Grain's does.
func GrainOf[M any](m *Membership, kind, key string) Grain[M] {
	return Grain[M]{m: m, kind: kind, key: key}
}

// Kind returns the grain's kind.
func (g Grain[M]) Kind() string { return g.kind }

// Key returns the grain's key.
func (g Grain[M]) Key() string { return g.key }

// Send sends msg to the grain, from from, and returns once the message is on its way to the
// grain's owner. It returns the first error of: ErrNotJoined for the zero Grain; an error wrapping
// ErrInvalidKey for a key that is not 1 to 256 bytes long; one wrapping ErrLeft once Leave was
// called or the node left; one wrapping ErrNotJoined before the node has joined; one wrapping
// lockstep.ErrWrongType when this node hosts the kind with a message type other than M; one
// wrapping ErrNoOwner when no up member of the latest View hosts the kind. Otherwise it returns the
// error of the send, wrapped, for example lockstep.ErrMailboxFull from this node's host. These
// errors start with "cluster: send <kind>/<key>: ", where the key is escaped as the name of the
// grain's activation is: bytes other than A-Z, a-z, 0-9, '-', '.' and '_' are written as %XX.
//
// When this node owns the grain, msg goes to this node's host as it is. When another node owns it,
// Send encodes msg with Config.Registry and Config.Codec and sends it to that node's host, which
// decodes it; an encoding error is returned wrapped, as "cluster: encode <kind>/<key>: <err>".
func (g Grain[M]) Send(from lockstep.Sender, msg M) error {
	owner, i, err := g.target()
	if err != nil {
		return err
	}
	return g.send(from, owner, i, msg)
}

// Ask sends the grain g a request and returns the reply, as lockstep.Ask does for an actor: it
// creates the reply with lockstep.NewReply and opts, calls build once with the reply's
// lockstep.ReplyTo to make the request and sends it as Grain.Send does. When an option is invalid
// it returns the reply NewReply resolved, and when Grain.Send would return an error it returns the
// reply already failed with that error; build is not called when a check before the send fails.
// A request that a host drops later fails nothing: the reply resolves at its timeout. If build
// panics, the reply fails with an error that starts with "cluster: send <kind>/<key>: " before the
// panic propagates. Ask panics if build is nil, and as NewReply does.
func Ask[R, M any](from lockstep.Sender, g Grain[M], build func(lockstep.ReplyTo[R]) M,
	opts ...lockstep.AskOption) *lockstep.Reply[R] {
	if build == nil {
		panic("cluster: Ask with nil build function")
	}
	rt, r := lockstep.NewReply[R](from, opts...)
	if rt == (lockstep.ReplyTo[R]{}) {
		return r // resolved by NewReply: an invalid option or a stopped system
	}
	owner, i, err := g.target()
	if err == nil {
		err = g.send(from, owner, i, buildRequest(from, g, rt, build))
	}
	if err != nil {
		rt.Fail(from, err) // before Ask returns (VAC-210)
	}
	return r
}

// errBuildPanicked fails the reply of an Ask whose build panicked.
var errBuildPanicked = errors.New("cluster: ask: build panicked")

// buildRequest calls build with rt and returns the request. If build panics, it fails rt before
// the panic propagates, so that the reply does not wait for its timeout.
func buildRequest[R, M any](from lockstep.Sender, g Grain[M], rt lockstep.ReplyTo[R], build func(lockstep.ReplyTo[R]) M) M {
	built := false
	defer func() {
		if !built {
			rt.Fail(from, g.wrap(errBuildPanicked))
		}
	}()
	msg := build(rt)
	built = true
	return msg
}

// target runs Send's checks in their order (VAC-200) and returns the grain's owner and the index
// in Config.Kinds of the kind, or -1 when this node does not host it. When the owner is this node,
// the index is not -1: this node hosts the kind. The owner lookup allocates nothing (VAC-110).
func (g Grain[M]) target() (string, int, error) {
	m := g.m
	if m == nil {
		return "", -1, ErrNotJoined
	}
	if !validKey(g.key) {
		return "", -1, g.wrap(ErrInvalidKey)
	}
	if m.leaving.Load() || m.left.Load() {
		return "", -1, g.wrap(ErrLeft)
	}
	// joined before the view: the view is then the one of the turn that joined, or a later one.
	if !m.joined.Load() {
		return "", -1, g.wrap(ErrNotJoined)
	}
	i := m.kindIndex(g.kind)
	if want := reflect.TypeFor[M](); i >= 0 && m.cfg.Kinds[i].typ != want {
		return "", -1, g.wrap(fmt.Errorf("%w: kind %q takes %v, not %v", lockstep.ErrWrongType, g.kind,
			m.cfg.Kinds[i].typ, want))
	}
	owner, ok := m.View().place.owner(g.kind, g.key)
	if !ok {
		return "", -1, g.wrap(ErrNoOwner)
	}
	return owner, i, nil
}

// send sends msg to the grain's owner (VAC-200): by reference to the local host of Config.Kinds[i]
// when the owner is this node, else encoded, as a grainEnvelope that has made no hops, to the host
// of the kind on the owner's node.
func (g Grain[M]) send(from lockstep.Sender, owner string, i int, msg M) error {
	m := g.m
	if owner == m.self.Node && i >= 0 { // i >= 0 always holds when this node owns the grain
		if err := lockstep.Send(from, m.hostPIDs[i], hostMsg(localGrain{Key: g.key, Msg: msg})); err != nil {
			return g.wrap(err)
		}
		return nil
	}
	key := escapeKey(g.key)
	name, data, err := m.cfg.Registry.Encode(m.cfg.Codec, msg)
	if err != nil {
		return fmt.Errorf("cluster: encode %s/%s: %w", g.kind, key, err)
	}
	env := grainEnvelope{Kind: g.kind, Key: key, Name: name, Data: data}
	if err := lockstep.Send(from, hostAt(owner, g.kind), hostMsg(env)); err != nil {
		return g.wrap(err)
	}
	return nil
}

// wrap returns err wrapped as "cluster: send <kind>/<escaped key>: <err>".
func (g Grain[M]) wrap(err error) error {
	return fmt.Errorf("cluster: send %s/%s: %w", g.kind, escapeKey(g.key), err)
}
