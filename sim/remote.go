// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"

	"github.com/hmdsefi/faultline/kernel"
	"github.com/hmdsefi/faultline/kernel/simnet"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/remoting"
	"github.com/hmdsefi/lockstep/remote"
)

// The simulation's remoting driver (REM-300 to REM-345). Every boot of a lockstep node gets a
// remoting core as its system's transport (SIM-165), and the core runs over simnet datagrams: a
// datagram is an 8-byte big-endian connection ID followed by one frame, length prefix included,
// whose length field equals the rest (REM-310). Each node keeps its connections in its driver,
// one per boot; a crash discards the driver with the core (REM-335).

// sendLabel is the label of the node event that sends a datagram a paused node wrote, once the
// node resumes (SIM-140).
const sendLabel = "lockstep/send"

// Record kinds the driver emits itself (REM §8).
const (
	recordLoss    = "lockstep.remote.loss"
	recordDiscard = "lockstep.remote.discard"
)

// discardUnknownConnection is the discard reason of a datagram whose connection ID is unknown
// (REM-325).
const discardUnknownConnection = "unknown-connection"

// Connection IDs (REM-315): uint64(kernel incarnation)<<32 | k, where k counts the dials of one
// incarnation of the dialing node from 1 and stays below 1<<31. Bit 31 is the direction: clear on
// the datagrams the dialer writes, set on the ones the acceptor writes. A receiver keys a
// connection by the sending kernel node, the ID with bit 31 cleared and the direction, so the IDs
// of two nodes' dials to each other never collide.
const (
	dirBit  = 1 << 31
	maxDial = dirBit - 1 // the largest k
	idLen   = 8          // the connection ID's bytes at the start of a datagram
)

// datagram is the simnet payload that carries one frame between nodes: the connection ID, then
// the frame (REM-310). It is never changed after it is sent.
type datagram []byte

// Describe returns a short text for the trace: the connection ID in hexadecimal, the frame's
// kind and sequence number and the frame's size.
func (d datagram) Describe() string {
	if len(d) < idLen {
		return "datagram len=" + strconv.Itoa(len(d))
	}
	kind, seq := frameHead(d[idLen:])
	return "datagram id=0x" + strconv.FormatUint(binary.BigEndian.Uint64(d), 16) + " " + kind +
		" seq=" + strconv.FormatUint(seq, 10) + " bytes=" + strconv.Itoa(len(d)-idLen)
}

// The offsets of a frame's kind byte (after the 4-byte length and the version byte) and of its
// sequence number (after the flags).
const kindAt, seqAt = 5, 8

// frameKind returns the kind byte of the frame b starts with, and false when b is too short to
// hold one.
func frameKind(b []byte) (remoting.Kind, bool) {
	if len(b) <= kindAt {
		return 0, false
	}
	return remoting.Kind(b[kindAt]), true
}

// frameHead returns the kind name and sequence number of the frame b starts with, as far as b
// holds them: an empty kind and 0 when b is too short.
func frameHead(b []byte) (string, uint64) {
	kind := ""
	if k, ok := frameKind(b); ok {
		kind = k.String()
	}
	var seq uint64
	if len(b) >= seqAt+8 {
		seq = binary.BigEndian.Uint64(b[seqAt:])
	}
	return kind, seq
}

// outDatagram is a datagram that waits in the engine's queue of deferred sends, with its
// destination node and the connection number and frame size of its loss record.
type outDatagram struct {
	to    kernel.NodeID
	conn  uint64
	frame int
	dg    datagram
}

// linkKey identifies a connection at one node: the other node, the connection ID with bit 31
// cleared, and whether this node dialed it (REM-315).
type linkKey struct {
	peer kernel.NodeID
	id   uint64
	out  bool
}

// simLink is one connection of the driver, a remoting.Link.
type simLink struct {
	d      *driver
	key    linkKey
	closed bool
}

// ID returns the connection ID with bit 31 cleared, so both ends record the same number.
func (l *simLink) ID() uint64 { return l.key.id }

// Write sends frame as one datagram to the other node. It always takes the frame: a simulated
// link has no buffer of its own, and loss on the network is not reported (REM-320). A closed link
// sends nothing.
func (l *simLink) Write(frame []byte) bool {
	if !l.closed {
		id := l.key.id
		if !l.key.out {
			id |= dirBit
		}
		l.d.send(l.key.peer, id, frame)
	}
	return true
}

// Close forgets the connection: datagrams that arrive for it later have an unknown connection
// ID (REM-325). A simulated connection has no teardown of its own, so the driver never reports a
// link closed to the core.
func (l *simLink) Close() {
	if !l.closed {
		l.closed = true
		delete(l.d.links, l.key)
	}
}

// driver is the remoting.Driver of one boot of a lockstep node: it dials kernel nodes by name,
// numbers connections and carries frames as datagrams. Its state belongs to the engine's node and
// is only used in engine context, on the simulation goroutine.
type driver struct {
	e    *simEngine
	core *remoting.Core
	// dials is the k of the last connection this boot dialed (REM-315).
	dials uint32
	// links holds the open connections; it is read by key only (SIM-330).
	links map[linkKey]*simLink
}

// Dial opens a connection to the kernel node named node, which need not be a lockstep node
// (AT-REM-36). It returns an error wrapping remoting.ErrUnknownNode when the world has no node of
// that name. advertise is ignored: a simulated node is found by its name.
func (d *driver) Dial(node, _ string) (remoting.Link, error) {
	n := d.e.s.w.Sim.Lookup(node)
	if n == nil {
		return nil, fmt.Errorf("%w: %q", remoting.ErrUnknownNode, node)
	}
	if d.dials == maxDial {
		return nil, fmt.Errorf("lockstep: sim: node %s used every connection ID of incarnation %d", d.e.n.Name(), d.e.inc)
	}
	d.dials++
	key := linkKey{peer: n.ID(), id: uint64(d.e.inc)<<32 | uint64(d.dials), out: true}
	return d.add(key), nil
}

// add opens the connection key.
func (d *driver) add(key linkKey) *simLink {
	l := &simLink{d: d, key: key}
	if d.links == nil {
		d.links = make(map[linkKey]*simLink)
	}
	d.links[key] = l
	return l
}

// send sends frame to node to as one datagram with connection ID id, direction bit included. A
// node that is down, or an engine that is dead, sends nothing: a crash discards the core silently
// (REM-335). A paused node sends it once it resumes (SIM-140): while deferred datagrams wait, a
// later one queues behind them, so datagrams leave in the order they were written.
func (d *driver) send(to kernel.NodeID, id uint64, frame []byte) {
	e := d.e
	if !e.canSchedule() {
		return
	}
	dg := make(datagram, idLen+len(frame))
	binary.BigEndian.PutUint64(dg, id)
	copy(dg[idLen:], frame)
	o := outDatagram{to: to, conn: id &^ dirBit, frame: len(frame), dg: dg}
	if e.n.State() == kernel.NodePaused || len(e.deferred) > 0 {
		e.deferred = append(e.deferred, o)
		e.post(sendLabel, e.sendDeferred)
		return
	}
	e.sendDatagram(o)
}

// sendDeferred is the lockstep/send event: it sends the oldest deferred datagram. Each deferred
// datagram has one event, and every event sends the queue's head, so the datagrams leave in the
// order they were written whatever order the events run in.
func (e *simEngine) sendDeferred() {
	o := e.deferred[0]
	e.deferred[0] = outDatagram{}
	e.deferred = e.deferred[1:]
	e.sendDatagram(o)
}

// sendDatagram sends o on its link from e's node (REM-320): the link's random decisions are drawn
// as for a Send; a lost datagram is recorded as lockstep.remote.loss, and any other leaves as
// exactly one copy, after the drawn delay, in FIFO order with the link's other traffic. A drawn
// duplicate is ignored: the remoting protocol assumes none.
func (e *simEngine) sendDatagram(o outDatagram) {
	net := e.s.w.Net
	if d := net.Decide(e.n.ID(), o.to); d.Drop {
		e.Record(recordLoss,
			engine.Attr{Key: "from", Value: e.n.Name()},
			engine.Attr{Key: "to", Value: e.s.w.Sim.Node(o.to).Name()},
			engine.Attr{Key: "conn", Value: strconv.FormatUint(o.conn, 10)},
			engine.Attr{Key: "bytes", Value: strconv.Itoa(o.frame)},
		)
	} else {
		net.SendRaw(e.n, o.to, o.dg, simnet.RawOptions{Delay: d.Delay, FIFO: true})
	}
}

// arrive is the simnet handler of the engine's node for the incarnation of e. It hands the frame
// of each datagram to the core: a datagram from a dialer whose connection is new and whose frame
// is a HELLO opens an incoming connection; one whose connection ID is otherwise unknown is
// discarded with reason unknown-connection and, unless its frame is a CLOSE, answered with CLOSE
// unknown-connection on that ID (REM-325). A payload that is not a datagram fails the seed with a
// misuse failure. A dead engine ignores every arrival (SIM-120). An arrival is an event of the
// engine's node (PER-510).
func (e *simEngine) arrive(from kernel.NodeID, payload any) {
	if e.dead {
		return
	}
	defer e.s.enter(execution{eng: e})()
	dg, ok := payload.(datagram)
	if !ok || len(dg) < idLen {
		e.misuse(errors.New("lockstep: node " + e.n.Name() + " received a non-lockstep payload " + kernel.Describe(payload)))
		return
	}
	e.driver.receive(from, binary.BigEndian.Uint64(dg), dg[idLen:])
}

// receive applies frame, which arrived from node from with connection ID id.
func (d *driver) receive(from kernel.NodeID, id uint64, frame []byte) {
	key := linkKey{peer: from, id: id &^ dirBit, out: id&dirBit != 0}
	l := d.links[key]
	if l == nil {
		if kind, ok := frameKind(frame); key.out || !ok || kind != remoting.KindHello {
			d.unknown(from, id, frame)
			return
		}
		l = d.add(key)
		d.core.Accepted(l)
	}
	d.core.Frame(l, frame)
}

// unknown discards frame, which arrived from node from on the unknown connection ID id, and
// answers it with CLOSE unknown-connection on that ID unless it is a CLOSE itself (REM-325).
func (d *driver) unknown(from kernel.NodeID, id uint64, frame []byte) {
	kind, seq := frameHead(frame)
	e := d.e
	e.Record(recordDiscard,
		engine.Attr{Key: "peer", Value: e.s.w.Sim.Node(from).Name()},
		engine.Attr{Key: "conn", Value: strconv.FormatUint(id&^dirBit, 10)},
		engine.Attr{Key: "kind", Value: kind},
		engine.Attr{Key: "seq", Value: strconv.FormatUint(seq, 10)},
		engine.Attr{Key: "reason", Value: discardUnknownConnection},
	)
	if k, ok := frameKind(frame); ok && k == remoting.KindClose {
		return
	}
	closeFrame := remoting.AppendFrame(nil, &remoting.Frame{Kind: remoting.KindClose, Payload: []byte(remoting.CloseUnknownConnection)})
	d.send(from, id^dirBit, closeFrame)
}

// transport is the system's transport: the core, once the boot has built it (hooks.AttachTransport
// hands the core what it needs, so the transport is attached first). An engine that is dead, or
// whose node is down, sends nothing (SIM-120).
type transport struct {
	e    *simEngine
	core *remoting.Core
}

// Deliver hands env to the core.
func (t *transport) Deliver(env engine.Envelope) error {
	if !t.e.canSchedule() || t.core == nil {
		return nil
	}
	return t.core.Deliver(env)
}

// errorsOf builds the errors of the simulation's cores from package remote's types, as the
// production driver does.
type errorsOf struct{}

// Remote returns the *remote.Error that node reported with a FAIL frame.
func (errorsOf) Remote(node string, code uint8, msg string) error {
	return &remote.Error{Node: node, Code: remote.Code(code), Message: msg}
}

// Unreachable returns an error wrapping remote.ErrUnreachable.
func (errorsOf) Unreachable(node string) error {
	return fmt.Errorf("%w: %s", remote.ErrUnreachable, node)
}

// PeerRestarted returns an error that matches remote.ErrPeerRestarted and lockstep.ErrTerminated.
func (errorsOf) PeerRestarted(node string) error {
	return fmt.Errorf("%w: %s (%w)", remote.ErrPeerRestarted, node, lockstep.ErrTerminated)
}

// FrameTooLarge returns an error wrapping remote.ErrFrameTooLarge.
func (errorsOf) FrameTooLarge(size, limit int) error {
	return fmt.Errorf("%w: %d bytes > max %d", remote.ErrFrameTooLarge, size, limit)
}

// Code returns the first code, in Code order, whose lockstep sentinel err matches, or
// remote.CodeOther. The sentinels come from remote.Error's own Unwrap, so remote keeps the one
// table of them.
func (errorsOf) Code(err error) uint8 {
	for c := remote.CodeTimeout; c <= remote.CodeStopped; c++ {
		if errors.Is(err, (&remote.Error{Code: c}).Unwrap()) {
			return uint8(c)
		}
	}
	return uint8(remote.CodeOther)
}
