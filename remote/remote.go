// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package remote

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/hooks"
	"github.com/hmdsefi/lockstep/internal/remoting"
)

// enableMu serializes Enable, so that the check for a core already registered for a system and
// the registration of its new core cannot interleave with another Enable (REM-010).
var enableMu sync.Mutex

// Enable switches remoting on for sys over TCP, or TLS when c.TLS is set. It listens on c.Listen
// and accepts connections from other nodes; it dials a peer when sys first sends to it, at the
// address c.Peers names, else the one c.Resolve returns, else the address the peer announced when
// it last connected to this node. A message to a node with no address becomes a dead letter.
//
// Enable checks, in order: that remoting is not enabled on sys already (ErrAlreadyEnabled), c with
// Validate, that c.Listen is set, that c.Codec supports every type in c.Registry (the error of
// codec.Registry.Check, unchanged), and that it can listen on c.Listen ("remote: listen <addr>:
// <err>"). It returns the first error and then leaves nothing open. On a system that is shutting
// down or stopped it returns an error wrapping lockstep.ErrStopped.
//
// Remoting stops with sys: Shutdown closes every connection, after telling each peer, once the
// last actor has stopped, and returns after the goroutines of remoting have exited. Messages
// still queued for a peer then become dead letters.
//
// When c.Advertise is empty, peers are told the address the listener got, unless it listens on
// every interface (an unspecified host such as 0.0.0.0 or [::]): then they are told nothing, so
// set c.Advertise. c.Resolve is called while remoting holds a lock that every send to another
// node takes, so it must return quickly, must not block and must never send to sys. Enable panics
// if sys is nil.
//
// On a system of a simulated node (package sim), whose messages to other nodes always travel
// through remoting, Enable only replaces the node's protocol settings. It returns an error
// wrapping lockstep.ErrStopped once the system's shutdown has begun, and after its node crashed.
// Otherwise it checks that it was not called for this boot of the node already
// (ErrAlreadyEnabled), then c with Validate, ignoring the network fields Listen, Advertise, Peers,
// Resolve and TLS. It never requires c.Listen nor listens, and does not check c.Codec against
// c.Registry, which the simulation does for its own options. Connections already up keep the
// frame limits and the codec agreed when they opened: Enable returns an error and changes nothing
// if c names a codec of another name or another registry while the node has a connection or a
// message waiting for one. So call Enable from the node's setup function, before the system sends
// to other nodes.
func Enable(sys *lockstep.System, c Config) error {
	if sys == nil {
		panic("remote: Enable with nil System")
	}
	enableMu.Lock()
	defer enableMu.Unlock()
	if remoting.Stopped(sys) {
		// A simulated system whose core shut down or whose node crashed.
		return errSimStopped(sys)
	}
	if core := remoting.CoreOf(sys); core != nil {
		// A simulated system has its core from its boot, marked simulated, and may still have
		// its settings replaced once; any other core means Enable ran already.
		if core.Simulated() && hooks.ShuttingDown(sys) {
			return errSimStopped(sys)
		}
		if !core.Settable() {
			return ErrAlreadyEnabled
		}
		return enableSimulated(core, c)
	}
	if err := c.Validate(); err != nil {
		return err
	}
	if c.Listen == "" {
		return errors.New("remote: Listen is required")
	}
	if err := c.Registry.Check(c.Codec); err != nil {
		return err
	}
	if n := len(c.Codec.Name()); n < 1 || n > remoting.MaxCodecName {
		return fmt.Errorf("remote: codec name %q is not 1 to %d bytes", c.Codec.Name(), remoting.MaxCodecName)
	}
	if len(c.Advertise) > remoting.MaxAdvertise {
		return fmt.Errorf("remote: advertise address of %d bytes exceeds %d", len(c.Advertise), remoting.MaxAdvertise)
	}

	ln, err := listen(c)
	if err != nil {
		return err
	}
	listenAddr := ln.Addr().String()
	advertise := c.Advertise
	if advertise == "" && !wildcard(ln.Addr()) {
		advertise = listenAddr
	}
	if len(advertise) > remoting.MaxAdvertise {
		_ = ln.Close()
		return fmt.Errorf("remote: advertise address of %d bytes exceeds %d", len(advertise), remoting.MaxAdvertise)
	}

	d := newDriver(c, ln)
	tr := &transport{ready: make(chan struct{})}
	att, err := hooks.AttachTransport(sys, tr, d.stop)
	if err != nil {
		_ = ln.Close()
		d.cancel()
		return fmt.Errorf("remote: %w", err)
	}
	// From here on the transport is attached for good: a failure leaves it refusing every
	// envelope with failed's error.
	failed := func(err error) error {
		_ = ln.Close()
		d.cancel()
		tr.err = fmt.Errorf("%w: remoting did not start on node %q: %w", ErrUnreachable, att.Node, err)
		close(tr.ready)
		return err
	}
	d.clock, d.node = att.Clock, att.Node
	if att.Log != nil {
		d.log = att.Log
	}
	core, err := remoting.New(remoting.Config{
		Node:        att.Node,
		Incarnation: att.Incarnation,
		Clock:       att.Clock,
		Entropy:     att.Entropy,
		Tracer:      att.Tracer,
		Failer:      att.Failer,
		Log:         att.Log,
		Host:        att.Host,
		System:      sys,
		Driver:      d,
		Errors:      coreErrors{},
		DeadLetter:  att.DeadLetter,
		Settings:    settings(&c, advertise, listenAddr),
	})
	if err != nil {
		// Unreachable: Validate and the checks above cover every check of remoting.New.
		return failed(fmt.Errorf("remote: %w", err))
	}
	if !remoting.Register(core) {
		// Unreachable: AttachTransport refuses a second transport.
		return failed(ErrAlreadyEnabled)
	}
	if !d.setCore(core) {
		// The system's shutdown ran the stop function while Enable was still running.
		remoting.Unregister(sys)
		return failed(fmt.Errorf("remote: %w", lockstep.ErrStopped))
	}
	core.Start()
	tr.core = core
	close(tr.ready)
	d.log.Info("remote: listening", "node", att.Node, "addr", listenAddr)
	d.startAccepting()
	return nil
}

// settings returns the protocol settings of c, which Validate has filled in, with the advertise
// and listen addresses given.
func settings(c *Config, advertise, listen string) remoting.Settings {
	return remoting.Settings{
		Registry:      c.Registry,
		Codec:         c.Codec,
		MaxFrame:      c.MaxFrame,
		OutboundQueue: c.OutboundQueue,
		Heartbeat:     c.Heartbeat,
		IdleTimeout:   c.IdleTimeout,
		DialTimeout:   c.DialTimeout,
		RedialMin:     c.RedialMin,
		RedialMax:     c.RedialMax,
		Advertise:     advertise,
		Listen:        listen,
	}
}

// enableSimulated is Enable on a simulated system, whose core the simulation built at the node's
// boot and marked simulated (REM-040): it ignores the network fields, as the simulation does with
// its own options (REM-300), checks the rest with Validate, and replaces the core's protocol
// settings. It neither requires Listen nor listens, and does not check the codec against the
// registry: the simulation checked its own options once per run (REM-030). The caller holds
// enableMu and has checked that the core's settings were not replaced yet.
func enableSimulated(core *remoting.Core, c Config) error {
	c.Listen, c.Advertise, c.Peers, c.Resolve, c.TLS = "", "", nil, nil, nil
	if err := c.Validate(); err != nil {
		return err
	}
	if n := len(c.Codec.Name()); n < 1 || n > remoting.MaxCodecName {
		return fmt.Errorf("remote: codec name %q is not 1 to %d bytes", c.Codec.Name(), remoting.MaxCodecName)
	}
	err := core.SetSettings(settings(&c, "", ""))
	switch {
	case err == nil:
		return nil
	case errors.Is(err, remoting.ErrCodecInUse):
		return errors.New("remote: the codec and registry cannot change while connections are open or messages wait for one")
	}
	// Unreachable: Validate and the check above cover SetSettings's checks, enableMu keeps a
	// second Enable out, and Enable checked the shutdown first.
	return fmt.Errorf("remote: %w", err)
}

// errSimStopped is Enable's error on a simulated system that is shutting down, has stopped or was
// discarded with its node's crash.
func errSimStopped(sys *lockstep.System) error {
	return fmt.Errorf("remote: %w: node %q", lockstep.ErrStopped, sys.Node())
}

// wildcard reports whether a is a TCP address on every interface (0.0.0.0 or [::]), which peers
// cannot dial back.
func wildcard(a net.Addr) bool {
	ta, ok := a.(*net.TCPAddr)
	return ok && (ta.IP == nil || ta.IP.IsUnspecified())
}

// listen opens c's listener, with TLS when c.TLS is set.
func listen(c Config) (net.Listener, error) {
	ln, err := net.Listen("tcp", c.Listen)
	if err != nil {
		return nil, fmt.Errorf("remote: listen %s: %w", c.Listen, err)
	}
	return newListener(ln, c.TLS), nil
}

// transport is the system's engine.Transport: the core, once Enable has built it. The runtime may
// call Deliver as soon as AttachTransport stored the transport; such a call waits the moment until
// Enable has built and started the core.
type transport struct {
	ready chan struct{} // closed once core or err is set
	core  *remoting.Core
	err   error // when Enable failed after attaching: the error Deliver returns
}

func (t *transport) Deliver(env engine.Envelope) error {
	<-t.ready
	if t.core == nil {
		return t.err
	}
	return t.core.Deliver(env)
}

// coreErrors is the core's remoting.Errors: the errors of package remote, and the one table from
// errors to failure codes.
type coreErrors struct{}

func (coreErrors) Remote(node string, code uint8, msg string) error {
	return &Error{Node: node, Code: Code(code), Message: msg}
}

func (coreErrors) Unreachable(node string) error {
	return fmt.Errorf("%w: %s", ErrUnreachable, node)
}

func (coreErrors) PeerRestarted(node string) error { return &restartError{node: node} }

func (coreErrors) FrameTooLarge(size, limit int) error {
	return fmt.Errorf("%w: %d bytes > max %d", ErrFrameTooLarge, size, limit)
}

// codeErrors maps each failure code but CodeOther to its sentinel, in code order.
var codeErrors = [...]struct {
	code Code
	err  error
}{
	{CodeTimeout, lockstep.ErrTimeout},
	{CodeTerminated, lockstep.ErrTerminated},
	{CodeNotFound, lockstep.ErrNotFound},
	{CodeWrongType, lockstep.ErrWrongType},
	{CodeMailboxFull, lockstep.ErrMailboxFull},
	{CodeOverloaded, lockstep.ErrOverloaded},
	{CodeStopped, lockstep.ErrStopped},
}

// Code returns err's failure code: the code of the first sentinel err matches, in code order, so
// that an *Error from another node keeps its code; CodeOther for any other error.
func (coreErrors) Code(err error) uint8 {
	var re *Error
	if errors.As(err, &re) && int(re.Code) >= len(codeNames) {
		return uint8(re.Code)
	}
	for _, x := range codeErrors {
		if errors.Is(err, x.err) {
			return uint8(x.code)
		}
	}
	return uint8(CodeOther)
}

// restartError is the failure of the asks, lookups and watches a restarted peer lost: it matches
// both ErrPeerRestarted and lockstep.ErrTerminated.
type restartError struct{ node string }

func (e *restartError) Error() string   { return "remote: peer restarted: " + e.node }
func (e *restartError) Unwrap() []error { return []error{ErrPeerRestarted, lockstep.ErrTerminated} }

// discardLog is the logger of a driver whose system gave it none.
var discardLog = slog.New(slog.DiscardHandler)

// linkBuffer is the number of bytes a connection holds that are not yet written before it refuses
// a frame; a connection with an empty buffer takes any frame. Enable reads it; tests make it
// smaller.
var linkBuffer = 256 << 10

// inboundLimit is the number of bytes of a connection's frames that the core has not yet taken
// beyond which its reader stops reading until the drain catches up; a reader always posts the
// frame it has read. Enable reads it; tests make it smaller.
var inboundLimit = 4 << 20
