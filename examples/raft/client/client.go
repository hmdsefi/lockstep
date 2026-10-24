// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package client is the closed-loop KV workload of the Raft demo.
package client

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/examples/raft/kv"
	"github.com/hmdsefi/lockstep/examples/raft/raft"
)

// Defaults applied by New to zero Config fields.
const (
	DefaultReadPercent = 50
	DefaultThinkMax    = 20 * time.Millisecond
	DefaultTimeout     = 500 * time.Millisecond
	DefaultRetryDelay  = 20 * time.Millisecond
	DefaultMaxRetries  = 3
)

// defaultKeys is the number of keys of the default key set "k0".."k3".
const defaultKeys = 4

// probeKey is the only key a probe writes (RFT-370).
const probeKey = "probe"

// Outcome is how a recorded operation ended.
type Outcome uint8

const (
	OK   Outcome = iota + 1 // took effect; output is its result
	Fail                    // did not take effect
	Info                    // unknown
)

// Recorder records operations, like faultline's history.Recorder.
type Recorder interface {
	Invoke(process, f string, input any) int64
	Complete(id int64, outcome Outcome, output any)
}

// Msg is the sealed message type of a Client. All its implementations are unexported.
type Msg interface{ clientMsg() }

// begin starts the next operation.
type begin struct{}

// retry makes the next attempt of operation seq.
type retry struct{ seq uint64 }

// answered is the resolution of the ask of one attempt of operation seq.
type answered struct {
	seq uint64
	res raft.Result
	err error
}

func (begin) clientMsg()    {}
func (retry) clientMsg()    {}
func (answered) clientMsg() {}

// Config configures one client. New applies the defaults to zero fields, so a zero ReadPercent,
// ThinkMax, Timeout, RetryDelay or MaxRetries means the default: 0 percent reads, a zero
// ThinkMax and zero retries cannot be configured. Keys defaults only when it is nil, so an empty
// non-nil Keys is an error.
type Config struct {
	Name        string        // process name in the history, for example "c1"
	ID          uint32        // client ID carried in commands
	Servers     []string      // server node names, in one fixed order
	Keys        []string      // keys to use; default "k0".."k3" when nil
	ReadPercent int           // default 50 when 0; 0..100
	ThinkMin    time.Duration // default 0
	ThinkMax    time.Duration // default 20ms when 0
	Timeout     time.Duration // per attempt; default 500ms when 0
	RetryDelay  time.Duration // before a retry without a hint; default 20ms when 0
	MaxRetries  int           // rejected attempts per operation; default 3 when 0
	StartAfter  time.Duration // delay of the first operation after Started; default 0
	MaxOps      int           // 0 = unlimited
	Probe       bool          // RFT-370
	Recorder    Recorder      // required
}

// Client is the workload actor. Create it with New.
type Client struct {
	cfg Config

	seq     uint64 // ordinal of the current or last operation
	target  int    // index in cfg.Servers of the server of the next attempt
	started bool   // the first server has been drawn

	// The operation in flight; pending is false between operations.
	pending bool
	id      int64  // the recorder's ID of the operation
	read    bool   // the operation is a read
	cmd     string // the KV-encoded command
	retries int    // rejected attempts so far
	done    bool   // a probe has seen its first OK: no more operations
}

// Started schedules the first operation StartAfter from now.
func (c *Client) Started(ctx *lockstep.Context[Msg]) {
	ctx.After(c.cfg.StartAfter, begin{})
}

// Receive handles one message.
func (c *Client) Receive(ctx *lockstep.Context[Msg], msg Msg) {
	switch m := msg.(type) {
	case begin:
		c.begin(ctx)
	case retry:
		if c.pending && m.seq == c.seq {
			c.attempt(ctx)
		}
	case answered:
		if c.pending && m.seq == c.seq {
			c.answer(ctx, m.res, m.err)
		}
	}
}

// begin starts operation seq+1: it draws the kind and the key (a probe draws neither), the first
// server once, records the invocation and makes the first attempt.
func (c *Client) begin(ctx *lockstep.Context[Msg]) {
	if c.pending || c.done || (c.cfg.MaxOps > 0 && c.seq >= uint64(c.cfg.MaxOps)) {
		return
	}
	rng := ctx.Rand()
	c.seq++
	key := probeKey
	c.read = false
	if !c.cfg.Probe {
		c.read = rng.IntN(100) < c.cfg.ReadPercent
		key = c.cfg.Keys[rng.IntN(len(c.cfg.Keys))]
	}
	if !c.started {
		c.started = true
		c.target = rng.IntN(len(c.cfg.Servers))
	}
	var input map[string]string
	var f string
	if c.read {
		f, input, c.cmd = "read", map[string]string{"key": key}, kv.EncodeGet(key)
	} else {
		value := c.cfg.Name + "-" + strconv.FormatUint(c.seq, 10)
		f, input, c.cmd = "write", map[string]string{"key": key, "value": value}, kv.EncodePut(key, value)
	}
	c.id = c.cfg.Recorder.Invoke(c.cfg.Name, f, input)
	c.pending = true
	c.retries = 0
	c.attempt(ctx)
}

// attempt asks the current server to propose the operation and pipes the answer back.
func (c *Client) attempt(ctx *lockstep.Context[Msg]) {
	seq, cmd, read, id := c.seq, c.cmd, c.read, c.cfg.ID
	server := lockstep.PIDAt[raft.Msg](c.cfg.Servers[c.target], raft.DefaultPath)
	reply := lockstep.Ask(ctx, server, func(rt lockstep.ReplyTo[raft.Result]) raft.Msg {
		return raft.Propose{Client: id, Seq: seq, Cmd: cmd, Read: read, ReplyTo: rt}
	}, lockstep.WithTimeout(c.cfg.Timeout))
	lockstep.Pipe(ctx, reply, func(res raft.Result, err error) Msg {
		return answered{seq: seq, res: res, err: err}
	})
}

// answer handles the resolution of the ask of the current attempt (RFT-360).
func (c *Client) answer(ctx *lockstep.Context[Msg], res raft.Result, err error) {
	if err != nil {
		c.unknown(ctx)
		return
	}
	switch res.Status {
	case raft.OK:
		c.complete(ctx, OK, c.output(res.Output))
	case raft.NotLeader:
		if c.exhausted(ctx) {
			return
		}
		if i := slices.Index(c.cfg.Servers, res.Leader); res.Leader != "" && i >= 0 {
			c.target = i
			c.attempt(ctx)
			return
		}
		c.target = (c.target + 1) % len(c.cfg.Servers)
		ctx.After(c.cfg.RetryDelay, retry{seq: c.seq})
	case raft.Busy:
		if c.exhausted(ctx) {
			return
		}
		ctx.After(c.cfg.RetryDelay, retry{seq: c.seq})
	default: // raft.Unknown, and a status this client does not know
		c.unknown(ctx)
	}
}

// exhausted counts a rejected attempt. It completes the operation as failed and reports true when
// the operation has used its MaxRetries retries.
func (c *Client) exhausted(ctx *lockstep.Context[Msg]) bool {
	if c.retries >= c.cfg.MaxRetries {
		c.complete(ctx, Fail, nil)
		return true
	}
	c.retries++
	return false
}

// unknown completes the operation as Info and moves on to the next server: an unknown attempt is
// never resent.
func (c *Client) unknown(ctx *lockstep.Context[Msg]) {
	c.target = (c.target + 1) % len(c.cfg.Servers)
	c.complete(ctx, Info, nil)
}

// output is the history output of a successful operation (RFT-350): nil for a write, and the
// value or nil if absent for a read.
func (c *Client) output(out string) any {
	if !c.read {
		return nil
	}
	if v, ok := kv.DecodeGetOutput(out); ok {
		return v
	}
	return nil
}

// complete records the outcome, then schedules the next operation after a think time. A probe
// stops after its first OK, and no client thinks after its last operation.
func (c *Client) complete(ctx *lockstep.Context[Msg], outcome Outcome, output any) {
	c.cfg.Recorder.Complete(c.id, outcome, output)
	c.pending = false
	if c.cfg.Probe && outcome == OK {
		c.done = true
	}
	if c.done || (c.cfg.MaxOps > 0 && c.seq >= uint64(c.cfg.MaxOps)) {
		return
	}
	span := int64(c.cfg.ThinkMax-c.cfg.ThinkMin) + 1
	ctx.After(c.cfg.ThinkMin+time.Duration(ctx.Rand().Int64N(span)), begin{})
}

// New validates cfg, applies defaults and returns the actor factory.
func New(cfg Config) (func() lockstep.Actor[Msg], error) {
	cfg.Servers = slices.Clone(cfg.Servers)
	if cfg.Keys == nil {
		cfg.Keys = make([]string, defaultKeys)
		for i := range cfg.Keys {
			cfg.Keys[i] = "k" + strconv.Itoa(i)
		}
	} else {
		cfg.Keys = slices.Clone(cfg.Keys)
	}
	if cfg.ReadPercent == 0 {
		cfg.ReadPercent = DefaultReadPercent
	}
	if cfg.ThinkMax == 0 {
		cfg.ThinkMax = DefaultThinkMax
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = DefaultTimeout
	}
	if cfg.RetryDelay == 0 {
		cfg.RetryDelay = DefaultRetryDelay
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = DefaultMaxRetries
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return func() lockstep.Actor[Msg] { return &Client{cfg: cfg} }, nil
}

// validate returns the first error of RFT-340.
func (cfg Config) validate() error {
	switch {
	case cfg.Name == "":
		return errors.New("client: Name is empty")
	case len(cfg.Servers) == 0:
		return errors.New("client: Servers is empty")
	case len(cfg.Keys) == 0:
		return errors.New("client: Keys is empty")
	case cfg.ReadPercent < 0 || cfg.ReadPercent > 100:
		return fmt.Errorf("client: ReadPercent (%d) must be in [0, 100]", cfg.ReadPercent)
	case cfg.ThinkMin < 0 || cfg.ThinkMin > cfg.ThinkMax:
		return fmt.Errorf("client: ThinkMin (%v) must be in [0, ThinkMax (%v)]", cfg.ThinkMin, cfg.ThinkMax)
	case cfg.Timeout <= 0:
		return fmt.Errorf("client: Timeout (%v) must be positive", cfg.Timeout)
	case cfg.MaxRetries < 0:
		return fmt.Errorf("client: MaxRetries (%d) must be >= 0", cfg.MaxRetries)
	case cfg.Recorder == nil:
		return errors.New("client: Recorder is nil")
	}
	return nil
}
