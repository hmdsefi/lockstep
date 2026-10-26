// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package prodengine

import (
	"context"
	"fmt"
	"runtime"
	"sync"

	"github.com/hmdsefi/lockstep/internal/engine"
)

// Config configures an Engine. Zero fields take the defaults listed.
type Config struct {
	Node          string // node name; non-empty
	Workers       int    // default runtime.GOMAXPROCS(0); negative panics
	Throughput    int    // budget passed to RunTurn; default 32; negative panics
	EffectWorkers int    // default 64; negative panics
	EffectQueue   int    // default 1024; -1 means 0 (no queue); below -1 panics

	// Admission is the bundle's Admission. nil means admit everything.
	Admission engine.Admission
}

// Defaults of Config (SCH-010).
const (
	defaultThroughput    = 32
	defaultEffectWorkers = 64
	defaultEffectQueue   = 1024
)

// withDefaults validates cfg and returns it with the defaults applied to its zero fields
// (SCH-010). It panics on the first invalid field, in declaration order.
func withDefaults(cfg Config) Config {
	if cfg.Node == "" {
		panic("lockstep: prodengine: empty node name")
	}
	checkField("Workers", cfg.Workers, 0)
	checkField("Throughput", cfg.Throughput, 0)
	checkField("EffectWorkers", cfg.EffectWorkers, 0)
	checkField("EffectQueue", cfg.EffectQueue, -1)
	if cfg.Workers == 0 {
		cfg.Workers = runtime.GOMAXPROCS(0)
	}
	if cfg.Throughput == 0 {
		cfg.Throughput = defaultThroughput
	}
	if cfg.EffectWorkers == 0 {
		cfg.EffectWorkers = defaultEffectWorkers
	}
	switch cfg.EffectQueue {
	case 0:
		cfg.EffectQueue = defaultEffectQueue
	case -1:
		cfg.EffectQueue = 0
	}
	return cfg
}

// checkField panics with SCH-010's text if v is below lowest.
func checkField(name string, v, lowest int) {
	if v < lowest {
		panic(fmt.Sprintf("lockstep: prodengine: invalid %s %d", name, v))
	}
}

// Engine is the production engine. All methods are safe for concurrent use.
type Engine struct {
	cfg      Config
	s        scheduler // the bundle's Scheduler (sched.go)
	stopOnce sync.Once

	// tmu guards timers. AfterFunc checks the closing mark and registers a timer in one critical
	// section, so a Stop that marks closing and then locks tmu sees every timer that was armed.
	tmu    sync.Mutex
	timers map[*timer]struct{} // pending timers (timer.go); its length is the pending count

	fx           pool               // the effect pool (effects.go)
	engineCtx    context.Context    // parent of every effect's context; cancelled by Stop
	cancelEngine context.CancelFunc // cancels engineCtx

	bundle engine.Engine // returned by Engine (bundle.go); built once in New
}

// New validates cfg, applies defaults, starts exactly cfg.Workers worker goroutines and
// returns the engine. It starts no other goroutine; effect goroutines (SCH-160) and runtime
// timers (SCH-130) come later, on demand.
func New(cfg Config) *Engine {
	e := &Engine{cfg: withDefaults(cfg), timers: make(map[*timer]struct{})}
	e.engineCtx, e.cancelEngine = context.WithCancel(context.Background())
	e.bundle = e.newBundle()
	e.s.start(e.cfg.Workers, e.cfg.Throughput)
	return e
}

// Config returns the configuration after defaults were applied.
func (e *Engine) Config() Config {
	return e.cfg
}

// Stop stops the engine (SCH-200). It panics when called from engine context.
//
// The first call marks the engine closing, so from then on Ready and ReadyFrom do nothing, and
// AfterFunc and Start run nothing and return a Cancel that reports false. It then stops every
// pending timer, drops the queued effects and cancels the contexts of the running ones, discards
// every queued turn and callback, wakes every parked worker and waits until every worker has
// exited. A worker exits at its next choice (SCH-070), so a turn or callback in progress
// finishes first, and no callback runs after Stop returns (ENG-140). Stop does not wait for an
// effect whose Run ignores its cancelled context; its done never runs. Later calls return once
// the first has finished.
func (e *Engine) Stop() {
	if e.s.InTurn() {
		panic("lockstep: prodengine: Stop called from engine context")
	}
	e.stopOnce.Do(e.stop)
}

// stop is the first Stop call, SCH-200 steps 1 to 4 in order.
func (e *Engine) stop() {
	// Step 1. The closing mark comes first: AfterFunc checks it under tmu, so stopTimers below
	// sees every timer armed before it; Ready, ReadyFrom and the workers check it too.
	e.s.closing.Store(true)
	e.fx.stop() // the pool's own closing mark (Start, SCH-150 step 1); drops the queued jobs
	// Step 2.
	e.stopTimers()
	e.cancelEngine()
	// Steps 2 (queued turns and callbacks), 3 and 4.
	e.s.stop()
}

// parked returns the number of workers registered as parked (SCH-090). Tests use it.
func (e *Engine) parked() int {
	return int(e.s.park.n.Load())
}
