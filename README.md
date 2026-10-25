[![build](https://github.com/hmdsefi/lockstep/actions/workflows/build.yml/badge.svg?branch=main)](https://github.com/hmdsefi/lockstep/actions/workflows/build.yml?query=branch%3Amain)
[![Go Reference](https://pkg.go.dev/badge/github.com/hmdsefi/lockstep.svg)](https://pkg.go.dev/github.com/hmdsefi/lockstep)
[![release](https://img.shields.io/github/v/release/hmdsefi/lockstep)](https://github.com/hmdsefi/lockstep/releases/latest)
[![license](https://img.shields.io/badge/license-MPL--2.0-blue)](LICENSE)
[![GitHub stars](https://img.shields.io/github/stars/hmdsefi/lockstep?style=social)](https://github.com/hmdsefi/lockstep)

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset=".github/assets/readme-dark.svg">
    <img alt="lockstep: an actor framework for Go with deterministic simulation testing" src=".github/assets/readme-light.svg" width="640">
  </picture>
</p>

<p align="center"><strong>Write actors. Get exact replay.</strong></p>

lockstep is an actor framework for Go with deterministic simulation testing (DST) built in. You
write typed actors with mailboxes, supervision, timers, and request and reply. The same actor code
runs in your program on `lockstep.New()`, and in `go test` on a seeded simulation that partitions
the network, drops and delays messages, and crashes and restarts nodes. Any failure in the
simulation replays exactly from its seed, so a bug that depends on timing becomes one command you
can rerun.

The actor model here is the one Erlang/OTP and Akka made familiar, and that Go libraries such as
protoactor-go, Hollywood, Ergo, and GoAkt also use. lockstep's runtime owns every message
delivery, timer, and I/O event, and that is what makes the simulation exact.

## Features

lockstep includes:

- **Typed actors and PIDs.** `Actor[M]` receives one message type and `PID[M]` accepts only that
  type, so a message of the wrong type does not compile.
- **Request and reply.** `Ask` with a typed `ReplyTo`, timeouts, and `Pipe`, which delivers the
  reply as a message instead of blocking the actor.
- **Bounded mailboxes** with an explicit policy when full: fail the send, drop the newest or
  oldest message, or block the sender.
- **Supervision trees** in the Erlang style: one-for-one and all-for-one strategies, restart
  limits with backoff, death watch, an actor cap, and a `Stopped` hook on every stop and restart.
- **Timers and effects** on the runtime's clock: `ctx.After`, `ctx.Every`, and I/O as named
  effects with `lockstep.Do`.
- **A pooled production engine.** `lockstep.New()` runs actors on a fixed pool of worker
  goroutines, so turns of different actors run in parallel while each actor handles one message at
  a time.
- **Deterministic simulation testing** in `lockstep/sim`, built on
  [faultline](https://github.com/hmdsefi/faultline): multiple nodes, network partitions, message
  drops and delays, crashes and restarts, memory pressure, and exact replay from a seed.
- **Deadlock and leak detection** in simulation. A cycle of pending asks fails the seed with
  `lockstep: deadlock: A → B → A`.
- **A static analyzer**, `lockstep-vet`, also available as a golangci-lint plugin. It reports
  `time.Now`, global randomness, goroutines, `select`, map ranges, and blocking calls in actor
  code.
- **A Raft example** with three planted bugs that a seeded partition or crash exposes.

## Install

lockstep needs Go 1.26 or later:

```sh
go version
go get github.com/hmdsefi/lockstep@v0.1.0
```

## Quick start: a first actor

An actor is a type with a `Receive` method for one message type. The following program is a
counter: `Add` messages change its total, and a `Get` message asks for it.

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/hmdsefi/lockstep"
)

// Msg is every message the counter accepts.
type Msg interface{ isMsg() }

// Add adds N to the total.
type Add struct{ N int }

// Get asks for the total.
type Get struct{ Reply lockstep.ReplyTo[int] }

func (Add) isMsg() {}
func (Get) isMsg() {}

// Counter is an actor that keeps a running total.
type Counter struct{ total int }

func (c *Counter) Receive(ctx *lockstep.Context[Msg], msg Msg) {
	switch m := msg.(type) {
	case Add:
		c.total += m.N
	case Get:
		m.Reply.Send(ctx, c.total)
	}
}

func newCounter() lockstep.Actor[Msg] { return &Counter{} }

func getTotal(r lockstep.ReplyTo[int]) Msg { return Get{Reply: r} }

func main() {
	sys := lockstep.New()
	counter, err := lockstep.Spawn(sys, "counter", newCounter)
	if err != nil {
		log.Fatal(err)
	}
	for _, n := range []int{1, 2, 3} {
		if err := lockstep.Send[Msg](sys, counter, Add{N: n}); err != nil {
			log.Fatal(err)
		}
	}
	total, err := lockstep.Ask(sys, counter, getTotal).Await(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("total:", total)

	if err := sys.Shutdown(context.Background()); err != nil {
		log.Fatal(err)
	}
}
```

```sh
go run .
```

```text
total: 6
```

`Spawn` returns a `PID[Msg]`, so the compiler rejects a message of the wrong type. `Send` takes
`[Msg]` explicitly because `Add{N: n}` alone has type `Add`. `Ask` puts a `ReplyTo[int]` into the
`Get` message and returns a `*Reply[int]`. `Await` blocks, so call it outside actors; inside an
actor, `lockstep.Pipe` delivers the reply as a message instead.

## Configure the production engine

`lockstep.New()` runs actor turns on a fixed pool of worker goroutines. Turns of different actors
run in parallel, the turns of one actor never overlap, and messages from one sender to one
receiver arrive in send order. Timers and effect results run on the same workers. Four options
configure the engine:

- `WithWorkers(n)` sets the number of workers. The default is `runtime.GOMAXPROCS(0)`, read when
  `New` runs.
- `WithThroughput(n)` sets how many user messages an actor processes in one turn before it yields
  its worker to other actors. The default is 32.
- `WithActorThroughput(n)` is a `Spawn` option that replaces that limit for one actor, for example
  a small `n` for an actor whose messages are expensive.
- `WithEffectPool(workers, queue)` bounds effects started with `lockstep.Do`: at most `workers`
  run at once, and at most `queue` more wait for a free slot. The defaults are 64 and 1024. A `Do`
  call that finds both full resolves with `ErrOverloaded`.

The quick start's `main` can begin like this instead:

```go
sys := lockstep.New(
	lockstep.WithWorkers(4),
	lockstep.WithThroughput(64),
	lockstep.WithEffectPool(16, 256),
)
counter, err := lockstep.Spawn(sys, "counter", newCounter, lockstep.WithActorThroughput(256))
if err != nil {
	log.Fatal(err)
}
```

The options apply only to `lockstep.New()`. Simulation checks the values and then ignores them:
every turn there processes one message, in an order the seed decides.

## Deterministic simulation testing: the same actor under faults

`sim.Simulate` runs a test body once per seed. Each run is a simulated world with virtual time
and a simulated network. The following test runs the counter on server node `n1`, sends 20 `Add`
messages from client node `c1`, and lets faultline crash and restart `n1` at random times. A
final check runs when the run ends:

```go
package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/faultline/kernel/fault"
	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/sim"
)

func TestCounter(t *testing.T) {
	opts := sim.Options{Options: faultline.Options{Seeds: 50, Duration: 10 * time.Second}}
	sim.Simulate(t, opts, func(s *sim.Sim) {
		w := s.World()
		var counter lockstep.PID[Msg]
		s.Node("n1", func(sys *lockstep.System) {
			counter, _ = lockstep.Spawn(sys, "counter", newCounter)
		})
		s.Client("c1", func(*lockstep.System) {})
		w.Plan(&fault.Random{MaxDown: 1, Rules: []fault.Rule{
			{Kind: fault.KindCrash, Every: 8 * time.Second, MaxFor: time.Second},
		}})

		for range 20 {
			w.RunFor(250 * time.Millisecond)
			if err := sim.Send[Msg](s, "c1", counter, Add{N: 1}); err != nil {
				w.T().Fatal(err)
			}
		}
		w.RunFor(4 * time.Second) // faults stop at 7.5s, a quarter before the end
		total, err := sim.Ask(s, "c1", counter, getTotal)
		w.Final("every add counted", func() error {
			if err != nil {
				return err
			}
			if total != 20 {
				return fmt.Errorf("total is %d after 20 adds", total)
			}
			return nil
		})
	})
}
```

The test imports faultline directly, so add it to `go.mod` and run the test:

```sh
go mod tidy
go test .
```

```text
--- FAIL: TestCounter (0.01s)
    counter_test.go:16: faultline: running 50 seeds from base 0x64b5629963419063 (test name)
    --- FAIL: TestCounter/seed=0x870af07a512d3866 (0.01s)
        faultline: final check "every add counted" failed after the run at t=10.000000000s
          total is 0 after 20 adds
        replay:    FAULTLINE_SEED=0x870af07a512d3866 go test -v -run '^TestCounter$' example.com/counter
        artifacts: $TMPDIR/faultline-501/example.com_counter/TestCounter/870af07a512d3866/
    counter_test.go:16: faultline: stopping after failing seed 0x870af07a512d3866; 48 of 50 seeds not run (set Options.KeepGoing to run all)
FAIL
FAIL	example.com/counter	0.330s
FAIL
```

The first seed passes and the second fails. The counter keeps its total in memory, so a crash
loses it. The run's `schedule.json` artifact shows when: `n1` crashed at t=4.87s, after 19 adds,
and restarted at t=5.71s with a new counter. The 20th add reached a node that was down.

The seeds come from the test's name, so they are the same on every machine. The `replay` line is
the command that runs the failing seed again:

```sh
FAULTLINE_SEED=0x870af07a512d3866 go test -v -run '^TestCounter$' example.com/counter
```

```text
=== RUN   TestCounter
    counter_test.go:16: faultline: FAULTLINE_SEED=0x870af07a512d3866: running 1 seed
=== RUN   TestCounter/seed=0x870af07a512d3866
    faultline: final check "every add counted" failed after the run at t=10.000000000s
      total is 0 after 20 adds
    replay:    FAULTLINE_SEED=0x870af07a512d3866 go test -v -run '^TestCounter$' example.com/counter
    artifacts: $TMPDIR/faultline-501/example.com_counter/TestCounter/870af07a512d3866/
--- FAIL: TestCounter (0.01s)
    --- FAIL: TestCounter/seed=0x870af07a512d3866 (0.01s)
FAIL
FAIL	example.com/counter	0.250s
FAIL
```

The replay fails the same check at the same virtual time, and its `report.json` holds the same
trace hash (a hash of every event in the run) as the first run: `0x56d752b8033ec3c3`. faultline
writes the artifacts under `os.TempDir()`, shown here as `$TMPDIR`; they include `trace.jsonl`,
`timeline.txt`, and a self-contained `timeline.html`.

Besides the checks you register, `sim` fails a seed when actors deadlock on each other's asks
(`lockstep: deadlock: A → B → A`), when an effect has no handler, and when an actor stops or
restarts with an effect still pending or a resource it owns fails to close.

## Static analysis: lockstep-vet and golangci-lint

A replay is exact only if actor code takes time, randomness, and I/O from the runtime.
`lockstep-vet` reports actor code that doesn't:

- `LS100`: nondeterminism and blocking, such as `time.Now`, global `math/rand`, `crypto/rand`, a
  `go` statement, `select`, a range over a map, environment variables, channel operations, locks,
  and blocking network, file, database, or process calls.
- `LS110`: `Reply.Await`, which blocks the actor; use `lockstep.Pipe` instead.
- `LS001` and `LS002`: malformed and unknown `//lockstep:` directives.

Install it and run it through `go vet`:

```sh
go install github.com/hmdsefi/lockstep/analysis/cmd/lockstep-vet@v0.1.0
go vet -vettool=$(which lockstep-vet) ./...
```

On the counter it prints nothing. If `Receive` also recorded `time.Now()` on every `Add` and
waited on an ask with `Await`, it would print:

```text
main.go:37:50: LS110: Reply.Await blocks the actor; use lockstep.Pipe to receive the reply as a message (actor code: (*Counter).Receive)
main.go:34:17: LS100: time.Now uses the wall clock; use ctx.Now, ctx.After or ctx.Every (actor code: (*Counter).Receive)
```

To accept one finding, add `//lockstep:allow LS100 <reason>` on the reported line, on the line
above it, or in the doc comment of the enclosing declaration.

The same checks run in golangci-lint as the module plugin `lockstep`. Build a custom binary with
this `.custom-gcl.yml`:

```yaml
version: v2.14.0
plugins:
  - module: github.com/hmdsefi/lockstep/analysis
    import: github.com/hmdsefi/lockstep/analysis/golangci
    version: v0.1.0
```

Enable the linter in `.golangci.yml`:

```yaml
version: "2"
linters:
  enable:
    - lockstep
  settings:
    custom:
      lockstep:
        type: module
```

Then run `golangci-lint custom` and `./custom-gcl run ./...`. golangci-lint's
[module plugin guide](https://golangci-lint.run/docs/plugins/module-plugins/) describes both files.

## Example: Raft under partitions and crashes

[`examples/raft`](examples/raft/README.md) is a Raft cluster written as lockstep actors. It runs
three or five servers and their clients through partitions, crashes, paused servers, and lossy
links, checks Raft's safety properties after every event, and replays any failing seed exactly.
Three planted bugs show what a failing seed looks like.

## Why the name "lockstep"

**Marching in lockstep** means moving in exact unison, every step identical.
That's the promise: every run with the same seed is identical.

The name also comes from **deterministic lockstep**, a game-networking technique
used by real-time strategy games such as *Age of Empires* and *StarCraft*. Instead
of sending game state over the network, every machine starts from the same state,
applies the same inputs in the same order, and runs a deterministic simulation,
so every machine computes the same game.

lockstep's simulation mode has the same properties:

| Deterministic lockstep (games) | lockstep (this project) |
|---|---|
| Same inputs, same order → same game state | Same seed → same message order → same system state |
| A replay file is tiny: only the inputs | A failing test comes down to a single seed |
| A *desync* means nondeterminism leaked in (floats, random numbers) | A divergence means nondeterminism leaked into an actor (`time.Now`, `math/rand`, a raw goroutine) |
| The engine must own every source of randomness | The runtime must own every scheduling, time, and I/O decision |

**What it doesn't mean:** in games, lockstep peers wait for each other every tick.
lockstep is **not** a game-sync library, and outside simulation it does not run in
lockstep: no actor waits for another, and turns follow the order in which messages
arrive. The lockstep behavior applies to **simulation mode**, where a single seeded
scheduler steps every actor in a fixed, replayable order.

## lockstep and faultline

lockstep's simulation mode, the `lockstep/sim` package, runs on
[faultline](https://github.com/hmdsefi/faultline), deterministic simulation testing for Go.
lockstep **imports** faultline; it isn't part of the faultline repo. Only `lockstep/sim` imports
it, so a program that uses lockstep without `sim` never links faultline.

```text
github.com/hmdsefi/lockstep          actor runtime
├── lockstep.New()                   production engine for your program
└── lockstep/sim ──imports──▶ github.com/hmdsefi/faultline (v0.1.2)
```

## Limitations

lockstep runs local actors in one process, plus the multi-node simulation in `lockstep/sim`.
`lockstep.New()` does not read the live heap: `WithAdmission`, `WithWatchdog`, and
`WithGoroutineWarning` are accepted and have no effect on it. The actor cap (`WithMaxActors`)
applies everywhere. There is no remoting, clustering, persistence, or OpenTelemetry support, so
actors on different nodes talk only in simulation.

The API can change before v1.0. The [roadmap](https://github.com/hmdsefi/lockstep/issues/232)
tracks what comes next.

## Built with

[gograph](https://github.com/hmdsefi/gograph) finds the cycles in the wait-for graph that
`lockstep/sim` builds from pending asks, which is how it reports a deadlock.

## License

lockstep is licensed under the [Mozilla Public License 2.0](LICENSE).

## Next steps

- [Why lockstep](docs/why-lockstep.md): the kind of bug lockstep is built for, and when to use
  something else.
- [Architecture](docs/architecture.md): the runtime, its two engines, and how simulation maps onto
  faultline.
- [The Raft demo](examples/raft/README.md): a larger lockstep program and its simulation tests.
