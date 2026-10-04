# lockstep

**Deterministic actors for Go.**
Write actors. Get exact replay.

lockstep is an actor runtime where every message delivery, timer, and I/O event goes
through the runtime. In production it's a fast actor system. In tests, the same
code runs on a seeded simulation — partitions, drops, reordering, crashes — and any
failure replays exactly from its seed.

> Status: **design phase**.

## What "lockstep" means here

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
| A replay file is tiny: just the inputs | A failing test comes down to a single seed |
| A *desync* means nondeterminism leaked in (floats, random numbers) | A divergence means nondeterminism leaked into an actor (`time.Now`, `math/rand`, a raw goroutine) |
| The engine must own every source of randomness | The runtime must own every scheduling, time, and I/O decision |

**What it doesn't mean:** in games, lockstep peers wait for each other every tick.
lockstep is **not** a game-sync library, and its **production mode does not run in
lockstep**: actors run concurrently at full speed. The lockstep behavior applies to
**simulation mode**, where a single seeded scheduler steps every actor in a fixed,
replayable order.

## Relationship with faultline

lockstep's simulation mode runs on the kernel of [faultline](https://github.com/hmdsefi/faultline)
(deterministic simulation testing for Go). lockstep **imports**
`github.com/hmdsefi/faultline/kernel`; it isn't part of the faultline repo.

```
github.com/hmdsefi/lockstep          actor runtime
├── production runtime (concurrent scheduler, remoting, clustering)
└── simulation mode ──imports──▶ github.com/hmdsefi/faultline/kernel
```

One engine, two products: faultline tests existing Go code, and lockstep is a
production actor runtime whose simulation mode is exactly as strong as faultline's.

## Built with

[gograph](https://github.com/hmdsefi/gograph), through faultline's kernel, powers
deadlock detection in simulation (`deadlock: A → B → C → A`) and diagrams of
failing runs.
