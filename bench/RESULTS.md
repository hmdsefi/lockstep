# Benchmark results

Each number is the median of the benchmark runs in [`results/baseline.txt`](results/baseline.txt), and `n/a` marks a library with no result for that row.

The header lines of that file give the platform, the commit and the date:

```text
goos: linux
goarch: amd64
cpu: AMD EPYC 7763 64-Core Processor
# commit: 439c4669ca6f15f6ca4bc0c2734bfbe630cff90e
# date: 2026-10-31
```

## PingPong

Pairs of actors exchange `Ping` and `Pong` messages; one op is one message, and `pairs=procs` runs `GOMAXPROCS` pairs.

Median `ns/op`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| pairs=1 | 782.7 | 2670 | 383.55 | 423 |
| pairs=procs | 317.5 | 967.35 | 199.65 | 253.85 |

Median `allocs/op`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| pairs=1 | 1 | 1 | 2 | 0 |
| pairs=procs | 1 | 1 | 2 | 0 |

## FanOut

One producer sends items to many consumers under credit flow control; one op is one item.

Median `ns/op`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| consumers=16 | 486.4 | 121.65 | 191.4 | 93.465 |
| consumers=1024 | 521.45 | 287.05 | 368.85 | 228.95 |

Median `allocs/op`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| consumers=16 | 2 | 1 | 1 | 1 |
| consumers=1024 | 3 | 1 | 2 | 1 |

## IdleActors

Each op spawns `n` idle actors and sends each one a `Hello` message; `B/actor` is the heap growth per actor.

Median `ns/op`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| n=10000 | 28486305 | 14371409.5 | 84247761.5 | 473239402 |
| n=100000 | 254744156 | 148541704 | 674039222 | 4814133174.5 |

Median `B/actor`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| n=10000 | 2026 | 876 | 27904 | 1297 |
| n=100000 | 2018 | 718 | 27899 | 1237 |

Median `allocs/op`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| n=10000 | 110005 | 158649 | 202318 | 2449246 |
| n=100000 | 1100482 | 1531632 | 2023701.5 | 24490715 |

## SendLatency

One outside goroutine sends to an echo actor; one op is one send, and latency runs from the send call until `Receive` starts.

Median `ns/op`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| - | 1358 | 2909 | 661.35 | 669.5 |

Median `p50-ns`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| - | 751 | 361 | 330 | 361 |

Median `p99-ns`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| - | 2415 | 1698 | 1243 | 942 |

Median `allocs/op`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| - | 1 | 1 | 3 | 1 |

## AskLatency

Callers ask an echo actor and wait for the reply; one op is one ask, and `callers=procs` runs `GOMAXPROCS` callers in parallel.

Median `ns/op`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| callers=1 | 2645.5 | 5996.5 | 2252 | 1319 |
| callers=procs | 2339 | 3683.5 | 2468.5 | 1016 |

Median `p50-ns`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| callers=1 | 2048.5 | 3712 | 1723 | 1072 |
| callers=procs | 5220 | 4078 | 1814 | 2283.5 |

Median `p99-ns`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| callers=1 | 10808.5 | 18505 | 9648 | 5897 |
| callers=procs | 124492 | 133619 | 141620.5 | 21430 |

Median `allocs/op`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| callers=1 | 7 | 10 | 12 | 2 |
| callers=procs | 7 | 9 | 12 | 2 |

## AskPipe

Only lockstep runs this workload: one actor asks another with `Ask` and pipes the reply to its mailbox; one op is one round trip.

Median `ns/op`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| - | 2828.5 | n/a | n/a | n/a |

Median `p50-ns`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| - | 2044 | n/a | n/a | n/a |

Median `p99-ns`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| - | 9049 | n/a | n/a | n/a |

Median `allocs/op`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| - | 12 | n/a | n/a | n/a |

## SimSeeds

Only lockstep runs this workload: `sim.Simulate` runs one simulated world per seed, and one op is one seed.

Median `ns/op`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| workload=pingpong | 10722867 | n/a | n/a | n/a |
| workload=crash3 | 305265973 | n/a | n/a | n/a |
