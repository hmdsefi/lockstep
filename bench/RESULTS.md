# Benchmark results

Each number is the median of the benchmark runs in [`results/baseline.txt`](results/baseline.txt), and `n/a` marks a library with no result for that row.

The header lines of that file give the platform, the commit and the date:

```text
goos: linux
goarch: amd64
cpu: AMD EPYC 9V45 96-Core Processor
# commit: 023f182398beed05f961708c4e5e885de3c503df
# date: 2026-11-02
```

## PingPong

Pairs of actors exchange `Ping` and `Pong` messages; one op is one message, and `pairs=procs` runs `GOMAXPROCS` pairs.

Median `ns/op`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| pairs=1 | 471.5 | 1661.5 | 248.45 | 325.55 |
| pairs=procs | 149.3 | 544.7 | 123.8 | 208.8 |

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
| consumers=16 | 250.2 | 103.2 | 154.2 | 70.925 |
| consumers=1024 | 275.3 | 188.2 | 229.15 | 193.5 |

Median `allocs/op`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| consumers=16 | 2 | 1 | 2 | 1 |
| consumers=1024 | 3 | 1 | 2 | 1 |

## IdleActors

Each op spawns `n` idle actors and sends each one a `Hello` message; `B/actor` is the heap growth per actor.

Median `ns/op`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| n=10000 | 17913251.5 | 10795802 | 55169183 | 266090734.5 |
| n=100000 | 169524981 | 130106503.5 | 486531201.5 | 2815890939.5 |

Median `B/actor`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| n=10000 | 1691 | 876 | 27905 | 1297 |
| n=100000 | 1683 | 718 | 27899 | 1237 |

Median `allocs/op`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| n=10000 | 110006 | 158649 | 202391.5 | 2449246.5 |
| n=100000 | 1100477 | 1531630.5 | 2021572.5 | 24490714 |

## SendLatency

One outside goroutine sends to an echo actor; one op is one send, and latency runs from the send call until `Receive` starts.

Median `ns/op`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| - | 801.2 | 1750.5 | 440.7 | 540.15 |

Median `p50-ns`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| - | 470.5 | 201 | 211 | 301 |

Median `p99-ns`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| - | 1192 | 1763 | 821.5 | 661 |

Median `allocs/op`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| - | 1 | 1 | 2 | 1 |

## AskLatency

Callers ask an echo actor and wait for the reply; one op is one ask, and `callers=procs` runs `GOMAXPROCS` callers in parallel.

Median `ns/op`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| callers=1 | 1531.5 | 3295.5 | 1474 | 955.9 |
| callers=procs | 1139.5 | 1623 | 1533 | 648.8 |

Median `p50-ns`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| callers=1 | 1172 | 2619 | 1112 | 801 |
| callers=procs | 3024.5 | 4248.5 | 1302 | 1534 |

Median `p99-ns`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| callers=1 | 5488 | 8913.5 | 5189.5 | 3931 |
| callers=procs | 58535 | 62331 | 72263.5 | 11537.5 |

Median `allocs/op`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| callers=1 | 4 | 10 | 12 | 2 |
| callers=procs | 4 | 9 | 12 | 2 |

## AskPipe

Only lockstep runs this workload: one actor asks another with `Ask` and pipes the reply to its mailbox; one op is one round trip.

Median `ns/op`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| - | 1649.5 | n/a | n/a | n/a |

Median `p50-ns`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| - | 1262 | n/a | n/a | n/a |

Median `p99-ns`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| - | 4246.5 | n/a | n/a | n/a |

Median `allocs/op`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| - | 10 | n/a | n/a | n/a |

## SimSeeds

Only lockstep runs this workload: `sim.Simulate` runs one simulated world per seed, and one op is one seed.

Median `ns/op`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| workload=pingpong | 6497559 | n/a | n/a | n/a |
| workload=crash3 | 185737902.5 | n/a | n/a | n/a |
