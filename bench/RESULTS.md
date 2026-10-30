# Benchmark results

Each number is the median of the benchmark runs in [`results/baseline.txt`](results/baseline.txt), and `n/a` marks a library with no result for that row.

The header lines of that file give the platform, the commit and the date:

```text
goos: linux
goarch: amd64
cpu: AMD EPYC 7763 64-Core Processor
# commit: ab9e41e6ae3c11c7d5dd8166ccda14f00e5cb984
# date: 2026-10-29
```

## PingPong

Pairs of actors exchange `Ping` and `Pong` messages; one op is one message, and `pairs=procs` runs `GOMAXPROCS` pairs.

Median `ns/op`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| pairs=1 | 780.7 | 2697.5 | 381.2 | 425.85 |
| pairs=procs | 348.9 | 971.65 | 201.85 | 258.65 |

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
| consumers=16 | 491.7 | 119.5 | 194 | 95.36 |
| consumers=1024 | 526.85 | 286.35 | 372.5 | 231.5 |

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
| n=10000 | 29011287.5 | 15043243.5 | 84773740 | 469269167 |
| n=100000 | 258812053.5 | 161584858.5 | 686543711.5 | 4833572933 |

Median `B/actor`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| n=10000 | 2026 | 876 | 27904 | 1297 |
| n=100000 | 2018 | 718 | 27899 | 1237 |

Median `allocs/op`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| n=10000 | 110005 | 158649 | 202339 | 2449244.5 |
| n=100000 | 1100483 | 1531632 | 2023897.5 | 24490701 |

## SendLatency

One outside goroutine sends to an echo actor; one op is one send, and latency runs from the send call until `Receive` starts.

Median `ns/op`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| - | 1381 | 2803.5 | 660.75 | 696.15 |

Median `p50-ns`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| - | 752 | 331 | 321 | 371 |

Median `p99-ns`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| - | 2319 | 1638 | 1217.5 | 962 |

Median `allocs/op`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| - | 1 | 1 | 2.5 | 1 |

## AskLatency

Callers ask an echo actor and wait for the reply; one op is one ask, and `callers=procs` runs `GOMAXPROCS` callers in parallel.

Median `ns/op`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| callers=1 | 3864 | 6031.5 | 2263 | 1338.5 |
| callers=procs | 4874 | 3613.5 | 2518 | 1025 |

Median `p50-ns`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| callers=1 | 2815 | 3817 | 1713 | 1082 |
| callers=procs | 7764.5 | 4388.5 | 1823 | 2305.5 |

Median `p99-ns`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| callers=1 | 15519 | 18941 | 10060.5 | 6026 |
| callers=procs | 148887.5 | 132174 | 144207.5 | 21903.5 |

Median `allocs/op`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| callers=1 | 14 | 10 | 12 | 2 |
| callers=procs | 14 | 9 | 12 | 2 |

## AskPipe

Only lockstep runs this workload: one actor asks another with `Ask` and pipes the reply to its mailbox; one op is one round trip.

Median `ns/op`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| - | 3821 | n/a | n/a | n/a |

Median `p50-ns`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| - | 2730 | n/a | n/a | n/a |

Median `p99-ns`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| - | 13766 | n/a | n/a | n/a |

Median `allocs/op`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| - | 20 | n/a | n/a | n/a |

## SimSeeds

Only lockstep runs this workload: `sim.Simulate` runs one simulated world per seed, and one op is one seed.

Median `ns/op`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| workload=pingpong | 10942976 | n/a | n/a | n/a |
| workload=crash3 | 313668773.5 | n/a | n/a | n/a |
