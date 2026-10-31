# Benchmark results

Each number is the median of the benchmark runs in [`results/baseline.txt`](results/baseline.txt), and `n/a` marks a library with no result for that row.

The header lines of that file give the platform, the commit and the date:

```text
goos: linux
goarch: amd64
cpu: AMD EPYC 7763 64-Core Processor
# commit: 39107e010343737aeadeb223a1a133c982757177
# date: 2026-10-30
```

## PingPong

Pairs of actors exchange `Ping` and `Pong` messages; one op is one message, and `pairs=procs` runs `GOMAXPROCS` pairs.

Median `ns/op`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| pairs=1 | 783.7 | 2758.5 | 385.8 | 431.85 |
| pairs=procs | 330.3 | 963.95 | 198.7 | 256.55 |

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
| consumers=16 | 486.7 | 121.8 | 199.15 | 94.255 |
| consumers=1024 | 522.2 | 291.15 | 373.5 | 226.95 |

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
| n=10000 | 30171851.5 | 16931474 | 87049616.5 | 486163318 |
| n=100000 | 264596463 | 174943140.5 | 741022441 | 4957887550.5 |

Median `B/actor`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| n=10000 | 2026 | 876 | 27905 | 1297 |
| n=100000 | 2018 | 718 | 27899 | 1237 |

Median `allocs/op`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| n=10000 | 110005 | 158649 | 202440.5 | 2449242.5 |
| n=100000 | 1100483 | 1531631.5 | 2023891 | 24490709 |

## SendLatency

One outside goroutine sends to an echo actor; one op is one send, and latency runs from the send call until `Receive` starts.

Median `ns/op`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| - | 1410 | 2871 | 679.55 | 704.9 |

Median `p50-ns`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| - | 761 | 331 | 331 | 371 |

Median `p99-ns`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| - | 2560 | 2053 | 1338 | 992 |

Median `allocs/op`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| - | 1 | 1 | 2.5 | 1 |

## AskLatency

Callers ask an echo actor and wait for the reply; one op is one ask, and `callers=procs` runs `GOMAXPROCS` callers in parallel.

Median `ns/op`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| callers=1 | 3942 | 5733 | 2368 | 1346.5 |
| callers=procs | 4976 | 3589.5 | 2569.5 | 1026 |

Median `p50-ns`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| callers=1 | 2836 | 3982 | 1748.5 | 1082 |
| callers=procs | 7929.5 | 4985.5 | 1853.5 | 2304.5 |

Median `p99-ns`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| callers=1 | 15960 | 19115.5 | 10835 | 6096 |
| callers=procs | 150508 | 131384.5 | 144325 | 21978.5 |

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
| - | 3875 | n/a | n/a | n/a |

Median `p50-ns`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| - | 2755 | n/a | n/a | n/a |

Median `p99-ns`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| - | 14407 | n/a | n/a | n/a |

Median `allocs/op`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| - | 20 | n/a | n/a | n/a |

## SimSeeds

Only lockstep runs this workload: `sim.Simulate` runs one simulated world per seed, and one op is one seed.

Median `ns/op`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| workload=pingpong | 11208474 | n/a | n/a | n/a |
| workload=crash3 | 317530045.5 | n/a | n/a | n/a |
