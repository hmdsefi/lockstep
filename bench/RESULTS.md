# Benchmark results

Each number is the median of the benchmark runs in [`results/baseline.txt`](results/baseline.txt), and `n/a` marks a library with no result for that row.

The header lines of that file give the platform, the commit and the date:

```text
goos: linux
goarch: amd64
cpu: AMD EPYC 9V45 96-Core Processor
# commit: 451ffcdb76bb41561e23914b488eb488137fc038
# date: 2026-11-25
```

## PingPong

Pairs of actors exchange `Ping` and `Pong` messages; one op is one message, and `pairs=procs` runs `GOMAXPROCS` pairs.

Median `ns/op`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| pairs=1 | 471.4 | 1875.5 | 251.8 | 324.6 |
| pairs=procs | 156.95 | 556.45 | 125.6 | 212.05 |

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
| consumers=16 | 249.9 | 111.8 | 161.5 | 70.675 |
| consumers=1024 | 273.8 | 202.45 | 242.7 | 195.55 |

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
| n=10000 | 18270199.5 | 10520761.5 | 55285827 | 265110681 |
| n=100000 | 178587939.5 | 123372680.5 | 455373621 | 2781350031 |

Median `B/actor`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| n=10000 | 1691 | 876 | 27905 | 1297 |
| n=100000 | 1683 | 718 | 27899 | 1237 |

Median `allocs/op`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| n=10000 | 110006 | 158649 | 202422 | 2449246 |
| n=100000 | 1100477.5 | 1531631.5 | 2020570.5 | 24490713 |

## SendLatency

One outside goroutine sends to an echo actor; one op is one send, and latency runs from the send call until `Receive` starts.

Median `ns/op`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| - | 809.45 | 1729.5 | 425.55 | 516.15 |

Median `p50-ns`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| - | 480.5 | 201 | 211 | 291 |

Median `p99-ns`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| - | 1242 | 1462 | 786 | 641 |

Median `allocs/op`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| - | 1 | 1 | 3 | 1 |

## AskLatency

Callers ask an echo actor and wait for the reply; one op is one ask, and `callers=procs` runs `GOMAXPROCS` callers in parallel.

Median `ns/op`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| callers=1 | 1481.5 | 2999.5 | 1499 | 912 |
| callers=procs | 1103.5 | 1796 | 1522.5 | 641.35 |

Median `p50-ns`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| callers=1 | 1171.5 | 2283 | 1136.5 | 776 |
| callers=procs | 2931.5 | 3247.5 | 1317 | 1502.5 |

Median `p99-ns`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| callers=1 | 4795 | 8117 | 5268 | 3786 |
| callers=procs | 57456.5 | 64275.5 | 71480 | 11114 |

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
| - | 1652.5 | n/a | n/a | n/a |

Median `p50-ns`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| - | 1282 | n/a | n/a | n/a |

Median `p99-ns`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| - | 4231 | n/a | n/a | n/a |

Median `allocs/op`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| - | 10 | n/a | n/a | n/a |

## SimSeeds

Only lockstep runs this workload: `sim.Simulate` runs one simulated world per seed, and one op is one seed.

Median `ns/op`:

| parameter | lockstep | protoactor | hollywood | goakt |
|---|---:|---:|---:|---:|
| workload=pingpong | 6180073 | n/a | n/a | n/a |
| workload=crash3 | 179037875.5 | n/a | n/a | n/a |
