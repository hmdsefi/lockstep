# Raft on lockstep

Run Raft, written as lockstep actors, through seeded network partitions, crashes, paused servers,
and lossy links, and replay any failing seed exactly, down to the same trace hash (a hash of every
event in the run).

The demo is built from these parts:

- `raft`: a Raft server is one lockstep actor. It handles leader election, log replication, and
  commit, and writes its term, vote, and log to disk through effects, so it never blocks.
- `kv`: a key-value store is the replicated state machine. Reads and writes both go through the
  log.
- `client`: closed-loop clients send one operation at a time and record every call and result in a
  history.
- `raftsim`: runs three or five servers and the clients under `lockstep/sim`, injects faults,
  checks Raft's safety properties after every event, and checks the history when the run ends.
  Import it only from tests.

`lockstep/sim` runs on the [faultline](https://github.com/hmdsefi/faultline) simulation kernel, so
seeds, replay commands, and artifacts use faultline's names (`FAULTLINE_SEED`, for example).

The same server code runs in both modes. In production mode, a one-server cluster runs on
`lockstep.New()` with `raft.MemStore`, an in-memory store (see `raft/production_test.go`).

## Run it

You need Go 1.26 or later and a clone of this repository:

```sh
go version
```

The module builds against the lockstep code in the same checkout, so run it from `examples/raft`.
Start with `-short`, which skips the tests that run 100 or more seeds:

```sh
cd examples/raft
go test -short ./...
```

```text
ok  	github.com/hmdsefi/lockstep/examples/raft/client	0.295s
ok  	github.com/hmdsefi/lockstep/examples/raft/kv	0.279s
ok  	github.com/hmdsefi/lockstep/examples/raft/raft	0.856s
ok  	github.com/hmdsefi/lockstep/examples/raft/raftsim	0.415s
```

Without `-short`, `go test ./...` runs more than 1,200 simulated seeds. Those tests are also
skipped under `-race`.

## Find a bug and replay it

`TestDemoPartitionBug` plants a bug in every server, finds the first seed that breaks the cluster,
and replays that seed alone:

```sh
go test -v -run '^TestDemoPartitionBug$' ./raftsim
```

```text
=== RUN   TestDemoPartitionBug
    scenario_test.go:163: Planted bug vote-ignores-log: a server grants its vote without checking that the candidate's log is up to date.
    scenario_test.go:164: The cluster runs 3 servers and 3 clients under random network partitions, for up to 200 seeds of 20s simulated time each.
    scenario_test.go:166: Seed 0x910a2dec89025cc1 (index 0 of the first 200) is the first to fail.
    scenario_test.go:167: It fails the "leader completeness" check: n3 became leader of term 7 with log ending at 291, missing committed index 328
    scenario_test.go:178: Replaying seed 0x910a2dec89025cc1 alone fails the same check with the same message and trace hash 0x28153dc9797e87c5.
    scenario_test.go:179: To watch the failure, run this in examples/raft:
    scenario_test.go:180: RAFTDEMO_BUG=vote-ignores-log FAULTLINE_SEED=0x910a2dec89025cc1 go test -v -run '^TestScenario$' ./raftsim
--- PASS: TestDemoPartitionBug (0.08s)
PASS
```

The test passes because it expects the failure. Server `n3` won the election of term 7 with a log
that ends at index 291, although index 328 was already committed. As leader, `n3` can overwrite
the committed entries 292 to 328 on the other servers. Raft's vote check exists to prevent this.

To watch the failure itself, run the command from the last log line. `RAFTDEMO_BUG` names the
planted bug and `FAULTLINE_SEED` picks the seed:

```sh
RAFTDEMO_BUG=vote-ignores-log FAULTLINE_SEED=0x910a2dec89025cc1 go test -v -run '^TestScenario$' ./raftsim
```

```text
=== RUN   TestScenario
    scenario_test.go:57: faultline: FAULTLINE_SEED=0x910a2dec89025cc1: running 1 seed
=== RUN   TestScenario/seed=0x910a2dec89025cc1
    faultline: invariant "leader completeness" violated at t=4.455954410s on n3 (event 7591)
      n3 became leader of term 7 with log ending at 291, missing committed index 328
    replay:    FAULTLINE_SEED=0x910a2dec89025cc1 go test -v -run '^TestScenario$' github.com/hmdsefi/lockstep/examples/raft/raftsim
    artifacts: $TMPDIR/faultline-501/github.com_hmdsefi_lockstep_examples_raft_raftsim/TestScenario/910a2dec89025cc1/
--- FAIL: TestScenario (0.13s)
    --- FAIL: TestScenario/seed=0x910a2dec89025cc1 (0.13s)
FAIL
```

faultline writes the artifacts under `os.TempDir()`, shown here as `$TMPDIR`. The directory holds
the failed run's files, among them `trace.jsonl`, `timeline.txt`, a self-contained
`timeline.html`, and `history.jsonl` with every client operation.

## Fault presets

Each seed runs 20 seconds of virtual time. `raftsim.Setup` builds servers `n1` to `n3` (or `n5`),
clients `c1` to `c3`, and a probe client that writes until one write succeeds after the faults
stop. A preset chooses the faults:

- `FaultsPartition`: network partitions, and isolating the current leader.
- `FaultsCrash`: servers crash and restart.
- `FaultsDefault`: both of the above, plus paused servers, slow and lossy links, and clock drift.

Without a planted bug, the cluster passes the first 200 seeds of each preset, with 3 and with 5
servers.

## The planted bugs

Each planted bug is one bit of `raft.Bug`, set through `raft.Config.Bugs` or
`raftsim.Cluster.Bugs`. The zero value plants none.

- `BugVoteIgnoresLog` (`vote-ignores-log`): a server grants its vote without checking that the
  candidate's log is up to date, so a server with a stale log can win an election. Under
  `FaultsPartition`, the leader completeness or state machine safety check fails within the first
  200 seeds.
- `BugStaleRead` (`stale-read`): a leader answers a read from its own state instead of through the
  log, so a read can be served from a point in the log before a write that already completed.
  Under `FaultsPartition`, the linearizable check fails within the first 300 seeds.
- `BugVoteNotDurable` (`vote-not-durable`): a server never writes its vote to disk, so after a
  crash it can vote twice in one term. Under `FaultsCrash`, the durable state or election safety
  check fails within the first 100 seeds.

`TestScenario` runs the cluster with the bug that `RAFTDEMO_BUG` names, under that bug's preset.
Without `FAULTLINE_SEED`, it runs seeds until the first one fails and prints the command that
replays it:

```sh
RAFTDEMO_BUG=stale-read go test -run '^TestScenario$' ./raftsim
```

```text
--- FAIL: TestScenario (0.82s)
    scenario_test.go:57: faultline: running 300 seeds from base 0x0000000000000001 (Options.BaseSeed)
    --- FAIL: TestScenario/seed=0x85e7bb0f12278575 (0.39s)
        faultline: final check "linearizable" failed after the run at t=20.000000000s
          write c2#470 (k0=c2-470) at index 712 completed before read c2#472 (k2) at after index 711 was invoked
        replay:    FAULTLINE_SEED=0x85e7bb0f12278575 go test -v -run '^TestScenario$' github.com/hmdsefi/lockstep/examples/raft/raftsim
        artifacts: $TMPDIR/faultline-501/github.com_hmdsefi_lockstep_examples_raft_raftsim/TestScenario/85e7bb0f12278575/
    scenario_test.go:57: faultline: stopping after failing seed 0x85e7bb0f12278575; 292 of 300 seeds not run (set Options.KeepGoing to run all)
FAIL
```

Client `c2`'s write committed at index 712 and returned before `c2` sent its read. A leader
answered the read from its state at index 711, a point in the log before that write. A correct
server orders the read after every write that completed before it.

## What a simulation checks

The servers report every state change through the `raft.Observer` interface. In simulation an
oracle implements it and checks these properties after every event; production code leaves
`raft.Config.Observer` nil.

- election safety: at most one leader per term;
- log matching: two logs with an entry of the same term at an index hold the same entries up to
  that index;
- leader completeness: a new leader holds every entry committed in an earlier term;
- state machine safety: every server applies the same command at a given index;
- durable state: a restarted server loads the state it last wrote to disk.

When the run ends, final checks read the client history and the servers:

- linearizable: the committed log order explains every acknowledged read and write;
- progress: the probe's write succeeds within 3 seconds after the faults stop;
- replicas agree: every running server has applied every acknowledged write, and its state hash
  matches the oracle's own key-value store, built from the committed log.

## Write your own scenario

`raftsim.Test` runs a cluster for a number of seeds inside a normal Go test. Start from
`DefaultCluster()` and change what you want to explore:

```go
package raftsim_test

import (
	"testing"

	"github.com/hmdsefi/lockstep/examples/raft/raftsim"
)

func TestFiveServersUnderCrashes(t *testing.T) {
	c := raftsim.DefaultCluster()
	c.Servers = 5
	c.Faults = raftsim.FaultsCrash
	raftsim.Test(t, raftsim.Options(50), c)
}
```

`raftsim.Options(50)` runs 50 seeds. When a seed fails, the test output shows the `FAULTLINE_SEED`
command that replays it.

## Actor rules

`raft`, `kv`, and `client` import the standard library, `lockstep`, and each other, never
`lockstep/sim` or faultline. A test in `raftsim` checks this with `go list -deps`.

The module passes `lockstep-vet` with no findings, and it suppresses none: it has no allow
directive, the comment that suppresses a finding. `lockstep-vet` reports actor code that blocks,
starts a goroutine, reads the wall clock or global randomness (`LS100`), or waits on a reply with
`Reply.Await` (`LS110`). It also reports:

- a variable that the code uses after it sent it in a message (`LS200`);
- a type marked `//lockstep:immutable` that holds a pointer, channel, function or interface field,
  or that code writes into (`LS201`);
- actor code that writes a package variable, or puts the actor, one of its fields that holds
  references, or its `*lockstep.Context` in a message (`LS210`);
- an `Ask` reply that nobody awaits or pipes, and an ignored error from `Send` or `Spawn`
  (`LS220`);
- a message type that `Receive` has no case for (`LS230`);
- an actor field that holds a resource and is neither passed to `ctx.Own` nor released in
  `Stopped` (`LS240`);
- a malformed or unknown directive (`LS001`, `LS002`).

To run it from the repository root:

```sh
go -C analysis build -o /tmp/lockstep-vet ./cmd/lockstep-vet
go -C examples/raft vet -vettool=/tmp/lockstep-vet ./...
```

Both commands print nothing. The server takes its time from `ctx.After` and `ctx.Every`, its
randomness from `ctx.Rand`, and writes to disk with `lockstep.Do`.

## Limits

The demo covers leader election, log replication, and commit from section 5 of the Raft paper. It
has no snapshots, log compaction, membership changes, PreVote, or leader leases. Clients never
resend an operation whose outcome is unknown, so servers never see duplicate requests.

A multi-server cluster runs only in simulation, because lockstep's production mode has no network
transport between processes.

The linearizable check is specific to this implementation: it checks that the committed log order
explains the history. It isn't a general linearizability checker.

## Next steps

- [lockstep](../../README.md): the actor runtime this demo runs on.
- [The Raft paper](https://raft.github.io/raft.pdf): the algorithm the `raft` package implements.
