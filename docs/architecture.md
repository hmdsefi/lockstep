# lockstep architecture

This page explains how lockstep is built: its parts, how a message moves through them, and
why the same actor code can run in your program and in a seeded simulation. To start using
lockstep, read the [README](../README.md) first. For the ideas behind actors and where they came
from, read [The actor model](actor-model.md).

## The parts

lockstep has a runtime, the engines that run it, and an analyzer. The runtime in package
`lockstep` holds everything an actor sees: systems, PIDs, mailboxes, supervision, timers, asks and
effects. It is written against a small internal interface, the engine seam. Two engines run your
code: the production engine behind `lockstep.New()`, and the simulation engine in
`lockstep/sim`, which runs on [faultline](https://github.com/hmdsefi/faultline). A third engine,
which runs every turn on one goroutine, exists only for the project's own tests.

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="assets/lockstep-parts-dark.svg">
    <img alt="lockstep's parts: your actors call the runtime in package lockstep, with systems and PIDs, mailboxes, asks and replies, supervision, timers and effects. The runtime talks only to the engine seam, an internal interface with Scheduler, Clock, Effects, Entropy, Transport and Tracer. The production engine behind lockstep.New() and the simulation engine behind sim.Simulate implement the seam, and the simulation engine runs on the faultline kernel. lockstep-vet checks your actor code. From https://github.com/hmdsefi/lockstep." src="assets/lockstep-parts-light.svg">
  </picture>
</p>

Your actors never see which engine runs them: the runtime reaches an engine only through the
seam.

Persistence adds two packages above the runtime. `lockstep/persist` runs event-sourced actors,
whose state is rebuilt from a journal of events. `lockstep/persist/filestore` is a journal and
snapshot store that keeps its data in files. Neither imports an engine: `persist` reaches the
engine through the runtime, and `filestore` runs on a small file system interface, so it uses the
operating system's files in production and faultline's simulated disk in tests.
[Persistence](#persistence) describes both.

## Nondeterminism goes through the runtime

A simulation can replay a run exactly only if nothing in it depends on the real world. lockstep
gets there by routing every source of nondeterminism in actor code through the runtime:

- Message delivery: the engine decides which actor runs next and in what order messages
  arrive.
- Time: `ctx.Now`, `ctx.After` and `ctx.Every` read the engine's clock, not the wall clock.
- Randomness: `ctx.Rand` returns a stream the engine owns.
- I/O: anything that waits, such as a network call or a file write, is an effect started with
  `lockstep.Do`, and the engine decides how it runs. A persistent actor reaches its journal and
  snapshot files the same way, through effects that lockstep starts for it.

Actor code that follows this rule never starts goroutines, never blocks, and never reads
`time.Now` or global `math/rand`. `lockstep-vet` reports code that breaks it.

## Systems, actors and PIDs

A `*lockstep.System` holds the actors of one process, or of one node in a simulation. Every
system has two guardians: `/user`, the parent of actors spawned from the system, and `/system`,
for lockstep's own actors. An actor spawned from another actor's context is that actor's child.

An actor is a value that implements `Actor[M]`, where `M` is the one message type it accepts.
`Spawn` takes a factory, `func() Actor[M]`, and calls it at every start and every restart, so a
restart always begins from fresh state.

`Spawn` returns a `PID[M]`. A PID is a comparable value that names one spawned actor, across its
restarts: its node, its path, and a 64-bit `uid` unique per spawn on that node. Its string form is
`lockstep://<node><path>#<uid>`, with the `uid` in hexadecimal. A restart keeps the PID. Stopping an actor and spawning a new one
with the same name gives a new `uid`, so old PIDs never reach the new actor.

## Messages and mailboxes

`Send[M]` delivers only `M` values to a `PID[M]`, so the compiler rejects a message of the wrong
type. The usual message type is a sealed interface: an interface with an unexported marker
method, implemented by a fixed set of structs.

Local delivery passes the message as it is, without copying it. A message is safe to send when it
holds no pointers, slices, maps, channels or functions, or when the sender never touches it again
after `Send`.

Each actor has two queues:

- The *user mailbox* holds `M` messages. It is bounded: 1024 sent messages by default, set per
  actor with `WithMailbox` or per system with `WithDefaultMailbox`. Timer messages, replies that
  `lockstep.Pipe` turns into messages, and effect results are never refused and do not count
  toward the capacity.
- The *system queue* holds lifecycle commands and signals. It is processed first and is not
  limited by the mailbox capacity, so supervision works even when the user mailbox is full.

When the user mailbox is full, its policy decides what happens. `FailSend`, the default, returns
`ErrMailboxFull`. `DropNewest` and `DropOldest` drop a message and trace it. `BlockSender` makes a
sender outside the system wait for room; for a sender inside an actor, and in simulation, it acts
as `FailSend`.

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="assets/message-path-dark.svg">
    <img alt="How a message reaches Receive: a sender calls Send(from, pid, msg), and the message goes into the actor's user mailbox, bounded at 1024 by default; when it is full, FailSend makes Send return ErrMailboxFull. The actor's system queue holds Stop, Terminated and ChildFailed and is never full. A turn processes the system queue first, then user messages up to the budget, 32 on the production engine and 1 in simulation, and Receive(ctx, msg) handles one message at a time. A footer says messages from one sender to one receiver arrive in send order. From https://github.com/hmdsefi/lockstep." src="assets/message-path-light.svg">
  </picture>
</p>

A message waits in one of two queues, and a turn always drains the system queue before it takes
user messages.

Messages from one sender to one receiver arrive in send order, on one node and across nodes.
There is no order across different senders. A message to an actor that has stopped becomes a
*dead letter*: it is traced and counted, and `Send` returns nil, because the sender cannot know
whether the message was already in flight.

## Requests and replies

`Ask` sends a request that carries a typed `ReplyTo[R]` and returns a `*Reply[R]`. The reply
resolves once, with the first of an answer, a failure, a timeout (5 seconds by default), or
`ErrTerminated` when the asked actor stops without answering.

Code outside actors waits with `Reply.Await`. An actor never waits: `lockstep.Pipe` delivers the
reply to it as one of its own messages. `Await` during an actor's turn returns `ErrAwaitInActor`,
and in simulation it fails the seed.

## Turns

A *turn* is one engine call that processes messages for one actor: its system queue first, then
user messages up to a budget the engine passes in. An actor handles one message at a time, so its
fields need no lock.

The budget is where the two engines differ most. The production engine passes 32 by default, so a
busy actor yields after 32 messages. `WithThroughput` changes the limit for every actor and
`WithActorThroughput` for one. The simulation engine passes 1, so every delivery is a separate
event and each seed can order deliveries to different actors differently.

## Lifecycle and supervision

An actor starts, runs, and stops. Starting calls the factory, then `Started` if the actor
implements `Starter[M]`, and only then delivers user messages.

`Stopped` runs exactly once per incarnation: on a stop, before every restart, and after a failure.
It does not run when a simulated node crashes, because a crash discards the whole system. Its
`StopReason` says why the actor stopped. Resources registered with `ctx.Own` are closed after `Stopped` returns,
in reverse order. A stopping actor stops its children first, so no actor outlives its parent.

A panic in `Receive`, or a call to `ctx.Fail`, marks the actor failed. Its parent's strategy then
picks a directive: restart, stop, resume (drop the message and keep the state), or escalate (the
parent fails with the child's error). `OneForOne` applies the directive to the failed child and
`AllForOne` to all of the parent's children. Both allow 10 restarts per minute and back off from
10 ms to 10 s between restarts, measured on the engine's clock, so simulation runs the backoff in
virtual time. Past the limit, the directive becomes escalate. Watchers registered with `Watch`
receive a `Terminated` signal when the actor stops for good.

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="assets/supervision-restart-dark.gif">
    <img alt="Animation of a supervision tree: the /user guardian, a supervisor named payments with OneForOne, and three workers. worker-2 panics on a message; the strategy picks restart while worker-1 and worker-3 keep counting. worker-2's Stopped hook runs, a 10 ms backoff passes, and the factory builds fresh state, so its count goes from 41 to 0. The PID and mailbox survive, so the next message reaches the new incarnation. Past 10 restarts a minute, the directive becomes escalate. From https://github.com/hmdsefi/lockstep." src="assets/supervision-restart-light.gif">
  </picture>
</p>

A restart replaces the failed actor's state and keeps its PID and mailbox, so the actors that send
to it carry on without a new address.

## Effects

An effect is a value with a stable `Name()` and a `Run(ctx)` method that does the real work.
`lockstep.Do` starts it and returns at once. When it finishes, the `done` function you pass
builds a message from its result, and that message arrives in the actor's mailbox. Each effect
has a 30 second timeout by default, after which `done` receives `ErrTimeout`. Effects started by
an incarnation are cancelled when it stops or restarts.

The engines run effects in different ways:

- Production engine: `Run` runs on a bounded pool, at most 64 effects at a time by default. Up to
  1024 more wait in a queue; past that, `done` receives `ErrOverloaded`. `WithEffectPool` sets
  both limits.
- Simulation engine: `Run` is never called, because real I/O cannot replay. The test registers
  a handler per effect name with `s.HandleEffect`. The handler returns a value, an error and a
  simulated latency, and can draw them from the run's seeded random stream. An effect with no
  handler fails the seed.

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="assets/effect-round-trip-dark.svg">
    <img alt="An effect's round trip: in Receive(ctx, Buy), lockstep.Do(ctx, Charge{...}, done) returns an EffectID at once. On the production engine, Charge.Run(ctx) does the real I/O on a bounded pool of 64 running and 1024 waiting; past that, done gets ErrOverloaded. In simulation, Run is never called and the handler registered with s.HandleEffect('payment.charge', h) returns a value, an error and a latency. done(v, err) builds a charged message, which the mailbox never refuses, and a later turn handles it in Receive(ctx, charged). After 30 s by default, done gets ErrTimeout, and a stop or restart cancels pending effects. From https://github.com/hmdsefi/lockstep." src="assets/effect-round-trip-light.svg">
  </picture>
</p>

The actor never waits for an effect: the result comes back as a message, whichever engine produced
it.

lockstep also starts effects for its own packages. The storage calls of a persistent actor are
*internal effects*. Their names begin with `lockstep.persist.`, and their completion runs a
callback in the actor's next turn instead of building a message. Names that begin with `lockstep.`
are reserved, so `s.HandleEffect` cannot register one. The production engine runs an internal
effect on the same pool, with the same timeout, as a `Do` effect. The simulation engine runs an
internal effect that is marked inline without a handler: it calls `Run` itself, in a kernel event
on the actor's node. Real I/O cannot replay, but a call to the simulated disk can, so the same
store code runs on both engines. The completion arrives in a second event, after the disk latency
that the calls of `Run` sampled.

## The production engine

`lockstep.New()` builds a system on the production engine. A fixed pool of worker goroutines runs
every turn, every timer callback and every effect completion. `WithWorkers` sets the pool size,
which defaults to `runtime.GOMAXPROCS(0)` at the time `New` runs.

An actor is not a goroutine. When a message arrives for an idle actor, the engine queues the actor
and a free worker runs its turn. Turns of different actors run in parallel on different workers.
The turns of one actor never overlap, so its fields still need no lock, and messages from one
sender to one receiver are handled in send order.

Each worker has a local queue of ready actors, and one global queue is shared by all workers. A
worker that runs out of work takes some from another worker's queue. An actor that still has
messages after its throughput budget goes to the back of the global queue, so a busy actor cannot
starve the others. The [README](../README.md#configure-the-production-engine) lists the options
that configure the pool.

The production engine reads the wall clock and seeds each actor's random stream from
`crypto/rand`. It has no transport to other processes, so actors on different nodes talk only in
simulation.

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="assets/engines-dark.svg">
    <img alt="Two engines side by side. The production engine behind lockstep.New() has a global queue and workers with local queues; workers 1 and 2 run turns of actors A and B, and idle worker 3 steals half of worker 1's local queue. The simulation engine behind sim.Simulate has one kernel event queue ordered by virtual time, where the seed orders events at the same time. A table compares them: turns run in parallel on N workers or one at a time as kernel events; the turn budget is 32 messages by default or 1; time is the wall clock or virtual time with a clock per node; randomness is seeded from crypto/rand or derived from the seed; effects run on a bounded pool or in handlers from s.HandleEffect; a rerun has new timings, while a seed replays exactly. From https://github.com/hmdsefi/lockstep." src="assets/engines-light.svg">
  </picture>
</p>

Both engines run the same turns; they differ in who picks the next one and what time it is.

## The simulation engine

`sim.Simulate` wraps `faultline.Run`. Each seed is a subtest, and each run gets a fresh world with
virtual time and a simulated network. Inside it, lockstep maps its decisions onto faultline's
kernel:

- A turn is one kernel event on the actor's node.
- A timer is a kernel timer on the node's local clock, which faultline can skew and jump.
- A send to another node goes through faultline's simulated network. lockstep makes the links
  first-in, first-out, numbers the messages of each sender and receiver pair, and treats a gap as
  a loss, so faults can drop and partition but never reorder within a pair.
- An effect calls its handler in a kernel event and delivers the result after the handler's
  latency.
- `ctx.Rand` returns the faultline stream `lockstep/<node>/<epoch>/<path>/<incarnation>`, so each
  actor's randomness depends only on the seed and on who it is.
- `s.Node(name, setup)` adds a server node. Each boot builds a new system and runs `setup`. A
  crash discards the system, and a restart boots a new one.

Every runtime decision also emits a kernel record named `lockstep.<event>`, such as
`lockstep.send`, `lockstep.turn` or `lockstep.restart`. faultline writes them to `trace.jsonl` and
hashes them into the run's trace hash, so a replay that matches the hash made the same decisions
in the same order.

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="assets/sim-replay-dark.gif">
    <img alt="Animation of the README counter test on seed 0x870af07a512d3866, drawn from its trace. c1 sends Add to the counter on n1 every 250 ms of virtual time and the total reaches 19. faultline crashes n1 at 4.866 s, the 20th Add is dropped while n1 is down, and n1 restarts at 5.710 s with a new counter whose total is 0. At 9 s c1 asks for the total and gets 0, and the final check 'every add counted' fails at 10 s with trace hash 0x56d752b8033ec3c3. Running FAULTLINE_SEED=0x870af07a512d3866 go test again replays the same 183 trace records with the same hash and the same failure. From https://github.com/hmdsefi/lockstep." src="assets/sim-replay-light.gif">
  </picture>
</p>

The seed fixes every crash time, latency and delivery order, so running the seed again replays the
failure record for record.

### Faults

Faults come from two places. faultline's planners crash, restart and pause nodes, partition the
network, make links lossy or slow, and fail disk syncs. lockstep adds actor-level faults, which
replay exactly from the seed like the rest of the run:
`s.InjectFail` fails one actor, `s.StopActor` stops one, `s.DelayBetween` and `s.DropBetween`
delay or drop messages between two actors, and `s.Pressure` simulates memory pressure on a node.

### Checks

On top of the checks your test registers, `sim` fails a seed through four named faultline checks:

- `lockstep/deadlock`: the world is idle and pending asks form a cycle. The failure text starts
  with `lockstep: deadlock:` and lists the actors in the cycle. lockstep builds a wait-for graph of the
  asks and finds the cycle with [gograph](https://github.com/hmdsefi/gograph).
- `lockstep/leak`: an incarnation ended with an effect still pending, a resource it owns that
  failed to close, or a file it opened through `Volume` still open.
- `lockstep/no-effect-handler`: an actor started an effect that has no handler.
- `lockstep/misuse`: actor code broke a runtime rule, such as calling `Await` during a turn, or
  using a node's `Volume` from another node or outside a node event.

## Overload

Every system has an actor cap: 1,000,000 live actors by default, set with `WithMaxActors`. Past
it, `Spawn` returns `ErrActorCap`. One percent of the cap is kept for restarts and system actors,
so supervision keeps working at the cap.

In simulation, `sim.Options.Budget` caps live actors and their estimated memory in bytes, and
`s.Pressure` turns memory pressure on and off. While either applies, `Spawn` returns
`ErrOverloaded`, sends to user actors are refused as if their mailboxes were full, and actors that
called `ctx.SubscribeOverload` receive an `Overloaded` signal. Simulation reads no host state, so
`WithAdmission`, `WithWatchdog`, and `WithGoroutineWarning` have no effect there and the same seed
still replays the same run.

The production engine takes its overload from the live heap. A sampler runs on the engine's clock
every 100 ms and reads the live heap, and `New` takes the first sample before it returns. Without
`WithAdmission`, admission stops at 90% of the Go memory limit and resumes at 80%, and with no
limit set memory never refuses. `WithAdmission(high, low)` sets absolute byte thresholds, and
`WithAdmission(0, 0)` turns memory admission off. While admission is stopped, the system behaves
as it does in simulation: `Spawn` returns `ErrOverloaded`, each mailbox policy applies to user
messages as if the mailbox were full, and subscribers receive `Overloaded`. Restarts, system
messages, replies, timer messages, and effect results are not refused. The system logs the change
as `admission refusing` at Warn and `admission resumed` at Info.

A watchdog goroutine finds stuck turns. It has its own goroutine so that it keeps running when every
worker is stuck. It polls the production engine for the turns in progress, and reports each turn
that has run for `WithWatchdog`'s timeout (5 seconds by default) once, at Error, with the actor's
path and the stack of the worker goroutine. The actor is then stuck: user messages to it get its
mailbox policy as if the mailbox were full, while system messages such as `Stop` still queue, and a
`Stop` that queued while the turn was stuck stops the actor after the turn returns, without a
restart. Otherwise, when the turn returns, the incarnation fails with an error that matches
`ErrTimeout`, and its supervisor decides what happens next. lockstep cannot preempt a turn, so a
turn that never returns keeps its worker, and `Shutdown` with a deadline returns the context's
error.

The sampler tick also counts the process's goroutines once a second. When the count reaches
`WithGoroutineWarning`'s bound (10,000 by default), the system logs one Warn with the count and the
ten functions that hold the most goroutines, and it warns again only after the count falls below
80% of the bound. All of these events go to the logger `slog.Default()` returned when `New` ran,
with the `node` attribute. [Protect the production engine from
overload](../README.md#protect-the-production-engine-from-overload) lists the options and their
defaults.

## Persistence

An event-sourced actor stores what happened, not what its state is. The state is the events folded
in order over an initial value, so the journal of events is the source of truth and the state in
memory is a copy that recovery rebuilds.

### The persist wrapper

`persist.Spawn` takes a factory for an `EventSourced` value. `Initial` returns the state before
the first event, `Handle` turns a command into events, and `Apply` folds one event into the state.
`Spawn` starts an ordinary lockstep actor, the *wrapper*, around the value the factory returns.
The wrapper's message type is the command type, so `Send`, `Ask`, supervision and `Watch` work as
for any actor. Every storage call the wrapper makes is an internal effect, so the actor never
blocks, and each result comes back in a later turn of the same incarnation.

Each incarnation recovers at every start and restart. It pauses user messages and then:

1. Loads the newest intact snapshot, if the actor has a snapshot store. Without one, the state
   starts at `Initial`.
2. Calls the journal's `Recover`, which reports the sequence numbers the journal holds. The actor
   fails with `ErrInconsistent` if the snapshot is ahead of the journal, or if the journal starts
   after the event the snapshot needs.
3. Reads the events after the snapshot in batches, 256 by default, decodes each and folds it with
   `Apply`. Their sequence numbers must continue without a gap.
4. Calls `Recovered` if the actor has one, passes the signals that arrived meanwhile to its
   `Signal` hook, and resumes user messages.

Commands that arrive during recovery wait in the mailbox, in arrival order.

A command is one `Handle` call, which returns the events to store. The wrapper encodes them,
numbers them after the last committed event, and pauses user messages. It writes them to the
journal as one batch and then commits the batch, which is the fsync. Only after the commit does it
apply the events to the state, run the `Then` callbacks in the order they were registered, and
resume user messages. A reply sent from a callback therefore promises that its events are durable.
A `Handle` call that returns no events runs its callbacks at once.

One batch is in flight at a time. Commands wait in the mailbox behind it, so the state never
changes between a `Handle` call and the apply of its events. A `Stop` inside a `Handle` call that
returns events waits for the commit and the callbacks, and then stops the actor. A `Fail` discards
the events and the callbacks.

A journal error, a snapshot that does not load, an event that does not encode or decode, a journal
that does not fit the snapshot, or a panic in `Handle`, `Apply` or a callback fails the actor. The
events of a failed batch get no callback and no reply. The wrapper resumes user messages before the
failure, so a supervisor that resumes the actor does not leave it paused for good. Then the
supervisor decides, as for any failure:

- A restart builds a new wrapper, which recovers from the store.
- A resume keeps the incarnation, but the wrapper recovers again before it handles its next
  message, so the actor never runs on a state that the store does not hold. A command or signal
  that arrived after the failure is kept and handled after that recovery. The result of an effect
  started before the failure is dropped.

With a snapshot store, the wrapper saves a snapshot of the encoded state after every `SnapshotEvery`
committed events, and when the actor calls `Context.Snapshot`, once the events up to that point are
committed. The state is encoded in the turn, and the save runs as an internal effect beside the
commands, which are not paused. One save runs at a time. After a save, the wrapper deletes the
snapshots before the previous one and, with `PruneEvents`, the events up to the previous snapshot.
The newest two snapshots and the events after the older one stay, so a damaged newest snapshot falls
back to the older one. A state that does not encode, a failed save and a failed deletion do not fail
the actor: lockstep traces the failure, and the next snapshot that falls due tries again.

In simulation each step leaves a record in the trace: `lockstep.persist.recover`, `.write`,
`.commit`, `.snapshot` and `.delete`, and `.fail` for a failure.

### The file store

`filestore.Open(dir)` returns a `Store` on a directory of the operating system's file system, and
`filestore.New(fsys)` returns one on any `FS`. A `Store` is both the journal and the snapshot store
of an actor. It keeps its data under one root directory:

```text
FORMAT
journal/id-<enc>/<first seq>.seg
snapshots/id-<enc>/<seq>.snap
```

`FORMAT` holds the store's format version, and a directory with another version fails with
`ErrFormat`. `<enc>` is the persistence ID encoded byte by byte, so two IDs never share a
directory, even on a file system that ignores case. The sequence numbers in file names have 20
digits.

A journal segment holds records, and a record is one batch of events: a header with the payload
length and a CRC-32C checksum of the payload, then the first sequence number, the event count and
the events, each with its length. A `Write` appends one record to the last segment. Once that
segment has reached the segment size, 64 MiB by default, the next `Write` starts a segment named
after its first sequence number.

`Commit` is the durability point. It syncs, in this order, every segment written since its last
sync, then the ID's directory if a segment was created or removed, then the journal directory if
the ID's directory was created. The order covers the data and its directory entries: a new file's
bytes can reach the disk while its entry does not, and a crash then loses the file. If a `Write`
fails, the store truncates the segment to its size before the call. If a `Commit` fails, it
truncates every segment written since the last commit and removes a segment created since then
that holds no committed event. The next `Recover` reports what survived. If the repair itself fails, the ID becomes unusable and every later call for it returns
`ErrUnusable`.

`Recover` makes every event it reports durable: records that survived a crash without a sync are
synced before they count. It reads the first record of the first segment and every record of the
last segment. An invalid first record of a segment that is not the last is corruption. In the last
segment, an invalid record is either a torn tail or corruption: it is corrupt if its length is in
range, it ends before the end of the file, and a valid record follows it. Anything else is a torn
tail, because an interrupted write damages only the end of the file.
`Recover` truncates and syncs a torn tail, and `Bounds.Repaired` reports how many bytes it
discarded. Corruption returns an error that wraps `ErrCorrupt`, and the store never skips it in
the journal. `Read` validates every record it returns and reports damage the same way.

A snapshot is one file named after its sequence number that holds one checksummed record. `Save`
writes a temporary file, syncs and closes it, renames it to its final name and syncs the directory,
so a crash leaves the old snapshots or the complete new one, never half of one. `Load` tries the
files from the highest sequence number down and skips a file that fails its checksum, so a damaged
newest snapshot gives way to an older intact one. `DeleteEvents` removes whole segments whose
events are all covered, and always keeps the last segment.

The `Store` opens every file inside the call that needs it and closes it before the call returns,
so no file handle outlives a call. Calls for one persistence ID run one after another, and calls
for different IDs may overlap. Every error wraps the error of the `FS` call that caused it, so
`errors.Is` reaches the `io/fs` sentinels.

### The runtime seam

The wrapper needs two things from the runtime, and running it on a simulated disk needs a third.

- Internal effects, described under [Effects](#effects).
- Paused user delivery. An actor can pause its user messages and resume them. The system queue
  still runs while they are paused, so supervision, signals and the results of internal effects
  arrive. Both engines support it, and a new incarnation starts unpaused.
- `(*sim.Sim).Volume(node)`. It returns the simulated disk of a node as a `filestore.FS`, with
  names mapped under `/`. Every call for one node returns the same value, and the disk keeps what
  a crash left durable across incarnations, so a restarted node finds its own files. `Volume` is
  the only way a simulation reaches the disk: `persist` and `filestore` name no faultline type.

`Options.Disk` is faultline's disk configuration, and it decides how the disk fails. It chooses
what a crash does to writes that were not synced (lose them all, keep a prefix, keep a subset, or
tear the next write at a sector boundary), when a create, remove or rename becomes durable, how a
failed sync treats pending writes, and the capacity. Planners add `fault.KindSyncFail` and
`fault.KindDiskCapacity`. The recovery code that runs after such a fault is the code that runs in
production.

Disk latency comes from the simulated disk, never from the wall clock. Each `Volume` call made by a
storage operation of `persist`, except `Size`, samples the latency model of `Options.Disk.Latency`
for its class (read, write, sync or metadata) from the seeded stream, and the operation's completion event is
scheduled after the sum. With no latency configured, no draw happens.

A `Volume` belongs to its node. A call from another node's event, or from outside any node event,
fails the seed with `lockstep/misuse` and returns an error that wraps `fs.ErrPermission`, without
touching the disk. A file also belongs to the incarnation whose turn or internal effect opened it.
When that incarnation stops or restarts with the file open, the seed fails with `lockstep/leak`
and a `lockstep.leak` record names the file. The `Store` closes every file before a call returns,
so only code that bypasses it leaks one.

## Static analysis

`lockstep-vet` lives in the nested module `github.com/hmdsefi/lockstep/analysis`, because it needs
`golang.org/x/tools`, and the root module requires only faultline, gograph and, for tests,
`go.uber.org/goleak`. It
finds actor code by its `Receive`, `Started`, `Stopped` and `Signal` methods and follows the calls
they make inside the same package. Its checks are:

- `LS100`: wall-clock time, global `math/rand`, `crypto/rand`, goroutines, `select`, channel
  operations, map ranges, environment reads and blocking calls in actor code.
- `LS110`: `Reply.Await` reachable from actor code.
- `LS001` and `LS002`: malformed and unknown `//lockstep:` directives.

`LS100` ports most of faultline's determinism rules to actor code and adds the blocking calls.
The same analyzers run in golangci-lint as a
module plugin.

## Modules and the production boundary

The repository holds three Go modules:

- `github.com/hmdsefi/lockstep`, the root module: the runtime, `sim`, `persist`,
  `persist/filestore`, and internal packages for the engine seam, the production engine, mailboxes,
  supervision, the registry and overload state.
- `github.com/hmdsefi/lockstep/analysis`: `lockstep-vet` and the golangci-lint plugin.
- `github.com/hmdsefi/lockstep/examples/raft`: the Raft example.

Only `sim` imports faultline. A program that imports `lockstep`, `persist` and
`persist/filestore` and not `sim` never links faultline or gograph; CI checks this with
`go list -deps` over the production packages. `sim` reaches the simulated disk through `Volume`,
which returns a `filestore.FS`, so `persist` and `filestore` never name a faultline type. faultline
and gograph still appear in your `go.mod`, because the root module requires them for `sim`.

lockstep's own simulation path follows faultline's determinism rules, and CI checks it in three
ways. Setting `LOCKSTEP_CHECK_DETERMINISM=1` runs every simulation test twice and compares the
trace hashes. Small actor systems have golden trace hashes pinned in the repository. A lint
reports goroutines, wall-clock reads and unsorted map iteration in the runtime and persistence
packages that the simulation calls.

## Next steps

- [Why lockstep](why-lockstep.md): the problem lockstep solves and when to use it.
- [The Raft example](../examples/raft/README.md): a larger lockstep program under partitions and
  crashes.
- [Package documentation](https://pkg.go.dev/github.com/hmdsefi/lockstep): the full API.
