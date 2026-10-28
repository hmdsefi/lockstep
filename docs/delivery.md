# Message delivery

Use this page to make your actors handle a request that arrives twice, and a Cancel message
that arrives before the Create it cancels. It says what lockstep promises about message order
and copies, shows a pattern for each case, and finds both bugs in an order service from a
seed.

## What lockstep guarantees

lockstep delivers each message at most once, and in send order between one sender and one
receiver. The following table lists the guarantees:

| Situation | What happens |
|---|---|
| One sender, one receiver | Messages arrive in send order, on one node and across nodes. |
| Several senders, one receiver | No order between senders. |
| Copies | At most one. The runtime never duplicates a message. |
| A message lost across nodes | It stays lost. Later messages of the same pair still arrive, in order. |
| The receiver has stopped | The message becomes a dead letter, and `Send` returns nil. |
| The receiver's mailbox is full | Under `FailSend`, the default, `Send` returns `ErrMailboxFull` and nothing is queued. |
| An `Ask` gets no answer | The reply fails with `ErrTimeout` after 5 seconds by default. |
| A failed send or ask | Nothing is retried. |

A sender is an actor, or a goroutine that sends from outside the actors. lockstep keeps no
table of seen messages and no reorder buffer, so exactly-once handling is the application's job.
Messages between nodes travel through the simulated network: the production engine has no
transport to other processes. In a simulation, a partition, a lossy link or a crashed node drops
messages but never reorders the messages of a pair.

A message from another node that finds a full mailbox is refused after its `Send` has returned
nil. lockstep records the refusal. An `Ask` request refused this way fails back to its asker,
but a plain message is gone without an error. [Architecture](architecture.md#messages-and-mailboxes)
describes mailboxes, their policies and dead letters.

## Where duplicates come from

The runtime never sends a message twice, so every duplicate comes from application code that
sends the same request again. The usual source is a retry after an `Ask` timeout. `ErrTimeout`
means that no answer arrived in time, not that the request was lost. The request may still wait
in a mailbox, its work may be in progress, or its answer may be on the way.

The order service in [`examples/orders`](../examples/orders) has a Checkout actor that asks
Orders to create an order, and asks again when the ask times out:

```go
case answered:
	if errors.Is(m.err, lockstep.ErrTimeout) && m.attempt < c.cfg.Attempts {
		c.create(ctx, m.order, m.attempt+1) // same ID: Orders can tell it is a retry
	}
```

If Orders charges the card for every Create, a slow payment provider makes it charge twice.
Other sources are a customer who submits a form twice and an upstream queue that delivers at
least once.

## Why a Cancel can arrive before its Create

Send order holds only for one sender and one receiver. A Cancel can still reach the receiver
before the Create that the client sent first:

- The two take different routes. The Create goes through Checkout, or through a router, and
  the Cancel goes to Orders directly. They are two pairs with different senders, so nothing
  orders them.
- The Create leaves later. Checkout sends the Create from the callback of an effect, such as
  a fraud check, or of a `Pipe` that turns an ask's reply into a message. A Cancel that Checkout forwards at once leaves first, even
  though both go from Checkout to Orders.
- The Create is lost. A full mailbox refuses it, or a partition drops it between nodes, and the
  Cancel sent after it gets through. The Create reaches the receiver only if someone sends it
  again.
- The receiver restarts between them. A restart builds the actor from its factory, so the
  record the Create made is gone. Queued messages survive the restart. Results of effects and
  asks that the actor started before the restart are dropped.

In each case the receiver gets a Cancel for an order it does not know. If it ignores that
Cancel, the Create that comes next places an order the customer cancelled.

## Patterns

Idempotency keys handle duplicates, and tombstones handle a Cancel that comes first. Most
services need both. A single route or a version per entity removes the race instead. Package
[`dedup`](../dedup) holds the state for each pattern: a `Window` of keys, a `Replies` cache of
first replies, and a `Versions` gate per entity.

### Idempotency keys

An *idempotency key* is an ID that the client picks once per request and sends again with every
retry. The receiver records each key it has handled and answers a repeat from that record instead
of doing the work again. Orders uses the order ID as the key and keeps it in two places, because
a retry arrives either while the charge is in flight or after it ended:

- While the charge is in flight, the order's record in the `orders` map holds the key. A retry
  joins the record's waiters and gets the `Result` when the charge ends.
- When the charge ends, Orders stores the `Result` in `settled`, a `dedup.Replies`, which keeps
  the first reply to each key. A retry that arrives later gets that first `Result`.

The following code is from `orders.go`, with the bug switch left out:

```go
func (s *Orders) create(ctx *lockstep.Context[Msg], m Create) {
	if r, ok := s.settled.Get(ctx.Now(), m.ID); ok {
		m.Reply.Send(ctx, r) // a retry of an order that has ended: the first Result
		return
	}
	if o, ok := s.orders[m.ID]; ok {
		// A retry of a live order: wait for its charge, or answer Created.
		if o.status == Pending {
			o.waiters = append(o.waiters, m.Reply)
			return
		}
		m.Reply.Send(ctx, Result{ID: m.ID, Status: o.status})
		return
	}
	// ... the tombstone check, described under Tombstones
	s.orders[m.ID] = &order{amount: m.Amount, status: Pending, waiters: []lockstep.ReplyTo[Result]{m.Reply}}
	// ... start the Charge effect
}
```

Record the order when the charge starts, not when it ends. A retry can arrive while the first
charge is still in flight, and then it must wait for that charge instead of starting another one.

When the charge ends, `charged` stores the `Result` and decides what stays in `orders`:

```go
func (s *Orders) charged(ctx *lockstep.Context[Msg], m charged) {
	// ... set o.status from the charge: Failed, Cancelled or Created
	res := Result{ID: m.id, Status: o.status}
	s.settled.Put(ctx.Now(), m.id, res)
	if o.status != Created {
		delete(s.orders, m.id)
	}
	// ... answer every waiter with res
}
```

A Failed or Cancelled order has nothing left to refund, so its record goes and `settled` answers
for it. A Created order keeps its record, because a later Cancel needs its amount. The record is
the order's own state, which `dedup` does not bound: a service of real size keeps it in a database
or a persistent actor.

### Tombstones

A *tombstone* is a record of a Cancel for an entity that the receiver has not seen. When the
Create arrives later, the receiver finds the tombstone and refuses the Create. Orders keeps its
tombstones in `tombs`, a `dedup.Window`: `Add` records an order ID, and `Contains` asks whether
an ID is recorded. The late Create finds its ID in `tombs`, gets the answer `Cancelled`, and
charges nothing. The following code is from `orders.go`, with the bug switch left out. `create`
checks `tombs` after the two checks above, and `cancel` fills it:

```go
func (s *Orders) create(ctx *lockstep.Context[Msg], m Create) {
	// ... answer a retry from settled or from the order's record
	if s.tombs.Contains(ctx.Now(), m.ID) {
		// The Cancel came first: refuse the Create and charge nothing.
		m.Reply.Send(ctx, Result{ID: m.ID, Status: Cancelled})
		return
	}
	// ... record the order and start the Charge effect
}
```

```go
func (s *Orders) cancel(ctx *lockstep.Context[Msg], m Cancel) {
	o, seen := s.orders[m.ID]
	switch {
	case !seen:
		s.tombs.Add(ctx.Now(), m.ID)
	case o.status == Pending:
		o.cancel = true // refund once the charge completes
	case o.status == Created:
		delete(s.orders, m.ID)
		s.refund(ctx, m.ID, o.amount)
	}
}
```

A tombstone must stay in `tombs` for as long as its Create can still arrive. The next section
says how long that is.

### Size the window

`dedup.Config` bounds `settled` and `tombs` by age and by count, and both bounds must fit your
retries. Orders builds one `Config` in `New`:

```go
cfg := dedup.Config{Capacity: 1024, TTL: time.Second}
```

`TTL` is how long an entry stays visible after it was written. It must be longer than the *retry
horizon*, the longest time from the first handling of a request to the handling of its last copy.
For a caller that makes `n` attempts, the horizon is at most the timeouts and backoff waits before
the last attempt, added up, plus the longest time a request takes from send to handling.
Checkout makes three attempts with a timeout of 100 ms and no backoff, and the simulated network
adds up to 5 ms. Orders therefore handles a retry within 2 x 100 + 5 = 205 ms of the moment
Checkout sends the first Create.

A tombstone needs longer. A Cancel can reach Orders before Checkout has sent the first Create,
because the submit takes up to 5 ms to reach Checkout and its fraud check up to 50 ms. A
tombstone must last 5 + 50 + 205 = 260 ms. One second covers both.

`Capacity` must hold every key written within one `TTL`. Orders sets 1024, which is room for 1024
orders per second. A full value removes its oldest entry to make room for a new one, so that key
stops being visible before its `TTL` ends.

A key that leaves early, by age or by capacity, lets a late copy through as a new request. If
`settled` has forgotten an order that ended Failed or Cancelled, a late retry is charged again,
and if `tombs` has forgotten a Cancel, the late Create is charged.

The values live in the actor's memory, so a restart forgets them, and the copies that arrive
after it are handled as new. To keep them across restarts, run the actor as a
[persistent actor](../README.md#persistent-actors-state-that-survives-a-restart) and keep the
values in its state. They encode to JSON, so snapshots hold them. Write to a value in `Apply`,
with a time that the event carries, and only read it in `Handle`. Recovery then rebuilds the
value the actor had.

### One route, or a version, per entity

When every message about one entity goes from the same sender to the same receiver, send order
does the work. Send the Cancel through Checkout too, and let Checkout order the two messages.
The example's Checkout does not take cancellations. In this sketch of one that does, a
`CancelOrder` that arrives during the fraud check stops the Create:

```go
case CancelOrder:
	if p, ok := c.screening[m.ID]; ok {
		p.cancelled = true // the Create is not sent yet: never send it
		return
	}
	if err := lockstep.Send(ctx, c.cfg.Orders, Msg(Cancel{ID: m.ID})); err != nil {
		// ... the mailbox refused it: try again from a timer, or report it
	}
```

When the messages for one entity must come from several senders, give each change a version
number from one source, such as the client. The receiver applies a message only if it is newer
than what it has. A version that is too old is a duplicate or a stale message, and the receiver
drops it. A `dedup.Versions` keeps the highest version per entity: `Accept` records a version
and returns true only if it is higher than the one held. Another sketch, with a `versions` field
of type `*dedup.Versions[string]`:

```go
case Update:
	if !s.versions.Accept(ctx.Now(), m.ID, m.Version) {
		return // a duplicate, or older than the newest update of this order
	}
	// ... apply the change
```

`Versions` forgets an entity as a `Window` forgets a key, and an order it has forgotten accepts
any version. Set `TTL` longer than the time an update can take to arrive, retries included, and
`Capacity` to the number of orders updated within one `TTL`. If the actor keeps a record per
order anyway, store the version in the record and compare it there.

## Find both bugs with a seed

[`examples/orders`](../examples/orders) runs the order service in `lockstep/sim`, which runs on
[faultline](https://github.com/hmdsefi/faultline), so seeds and replay commands use faultline's
names. Orders runs on node `orders`, Checkout on node `web`, and the test sends from a client
node `shopper`. The payment provider is a set of effect handlers. They answer after a latency
drawn from the seed: 5 to 50 ms for the fraud check, and 20 to 200 ms for a charge or a refund.
Checkout's ask times out after 100 ms and tries three times in all. The handlers count every charge and refund, and
two final checks read the counts:

- `charge each order at most once`;
- `a cancelled order ends up not charged`.

`orders.Bugs` turns off one defense at a time, and the tests read it from the `ORDERS_BUG`
environment variable. Without it, both scenarios pass 200 seeds each:

```sh
cd examples/orders
go test -run '^(TestRetry|TestCancelBeforeCreate)$' -v . | grep -E '^(--- |ok)'
```

```text
--- PASS: TestRetry (0.01s)
--- PASS: TestCancelBeforeCreate (0.01s)
ok  	github.com/hmdsefi/lockstep/examples/orders	0.257s
```

### A retry that charges twice

`TestRetry` submits order `o1` once. With the idempotency check turned off, the first seed fails:

```sh
ORDERS_BUG=no-dedup go test -run '^TestRetry$' .
```

```text
--- FAIL: TestRetry (0.01s)
    sim_test.go:37: faultline: running 200 seeds from base 0x087cc5272fcdf319 (test name)
    --- FAIL: TestRetry/seed=0x56247cd01be260d5 (0.01s)
        faultline: final check "charge each order at most once" failed after the run at t=2.000000000s
          order o1 charged 2 times
        replay:    FAULTLINE_SEED=0x56247cd01be260d5 go test -v -run '^TestRetry$' github.com/hmdsefi/lockstep/examples/orders
        artifacts: $TMPDIR/faultline-501/github.com_hmdsefi_lockstep_examples_orders/TestRetry/56247cd01be260d5/
    sim_test.go:37: faultline: stopping after failing seed 0x56247cd01be260d5; 199 of 200 seeds not run (set Options.KeepGoing to run all)
FAIL
FAIL	github.com/hmdsefi/lockstep/examples/orders	0.241s
FAIL
```

The trace in the artifacts directory shows what happened, in virtual time. The first Create
reaches Orders at 38.5 ms, with sequence number 1, and Orders starts a charge. The charge takes
171 ms. Checkout's ask times out at 136.4 ms, and Checkout asks again. The second Create arrives
at 138.5 ms with sequence number 2, in send order, and Orders starts a second charge. Both
charges succeed.

To replay it, keep `ORDERS_BUG` set and add the seed from the `replay` line:

```sh
ORDERS_BUG=no-dedup FAULTLINE_SEED=0x56247cd01be260d5 go test -v -run '^TestRetry$' .
```

### A Cancel lost before its Create

`TestCancelBeforeCreate` submits order `o1` to Checkout and, 20 ms later, sends a Cancel straight
to Orders. With tombstones turned off, the second seed fails:

```sh
ORDERS_BUG=drop-early-cancel go test -run '^TestCancelBeforeCreate$' .
```

```text
--- FAIL: TestCancelBeforeCreate (0.01s)
    sim_test.go:48: faultline: running 200 seeds from base 0x2a6be864d313f470 (test name)
    --- FAIL: TestCancelBeforeCreate/seed=0x144668b471a38445 (0.01s)
        faultline: final check "a cancelled order ends up not charged" failed after the run at t=2.000000000s
          order o1 was cancelled but is still charged: charges=1 refunds=0
        replay:    FAULTLINE_SEED=0x144668b471a38445 go test -v -run '^TestCancelBeforeCreate$' github.com/hmdsefi/lockstep/examples/orders
        artifacts: $TMPDIR/faultline-501/github.com_hmdsefi_lockstep_examples_orders/TestCancelBeforeCreate/144668b471a38445/
    sim_test.go:48: faultline: stopping after failing seed 0x144668b471a38445; 198 of 200 seeds not run (set Options.KeepGoing to run all)
FAIL
FAIL	github.com/hmdsefi/lockstep/examples/orders	0.308s
FAIL
```

On this seed the fraud check runs from 13.7 ms to 49.5 ms. The shopper's Cancel reaches Orders at
31.5 ms, while the check still runs. The Create reaches Orders at 54.3 ms. Both messages carry
sequence number 1, because each is the first message of its own pair. Orders had not seen `o1`
when the Cancel came, so it dropped the Cancel and then charged the order. The ask also timed out
on this seed. The retry reached Orders at 152.4 ms, while the charge was still in flight, found
the order's record, and waited for that charge instead of starting another.

### Pin the seeds

Each failing seed becomes a regression test that runs the same scenario on that seed alone:

```go
func TestRetryRegression(t *testing.T) {
	sim.SimulateSeeds(t, options(0), retry(bugs(t)), retrySeed)
}
```

Without `ORDERS_BUG`, the regression tests pass. `TestPlantedBugs` plants each bug in a child
process and runs the scenario and its regression test. It checks that the pinned seed is still
the first to fail, with the same check. If a change to the example moves the first failing
seed, that test names the new one.

## Next steps

- [Architecture](architecture.md): mailboxes, asks and the simulated network in more detail.
- [Why lockstep](why-lockstep.md): the kind of bug a seeded simulation finds, and when to use
  something else.
- [Package documentation](https://pkg.go.dev/github.com/hmdsefi/lockstep): `Send`, `Ask`, `Pipe`
  and the mailbox policies.
