# An order service on lockstep

Run an order service through a seeded simulation and watch it charge a card twice, or charge an
order that the customer cancelled. Then see the fixed service pass 200 seeds. The example goes with
[Message delivery](../../docs/delivery.md), which explains the delivery guarantees behind both
bugs and the patterns that fix them.

The service has two actors:

- `Orders` creates, charges, cancels and refunds orders. The order ID is an idempotency key, and a
  Cancel for an order it has not seen leaves a tombstone.
- `Checkout` runs a fraud check on each order, asks `Orders` to create it, and asks again when the
  ask times out.

Charges, refunds and fraud checks are effects. In the tests, handlers answer them after a latency
drawn from the seed and count every charge and refund.

## Run it

You need Go 1.26 or later and a clone of this repository:

```sh
go version
```

The module builds against the lockstep code in the same checkout, so run it from
`examples/orders`:

```sh
cd examples/orders
go test ./...
```

```text
ok  	github.com/hmdsefi/lockstep/examples/orders	0.994s
```

## Plant a bug

`ORDERS_BUG` turns off one defense, and the scenario tests then fail on a seed:

| `ORDERS_BUG` | Test that fails | Final check |
|---|---|---|
| `no-dedup` | `TestRetry` | `charge each order at most once` |
| `drop-early-cancel` | `TestCancelBeforeCreate` | `a cancelled order ends up not charged` |

```sh
ORDERS_BUG=no-dedup go test -run '^TestRetry$' .
```

Keep `ORDERS_BUG` set when you run the `replay` command that the failure prints.
[Find both bugs with a seed](../../docs/delivery.md#find-both-bugs-with-a-seed) shows the output
and walks through each trace.

## The tests

- `TestRetry` and `TestCancelBeforeCreate` run 200 seeds of each scenario.
- `TestRetryRegression` and `TestCancelBeforeCreateRegression` run the seed that found each bug.
- `TestPlantedBugs` plants each bug in a child process and checks that the pinned seed is still
  the first to fail.
- `TestProduction` runs the same actors on `lockstep.New()` with a slow payment provider.

## Next steps

- [Message delivery](../../docs/delivery.md): the guarantees behind both bugs, and the patterns
  that fix them.
- [The Raft example](../raft/README.md): a larger lockstep program under partitions and crashes.
