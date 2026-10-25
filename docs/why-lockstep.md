# Why lockstep

lockstep turns a bug that depends on timing into a seed you can replay. This page shows the kind
of bug it is built for, what lockstep does about it, and when you are better off with something
else. The architecture page explains how it works inside.

## Bugs that depend on timing

Code that waits on a network call, a disk or another service has bugs that appear only when
events happen in a particular order. The following shop sells a limited stock. It holds a lock
while it checks and updates the stock, and releases it during the slow payment call, as you
would in real code:

```go
func (s *Shop) Buy(ctx context.Context, user string) error {
	s.mu.Lock()
	if s.stock == 0 {
		s.mu.Unlock()
		return ErrSoldOut
	}
	s.mu.Unlock() // don't hold the lock during a slow network call

	if err := s.pay(ctx, user); err != nil {
		return err
	}

	s.mu.Lock()
	s.stock--
	s.sold++
	s.mu.Unlock()
	return nil
}
```

With one item in stock, Alice and Bob can both pass the stock check while Alice's payment is in
flight, and the shop sells two items. A test that starts both buyers 30 ms apart, with a random
payment latency of 10 to 59 ms, failed in about half of the runs of `go test -race -count=20`.
The race detector reported nothing, because every access to `stock` holds the lock. The bug is in the order of events, not in
memory access.

A failing run of that test cannot be run again. The next run picks new latencies and new goroutine
timings, so it may pass, and you are left with a log line and a guess.

## The same shop as an actor

In lockstep the shop is an actor. It handles one message at a time, so its fields need no lock,
and the payment is an effect: `lockstep.Do` starts it and returns, and the result comes back later
as a message. The bug is the same, written the actor way:

```go
func (s *Shop) Receive(ctx *lockstep.Context[Msg], msg Msg) {
	switch m := msg.(type) {
	case Buy:
		if s.stock == 0 {
			return // sold out
		}
		// Charge the card, then take the item when the payment succeeds.
		lockstep.Do(ctx, Charge{User: m.User}, func(_ struct{}, err error) Msg {
			return charged{user: m.User, err: err}
		})
	case charged:
		if m.err != nil {
			return
		}
		s.stock--
		s.sold++
	case Sold:
		m.Reply.Send(ctx, s.sold)
	}
}
```

The test runs the shop in `lockstep/sim`. It replaces the payment API with a handler that answers
after a latency drawn from the seed, sends Alice's and Bob's purchases 30 ms apart, and checks how
many items were sold when the run ends:

```go
func TestOneItemTwoBuyers(t *testing.T) {
	opts := sim.Options{Options: faultline.Options{Seeds: 100, Duration: 2 * time.Second}}
	sim.Simulate(t, opts, oneItemTwoBuyers)
}

func oneItemTwoBuyers(s *sim.Sim) {
	w := s.World()
	var shop lockstep.PID[Msg]
	s.Node("shop", func(sys *lockstep.System) {
		shop, _ = lockstep.Spawn(sys, "shop", NewShop(1)) // one item in stock
	})
	s.Client("alice", func(*lockstep.System) {})
	s.Client("bob", func(*lockstep.System) {})

	// The payment API answers after 10 to 60 ms, chosen by the seed.
	s.HandleEffect("payment.charge", func(c sim.EffectCall) sim.EffectResult {
		return sim.EffectResult{Value: struct{}{}, Latency: kernel.Uniform(c.Rand(), 10*time.Millisecond, 60*time.Millisecond)}
	})

	w.RunFor(10 * time.Millisecond) // let the nodes boot

	// Alice buys first; Bob buys 30 ms later.
	_ = sim.Send[Msg](s, "alice", shop, Buy{User: "alice"})
	w.RunFor(30 * time.Millisecond)
	_ = sim.Send[Msg](s, "bob", shop, Buy{User: "bob"})
	w.RunFor(time.Second)

	sold, err := sim.Ask(s, "alice", shop, func(r lockstep.ReplyTo[int]) Msg { return Sold{Reply: r} })
	w.Final("never sell more than the stock", func() error {
		if err != nil {
			return err
		}
		if sold > 1 {
			return fmt.Errorf("sold %d items with 1 in stock", sold)
		}
		return nil
	})
}
```

The first seed fails:

```text
--- FAIL: TestOneItemTwoBuyers (0.02s)
    shop_test.go:16: faultline: running 100 seeds from base 0x53717ce391e40b92 (test name)
    --- FAIL: TestOneItemTwoBuyers/seed=0xb12302fa26394756 (0.02s)
        faultline: final check "never sell more than the stock" failed after the run at t=2.000000000s
          sold 2 items with 1 in stock
        replay:    FAULTLINE_SEED=0xb12302fa26394756 go test -v -run '^TestOneItemTwoBuyers$' example.com/shop
        artifacts: $TMPDIR/faultline-501/example.com_shop/TestOneItemTwoBuyers/b12302fa26394756/
    shop_test.go:16: faultline: stopping after failing seed 0xb12302fa26394756; 99 of 100 seeds not run (set Options.KeepGoing to run all)
FAIL
FAIL	example.com/shop	0.229s
FAIL
```

The `replay` line is a command. It runs that seed again and fails the same check with the same
message, on any machine, as often as you run it. You can step through it, add logging, and try a
fix against the exact run that failed.

The fix reserves the item before the payment and returns it if the payment fails:

```go
	case Buy:
		if s.stock == 0 {
			return // sold out
		}
		s.stock-- // reserve the item before the slow payment call
		lockstep.Do(ctx, Charge{User: m.User}, func(_ struct{}, err error) Msg {
			return charged{user: m.User, err: err}
		})
	case charged:
		if m.err != nil {
			s.stock++ // payment failed: put the item back
			return
		}
		s.sold++
```

With the fix, all 100 seeds pass. To keep the failing seed as a regression test, run the same body
on that seed alone with `sim.SimulateSeeds`:

```go
func TestOneItemTwoBuyersRegression(t *testing.T) {
	opts := sim.Options{Options: faultline.Options{Duration: 2 * time.Second}}
	sim.SimulateSeeds(t, opts, oneItemTwoBuyers, 0xb12302fa26394756)
}
```

## What makes the replay exact

The seed decides everything that could differ between two runs: the payment latencies, the order
of deliveries, and any partitions, crashes or dropped messages the test asks for. lockstep can
promise this because its runtime owns every message delivery, timer, random number and I/O call
an actor makes. [Architecture](architecture.md) explains how.

The shop's `Receive` is the same code that runs in your program on `lockstep.New()`. There,
`Charge.Run` calls the real payment API. The simulation never calls `Run`; it uses the handler the
test registered.

`lockstep-vet` keeps this true over time. It reports actor code that reads `time.Now`, uses global
randomness, starts a goroutine, ranges over a map, or blocks, because any of those would make a
seed stop replaying.

## When lockstep fits

lockstep fits code where correctness depends on the order of messages and failures:

- consensus and replication protocols, such as the [Raft example](../examples/raft/README.md);
- ledgers, inventories and matching engines, where a lost or doubled update costs money;
- services that retry, time out and fail over, where the bug is in the recovery path;
- workflows and agent orchestration, where many long-lived workers each keep state.

You get an actor framework at the same time: typed actors and PIDs, bounded mailboxes, request and
reply, timers, and supervision trees in the Erlang style.

## When to use something else

lockstep asks you to write the system as actors. That trade does not pay off everywhere:

- Existing goroutine code you do not want to restructure: lockstep simulates actors, not
  arbitrary goroutines. [faultline](https://github.com/hmdsefi/faultline), which lockstep runs on,
  tests code written as callbacks against its node API, and ships a harness for etcd/raft.
- One piece of state that many cores must update at once: an actor handles one message at a time,
  so its work runs on one worker at a time. Parallelism comes from having many actors, not from
  one.
- Actors on several machines in production: lockstep has no remoting or clustering; actors on
  different nodes talk only in simulation.
- Erlang/OTP compatibility: lockstep borrows OTP's supervision model, not its protocols.

[Limitations](../README.md#limitations) lists the rest.

## Next steps

- [README](../README.md): install lockstep and write a first actor and simulation test.
- [Architecture](architecture.md): the runtime, the engines, and how simulation maps onto
  faultline.
- [The Raft example](../examples/raft/README.md): three planted bugs that a seeded partition or
  crash exposes.
