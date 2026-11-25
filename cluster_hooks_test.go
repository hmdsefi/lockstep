// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/internal/hooks"
)

var (
	errBoom  = errors.New("boom")
	errAgain = errors.New("node down again")
)

// nodeSigActor is an any actor that logs what it receives ("recv <m>") and the signals it gets
// ("signal <%#v>"), in order.
type nodeSigActor struct{ log *[]string }

func (a nodeSigActor) Receive(_ *lockstep.Context[any], m any) {
	*a.log = append(*a.log, fmt.Sprintf("recv %v", m))
}

func (a nodeSigActor) Signal(_ *lockstep.Context[any], sig lockstep.Signal) {
	*a.log = append(*a.log, fmt.Sprintf("signal %#v", sig))
}

// nodePlainActor is an any actor without SignalReceiver that logs what it receives.
type nodePlainActor struct{ log *[]string }

func (a nodePlainActor) Receive(_ *lockstep.Context[any], m any) {
	*a.log = append(*a.log, fmt.Sprintf("recv %v", m))
}

// CLU §4.1: package lockstep sets the cluster hooks in its init, and ValidNodeName is
// engine.ValidNodeName. EngineAddress converts a lockstep.Address and panics for anything else.
func TestClusterHooksSet(t *testing.T) {
	for name, set := range map[string]bool{
		"SystemSpawner":     hooks.SystemSpawner != nil,
		"NodeDown":          hooks.NodeDown != nil,
		"NodeUp":            hooks.NodeUp != nil,
		"EngineAddress":     hooks.EngineAddress != nil,
		"ClaimCluster":      hooks.ClaimCluster != nil,
		"ReleaseCluster":    hooks.ReleaseCluster != nil,
		"CheckSpawnOptions": hooks.CheckSpawnOptions != nil,
	} {
		if !set {
			t.Errorf("hooks.%s is nil after package lockstep's init", name)
		}
	}
	for _, name := range []string{"n1", "a.b-c_d", "", "-n", "n/1", strings.Repeat("n", 65)} {
		if got, want := hooks.ValidNodeName(name), engine.ValidNodeName(name); got != want {
			t.Errorf("ValidNodeName(%q) = %v, want %v", name, got, want)
		}
	}

	a := lockstep.PIDAt[int]("n2", "/user/a").Address()
	if got, want := hooks.EngineAddress(a), (engine.Address{Node: "n2", Path: "/user/a"}); got != want {
		t.Errorf("EngineAddress(%v) = %v, want %v", a, got, want)
	}
	if got := hooks.EngineAddress(lockstep.Address{}); got != (engine.Address{}) {
		t.Errorf("EngineAddress of the zero Address = %v, want the zero engine.Address", got)
	}
	panicsWith(t, "lockstep: hooks.EngineAddress: addr has type lockstep.PID[int], want a lockstep.Address", func() {
		hooks.EngineAddress(lockstep.PIDAt[int]("n2", "/user/a"))
	})
}

// AT-MSG-32 (Phase 3 assertions): NodeDown and NodeUp are Signals. Queued with hooks.Signal they
// reach an actor with SignalReceiver only in Signal, as they are and in order, also when its
// mailbox is full; an actor without SignalReceiver ignores them, and neither Receive gets them.
// CLU §4.1, CLU-020: ClaimCluster reports true once per System and false after, also when many
// goroutines claim at once; ReleaseCluster clears the mark; each System has its own mark; both
// hooks panic on a sys that is not a *System.
func TestClaimCluster(t *testing.T) {
	a, b := newProdSystem(t, lockstep.WithNodeName("n1")), newProdSystem(t, lockstep.WithNodeName("n2"))
	if !hooks.ClaimCluster(a) || hooks.ClaimCluster(a) {
		t.Error("ClaimCluster of a = first true, then false: not so")
	}
	if !hooks.ClaimCluster(b) {
		t.Error("ClaimCluster of b after a's claim = false, want true: each System has its own mark")
	}
	hooks.ReleaseCluster(a)
	var wg sync.WaitGroup
	var mu sync.Mutex
	won := 0
	for range 16 {
		wg.Go(func() {
			if hooks.ClaimCluster(a) {
				mu.Lock()
				won++
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	if won != 1 {
		t.Errorf("%d of 16 concurrent claims after ReleaseCluster won, want 1", won)
	}
	panicsWith(t, "lockstep: hooks.ClaimCluster: sys has type <nil>, want a non-nil *lockstep.System", func() { hooks.ClaimCluster(nil) })
	panicsWith(t, "lockstep: hooks.ReleaseCluster: sys has type int, want a non-nil *lockstep.System", func() { hooks.ReleaseCluster(1) })
}

// VAC-010: CheckSpawnOptions returns the error a Spawn with the options returns (the first invalid
// option's, in argument order, wrapping ErrInvalidOption), nil for valid options, and an
// ErrInvalidOption error instead of a panic for an element that is not a SpawnOption.
func TestCheckSpawnOptions(t *testing.T) {
	sys := newProdSystem(t, lockstep.WithNodeName("n1"))
	spawn := func(opts ...lockstep.SpawnOption) error {
		_, err := lockstep.Spawn(sys, "", func() lockstep.Actor[int] { return nodePlainIntActor{} }, opts...)
		return err
	}
	as := func(opts ...lockstep.SpawnOption) []any {
		out := make([]any, len(opts))
		for i, o := range opts {
			out[i] = o
		}
		return out
	}
	valid := []lockstep.SpawnOption{lockstep.WithMailbox(8, lockstep.FailSend), lockstep.WithActorThroughput(2), lockstep.WithStrategy(lockstep.OneForOne(nil))}
	if err := hooks.CheckSpawnOptions(as(valid...)); err != nil {
		t.Fatalf("CheckSpawnOptions(valid) = %v, want nil", err)
	}
	if err := hooks.CheckSpawnOptions(nil); err != nil {
		t.Fatalf("CheckSpawnOptions(nil) = %v, want nil", err)
	}
	for name, opts := range map[string][]lockstep.SpawnOption{
		"mailbox":     {lockstep.WithMailbox(0, lockstep.FailSend)},
		"throughput":  {lockstep.WithActorThroughput(0)},
		"first wins":  {lockstep.WithActorThroughput(-1), lockstep.WithMailbox(0, lockstep.FailSend)},
		"after valid": {lockstep.WithMailbox(8, lockstep.FailSend), lockstep.WithActorThroughput(0)},
	} {
		got, want := hooks.CheckSpawnOptions(as(opts...)), spawn(opts...)
		if !errors.Is(got, lockstep.ErrInvalidOption) || want == nil || got.Error() != want.Error() {
			t.Errorf("%s: CheckSpawnOptions = %v, Spawn = %v: want the same ErrInvalidOption error", name, got, want)
		}
	}
	for name, opts := range map[string][]any{
		"nil option": {lockstep.SpawnOption(nil)},
		"nil any":    {nil},
		"wrong type": {42},
	} {
		if err := hooks.CheckSpawnOptions(opts); !errors.Is(err, lockstep.ErrInvalidOption) {
			t.Errorf("%s: CheckSpawnOptions = %v, want an ErrInvalidOption error", name, err)
		}
	}
}

type nodePlainIntActor struct{}

func (nodePlainIntActor) Receive(*lockstep.Context[int], int) {}

func TestATMSG32NodeSignals(t *testing.T) {
	var _ lockstep.Signal = lockstep.NodeDown{}
	var _ lockstep.Signal = lockstep.NodeUp{}

	n := newRemoteNode(t, "n1")
	var log, plainLog []string
	pid := spawnAt(t, n, "sub", func() lockstep.Actor[any] { return nodeSigActor{&log} }, lockstep.WithMailbox(1, lockstep.FailSend))
	plain := spawnAt(t, n, "plain", func() lockstep.Actor[any] { return nodePlainActor{&plainLog} })

	up := lockstep.NodeUp{Node: "n2", ID: 7}
	down := lockstep.NodeDown{Node: "n2", ID: 7, Left: true}
	// The mailbox is full and no turn has run: the signals still go on the system queue.
	if err := lockstep.Send(n.sys, pid, any("queued")); err != nil {
		t.Fatal(err)
	}
	if err := lockstep.Send(n.sys, pid, any("refused")); !errors.Is(err, lockstep.ErrMailboxFull) {
		t.Fatalf("Send to the full mailbox = %v, want ErrMailboxFull", err)
	}
	for _, sig := range []lockstep.Signal{up, down} {
		if !hooks.Signal(n.sys, eaddr(pid.Address()), sig) {
			t.Fatalf("hooks.Signal(%#v) to the live subscriber reported false", sig)
		}
		if !hooks.Signal(n.sys, eaddr(plain.Address()), sig) {
			t.Fatalf("hooks.Signal(%#v) to the live plain actor reported false", sig)
		}
	}
	n.eng.RunUntilIdle()
	for _, p := range []lockstep.PID[any]{pid, plain} {
		if err := lockstep.Send(n.sys, p, any("after")); err != nil {
			t.Fatal(err)
		}
	}
	n.eng.RunUntilIdle()

	want := []string{
		fmt.Sprintf("signal %#v", up),
		fmt.Sprintf("signal %#v", down),
		"recv queued",
		"recv after",
	}
	if !slices.Equal(log, want) {
		t.Errorf("subscriber log %q, want %q", log, want)
	}
	if want := []string{"recv after"}; !slices.Equal(plainLog, want) {
		t.Errorf("plain actor log %q, want %q: a signal never reaches Receive", plainLog, want)
	}
}

// startCounter is an int actor that counts its starts.
type startCounter struct{ starts *int }

func (s startCounter) Started(*lockstep.Context[int]) { *s.starts++ }

func (startCounter) Receive(*lockstep.Context[int], int) {}

// CLU §4.1, ACT-030: Spawn with hooks.SystemSpawner's Spawner spawns a child of the system
// guardian, under /system, which the system guardian supervises: with a user guardian strategy
// that stops failed children, a failed system child restarts and a failed user child stops.
func TestSystemSpawner(t *testing.T) {
	stopAll := lockstep.OneForOne(func(error) lockstep.Directive { return lockstep.DirectiveStop })
	n := newRemoteNodeWith(t, "n1", nil, []lockstep.Option{lockstep.WithGuardianStrategy(stopAll)})
	sp, ok := hooks.SystemSpawner(n.sys).(lockstep.Spawner)
	if !ok {
		t.Fatalf("hooks.SystemSpawner returned %T, want a lockstep.Spawner", hooks.SystemSpawner(n.sys))
	}

	r := len(n.eng.Records())
	var sysStarts, userStarts int
	child, err := lockstep.Spawn(sp, "member", func() lockstep.Actor[int] { return startCounter{&sysStarts} })
	if err != nil {
		t.Fatalf("Spawn with the system Spawner: %v", err)
	}
	gen, err := lockstep.Spawn(sp, "", func() lockstep.Actor[int] { return startCounter{new(int)} })
	if err != nil {
		t.Fatalf("Spawn with the system Spawner and a generated name: %v", err)
	}
	user := spawnAt(t, n, "member", func() lockstep.Actor[int] { return startCounter{&userStarts} })
	n.eng.RunUntilIdle()
	if got := child.Address().Path(); got != "/system/member" {
		t.Errorf("system child path %q, want /system/member", got)
	}
	if got := gen.Address().Path(); !strings.HasPrefix(got, "/system/") {
		t.Errorf("generated system child path %q, want one under /system/", got)
	}
	guardian := fmt.Sprintf("lockstep://n1/system#%x", lockstep.SystemGuardianUID(n.sys))
	spawns := recordsSince(n, r, "lockstep.spawn")
	if len(spawns) != 3 || spawns[0]["parent"] != guardian || spawns[1]["parent"] != guardian {
		t.Errorf("lockstep.spawn records %v, want the first two with parent %s", spawns, guardian)
	}

	r = len(n.eng.Records())
	for _, a := range []lockstep.Address{child.Address(), user.Address()} {
		if err := hooks.InjectFail(n.sys, eaddr(a), errBoom); err != nil {
			t.Fatalf("InjectFail(%v): %v", a, err)
		}
	}
	n.eng.RunUntilIdle()
	n.eng.Advance(time.Second) // the restart's backoff
	n.eng.RunUntilIdle()
	if sysStarts != 2 || userStarts != 1 {
		t.Errorf("starts: system child %d, user child %d; want 2 and 1", sysStarts, userStarts)
	}
	restarts := recordsSince(n, r, "lockstep.restart")
	if len(restarts) != 1 || restarts[0]["actor"] != child.Address().String() {
		t.Errorf("lockstep.restart records %v, want one of %v", restarts, child.Address())
	}
	if !hooks.Signal(n.sys, eaddr(child.Address()), lockstep.NodeUp{Node: "n2"}) {
		t.Error("the restarted system child is not alive")
	}
	if hooks.Signal(n.sys, eaddr(user.Address()), lockstep.NodeUp{Node: "n2"}) {
		t.Error("the failed user child is still alive under a stopping user guardian")
	}
	n.eng.RunUntilIdle()

	panicsWith(t, "lockstep: hooks.SystemSpawner: sys has type <nil>, want a non-nil *lockstep.System", func() {
		hooks.SystemSpawner(nil)
	})
	panicsWith(t, "lockstep: hooks.SystemSpawner: sys has type *lockstep.System, want a non-nil *lockstep.System", func() {
		hooks.SystemSpawner((*lockstep.System)(nil))
	})
}

// CLU §4.1, REM-294: NodeDown ends the remote watches of the node's actors with Failed(reason) and
// marks the node; a Watch of an address on a marked node then gets its Terminated at once, records
// lockstep.watch alive=false and hands the transport nothing, while sends still go to it. A second
// NodeDown replaces the reason; NodeUp clears the mark and an unmarked NodeUp does nothing. Without
// a transport Watch still returns ErrNotLocal. NodeDown of the system's own node is ignored, and a
// nil reason panics.
func TestNodeDownHook(t *testing.T) {
	n1 := newRemoteNode(t, "n1")
	var log []string
	w1 := spawnWatcher(t, n1, "w1", &log)
	w2 := spawnWatcher(t, n1, "w2", &log)
	a := lockstep.PIDAt[int]("n2", "/user/a").Address()
	c := lockstep.PIDAt[int]("n3", "/user/c").Address()
	for _, x := range []lockstep.Address{a, c} {
		if err := watchIn(t, n1, w1, x); err != nil {
			t.Fatal(err)
		}
	}
	failed := func(err error) string { return lockstep.Failed(err).String() }

	// The existing watch of a ends; the watch of n3's actor stays.
	hooks.NodeDown(n1.sys, "n2", errDown)
	n1.eng.RunUntilIdle()
	if want := []string{"/user/w1 " + a.String() + " " + failed(errDown)}; !slices.Equal(log, want) {
		t.Errorf("Terminated signals at NodeDown %q, want %q", log, want)
	}
	if got := lockstep.DownNodes(n1.sys); !slices.Equal(got, []string{"n2"}) {
		t.Errorf("down nodes %q, want [n2]", got)
	}

	// A new Watch of an address on n2, the second one in the same turn a no-op.
	log = nil
	d, r := len(n1.eng.Delivered()), len(n1.eng.Records())
	var errs [2]error
	during(t, n1, w2, func(ctx *lockstep.Context[rwFunc]) {
		errs[0] = lockstep.Watch(ctx, a)
		errs[1] = lockstep.Watch(ctx, a)
	})
	if errs[0] != nil || errs[1] != nil {
		t.Fatalf("Watch of %v on the marked node = %v, want nil", a, errs)
	}
	if want := []string{"/user/w2 " + a.String() + " " + failed(errDown)}; !slices.Equal(log, want) {
		t.Errorf("Terminated signals of the new Watch %q, want %q", log, want)
	}
	if got := len(n1.eng.Delivered()) - d; got != 0 {
		t.Errorf("Watch on the marked node handed the transport %d envelopes, want none", got)
	}
	recs := recordsSince(n1, r, "lockstep.watch")
	wantRec := map[string]string{"watcher": w2.Address().String(), "addr": a.String(), "alive": "false"}
	if len(recs) != 1 || !mapsEqual(recs[0], wantRec) {
		t.Errorf("lockstep.watch records %v, want one %v", recs, wantRec)
	}
	// The delivered Terminated consumed the watch: Watch again resolves at once again.
	log = nil
	if err := watchIn(t, n1, w2, a); err != nil {
		t.Fatal(err)
	}
	if len(log) != 1 {
		t.Errorf("Terminated signals of a repeated Watch %q, want one", log)
	}

	// Routing is unchanged: a send to the marked node goes to the transport.
	d = len(n1.eng.Delivered())
	if err := lockstep.Send(n1.sys, lockstep.PIDAt[int]("n2", "/user/a"), 5); err != nil {
		t.Fatalf("Send to the marked node: %v", err)
	}
	if got := n1.eng.Delivered()[d:]; len(got) != 1 {
		t.Errorf("Send to the marked node handed the transport %d envelopes, want 1", len(got))
	} else if p, _ := hooks.Payload(got[0].Payload); p.Kind != hooks.RemoteMsg || p.Value != 5 {
		t.Errorf("Send to the marked node handed the transport %+v, want a RemoteMsg of 5", p)
	}

	// So does an ask.
	d = len(n1.eng.Delivered())
	askDouble(n1.sys, lockstep.PIDAt[double]("n2", "/user/d"), 4)
	if got := n1.eng.Delivered()[d:]; len(got) != 1 {
		t.Errorf("Ask to the marked node handed the transport %d envelopes, want 1", len(got))
	} else if p, _ := hooks.Payload(got[0].Payload); p.Kind != hooks.RemoteAsk {
		t.Errorf("Ask to the marked node handed the transport %+v, want a RemoteAsk", p)
	}

	// A second NodeDown replaces the reason.
	log = nil
	hooks.NodeDown(n1.sys, "n2", errAgain)
	if err := watchIn(t, n1, w2, a); err != nil {
		t.Fatal(err)
	}
	if want := []string{"/user/w2 " + a.String() + " " + failed(errAgain)}; !slices.Equal(log, want) {
		t.Errorf("Terminated signals after a second NodeDown %q, want %q", log, want)
	}

	// NodeUp of an unmarked node changes nothing; NodeUp of n2 clears its mark, and Watch goes
	// to the transport again.
	hooks.NodeUp(n1.sys, "n4")
	if got := lockstep.DownNodes(n1.sys); !slices.Equal(got, []string{"n2"}) {
		t.Errorf("down nodes after NodeUp(n4) %q, want [n2]", got)
	}
	hooks.NodeUp(n1.sys, "n2")
	hooks.NodeUp(n1.sys, "n2")
	if got := lockstep.DownNodes(n1.sys); len(got) != 0 {
		t.Errorf("down nodes after NodeUp(n2) %q, want none", got)
	}
	log = nil
	d, r = len(n1.eng.Delivered()), len(n1.eng.Records())
	if err := watchIn(t, n1, w2, a); err != nil {
		t.Fatal(err)
	}
	if want := []string{"watch " + w2.Address().String() + " " + a.String()}; !slices.Equal(watchEnvelopes(t, n1, d), want) {
		t.Errorf("envelopes after NodeUp %q, want %q", watchEnvelopes(t, n1, d), want)
	}
	if recs := recordsSince(n1, r, "lockstep.watch"); len(recs) != 1 || recs[0]["alive"] != "remote" {
		t.Errorf("lockstep.watch records after NodeUp %v, want one with alive=remote", recs)
	}
	if len(log) != 0 {
		t.Errorf("Terminated signals after NodeUp %q, want none", log)
	}
	terminate(t, n1, a, w2.Address(), lockstep.Normal)
	if want := []string{"/user/w2 " + a.String() + " normal"}; !slices.Equal(log, want) {
		t.Errorf("Terminated signals %q, want %q", log, want)
	}

	// The system's own node is never marked, and its watches are not touched.
	log = nil
	local := spawnWatcher(t, n1, "local", &log)
	if err := watchIn(t, n1, w1, local.Address()); err != nil {
		t.Fatal(err)
	}
	hooks.NodeDown(n1.sys, "n1", errDown)
	n1.eng.RunUntilIdle()
	if got := lockstep.DownNodes(n1.sys); len(got) != 0 {
		t.Errorf("down nodes after NodeDown of the own node %q, want none", got)
	}
	if len(log) != 0 {
		t.Errorf("Terminated signals after NodeDown of the own node %q, want none", log)
	}
	// The watch of n3's actor is still in effect.
	terminate(t, n1, c, w1.Address(), lockstep.Normal)
	if want := []string{"/user/w1 " + c.String() + " normal"}; !slices.Equal(log, want) {
		t.Errorf("Terminated signals %q, want %q", log, want)
	}

	// Without a transport, Watch's checks come first.
	nt := noTransport(t)
	var ntLog []string
	wt := spawnWatcher(t, nt, "w", &ntLog)
	hooks.NodeDown(nt.sys, "n2", errDown)
	if err := watchIn(t, nt, wt, a); !errors.Is(err, lockstep.ErrNotLocal) {
		t.Errorf("Watch on a marked node without a transport = %v, want ErrNotLocal", err)
	}
	if len(ntLog) != 0 {
		t.Errorf("Terminated signals without a transport %q, want none", ntLog)
	}

	panicsWith(t, "lockstep: hooks.NodeDown: nil error", func() { hooks.NodeDown(n1.sys, "n2", nil) })
	panicsWith(t, "lockstep: hooks.NodeDown: nil error", func() { hooks.NodeDown(n1.sys, "n1", nil) })
	panicsWith(t, "lockstep: hooks.NodeDown: sys has type int, want a non-nil *lockstep.System", func() {
		hooks.NodeDown(1, "n2", errDown)
	})
	panicsWith(t, "lockstep: hooks.NodeUp: sys has type int, want a non-nil *lockstep.System", func() {
		hooks.NodeUp(1, "n2")
	})
	if got := lockstep.DownNodes(n1.sys); len(got) != 0 {
		t.Errorf("down nodes after the panics %q, want none", got)
	}
}

// CLU §4.1, ACT-630, ACT-640: a watch that a down mark resolved never reached the transport, so
// neither Unwatch nor the end of the incarnation hands the transport a RemoteUnwatch for it, also
// when NodeUp cleared the mark in between; Unwatch still records lockstep.unwatch, and the
// Terminated the mark queued is skipped. A watch handed to the transport still gets its
// RemoteUnwatch.
func TestNodeDownMarkedUnwatch(t *testing.T) {
	n1 := newRemoteNode(t, "n1")
	a := lockstep.PIDAt[int]("n2", "/user/a").Address()
	c := lockstep.PIDAt[int]("n3", "/user/c").Address()

	t.Run("unwatch", func(t *testing.T) {
		var log []string
		w := spawnWatcher(t, n1, "u", &log)
		hooks.NodeDown(n1.sys, "n2", errDown)
		t.Cleanup(func() { hooks.NodeUp(n1.sys, "n2") })
		d, r := len(n1.eng.Delivered()), len(n1.eng.Records())
		during(t, n1, w, func(ctx *lockstep.Context[rwFunc]) {
			if err := lockstep.Watch(ctx, a); err != nil {
				t.Error(err)
			}
			lockstep.Unwatch(ctx, a)
		})
		// NodeUp between the Watch and the Unwatch: the watch was still resolved by the mark.
		during(t, n1, w, func(ctx *lockstep.Context[rwFunc]) {
			if err := lockstep.Watch(ctx, a); err != nil {
				t.Error(err)
			}
			hooks.NodeUp(n1.sys, "n2")
			lockstep.Unwatch(ctx, a)
		})
		if got := watchEnvelopes(t, n1, d); len(got) != 0 {
			t.Errorf("envelopes %q, want none", got)
		}
		if recs := recordsSince(n1, r, "lockstep.unwatch"); len(recs) != 2 || recs[0]["addr"] != a.String() {
			t.Errorf("lockstep.unwatch records %v, want two for %v", recs, a)
		}
		if len(log) != 0 {
			t.Errorf("Terminated signals after Unwatch %q, want none", log)
		}
	})

	t.Run("restart", func(t *testing.T) {
		var log []string
		w := spawnWatcher(t, n1, "r", &log)
		if err := watchIn(t, n1, w, c); err != nil {
			t.Fatal(err)
		}
		hooks.NodeDown(n1.sys, "n2", errDown)
		t.Cleanup(func() { hooks.NodeUp(n1.sys, "n2") })
		d := len(n1.eng.Delivered())
		during(t, n1, w, func(ctx *lockstep.Context[rwFunc]) {
			if err := lockstep.Watch(ctx, a); err != nil {
				t.Error(err)
			}
			panic("boom")
		})
		want := []string{"unwatch " + w.Address().String() + " " + c.String()}
		if got := watchEnvelopes(t, n1, d); !slices.Equal(got, want) {
			t.Errorf("envelopes at the restart %q, want %q", got, want)
		}
		if len(log) != 0 {
			t.Errorf("Terminated signals after the restart %q, want none", log)
		}
	})

	t.Run("stop", func(t *testing.T) {
		var log []string
		w := spawnWatcher(t, n1, "s", &log)
		if err := watchIn(t, n1, w, c); err != nil {
			t.Fatal(err)
		}
		hooks.NodeDown(n1.sys, "n2", errDown)
		t.Cleanup(func() { hooks.NodeUp(n1.sys, "n2") })
		d := len(n1.eng.Delivered())
		// The stop is queued before the Terminated the mark queues, so the incarnation ends first.
		during(t, n1, w, func(ctx *lockstep.Context[rwFunc]) {
			if err := lockstep.Stop(ctx, ctx.Self().Address()); err != nil {
				t.Error(err)
			}
			if err := lockstep.Watch(ctx, a); err != nil {
				t.Error(err)
			}
		})
		want := []string{"unwatch " + w.Address().String() + " " + c.String()}
		if got := watchEnvelopes(t, n1, d); !slices.Equal(got, want) {
			t.Errorf("envelopes at the stop %q, want %q", got, want)
		}
		if len(log) != 0 {
			t.Errorf("Terminated signals after the stop %q, want none", log)
		}
	})
}

// CLU §4.1, ACT-630, ACT-640: NodeDown hands the transport a RemoteUnwatch for every watch it
// ends, with the watch's number, in ascending (watched, watcher) text order and before the
// Terminated signals run, so that the transport and the peer drop the watch too. A watch it ended
// gets no second RemoteUnwatch from an Unwatch or the end of the incarnation that comes before its
// Terminated is delivered. The watches of other nodes' actors are untouched.
func TestNodeDownUnwatches(t *testing.T) {
	n1 := newRemoteNode(t, "n1")
	a := lockstep.PIDAt[int]("n2", "/user/a").Address()
	b := lockstep.PIDAt[int]("n2", "/user/b").Address()
	c := lockstep.PIDAt[int]("n3", "/user/c").Address()
	failed := lockstep.Failed(errDown).String()

	t.Run("delivered", func(t *testing.T) {
		var log []string
		w1 := spawnWatcher(t, n1, "d1", &log)
		w2 := spawnWatcher(t, n1, "d2", &log)
		for _, x := range []lockstep.Address{b, a, c} {
			if err := watchIn(t, n1, w1, x); err != nil {
				t.Fatal(err)
			}
		}
		if err := watchIn(t, n1, w2, a); err != nil {
			t.Fatal(err)
		}
		d := len(n1.eng.Delivered())
		hooks.NodeDown(n1.sys, "n2", errDown)
		t.Cleanup(func() { hooks.NodeUp(n1.sys, "n2") })
		if len(log) != 0 {
			t.Errorf("Terminated signals ran inside NodeDown: %q", log)
		}
		want := []string{
			"unwatch " + w1.Address().String() + " " + a.String(),
			"unwatch " + w2.Address().String() + " " + a.String(),
			"unwatch " + w1.Address().String() + " " + b.String(),
		}
		if got := watchEnvelopes(t, n1, d); !slices.Equal(got, want) {
			t.Errorf("envelopes at NodeDown %q, want %q", got, want)
		}
		checkUnwatchSeqs(t, n1, d)
		n1.eng.RunUntilIdle()
		// The two watchers run in the engine's order, so the signals are compared as a set.
		slices.Sort(log)
		wantLog := []string{
			"/user/d1 " + a.String() + " " + failed,
			"/user/d1 " + b.String() + " " + failed,
			"/user/d2 " + a.String() + " " + failed,
		}
		if !slices.Equal(log, wantLog) {
			t.Errorf("Terminated signals %q, want %q", log, wantLog)
		}

		// The end of the incarnation unwatches only c.
		d = len(n1.eng.Delivered())
		during(t, n1, w1, func(*lockstep.Context[rwFunc]) { panic("boom") })
		want = []string{"unwatch " + w1.Address().String() + " " + c.String()}
		if got := watchEnvelopes(t, n1, d); !slices.Equal(got, want) {
			t.Errorf("envelopes after the delivery %q, want %q", got, want)
		}
	})

	t.Run("unwatch", func(t *testing.T) {
		var log []string
		w := spawnWatcher(t, n1, "u", &log)
		if err := watchIn(t, n1, w, a); err != nil {
			t.Fatal(err)
		}
		d := len(n1.eng.Delivered())
		// NodeDown in the watcher's turn queues the Terminated behind it, so Unwatch comes first.
		during(t, n1, w, func(ctx *lockstep.Context[rwFunc]) {
			hooks.NodeDown(n1.sys, "n2", errDown)
			lockstep.Unwatch(ctx, a)
		})
		t.Cleanup(func() { hooks.NodeUp(n1.sys, "n2") })
		want := []string{"unwatch " + w.Address().String() + " " + a.String()}
		if got := watchEnvelopes(t, n1, d); !slices.Equal(got, want) {
			t.Errorf("envelopes %q, want %q from NodeDown only", got, want)
		}
		checkUnwatchSeqs(t, n1, d)
		if len(log) != 0 {
			t.Errorf("Terminated signals after Unwatch %q, want none", log)
		}
	})

	t.Run("restart", func(t *testing.T) {
		var log []string
		w := spawnWatcher(t, n1, "r", &log)
		for _, x := range []lockstep.Address{a, c} {
			if err := watchIn(t, n1, w, x); err != nil {
				t.Fatal(err)
			}
		}
		d := len(n1.eng.Delivered())
		during(t, n1, w, func(*lockstep.Context[rwFunc]) {
			hooks.NodeDown(n1.sys, "n2", errDown)
			panic("boom")
		})
		t.Cleanup(func() { hooks.NodeUp(n1.sys, "n2") })
		want := []string{
			"unwatch " + w.Address().String() + " " + a.String(),
			"unwatch " + w.Address().String() + " " + c.String(),
		}
		if got := watchEnvelopes(t, n1, d); !slices.Equal(got, want) {
			t.Errorf("envelopes %q, want %q: NodeDown's, then the restart's of c only", got, want)
		}
		checkUnwatchSeqs(t, n1, d)
		if len(log) != 0 {
			t.Errorf("Terminated signals after the restart %q, want none", log)
		}
	})
}

// checkUnwatchSeqs checks that every RemoteUnwatch n's transport got from index from on carries the
// number of the last RemoteWatch of its pair.
func checkUnwatchSeqs(t *testing.T, n *remoteNode, from int) {
	t.Helper()
	all := n.eng.Delivered()
	for i := from; i < len(all); i++ {
		p, ok := hooks.Payload(all[i].Payload)
		if !ok || p.Kind != hooks.RemoteUnwatch {
			continue
		}
		var want uint64
		for _, env := range all[:i] {
			if q, ok := hooks.Payload(env.Payload); ok && q.Kind == hooks.RemoteWatch && env.From == all[i].From && env.To == all[i].To {
				want = q.Seq
			}
		}
		if want == 0 || p.Seq != want {
			t.Errorf("RemoteUnwatch from %v to %v carries %d, want %d, the number of its RemoteWatch", all[i].From, all[i].To, p.Seq, want)
		}
	}
}

// lockedTransport counts what it delivers; it is safe for concurrent use.
type lockedTransport struct {
	mu  sync.Mutex
	got int
}

func (l *lockedTransport) Deliver(engine.Envelope) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.got++
	return nil
}

// raceWatcher watches each address it receives and hands on every Terminated it gets.
type raceWatcher struct {
	terms chan<- lockstep.Terminated
	errs  chan<- error
}

func (w raceWatcher) Receive(ctx *lockstep.Context[lockstep.Address], a lockstep.Address) {
	if err := lockstep.Watch(ctx, a); err != nil {
		w.errs <- err
	}
}

func (w raceWatcher) Signal(_ *lockstep.Context[lockstep.Address], sig lockstep.Signal) {
	if t, ok := sig.(lockstep.Terminated); ok {
		w.terms <- t
	}
}

// CLU §4.1, -race: on a production system a Watch in an actor's turn that races NodeDown of the
// watched address's node is either ended by NodeDown or finds the mark: either way the watcher
// gets exactly one Terminated with Failed(reason), and no watch is left waiting on the transport.
// NodeDown runs off the engine loop here, against the hook's contract, to force the race on the
// system's side; that is harmless only because lockedTransport, unlike the remoting core, keeps no
// watches.
func TestNodeDownWatchRace(t *testing.T) {
	sys := newProdSystem(t, lockstep.WithNodeName("n1"))
	if _, err := hooks.AttachTransport(sys, &lockedTransport{}, func() {}); err != nil {
		t.Fatal(err)
	}
	terms := make(chan lockstep.Terminated, 4)
	errs := make(chan error, 4)
	pid, err := lockstep.Spawn(sys, "w", func() lockstep.Actor[lockstep.Address] { return raceWatcher{terms, errs} })
	if err != nil {
		t.Fatal(err)
	}
	for i := range 200 {
		node := fmt.Sprintf("p%d", i)
		a := lockstep.PIDAt[int](node, "/user/a").Address()
		var wg sync.WaitGroup
		wg.Go(func() {
			if err := lockstep.Send(sys, pid, a); err != nil {
				t.Error(err)
			}
		})
		wg.Go(func() { hooks.NodeDown(sys, node, errDown) })
		wg.Wait()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		select {
		case term := <-terms:
			if term.Addr != a || term.Reason.Kind != lockstep.StopFailed || !errors.Is(term.Reason.Err, errDown) {
				t.Fatalf("iteration %d: Terminated %+v, want one of %v with Failed(%v)", i, term, a, errDown)
			}
		case err := <-errs:
			t.Fatalf("iteration %d: Watch(%v): %v", i, a, err)
		case <-ctx.Done():
			t.Fatalf("iteration %d: no Terminated for %v: the watch was left waiting on the transport", i, a)
		}
		cancel()
		hooks.NodeUp(sys, node)
	}
	select {
	case term := <-terms:
		t.Errorf("an extra Terminated %+v", term)
	default:
	}
}
