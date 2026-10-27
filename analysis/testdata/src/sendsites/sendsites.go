// Package sendsites holds send sites and the expressions they place in messages. A probe reports
// "send <api>" at every send site and "leaf <expr> root=<v> shared=<bool>" at every placed leaf.
package sendsites

import (
	"context"
	"os"
	"time"

	"github.com/hmdsefi/lockstep"
)

type Msg struct {
	Items []int
	Names map[string]int
	Inner *Inner
	Value Inner
	Any   any
	Count int
}

type Inner struct {
	Data []byte
}

type Pair struct {
	A, B Msg
}

type Ints []int

type holder struct {
	items []int
	inner Inner
	ptr   *Inner
	arr   [4]int
	lists [][]int
}

var global []int

func get() Msg { return Msg{} }

func clone(s []int) []int { return append([]int(nil), s...) }

type effect struct{}

func (effect) Name() string                     { return "effect" }
func (effect) Run(context.Context) (int, error) { return 0, nil }

func compositeLiterals(sys *lockstep.System, pid lockstep.PID[Msg], mpid lockstep.PID[map[*Inner][]int], items []int, keys map[string]int, key *Inner, data []byte) {
	_ = lockstep.Send(sys, pid, Msg{Items: items, Names: keys}) // want `^send lockstep\.Send$` `^leaf items root=items shared=false$` `^leaf keys root=keys shared=false$`

	_ = lockstep.Send(sys, mpid, map[*Inner][]int{key: items}) // want `^send lockstep\.Send$` `^leaf key root=key shared=false$` `^leaf items root=items shared=false$`

	_ = lockstep.Send(sys, pid, Msg{Inner: &Inner{Data: data}, Value: Inner{data}}) // want `^send lockstep\.Send$` `^leaf data root=data shared=false$` `^leaf data root=data shared=false$`
}

func wrappers(sys *lockstep.System, pid lockstep.PID[Msg], ipid lockstep.PID[Ints], items []int, v any) {
	_ = lockstep.Send(sys, pid, (Msg{Items: (items)})) // want `^send lockstep\.Send$` `^leaf items root=items shared=false$`

	_ = lockstep.Send(sys, ipid, Ints(items)) // want `^send lockstep\.Send$` `^leaf items root=items shared=false$`

	_ = lockstep.Send(sys, pid, Msg{Inner: v.(*Inner)}) // want `^send lockstep\.Send$` `^leaf v root=v shared=false$`
}

func sharedLeaves(sys *lockstep.System, pid lockstep.PID[Msg], h *holder, in Inner, items []int, n int) {
	_ = lockstep.Send(sys, pid, Msg{Inner: &in}) // want `^send lockstep\.Send$` `^leaf in root=in shared=true$`

	_ = lockstep.Send(sys, pid, Msg{Inner: &h.inner}) // want `^send lockstep\.Send$` `^leaf h\.inner root=h shared=true$`

	_ = lockstep.Send(sys, pid, Msg{Items: h.arr[1:3]}) // want `^send lockstep\.Send$` `^leaf h\.arr root=h shared=true$`

	_ = lockstep.Send(sys, pid, Msg{Items: items[:n]}) // want `^send lockstep\.Send$` `^leaf items root=items shared=true$`
}

func chains(sys *lockstep.System, pid lockstep.PID[Msg], h *holder, p *Inner, i int) {
	_ = lockstep.Send(sys, pid, Msg{Items: h.lists[i], Value: *p}) // want `^send lockstep\.Send$` `^leaf h\.lists\[i\] root=h shared=false$` `^leaf \*p root=p shared=false$`

	_ = lockstep.Send(sys, pid, Msg{Items: (*h).items, Inner: h.ptr}) // want `^send lockstep\.Send$` `^leaf \(\*h\)\.items root=h shared=false$` `^leaf h\.ptr root=h shared=false$`

	_ = lockstep.Send(sys, pid, Msg{Items: global, Names: get().Names}) // want `^send lockstep\.Send$` `^leaf global root=global shared=false$` `^leaf get\(\)\.Names root=- shared=false$`
}

func qualified(sys *lockstep.System, spid lockstep.PID[[]string]) {
	_ = lockstep.Send(sys, spid, os.Args) // want `^send lockstep\.Send$` `^leaf os\.Args root=Args shared=false$`
}

func locals(sys *lockstep.System, pid lockstep.PID[Msg], ppid lockstep.PID[Pair], cpid lockstep.PID[*[]int], items []int) {
	// m is expanded at each of its two sites.
	m := Msg{Items: items}         // want `^leaf items root=items shared=false$` `^leaf items root=items shared=false$`
	_ = lockstep.Send(sys, pid, m) // want `^send lockstep\.Send$` `^leaf m root=m shared=false$`
	_ = lockstep.Send(sys, pid, m) // want `^send lockstep\.Send$` `^leaf m root=m shared=false$`

	// Each variable is expanded once per message: items is reported once.
	p := Msg{Items: items}                         // want `^leaf items root=items shared=false$`
	_ = lockstep.Send(sys, ppid, Pair{A: p, B: p}) // want `^send lockstep\.Send$` `^leaf p root=p shared=false$` `^leaf p root=p shared=false$`

	a := items                      // want `^leaf items root=items shared=false$`
	var b = a                       // want `^leaf a root=a shared=false$`
	c := &b                         // want `^leaf b root=b shared=true$`
	_ = lockstep.Send(sys, cpid, c) // want `^send lockstep\.Send$` `^leaf c root=c shared=false$`

	d := &b
	_ = lockstep.Send(sys, pid, Msg{Items: *d}) // want `^send lockstep\.Send$` `^leaf \*d root=d shared=false$`

	twice := Msg{Items: items}
	twice = Msg{}
	_ = lockstep.Send(sys, pid, twice) // want `^send lockstep\.Send$` `^leaf twice root=twice shared=false$`

	incremented := 0
	incremented++
	_ = lockstep.Send(sys, pid, Msg{Count: incremented}) // want `^send lockstep\.Send$` `^leaf incremented root=incremented shared=false$`

	x, y := items, items                       // want `^leaf items root=items shared=false$`
	_ = lockstep.Send(sys, pid, Msg{Items: x}) // want `^send lockstep\.Send$` `^leaf x root=x shared=false$`
	_ = y

	r, err := clone(items), context.Canceled             // want `^leaf context\.Canceled root=Canceled shared=false$`
	_ = lockstep.Send(sys, pid, Msg{Items: r, Any: err}) // want `^send lockstep\.Send$` `^leaf r root=r shared=false$` `^leaf err root=err shared=false$`

	var unset []int
	_ = lockstep.Send(sys, pid, Msg{Items: unset}) // want `^send lockstep\.Send$` `^leaf unset root=unset shared=false$`
}

func noLeaves(sys *lockstep.System, pid lockstep.PID[Msg], items []int, n int) {
	_ = lockstep.Send(sys, pid, Msg{Items: clone(items), Count: n + 1})                 // want `^send lockstep\.Send$`
	_ = lockstep.Send(sys, pid, Msg{Items: append(items, 1), Count: -n})                // want `^send lockstep\.Send$`
	_ = lockstep.Send(sys, pid, Msg{Items: []int{1, 2}, Names: map[string]int{"a": 1}}) // want `^send lockstep\.Send$`
}

func replies(sys *lockstep.System, rt lockstep.ReplyTo[[]int], items []int) {
	rt.Send(sys, items) // want `^send ReplyTo\.Send$` `^leaf items root=items shared=false$`

	lockstep.ReplyTo[[]int].Send(rt, sys, items) // want `^send ReplyTo\.Send$` `^leaf items root=items shared=false$`
}

func instantiated(sys *lockstep.System, pid lockstep.PID[Msg], items []int) {
	_ = lockstep.Send[Msg](sys, pid, Msg{Items: items}) // want `^send lockstep\.Send$` `^leaf items root=items shared=false$`
}

func build(rt lockstep.ReplyTo[int]) Msg { return Msg{} }

func asks(sys *lockstep.System, pid lockstep.PID[Msg], items []int, keys map[string]int, flag bool) {
	_ = lockstep.Ask(sys, pid, func(rt lockstep.ReplyTo[int]) Msg { // want `^send lockstep\.Ask$`
		if flag {
			return Msg{Items: items} // want `^leaf items root=items shared=false$`
		}
		local := Msg{Names: keys} // want `^leaf keys root=keys shared=false$`
		nested := func() Msg { return Msg{Items: items} }
		_ = nested
		return local // want `^leaf local root=local shared=false$`
	})

	_ = lockstep.Ask(sys, pid, build) // want `^send lockstep\.Ask$`
}

func notSendSites(ctx *lockstep.Context[Msg], items []int) {
	ctx.After(time.Second, Msg{Items: items})
	ctx.Every(time.Second, Msg{Items: items})
	_ = lockstep.Do(ctx, effect{}, func(int, error) Msg { return Msg{Items: items} })
	var pending *lockstep.Reply[int]
	lockstep.Pipe(ctx, pending, func(int, error) Msg { return Msg{Items: items} })
}

func closures(sys *lockstep.System, pid lockstep.PID[Msg], items []int) {
	func() {
		_ = lockstep.Send(sys, pid, Msg{Items: items}) // want `^send lockstep\.Send$` `^leaf items root=items shared=false$`
	}()
}

// Snap is immutable, so a Snap literal places nothing: the receiver gets a safe value.
//
//lockstep:immutable
type Snap struct {
	Rows []int
}

// Open has the same field without the marker.
type Open struct {
	Rows []int
}

type Point struct {
	X, Y int
}

func safeLiterals(sys *lockstep.System, spid lockstep.PID[Snap], ppid lockstep.PID[*Snap], opid lockstep.PID[Open], xpid lockstep.PID[Point], rows []int, x int) {
	_ = lockstep.Send(sys, spid, Snap{Rows: rows})  // want `^send lockstep\.Send$`
	_ = lockstep.Send(sys, ppid, &Snap{Rows: rows}) // want `^send lockstep\.Send$`
	_ = lockstep.Send(sys, opid, Open{Rows: rows})  // want `^send lockstep\.Send$` `^leaf rows root=rows shared=false$`
	_ = lockstep.Send(sys, xpid, Point{X: x})       // want `^send lockstep\.Send$`

	s := Snap{Rows: rows}
	_ = lockstep.Send(sys, spid, s) // want `^send lockstep\.Send$` `^leaf s root=s shared=false$`
}
