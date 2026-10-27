// Package bad holds the LS200 findings: uses of a message's references after the send that gave
// them away (ANL-200 to ANL-230, AT-ANL-13). Each case is its own function.
package bad

import "github.com/hmdsefi/lockstep"

type Batch struct{ Items []int }

type Point struct{ X, Y int }

type Move struct{ P *Point }

type Chunk struct{ S []int }

type Get struct {
	Keys    []string
	ReplyTo lockstep.ReplyTo[int]
}

type Count struct{ N *int }

type Index struct{ Names map[string]int }

type Row struct{ ID int }

type Query struct{ ReplyTo lockstep.ReplyTo[[]*Row] }

type Work struct{ Data []int }

type Job struct {
	Data    []int
	ID      int
	ReplyTo lockstep.ReplyTo[int]
}

// sendThenWrite is AT-ANL-13(a): the sent slice is written after the send. The line in the
// message is the line of the send.
func sendThenWrite(sys *lockstep.System, pid lockstep.PID[Batch]) {
	items := []int{1}
	lockstep.Send(sys, pid, Batch{Items: items})
	items[0] = 2 // want `^LS200: items is used after it was sent with lockstep\.Send on line 40; the receiver owns it now, so copy it before sending or stop using it$`
}

// expandedLocal is AT-ANL-13(b): m's initializer places xs in the message.
func expandedLocal(sys *lockstep.System, pid lockstep.PID[Batch], xs []int) {
	m := Batch{Items: xs}
	lockstep.Send(sys, pid, m)
	xs[0] = 1 // want `^LS200: xs is used after it was sent with lockstep\.Send on line \d+; the receiver owns it now, so copy it before sending or stop using it$`
}

// pointerField is AT-ANL-13(c): the pointee is written through p.
func pointerField(sys *lockstep.System, pid lockstep.PID[Move]) {
	p := &Point{}
	lockstep.Send(sys, pid, Move{P: p})
	p.X = 1 // want `^LS200: p is used after it was sent with lockstep\.Send on line \d+; the receiver owns it now, so copy it before sending or stop using it$`
}

// slicedArray is AT-ANL-13(d): slicing shares the array, which holds no references itself.
func slicedArray(sys *lockstep.System, pid lockstep.PID[Chunk]) {
	var arr [4]int
	lockstep.Send(sys, pid, Chunk{S: arr[:]})
	arr[0] = 1 // want `^LS200: arr is used after it was sent with lockstep\.Send on line \d+; the receiver owns it now, so copy it before sending or stop using it$`
}

// loopCarried is AT-ANL-13(e): the next iteration appends to the slice the send gave away.
func loopCarried(sys *lockstep.System, pid lockstep.PID[Batch], in []int) {
	var buf []int
	for _, x := range in {
		buf = append(buf, x) // want `^LS200: buf is used after it was sent with lockstep\.Send on line \d+; the receiver owns it now, so copy it before sending or stop using it$`
		if len(buf) == 3 {
			lockstep.Send(sys, pid, Batch{Items: buf})
		}
	}
}

// askBuild is AT-ANL-13(f): the build literal's result places keys in the request.
func askBuild(sys *lockstep.System, pid lockstep.PID[Get], keys []string) {
	lockstep.Ask(sys, pid, func(r lockstep.ReplyTo[int]) Get { return Get{Keys: keys, ReplyTo: r} })
	keys[0] = "x" // want `^LS200: keys is used after it was sent with lockstep\.Ask on line \d+; the receiver owns it now, so copy it before sending or stop using it$`
}

// Store is an actor answering queries.
type Store struct{}

// Receive is AT-ANL-13(g): the reply's rows are written after the reply.
func (s *Store) Receive(ctx *lockstep.Context[Query], m Query) {
	rows := make([]*Row, 1)
	m.ReplyTo.Send(ctx, rows)
	rows[0] = nil // want `^LS200: rows is used after it was sent with ReplyTo\.Send on line \d+; the receiver owns it now, so copy it before sending or stop using it$`
}

// firstUseOnly is AT-ANL-13(h): only the first of two later uses is reported.
func firstUseOnly(sys *lockstep.System, pid lockstep.PID[Batch]) {
	items := []int{1}
	lockstep.Send(sys, pid, Batch{Items: items})
	items[0] = 2 // want `^LS200: items is used after it was sent with lockstep\.Send on line \d+; the receiver owns it now, so copy it before sending or stop using it$`
	items[0] = 3
}

// aliasInitializer places items through the single-assignment alias.
func aliasInitializer(sys *lockstep.System, pid lockstep.PID[Batch]) {
	items := []int{1}
	alias := items
	lockstep.Send(sys, pid, Batch{Items: alias})
	items[0] = 2 // want `^LS200: items is used after it was sent with lockstep\.Send on line \d+; the receiver owns it now, so copy it before sending or stop using it$`
}

// sentTwice gives the same slice to two receivers: the second send is a use.
func sentTwice(sys *lockstep.System, a, b lockstep.PID[Batch], items []int) {
	lockstep.Send(sys, a, Batch{Items: items})
	lockstep.Send(sys, b, Batch{Items: items}) // want `^LS200: items is used after it was sent with lockstep\.Send on line \d+; the receiver owns it now, so copy it before sending or stop using it$`
}

// sharedInt places the address of an int: the leaf is shared.
func sharedInt(sys *lockstep.System, pid lockstep.PID[Count]) {
	n := 0
	lockstep.Send(sys, pid, Count{N: &n})
	n++ // want `^LS200: n is used after it was sent with lockstep\.Send on line \d+; the receiver owns it now, so copy it before sending or stop using it$`
}

// mapLen reads the length of a sent map: len exempts only slices, arrays and strings.
func mapLen(sys *lockstep.System, pid lockstep.PID[Index], names map[string]int) int {
	lockstep.Send(sys, pid, Index{Names: names})
	return len(names) // want `^LS200: names is used after it was sent with lockstep\.Send on line \d+; the receiver owns it now, so copy it before sending or stop using it$`
}

// laterClosure uses the slice in a function literal written after the send.
func laterClosure(sys *lockstep.System, pid lockstep.PID[Batch], items []int) func() {
	lockstep.Send(sys, pid, Batch{Items: items})
	return func() {
		items[0] = 1 // want `^LS200: items is used after it was sent with lockstep\.Send on line \d+; the receiver owns it now, so copy it before sending or stop using it$`
	}
}

// sendInClosure sends from a function literal, which is a function of its own.
func sendInClosure(sys *lockstep.System, pid lockstep.PID[Batch]) func() {
	return func() {
		items := []int{1}
		lockstep.Send(sys, pid, Batch{Items: items})
		items[0] = 2 // want `^LS200: items is used after it was sent with lockstep\.Send on line \d+; the receiver owns it now, so copy it before sending or stop using it$`
	}
}

// oneBranchKill is AT-ANL-13(i): the kill is in one branch only, and the use is reached through
// the path where c is false.
func oneBranchKill(sys *lockstep.System, pid lockstep.PID[Batch], items []int, c bool) {
	lockstep.Send(sys, pid, Batch{Items: items})
	if c {
		items = nil
	}
	items[0] = 2 // want `^LS200: items is used after it was sent with lockstep\.Send on line \d+; the receiver owns it now, so copy it before sending or stop using it$`
}

// closureKill assigns the slice in a function literal: no kill.
func closureKill(sys *lockstep.System, pid lockstep.PID[Batch], items []int) {
	lockstep.Send(sys, pid, Batch{Items: items})
	func() { items = nil }()
	items[0] = 1 // want `^LS200: items is used after it was sent with lockstep\.Send on line \d+; the receiver owns it now, so copy it before sending or stop using it$`
}

// killRHS reassigns the slice from itself: the assignment kills, its right-hand side is a use.
func killRHS(sys *lockstep.System, pid lockstep.PID[Batch], items []int) {
	lockstep.Send(sys, pid, Batch{Items: items})
	items = append(items, 4) // want `^LS200: items is used after it was sent with lockstep\.Send on line \d+; the receiver owns it now, so copy it before sending or stop using it$`
	items[0] = 1
}

// useBeforeKill reads the slice between the send and the kill.
func useBeforeKill(sys *lockstep.System, pid lockstep.PID[Batch], items []int) {
	lockstep.Send(sys, pid, Batch{Items: items})
	_ = items[0] // want `^LS200: items is used after it was sent with lockstep\.Send on line \d+; the receiver owns it now, so copy it before sending or stop using it$`
	items = nil
	_ = items
}

// killBeforeSend assigns the slice in the loop before the send: the uses after the send stay.
func killBeforeSend(sys *lockstep.System, pid lockstep.PID[Batch], in []int) {
	var buf []int
	for _, x := range in {
		buf = []int{x}
		lockstep.Send(sys, pid, Batch{Items: buf})
		buf[0] = 0 // want `^LS200: buf is used after it was sent with lockstep\.Send on line \d+; the receiver owns it now, so copy it before sending or stop using it$`
	}
}

// nestedLoops sends in the inner loop; buf is declared in the outer loop's body, outside the
// innermost loop, so the next inner iteration's use is loop-carried.
func nestedLoops(sys *lockstep.System, pid lockstep.PID[Batch], groups [][]int) {
	for _, group := range groups {
		var buf []int
		for i := 0; i < len(group); i++ {
			buf = append(buf, group[i]) // want `^LS200: buf is used after it was sent with lockstep\.Send on line \d+; the receiver owns it now, so copy it before sending or stop using it$`
			if len(buf) == 2 {
				lockstep.Send(sys, pid, Batch{Items: buf})
			}
		}
	}
}

// loopCarriedClosure uses the slice in a function literal before the send in the loop.
func loopCarriedClosure(sys *lockstep.System, pid lockstep.PID[Batch], n int) {
	var buf []int
	for range n {
		add := func(x int) { buf = append(buf, x) } // want `^LS200: buf is used after it was sent with lockstep\.Send on line \d+; the receiver owns it now, so copy it before sending or stop using it$`
		add(1)
		lockstep.Send(sys, pid, Batch{Items: buf})
	}
}

// replyMethodExpr replies through the method expression.
func replyMethodExpr(ctx *lockstep.Context[Query], rt lockstep.ReplyTo[[]*Row], rows []*Row) {
	lockstep.ReplyTo[[]*Row].Send(rt, ctx, rows)
	rows[0] = nil // want `^LS200: rows is used after it was sent with ReplyTo\.Send on line \d+; the receiver owns it now, so copy it before sending or stop using it$`
}

// continueSkipsKill skips the kill with continue: the next iteration's append is reached.
func continueSkipsKill(sys *lockstep.System, pid lockstep.PID[Batch], in []int) {
	var buf []int
	for _, x := range in {
		buf = append(buf, x) // want `^LS200: buf is used after it was sent with lockstep\.Send on line \d+; the receiver owns it now, so copy it before sending or stop using it$`
		if x > 0 {
			lockstep.Send(sys, pid, Batch{Items: buf})
			continue
		}
		buf = nil
	}
}

// labelledBreak leaves both loops after the send, past the kill in the outer loop.
func labelledBreak(sys *lockstep.System, pid lockstep.PID[Batch], in [][]int) []int {
	var buf []int
outer:
	for _, xs := range in {
		for _, x := range xs {
			if x < 0 {
				lockstep.Send(sys, pid, Batch{Items: buf})
				break outer
			}
			buf = append(buf, x)
		}
		buf = nil
	}
	return buf // want `^LS200: buf is used after it was sent with lockstep\.Send on line \d+; the receiver owns it now, so copy it before sending or stop using it$`
}

// gotoRetry jumps back to a use written before the send.
func gotoRetry(sys *lockstep.System, pid lockstep.PID[Batch], more func() bool) {
	items := []int{1}
retry:
	items[0]++ // want `^LS200: items is used after it was sent with lockstep\.Send on line \d+; the receiver owns it now, so copy it before sending or stop using it$`
	lockstep.Send(sys, pid, Batch{Items: items})
	if more() {
		goto retry
	}
}

// rangeAssignAfterLoop assigns the slice with range =: leaving the loop does not assign it, so
// the use after the loop sees the last slice sent.
func rangeAssignAfterLoop(sys *lockstep.System, pid lockstep.PID[Batch], bufs [][]int) {
	var buf []int
	for _, buf = range bufs {
		lockstep.Send(sys, pid, Batch{Items: buf})
	}
	buf[0] = 1 // want `^LS200: buf is used after it was sent with lockstep\.Send on line \d+; the receiver owns it now, so copy it before sending or stop using it$`
}

// typeSwitchClause uses the clause's variable after the send in the same clause.
func typeSwitchClause(sys *lockstep.System, pid lockstep.PID[Batch], in []any) {
	for _, m := range in {
		switch x := m.(type) {
		case []int:
			lockstep.Send(sys, pid, Batch{Items: x})
			x[0] = 1 // want `^LS200: x is used after it was sent with lockstep\.Send on line \d+; the receiver owns it now, so copy it before sending or stop using it$`
		}
	}
}

// sharedAssign assigns the bare variable whose address was sent: the receiver sees the write
// (ANL-230), so the assignment is a use, not a kill.
func sharedAssign(sys *lockstep.System, pid lockstep.PID[Count]) {
	n := 0
	lockstep.Send(sys, pid, Count{N: &n})
	n = 5 // want `^LS200: n is used after it was sent with lockstep\.Send on line \d+; the receiver owns it now, so copy it before sending or stop using it$`
}

// sharedOpAssign adds to the bare variable whose address was sent.
func sharedOpAssign(sys *lockstep.System, pid lockstep.PID[Count]) {
	n := 0
	lockstep.Send(sys, pid, Count{N: &n})
	n += 1 // want `^LS200: n is used after it was sent with lockstep\.Send on line \d+; the receiver owns it now, so copy it before sending or stop using it$`
}

// aliasSourceWritten sends an element through a per-iteration alias and then clears that element
// of the source: the write is on the sent element's path.
func aliasSourceWritten(sys *lockstep.System, pid lockstep.PID[Batch], batches [][]int) {
	for i := range batches {
		b := batches[i]
		lockstep.Send(sys, pid, Batch{Items: b})
		batches[i] = nil // want `^LS200: batches is used after it was sent with lockstep\.Send on line \d+; the receiver owns it now, so copy it before sending or stop using it$`
	}
}

// sameElement writes inside the element that was sent.
func sameElement(sys *lockstep.System, pid lockstep.PID[Batch], batches [][]int) {
	lockstep.Send(sys, pid, Batch{Items: batches[0]})
	batches[0][0] = 1 // want `^LS200: batches is used after it was sent with lockstep\.Send on line \d+; the receiver owns it now, so copy it before sending or stop using it$`
}

// Worker forwards work and keeps reading what it forwarded.
type Worker struct {
	next lockstep.PID[Work]
	log  func(Job)
}

// Receive forwards m.Data and then writes into it.
func (w *Worker) Receive(ctx *lockstep.Context[Job], m Job) {
	lockstep.Send(ctx, w.next, Work{Data: m.Data})
	m.Data[0] = 1 // want `^LS200: m is used after it was sent with lockstep\.Send on line \d+; the receiver owns it now, so copy it before sending or stop using it$`
}

// forwardAll forwards m.Data and then passes all of m on.
func (w *Worker) forwardAll(ctx *lockstep.Context[Job], m Job) {
	lockstep.Send(ctx, w.next, Work{Data: m.Data})
	w.log(m) // want `^LS200: m is used after it was sent with lockstep\.Send on line \d+; the receiver owns it now, so copy it before sending or stop using it$`
}

// selectCase sends in one case of a select; the use after the select is reached.
func selectCase(sys *lockstep.System, pid lockstep.PID[Batch], items []int, done <-chan struct{}) {
	select {
	case <-done:
		lockstep.Send(sys, pid, Batch{Items: items})
	default:
	}
	items[0] = 1 // want `^LS200: items is used after it was sent with lockstep\.Send on line \d+; the receiver owns it now, so copy it before sending or stop using it$`
}

// goSend sends from a new goroutine: the write after it races with the send.
func goSend(sys *lockstep.System, pid lockstep.PID[Batch], items []int) {
	go lockstep.Send(sys, pid, Batch{Items: items})
	items[0] = 1 // want `^LS200: items is used after it was sent with lockstep\.Send on line \d+; the receiver owns it now, so copy it before sending or stop using it$`
}

// sendInPost sends in the for loop's post statement; the next iteration's append is reached.
func sendInPost(sys *lockstep.System, pid lockstep.PID[Batch], n int) {
	var buf []int
	for i := 0; i < n; lockstep.Send(sys, pid, Batch{Items: buf}) {
		buf = append(buf, i) // want `^LS200: buf is used after it was sent with lockstep\.Send on line \d+; the receiver owns it now, so copy it before sending or stop using it$`
		i++
	}
}

// fallthroughCase falls through from the send's case into a use.
func fallthroughCase(sys *lockstep.System, pid lockstep.PID[Batch], items []int, k int) {
	switch k {
	case 0:
		lockstep.Send(sys, pid, Batch{Items: items})
		fallthrough
	case 1:
		items[0] = 1 // want `^LS200: items is used after it was sent with lockstep\.Send on line \d+; the receiver owns it now, so copy it before sending or stop using it$`
	}
}

// deferArgument sends while the defer statement runs: only the deferred call waits.
func deferArgument(sys *lockstep.System, pid lockstep.PID[Batch], items []int, check func(error)) {
	defer check(lockstep.Send(sys, pid, Batch{Items: items}))
	items[0] = 1 // want `^LS200: items is used after it was sent with lockstep\.Send on line \d+; the receiver owns it now, so copy it before sending or stop using it$`
}

// arrayReset sends a slice of an array and assigns the array: the receiver's elements change.
func arrayReset(sys *lockstep.System, pid lockstep.PID[Chunk]) {
	var a [4]int
	lockstep.Send(sys, pid, Chunk{S: a[:]})
	a = [4]int{} // want `^LS200: a is used after it was sent with lockstep\.Send on line \d+; the receiver owns it now, so copy it before sending or stop using it$`
}

// aliasAddress sends the address of n through an alias and assigns n.
func aliasAddress(sys *lockstep.System, pid lockstep.PID[Count]) {
	n := 0
	b := &n
	lockstep.Send(sys, pid, Count{N: b})
	n = 5 // want `^LS200: n is used after it was sent with lockstep\.Send on line \d+; the receiver owns it now, so copy it before sending or stop using it$`
}

// slicedThenIndexed sends one element's slice and writes it through a full slice of the source.
func slicedThenIndexed(sys *lockstep.System, pid lockstep.PID[Batch], v []Batch) {
	lockstep.Send(sys, pid, Batch{Items: v[2].Items})
	v[:][2].Items[0] = 1 // want `^LS200: v is used after it was sent with lockstep\.Send on line \d+; the receiver owns it now, so copy it before sending or stop using it$`
}

// shiftedSlice reaches the sent element through a slice that starts later: after a slice, any
// index may name it.
func shiftedSlice(sys *lockstep.System, pid lockstep.PID[Batch], v []Batch) {
	lockstep.Send(sys, pid, Batch{Items: v[2].Items})
	v[1:][1].Items[0] = 1 // want `^LS200: v is used after it was sent with lockstep\.Send on line \d+; the receiver owns it now, so copy it before sending or stop using it$`
}
