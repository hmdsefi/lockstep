// Package bad holds the LS210 findings: writes to package variables in actor code, in every form of
// the rule (ANL-270, AT-ANL-17).
package bad

import (
	"maps"
	"net/http"
	"os"
	"slices"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/hmdsefi/lockstep"
)

// The package variables the actor code writes.
var (
	counter int
	total   int
	cache   = map[string]int{}
	hits    int64
	list    = []int{3, 1, 2}
	sink    []int
	n       atomic.Int64
	mu      sync.Mutex
	grid    [4]int
	ring    [2]atomic.Int64
	cfg     struct {
		Name string
		Tags []string
	}
	g guarded
)

// guarded embeds a mutex by value, so Lock is promoted with a pointer receiver.
type guarded struct {
	sync.Mutex
	n int
}

// Tick is Meter's message type.
type Tick int

// Meter is an actor type whose roots write package variables.
type Meter struct{}

// Started writes from a root other than Receive.
func (m *Meter) Started(ctx *lockstep.Context[Tick]) {
	total = 0 // want `^LS210: actor code writes package variable total \(assignment\); keep state in the actor's fields \(actor code: \(\*Meter\)\.Started\)$`
}

// Stopped writes from the last root.
func (m *Meter) Stopped(ctx *lockstep.Context[Tick], r lockstep.StopReason) {
	sink = nil // want `^LS210: actor code writes package variable sink \(assignment\); keep state in the actor's fields \(actor code: \(\*Meter\)\.Stopped\)$`
}

// Receive holds one write per form. The literal comes first so that its chain is Receive$1.
func (m *Meter) Receive(ctx *lockstep.Context[Tick], msg Tick) {
	later := func() {
		counter = 0 // want `^LS210: actor code writes package variable counter \(assignment\); keep state in the actor's fields \(actor code: \(\*Meter\)\.Receive -> \(\*Meter\)\.Receive\$1\)$`
	}
	later()
	bump()
	counter++                          // want `^LS210: actor code writes package variable counter \(increment\); keep state in the actor's fields \(actor code: \(\*Meter\)\.Receive\)$`
	counter--                          // want `^LS210: actor code writes package variable counter \(increment\); keep state in the actor's fields \(actor code: \(\*Meter\)\.Receive\)$`
	total = 5                          // want `^LS210: actor code writes package variable total \(assignment\); keep state in the actor's fields \(actor code: \(\*Meter\)\.Receive\)$`
	total += 2                         // want `^LS210: actor code writes package variable total \(assignment\); keep state in the actor's fields \(actor code: \(\*Meter\)\.Receive\)$`
	cache["k"] = 1                     // want `^LS210: actor code writes package variable cache \(assignment\); keep state in the actor's fields \(actor code: \(\*Meter\)\.Receive\)$`
	atomic.AddInt64(&hits, 1)          // want `^LS210: actor code writes package variable hits \(taking its address\); keep state in the actor's fields \(actor code: \(\*Meter\)\.Receive\)$`
	delete(cache, "k")                 // want `^LS210: actor code writes package variable cache \(delete\); keep state in the actor's fields \(actor code: \(\*Meter\)\.Receive\)$`
	clear(cache)                       // want `^LS210: actor code writes package variable cache \(clear\); keep state in the actor's fields \(actor code: \(\*Meter\)\.Receive\)$`
	copy(list, sink)                   // want `^LS210: actor code writes package variable list \(copy\); keep state in the actor's fields \(actor code: \(\*Meter\)\.Receive\)$`
	slices.Sort(list)                  // want `^LS210: actor code writes package variable list \(slices\.Sort\); keep state in the actor's fields \(actor code: \(\*Meter\)\.Receive\)$`
	sort.Ints(list)                    // want `^LS210: actor code writes package variable list \(sort\.Ints\); keep state in the actor's fields \(actor code: \(\*Meter\)\.Receive\)$`
	maps.Copy(cache, map[string]int{}) // want `^LS210: actor code writes package variable cache \(maps\.Copy\); keep state in the actor's fields \(actor code: \(\*Meter\)\.Receive\)$`
	n.Add(1)                           // want `^LS210: actor code writes package variable n \(pointer method Add\); keep state in the actor's fields \(actor code: \(\*Meter\)\.Receive\)$`
	mu.Lock()                          // want `^LS210: actor code writes package variable mu \(pointer method Lock\); keep state in the actor's fields \(actor code: \(\*Meter\)\.Receive\)$`
	g.Lock()                           // want `^LS210: actor code writes package variable g \(pointer method Lock\); keep state in the actor's fields \(actor code: \(\*Meter\)\.Receive\)$`
	ring[0].Add(1)                     // want `^LS210: actor code writes package variable ring \(pointer method Add\); keep state in the actor's fields \(actor code: \(\*Meter\)\.Receive\)$`
	http.DefaultClient.Timeout = 0     // want `^LS210: actor code writes package variable http\.DefaultClient \(assignment\); keep state in the actor's fields \(actor code: \(\*Meter\)\.Receive\)$`
	os.Args[0] = ""                    // want `^LS210: actor code writes package variable os\.Args \(assignment\); keep state in the actor's fields \(actor code: \(\*Meter\)\.Receive\)$`
	cfg.Name = "x"                     // want `^LS210: actor code writes package variable cfg \(assignment\); keep state in the actor's fields \(actor code: \(\*Meter\)\.Receive\)$`
	cfg.Tags[0] = "x"                  // want `^LS210: actor code writes package variable cfg \(assignment\); keep state in the actor's fields \(actor code: \(\*Meter\)\.Receive\)$`
	grid[1] = 2                        // want `^LS210: actor code writes package variable grid \(assignment\); keep state in the actor's fields \(actor code: \(\*Meter\)\.Receive\)$`
	for _, total = range list {        // want `^LS210: actor code writes package variable total \(assignment\); keep state in the actor's fields \(actor code: \(\*Meter\)\.Receive\)$`
		_ = total
	}
	p := &counter // want `^LS210: actor code writes package variable counter \(taking its address\); keep state in the actor's fields \(actor code: \(\*Meter\)\.Receive\)$`
	_ = p
}

// bump is reached from Receive.
func bump() {
	counter += 1 // want `^LS210: actor code writes package variable counter \(assignment\); keep state in the actor's fields \(actor code: \(\*Meter\)\.Receive -> bump\)$`
}
