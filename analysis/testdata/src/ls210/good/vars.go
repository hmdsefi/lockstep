// Package good holds actor code that reads package variables and writes its own state, and writes
// to package variables outside actor code (ANL-270, AT-ANL-17).
package good

import (
	"net/http"
	"regexp"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/hmdsefi/lockstep"
)

// The package variables.
var (
	counter int
	list    = []int{3, 1, 2}
	re      = regexp.MustCompile("a")
	ptr     = new(atomic.Int64)
	hook    func()
	cache   = map[string]int{}
	conns   = []*atomic.Int64{new(atomic.Int64)}
	shared  withPtr
)

// withPtr embeds a pointer, so Lock is promoted through it and no address is taken.
type withPtr struct {
	*sync.Mutex
}

// Tick is Reader's message type.
type Tick string

// Reader is an actor type that only reads package variables.
type Reader struct {
	counter int
	list    []int
	local   atomic.Int64
}

// Receive reads, and writes the actor's own state.
func (r *Reader) Receive(ctx *lockstep.Context[Tick], msg Tick) {
	_ = counter + 1
	_ = re.MatchString(string(msg))
	_ = ptr.Add(1)
	req, _ := http.NewRequest("GET", "http://example.invalid", nil)
	_, _ = http.DefaultClient.Do(req)
	r.counter++
	r.list = append(r.list, counter)
	r.local.Add(1)
	if hook != nil {
		hook()
	}
	_ = len(cache)
	_ = slices.Clone(list)
	copy(r.list, list)
	_ = conns[0].Add(1)
	shared.Lock()
	shadow(1)
	local()
	_ = append(list, 1)
	for _, v := range list {
		_ = v
	}
}

// shadow writes a parameter that has the name of a package variable.
func shadow(counter int) {
	counter++
}

// local writes locals whose names are those of package variables, and values copied from them.
func local() {
	counter := 0
	counter++
	list := slices.Clone(list)
	slices.Sort(list)
	cache := map[string]int{}
	delete(cache, "k")
	clear(cache)
	var n atomic.Int64
	n.Add(1)
}

// unreached makes the writes of the bad corpus, but no root reaches it.
func unreached() {
	counter++
	counter = 5
	cache["k"] = 1
	delete(cache, "k")
	slices.Sort(list)
	ptr.Store(2)
	http.DefaultClient.Timeout = 0
}
