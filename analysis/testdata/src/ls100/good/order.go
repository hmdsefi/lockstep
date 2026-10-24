package good

import (
	"maps"
	"net"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
)

// sum ranges over a map where the order does not matter, with a directive saying so.
func (a *Account) sum() int {
	n := 0
	//lockstep:allow LS100 order does not matter for a sum
	for _, v := range a.m {
		n += v
	}
	return n
}

// sorted iterates maps in sorted key order and ranges over slices, also of a type parameter.
func sorted[S interface{ ~[]int }](m map[string]int, s S) {
	for _, k := range slices.Sorted(maps.Keys(m)) {
		_ = k
	}
	_ = slices.SortedFunc(maps.Keys(m), strings.Compare)
	_ = slices.Sorted(maps.Values(m))
	for _, v := range s {
		_ = v
	}
	_ = len(m)
}

// nonBlocking uses names of the ANL-140 packages that do not block.
func nonBlocking(mu *sync.Mutex, wg *sync.WaitGroup, conn net.Conn, f *os.File, ch chan int) {
	mu.Unlock()
	wg.Add(1)
	_ = conn.LocalAddr()
	_ = net.ParseIP("127.0.0.1")
	_, _ = http.NewRequest("GET", "http://localhost", nil)
	_ = f.Name()
	_ = f.Fd()
	close(ch)
	_ = len(ch)
}
