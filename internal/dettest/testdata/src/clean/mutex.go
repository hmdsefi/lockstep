package clean

import "sync"

// Counter guards its state with a sync.Mutex, which no rule reports (DET-040).
type Counter struct {
	mu sync.Mutex
	n  int
}

// Inc adds one to c.
func (c *Counter) Inc() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n++
}
