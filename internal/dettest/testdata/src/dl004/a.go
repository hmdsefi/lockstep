package dl004

import (
	"context"
	"sync"
)

func f() {}

func F(ctx context.Context) {
	go f() // want DL004

	// Library calls that start a goroutine without a go statement.
	var wg sync.WaitGroup
	wg.Go(f)                      // want DL004
	g := wg.Go                    // want DL004
	_ = (*sync.WaitGroup).Go      // want DL004
	_ = context.AfterFunc(ctx, f) // want DL004
	_ = g
	wg.Wait()
	pool{}.Go(f)
}

// pool has a Go method, but it is not sync.WaitGroup's.
type pool struct{}

func (pool) Go(func()) {}
