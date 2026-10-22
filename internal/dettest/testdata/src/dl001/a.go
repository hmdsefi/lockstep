package dl001

import (
	"context"
	"time"
)

func F(ctx context.Context) {
	_ = time.Now()  // want DL001
	f := time.Since // want DL001
	_ = f
	time.Sleep(1)                    // want DL001
	<-time.After(1)                  // want DL001
	_ = time.Tick(1)                 // want DL001
	_ = time.Until(time.Time{})      // want DL001
	_ = time.AfterFunc(1, func() {}) // want DL001
	_ = time.NewTimer(1)             // want DL001
	_ = time.NewTicker(1)            // want DL001
	_ = time.Duration(5).String()

	// The context functions with a deadline read the wall clock and arm a runtime timer.
	_, _ = context.WithTimeout(ctx, 1)                      // want DL001
	_, _ = context.WithTimeoutCause(ctx, 1, nil)            // want DL001
	_, _ = context.WithDeadline(ctx, time.Time{})           // want DL001
	_, _ = context.WithDeadlineCause(ctx, time.Time{}, nil) // want DL001
	g := context.WithDeadline                               // want DL001
	_ = g
	_, cancel := context.WithCancel(ctx)
	cancel()
	_ = time.Time{}.After(time.Time{})
	_ = Now()
}

// Now is not time.Now: a function of another package with a forbidden name is allowed.
func Now() int { return 0 }
