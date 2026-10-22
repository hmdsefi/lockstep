package dl004

import "runtime"

// G uses the runtime functions that run a function on a runtime goroutine when the garbage
// collector decides.
func G(p *int) {
	runtime.SetFinalizer(p, nil)               // want DL004
	_ = runtime.AddCleanup(p, func(int) {}, 0) // want DL004
	_ = runtime.AddCleanup[int, int]           // want DL004
	setFinalizer := runtime.SetFinalizer       // want DL004
	_ = setFinalizer
	runtime.KeepAlive(p)
	SetFinalizer(p, nil)
	AddCleanup(p, func(int) {}, 0)
}

// SetFinalizer and AddCleanup are named like the runtime functions, but are not them.
func SetFinalizer(any, any) {}

func AddCleanup(any, func(int), int) {}
