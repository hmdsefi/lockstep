// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import "runtime/debug"

// guard calls fn and recovers a panic in it (SUP-140): failed is true when fn did not return
// normally, and err is then &PanicError{Value: v, Stack: debug.Stack()} with v the value passed to
// panic. Every call into actor code that can fail goes through guard: the factory, Started,
// Receive, Signal and Stopped, and with supervision the deciders and owned resources' Close.
//
// failed is decided by whether fn returned, not by recover's result, so a panic whose value
// recover reports as nil is still a failure. A runtime.Goexit in fn is not a panic: it keeps
// unwinding past guard and its results are never used (SUP §7).
func guard(fn func()) (failed bool, err error) {
	defer func() {
		if failed {
			err = &PanicError{Value: recover(), Stack: debug.Stack()}
		}
	}()
	failed = true
	fn()
	return false, nil
}
