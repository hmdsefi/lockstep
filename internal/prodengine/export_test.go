// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package prodengine

// PendingTimers returns the number of pending timers (SCH-140), for the tests of package
// prodengine_test.
func (e *Engine) PendingTimers() int { return e.pendingTimers() }
