// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package toys

import "fmt"

// invalid is the error of a spawn function for an argument below its minimum of 1 (DET §7).
func invalid(name string, value int) error {
	return fmt.Errorf("toys: invalid %s %d", name, value)
}
