//go:build cgo

package clean

import "time"

// No check context enables cgo, so every check ignores this file.
var _ = time.Now()
