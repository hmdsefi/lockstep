//go:build !cgo

package dl001

import "time"

// Every check context disables cgo, so the lint checks this file whatever CGO_ENABLED is.
func G() {
	_ = time.Now() // want DL001
}
