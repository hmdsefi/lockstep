// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package remote

// SetLinkBuffer makes the connections of the systems enabled from now on refuse a frame once they
// hold n bytes not yet written, and returns the function that restores the previous limit.
func SetLinkBuffer(n int) (restore func()) {
	old := linkBuffer
	linkBuffer = n
	return func() { linkBuffer = old }
}
