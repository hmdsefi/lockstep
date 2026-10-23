// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package overload

// Hysteresis turns on at v >= High and off at v <= Low (OVL-320). The zero state is off. It is
// not safe for concurrent use; one sampler feeds it.
type Hysteresis struct {
	High, Low uint64
	on        bool
}

// Update feeds v and reports whether the state changed and the new state. A value between Low
// and High keeps the state it has.
func (h *Hysteresis) Update(v uint64) (changed, on bool) {
	switch {
	case !h.on && v >= h.High:
		h.on = true
		return true, true
	case h.on && v <= h.Low:
		h.on = false
		return true, false
	}
	return false, h.on
}
