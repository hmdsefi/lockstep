package dl007

import "time"

type S struct {
	R float64 // want DL007
}

func F(d time.Duration) {
	var _ float64    // want DL007
	_ = float32(1)   // want DL007
	_ = d.Seconds()  // want DL007
	var _ complex128 // want DL007
	const c = 1e6
	var n int64 = c
	_ = n
}
