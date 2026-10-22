package kinds

const (
	Big  = 1 << 100
	Half = 1.5
	Re   = real(3 + 2i)
	R    = 'a'
	C    = 2i
	Str  = "s"
	T    = true
)

var (
	P  *func(n int) error
	L  []func(s string)
	A  [2]func(i int)
	M  map[string]func(k int) (ok bool)
	Ch <-chan func(c int)
	N  func(f func(x int))
)

type G[T any, U comparable] struct{ X T }

type E struct {
	inner
	X int
}

type inner struct{}
