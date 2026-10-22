package fix

import "time"

const A = 1
const B time.Duration = 2

type K string

const KX K = "x"

var V error

type S struct {
	X int
	y int
	time.Time
}
type I interface {
	M(int) (string, error)
	n()
}
type F func(...string) bool
type Al = int

func G(a, b int, opts ...string) *S { return nil }
func (S) Val() int                  { return 0 }
func (*S) Ptr(d time.Duration)      {}
