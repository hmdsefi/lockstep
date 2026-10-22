package generic

type Box[T any] struct{ V T }

func (b *Box[X]) Get() X { return b.V }

type Getter[T any] interface{ Get() T }

func Map[T, U any](xs []T, f func(T) U) []U { return nil }

type Num interface{ ~int | ~int64 }

func Sum[N Num](xs ...N) N { var z N; return z }

type Empty interface{}
