package dl006

type Named map[string]int

type MapConstraint interface{ map[string]int }

func F(m map[string]int, n Named, s []int) {
	for k := range m { // want DL006
		_ = k
	}
	for k := range n { // want DL006
		_ = k
	}
	for i := range s {
		_ = i
	}
}

func G[M ~map[string]int](m M) {
	for k := range m { // want DL006
		_ = k
	}
}

func H[M MapConstraint](m M) {
	for k := range m { // want DL006
		_ = k
	}
}

// MapLike allows only maps; the constraints below reach it through embedding.
type MapLike interface{ ~map[string]int }

type NestedMap interface{ MapLike }

func Embedded[M interface{ MapLike }](m M) {
	for k := range m { // want DL006
		_ = k
	}
}

func Nested[M NestedMap](m M) {
	for k := range m { // want DL006
		_ = k
	}
}

func Union[M interface{ MapLike | NestedMap }](m M) {
	for k := range m { // want DL006
		_ = k
	}
}

// SliceLike is embedded the same way, but allows only slices.
type SliceLike interface{ ~[]int }

func Slice[S interface{ SliceLike }](s S) {
	for i := range s {
		_ = i
	}
}

// Lener allows any type with a Len method; Second's constraint embeds it before MapLike.
type Lener interface{ Len() int }

func Second[M interface {
	Lener
	MapLike
}](m M) {
	for k := range m { // want DL006
		_ = k
	}
}
