package dl009

import (
	"reflect"
	"sync"
)

func F(m map[string]int, f func(k, v any) bool) {
	_ = reflect.ValueOf(m).MapKeys()  // want DL009
	_ = reflect.ValueOf(m).MapRange() // want DL009
	var sm sync.Map
	sm.Range(f) // want DL009
	_ = reflect.ValueOf(m).Len()
}
