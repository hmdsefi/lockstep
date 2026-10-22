package dl009

import (
	"maps"
	"reflect"
	"slices"
)

// G uses the functions that yield a map, or call a function, in map order.
func G(m map[string]int, keys []string) {
	_ = reflect.ValueOf(m).Seq()                                  // want DL009
	_ = reflect.ValueOf(m).Seq2()                                 // want DL009
	_ = reflect.Value.Seq2                                        // want DL009
	seq := reflect.ValueOf(m).Seq                                 // want DL009
	maps.DeleteFunc(m, func(string, int) bool { return true })    // want DL009
	_ = maps.DeleteFunc[map[string]int]                           // want DL009
	_ = maps.EqualFunc(m, m, func(int, int) bool { return true }) // want DL009
	_ = seq
	_ = slices.DeleteFunc(keys, func(string) bool { return false })
	_ = slices.EqualFunc(keys, keys, func(string, string) bool { return true })
	_ = list{}.Seq()
	_ = list{}.Seq2()
}

// list has Seq and Seq2 methods, but they are not reflect.Value's.
type list struct{}

func (list) Seq() int  { return 0 }
func (list) Seq2() int { return 0 }
