// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package codec_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/codec"
)

// Types for AT-COD-07. Each marshaler type with only stub methods also has a field that rule 2
// would reject, so an accepted one shows that the walk does not descend into it.
type (
	order struct{ Items []item }
	item  struct{ Note error }

	jsonOnly     struct{ C chan int }
	jsonPair     struct{ C chan int }
	textOnly     struct{ C chan int }
	textPair     struct{ C chan int }
	jsonOverText struct{ C chan int } // MarshalJSON only, plus a full text pair
	jsonAndText  struct{ C chan int } // a full JSON pair, plus MarshalText only

	ptrJSON           struct{ N int } // MarshalJSON and UnmarshalJSON on the pointer
	ptrText           struct{ N int } // MarshalText and UnmarshalText on the pointer
	unmarshalJSONOnly struct{ N int }
	unmarshalTextOnly string
	textPlusUnmarshal string // a full text pair, plus UnmarshalJSON only

	embTime struct {
		time.Time
		N int
	}
	embTimePtr struct {
		*time.Time
		N int
	}
	embAddr struct {
		lockstep.Address
		Extra int
	}
	embTimeOnly struct{ time.Time }
	embTimeSkip struct {
		time.Time
		N int `json:"-"`
	}
	ownPairEmbedding struct { // its own JSON pair, and an embedded one
		jsonPair
		N int
	}
	embTimePtrOnly struct{ *time.Time }
	embAddrPtrOnly struct{ *lockstep.Address }

	ptrMarshalOnly struct{ N int } // MarshalJSON on the pointer, no UnmarshalJSON
	ptrHolder      struct {
		A *ptrJSON
		L []*ptrJSON
		M map[string]*ptrJSON
	}
	bigHolder struct{ V *big.Int }
	bigEmb    struct {
		big.Int
		N int
	}

	JA    struct{ a int } // a JSON pair
	JB    struct{ b int } // a JSON pair
	tA    struct{ a int } // a text pair
	tB    struct{ b int } // a text pair
	ambig struct {
		JA
		JB
	}
	ambigText struct {
		tA
		tB
	}
	ambigTagged struct {
		JA `json:"a"`
		JB `json:"b"`
	}
	taggedUnexported struct {
		tA `json:"a"`
		tB `json:"b"`
	}

	keyString            string
	keyInt               uint16
	keyStringMarshalOnly string // written and read as it is
	keyTextString        string // written as it is, read through UnmarshalText
	keyIntPair           int
	keyIntMarshalOnly    int
	keyIntUnmarshalOnly  int

	Inner     struct{ Note error }
	inner     struct{ Note error }
	innerInt  int
	withInner struct {
		N int
		Inner
	}
	withInnerPtr         struct{ *Inner }
	withUnexported       struct{ inner }
	withUnexportedPtr    struct{ *inner }
	withUnexportedInt    struct{ innerInt }
	withUnexportedIntPtr struct{ *innerInt }

	skipped struct {
		C      chan int `json:"-"`
		secret int      `json:"-"`
		N      int
	}
	dashNamed struct {
		C chan int `json:"-,"` //nolint:staticcheck // a field named "-", which rule 5 must not skip
	}
	hidden struct {
		N      int
		secret int
	}

	CA struct{ X int }
	CB struct{ X int }
	CT struct {
		Y int `json:"X"`
	}
	Leaf     struct{ Z int }
	DA       struct{ Leaf }
	DB       struct{ Leaf }
	conflict struct {
		CA
		CB
	}
	shadow struct {
		X int
		CA
	}
	taggedFirst struct {
		CA
		CT
	}
	diamond struct {
		DA
		DB
	}
	renamed struct {
		X int `json:"y"`
		CA
	}
	holdsConflict struct{ C conflict }
	Base          struct{ ID int }
	Other         struct {
		Base
		Name string
	}
	twoDepths struct {
		Base
		Other
	} // Base embedded at depths 1 and 2
	D4          struct{ X int }
	C4          struct{ D4 }
	A4          struct{ C4 }
	B4          struct{ C4 }
	deepDiamond struct {
		A4
		B4
	}
	Node struct {
		*Node
		V int
	}

	list struct {
		Next   *list
		Kids   []list
		ByName map[string]*list
		V      int
	}
	badList struct {
		Next *badList
		F    func()
	}
	ping2 struct {
		Pong *pong2
		N    int
	}
	pong2 struct {
		Ping *ping2
		C    chan int
	}
)

// The unexported fields above are never read; naming them here keeps the unused check quiet.
var _ = []any{withUnexportedInt{innerInt: 1}, skipped{secret: 1}, hidden{secret: 1}}

func (jsonOnly) MarshalJSON() ([]byte, error)     { return []byte("0"), nil }
func (jsonPair) MarshalJSON() ([]byte, error)     { return []byte("0"), nil }
func (*jsonPair) UnmarshalJSON([]byte) error      { return nil }
func (textOnly) MarshalText() ([]byte, error)     { return nil, nil }
func (textPair) MarshalText() ([]byte, error)     { return nil, nil }
func (*textPair) UnmarshalText([]byte) error      { return nil }
func (jsonOverText) MarshalJSON() ([]byte, error) { return []byte("0"), nil }
func (jsonOverText) MarshalText() ([]byte, error) { return nil, nil }
func (*jsonOverText) UnmarshalText([]byte) error  { return nil }
func (jsonAndText) MarshalJSON() ([]byte, error)  { return []byte("0"), nil }
func (*jsonAndText) UnmarshalJSON([]byte) error   { return nil }
func (jsonAndText) MarshalText() ([]byte, error)  { return nil, nil }

func (ownPairEmbedding) MarshalJSON() ([]byte, error) { return []byte("0"), nil }
func (*ownPairEmbedding) UnmarshalJSON([]byte) error  { return nil }

// The methods below really encode, so the round trips in TestATCOD07Supports show the loss.

func (p *ptrJSON) MarshalJSON() ([]byte, error) { return []byte(strconv.Quote(strconv.Itoa(p.N))), nil }
func (p *ptrJSON) UnmarshalJSON(b []byte) error {
	s, err := strconv.Unquote(string(b))
	if err == nil {
		p.N, err = strconv.Atoi(s)
	}
	return err
}
func (p *ptrText) MarshalText() ([]byte, error) { return []byte(strconv.Itoa(p.N)), nil }
func (p *ptrText) UnmarshalText(b []byte) (err error) {
	p.N, err = strconv.Atoi(string(b))
	return err
}
func (p *ptrMarshalOnly) MarshalJSON() ([]byte, error) { return []byte(strconv.Itoa(p.N)), nil }
func (x JA) MarshalJSON() ([]byte, error)              { return []byte(strconv.Itoa(x.a)), nil }
func (x *JA) UnmarshalJSON(b []byte) (err error) {
	x.a, err = strconv.Atoi(string(b))
	return err
}
func (x JB) MarshalJSON() ([]byte, error) { return []byte(strconv.Itoa(x.b)), nil }
func (x *JB) UnmarshalJSON(b []byte) (err error) {
	x.b, err = strconv.Atoi(string(b))
	return err
}
func (x tA) MarshalText() ([]byte, error) { return []byte(strconv.Itoa(x.a)), nil }
func (x *tA) UnmarshalText(b []byte) (err error) {
	x.a, err = strconv.Atoi(string(b))
	return err
}
func (x tB) MarshalText() ([]byte, error) { return []byte(strconv.Itoa(x.b)), nil }
func (x *tB) UnmarshalText(b []byte) (err error) {
	x.b, err = strconv.Atoi(string(b))
	return err
}
func (u *unmarshalJSONOnly) UnmarshalJSON([]byte) error { u.N = 99; return nil }
func (s *unmarshalTextOnly) UnmarshalText(b []byte) error {
	*s = unmarshalTextOnly(strings.ToUpper(string(b)))
	return nil
}
func (s textPlusUnmarshal) MarshalText() ([]byte, error) { return []byte(s), nil }
func (s *textPlusUnmarshal) UnmarshalText(b []byte) error {
	*s = textPlusUnmarshal(b)
	return nil
}
func (s *textPlusUnmarshal) UnmarshalJSON([]byte) error { *s = "json"; return nil }

func (k keyStringMarshalOnly) MarshalText() ([]byte, error) { return []byte("k:" + k), nil }
func (k keyTextString) MarshalText() ([]byte, error)        { return []byte("k:" + k), nil }
func (k *keyTextString) UnmarshalText(b []byte) error {
	s, ok := strings.CutPrefix(string(b), "k:")
	if !ok {
		return fmt.Errorf("bad key %q", b)
	}
	*k = keyTextString(s)
	return nil
}
func (k keyIntPair) MarshalText() ([]byte, error) { return []byte("i" + strconv.Itoa(int(k))), nil }
func (k *keyIntPair) UnmarshalText(b []byte) error {
	n, err := strconv.Atoi(strings.TrimPrefix(string(b), "i"))
	*k = keyIntPair(n)
	return err
}
func (k keyIntMarshalOnly) MarshalText() ([]byte, error) {
	return []byte("i" + strconv.Itoa(int(k))), nil
}
func (k *keyIntUnmarshalOnly) UnmarshalText(b []byte) error {
	s, ok := strings.CutPrefix(string(b), "i")
	if !ok {
		return fmt.Errorf("bad key %q", b)
	}
	n, err := strconv.Atoi(s)
	*k = keyIntUnmarshalOnly(n)
	return err
}

// dupTag is struct{ A int `json:"x"`; B int `json:"x"` }, built at run time because go vet rejects
// the repeated tag in source. dupTagValue is a value of it with A 1 and B 2.
var dupTag, dupTagValue = func() (reflect.Type, any) {
	typ := reflect.StructOf([]reflect.StructField{
		{Name: "A", Type: reflect.TypeFor[int](), Tag: `json:"x"`},
		{Name: "B", Type: reflect.TypeFor[int](), Tag: `json:"x"`},
	})
	v := reflect.New(typ).Elem()
	v.Field(0).SetInt(1)
	v.Field(1).SetInt(2)
	return typ, v.Interface()
}()

// AT-COD-07, COD-090: each rule with its exact why text, in rule order, and recursive types.
func TestATCOD07Supports(t *testing.T) {
	if got := (codec.JSON{}).Name(); got != "json" {
		t.Errorf("Name() = %q, want json", got)
	}

	tests := []struct {
		typ reflect.Type
		why string // "" if supported
	}{
		// The example of COD-090: field names outermost first.
		{reflect.TypeFor[order](), "field Items: field Note: interface type error"},

		// Rule 1: marshaler methods first; a full pair is accepted without descending.
		{reflect.TypeFor[jsonOnly](), "codec_test.jsonOnly has MarshalJSON but no UnmarshalJSON"},
		{reflect.TypeFor[jsonPair](), ""},
		{reflect.TypeFor[textOnly](), "codec_test.textOnly has MarshalText but no UnmarshalText"},
		{reflect.TypeFor[textPair](), ""},
		{reflect.TypeFor[jsonOverText](), "codec_test.jsonOverText has MarshalJSON but no UnmarshalJSON"},
		{reflect.TypeFor[jsonAndText](), ""},
		{reflect.TypeFor[time.Time](), ""},
		{reflect.TypeFor[lockstep.Address](), ""},
		{reflect.TypeFor[ptrJSON](), "codec_test.ptrJSON has MarshalJSON on its pointer only"},
		{reflect.TypeFor[ptrText](), "codec_test.ptrText has MarshalText on its pointer only"},
		{reflect.TypeFor[unmarshalJSONOnly](), "codec_test.unmarshalJSONOnly has UnmarshalJSON but no MarshalJSON"},
		{reflect.TypeFor[unmarshalTextOnly](), "codec_test.unmarshalTextOnly has UnmarshalText but no MarshalText"},
		{reflect.TypeFor[textPlusUnmarshal](), "codec_test.textPlusUnmarshal has UnmarshalJSON but no MarshalJSON"},
		// A marshal method promoted from an embedded field hides the other fields.
		{reflect.TypeFor[embTime](), "field Time: embedded MarshalJSON hides the other fields"},
		{reflect.TypeFor[embAddr](), "field Address: embedded MarshalText hides the other fields"},
		{reflect.TypeFor[ownPairEmbedding](), "field jsonPair: embedded MarshalJSON hides the other fields"},
		{reflect.TypeFor[embTimeOnly](), ""},
		{reflect.TypeFor[embTimeSkip](), ""},
		// A marshal method promoted through an embedded pointer panics when it is nil.
		{reflect.TypeFor[embTimePtr](), "field Time: embedded pointer MarshalJSON panics when nil"},
		{reflect.TypeFor[embTimePtrOnly](), "field Time: embedded pointer MarshalJSON panics when nil"},
		{reflect.TypeFor[embAddrPtrOnly](), "field Address: embedded pointer MarshalText panics when nil"},
		// Pointers go to rule 3, so rule 1 looks at the element type...
		{reflect.TypeFor[*time.Time](), ""},
		{reflect.TypeFor[*lockstep.Address](), ""},
		{reflect.TypeFor[*jsonPair](), ""},
		{reflect.TypeFor[**jsonPair](), ""},
		{reflect.TypeFor[*jsonOnly](), "codec_test.jsonOnly has MarshalJSON but no UnmarshalJSON"},
		{reflect.TypeFor[[]textOnly](), "codec_test.textOnly has MarshalText but no UnmarshalText"},
		// ...except a pointer whose element has its methods on the pointer only: encoding/json
		// always uses them there.
		{reflect.TypeFor[*ptrJSON](), ""},
		{reflect.TypeFor[*ptrText](), ""},
		{reflect.TypeFor[ptrHolder](), ""},
		{reflect.TypeFor[*big.Int](), ""},
		{reflect.TypeFor[bigHolder](), ""},
		{reflect.TypeFor[*ptrMarshalOnly](), "*codec_test.ptrMarshalOnly has MarshalJSON but no UnmarshalJSON"},
		{reflect.TypeFor[*bigEmb](), "field Int: embedded MarshalJSON hides the other fields"},
		{reflect.TypeFor[bigEmb](), "codec_test.bigEmb has MarshalJSON on its pointer only"},
		{reflect.TypeFor[map[string]ptrJSON](), "codec_test.ptrJSON has MarshalJSON on its pointer only"},
		{reflect.TypeFor[**ptrJSON](), ""},
		// An interface type with MarshalJSON meets rule 1 before rule 2.
		{reflect.TypeFor[json.Marshaler](), "json.Marshaler has MarshalJSON but no UnmarshalJSON"},

		// Rule 2: rejected kinds.
		{reflect.TypeFor[error](), "interface type error"},
		{reflect.TypeFor[any](), "interface type interface {}"},
		{reflect.TypeFor[chan int](), "chan type chan int"},
		{reflect.TypeFor[func()](), "func type func()"},
		{reflect.TypeFor[complex64](), "complex64 type complex64"},
		{reflect.TypeFor[complex128](), "complex128 type complex128"},
		{reflect.TypeFor[unsafe.Pointer](), "unsafe.Pointer type unsafe.Pointer"},

		// Rule 3: pointers, slices and arrays add no text.
		{reflect.TypeFor[*error](), "interface type error"},
		{reflect.TypeFor[[]error](), "interface type error"},
		{reflect.TypeFor[[2]chan int](), "chan type chan int"},
		{reflect.TypeFor[[][]*func()](), "func type func()"},
		{reflect.TypeFor[*[]*int](), ""},
		{reflect.TypeFor[[3][]string](), ""},

		// Rule 4: map keys, then the value type without text.
		{reflect.TypeFor[map[string]int](), ""},
		{reflect.TypeFor[map[keyString][]int](), ""},
		{reflect.TypeFor[map[keyStringMarshalOnly]int](), ""},
		{reflect.TypeFor[map[int8]bool](), ""},
		{reflect.TypeFor[map[keyInt]string](), ""},
		{reflect.TypeFor[map[keyIntPair]string](), ""},
		{reflect.TypeFor[map[uintptr]string](), ""},
		{reflect.TypeFor[map[textPair]int](), ""},
		{reflect.TypeFor[map[lockstep.Address]int](), ""},
		{reflect.TypeFor[map[bool]int](), "map key type bool"},
		{reflect.TypeFor[map[float64]int](), "map key type float64"},
		{reflect.TypeFor[map[[2]int]int](), "map key type [2]int"},
		{reflect.TypeFor[map[textOnly]int](), "map key type codec_test.textOnly"},
		{reflect.TypeFor[map[*lockstep.Address]int](), "map key type *lockstep.Address"},
		{reflect.TypeFor[map[keyTextString]int](), "map key type codec_test.keyTextString"},
		{reflect.TypeFor[map[keyIntMarshalOnly]int](), "map key type codec_test.keyIntMarshalOnly"},
		{reflect.TypeFor[map[keyIntUnmarshalOnly]int](), "map key type codec_test.keyIntUnmarshalOnly"},
		{reflect.TypeFor[map[string]error](), "interface type error"},
		{reflect.TypeFor[map[string][]chan int](), "chan type chan int"},

		// Rule 5: structs.
		{reflect.TypeFor[skipped](), ""},
		{reflect.TypeFor[dashNamed](), "field C: chan type chan int"},
		{reflect.TypeFor[hidden](), "unexported field secret"},
		{reflect.TypeFor[withInner](), "field Inner: field Note: interface type error"},
		{reflect.TypeFor[withInnerPtr](), "field Inner: field Note: interface type error"},
		{reflect.TypeFor[withUnexported](), "field inner: field Note: interface type error"},
		{reflect.TypeFor[withUnexportedPtr](), "unexported embedded pointer field inner"},
		{reflect.TypeFor[withUnexportedInt](), "unexported field innerInt"},
		{reflect.TypeFor[withUnexportedIntPtr](), "unexported field innerInt"},
		{reflect.TypeFor[struct{}](), ""},
		// encoding/json ignores the methods of an untagged embedded struct; a tagged one is an
		// ordinary field.
		{reflect.TypeFor[ambig](), "field JA: embedded MarshalJSON is ignored"},
		{reflect.TypeFor[ambigText](), "field tA: embedded MarshalText is ignored"},
		{reflect.TypeFor[ambigTagged](), ""},
		// ...so a tagged unexported one is an unexported field.
		{reflect.TypeFor[taggedUnexported](), "unexported field tA"},
		// JSON names that encoding/json gives to more than one field, so it drops some.
		{reflect.TypeFor[conflict](), `fields CA.X and CB.X share the JSON name "X"`},
		{dupTag, `fields A and B share the JSON name "x"`},
		{reflect.TypeFor[shadow](), `fields X and CA.X share the JSON name "X"`},
		{reflect.TypeFor[taggedFirst](), `fields CT.Y and CA.X share the JSON name "X"`},
		{reflect.TypeFor[diamond](), `fields DA.Leaf.Z and DB.Leaf.Z share the JSON name "Z"`},
		{reflect.TypeFor[holdsConflict](), `field C: fields CA.X and CB.X share the JSON name "X"`},
		{reflect.TypeFor[renamed](), ""},
		// Every embedding path is expanded, so fields encoding/json drops without a clash in its
		// own field list are found too.
		{reflect.TypeFor[twoDepths](), `fields Base.ID and Other.Base.ID share the JSON name "ID"`},
		{reflect.TypeFor[deepDiamond](), `fields A4.C4.D4.X and B4.C4.D4.X share the JSON name "X"`},
		{reflect.TypeFor[Node](), "field Node: recursive embedding hides its fields"},

		// Rule 6, and recursive types, which the visited set ends.
		{reflect.TypeFor[uintptr](), ""},
		{reflect.TypeFor[list](), ""},
		{reflect.TypeFor[*list](), ""},
		{reflect.TypeFor[badList](), "field F: func type func()"},
		{reflect.TypeFor[ping2](), "field Pong: field C: chan type chan int"},
		// The walk reaches pong2 first, then ping2, whose Pong field leads back to pong2: already
		// visited, so accepted, and the walk goes on to pong2's field C.
		{reflect.TypeFor[[]pong2](), "field C: chan type chan int"},
	}
	for _, tt := range tests {
		err := codec.JSON{}.Supports(tt.typ)
		if tt.why == "" {
			if err != nil {
				t.Errorf("Supports(%v) = %v, want nil", tt.typ, err)
			}
			continue
		}
		want := "codec: unsupported type: json cannot encode " + tt.typ.String() + ": " + tt.why
		if err == nil || err.Error() != want || !errors.Is(err, codec.ErrUnsupported) {
			t.Errorf("Supports(%v) = %v; want %q wrapping ErrUnsupported", tt.typ, err, want)
		}
	}

	// Every built-in type is supported.
	r := codec.NewRegistry()
	names := r.Names()
	if len(names) != 19 {
		t.Fatalf("NewRegistry has %d names, want the 19 built-ins", len(names))
	}
	for _, name := range names {
		typ, err := r.TypeOf(name)
		if err != nil {
			t.Fatalf("TypeOf(%q): %v", name, err)
		}
		if err := (codec.JSON{}).Supports(typ); err != nil {
			t.Errorf("Supports(%v), built-in %s: %v", typ, name, err)
		}
	}

	// Against encoding/json itself: a value of a supported type round trips, and one of each
	// rejected type does not, or panics.
	at := time.Unix(1, 0).UTC()
	var addr lockstep.Address
	fromText(t, &addr, "lockstep://n1/user/w#1f")
	n := innerInt(3)
	for _, v := range []any{
		embTimeOnly{at},
		embTimeSkip{Time: at},
		renamed{X: 1, CA: CA{2}},
		map[keyStringMarshalOnly]int{"x": 1},
		map[keyIntPair]int{3: 1},
		list{Next: &list{V: 2}, Kids: []list{{V: 3}}, ByName: map[string]*list{"a": {V: 4}}, V: 1},
		&ptrJSON{N: 5},
		&ptrText{N: 5},
		ptrHolder{A: &ptrJSON{N: 5}, L: []*ptrJSON{{N: 6}}, M: map[string]*ptrJSON{"k": {N: 7}}},
		big.NewInt(42),
		bigHolder{V: big.NewInt(42)},
		ambigTagged{JA{1}, JB{2}},
	} {
		if err := roundTrip(v); err != nil {
			t.Errorf("supported %T: %v", v, err)
		}
	}
	for _, v := range []any{
		ptrJSON{N: 5},
		ptrText{N: 5},
		unmarshalJSONOnly{N: 1},
		unmarshalTextOnly("abc"),
		textPlusUnmarshal("abc"),
		embTime{Time: at, N: 7},
		embAddr{Address: addr, Extra: 7},
		map[keyTextString]int{"x": 1},
		map[keyIntMarshalOnly]int{3: 1},
		map[keyIntUnmarshalOnly]int{3: 1},
		withUnexportedInt{innerInt: 3},
		withUnexportedIntPtr{innerInt: &n},
		conflict{CA{1}, CB{2}},
		dupTagValue,
		shadow{X: 1, CA: CA{2}},
		taggedFirst{CA{1}, CT{2}},
		diamond{DA{Leaf{1}}, DB{Leaf{2}}},
		embTimePtr{Time: &at, N: 7},
		embTimePtrOnly{},
		embTimePtrOnly{&at},
		embAddrPtrOnly{},
		&bigEmb{*big.NewInt(4), 7},
		map[string]ptrJSON{"k": {N: 5}},
		ambig{JA{1}, JB{2}},
		taggedUnexported{tA{1}, tB{2}},
		ambigText{tA{1}, tB{2}},
		twoDepths{Base{1}, Other{Base{2}, "n"}},
		deepDiamond{A4{C4{D4{1}}}, B4{C4{D4{2}}}},
		Node{&Node{V: 2}, 1},
	} {
		if err := (codec.JSON{}).Supports(reflect.TypeOf(v)); err == nil {
			t.Errorf("Supports(%T) = nil", v)
		}
		if err := roundTrip(v); err == nil {
			t.Errorf("rejected %T round trips through encoding/json", v)
		}
	}
}

// roundTrip marshals v with JSON, unmarshals the bytes into a new value of v's type and returns
// an error if that fails, panics or gives a value that is not deeply equal to v.
func roundTrip(v any) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("panic: %v", p)
		}
	}()
	data, err := codec.JSON{}.Marshal(v)
	if err != nil {
		return err
	}
	p := reflect.New(reflect.TypeOf(v))
	if err := (codec.JSON{}).Unmarshal(data, p.Interface()); err != nil {
		return fmt.Errorf("unmarshal %s: %w", data, err)
	}
	if got := p.Elem().Interface(); !reflect.DeepEqual(got, v) {
		return fmt.Errorf("%s decodes as %#v", data, got)
	}
	return nil
}

type (
	detInner struct {
		Tags map[string]int
		IDs  map[int]string
	}
	detMsg struct {
		Name  string
		Inner detInner
		Ptr   *detInner
		List  []detInner
	}
	failing struct{}
)

var errFailing = errors.New("failing marshal")

func (failing) MarshalJSON() ([]byte, error) { return nil, errFailing }

// detValue builds a detMsg whose maps are filled in key order perm.
func detValue(perm []int) detMsg {
	tags := make(map[string]int)
	ids := make(map[int]string)
	for _, i := range perm {
		tags["abcdefghijklmnop"[i:i+1]] = i
		ids[i*7] = strings.Repeat("x", i)
	}
	in := detInner{Tags: tags, IDs: ids}
	return detMsg{Name: "<a&b>", Inner: in, Ptr: &in, List: []detInner{in, {}}}
}

// AT-COD-08, COD-100, COD-110: Marshal gives identical bytes for equal values, sorted map keys,
// no HTML escaping and no trailing newline; encoding/json's errors come back unchanged; Unmarshal
// is json.Unmarshal.
func TestATCOD08Deterministic(t *testing.T) {
	c := codec.JSON{}

	t.Run("identical bytes", func(t *testing.T) {
		const n = 16
		forward := make([]int, n)
		backward := make([]int, n)
		for i := range n {
			forward[i], backward[i] = i, n-1-i
		}
		want, err := c.Marshal(detValue(forward))
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		for round := range 20 {
			perm := backward
			if round%2 == 0 {
				perm = forward
			}
			got, err := c.Marshal(detValue(perm))
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("round %d: Marshal = %s, %v; want %s", round, got, err, want)
			}
		}
	})

	t.Run("sorted keys", func(t *testing.T) {
		got, err := c.Marshal(map[string]int{"b": 2, "c": 3, "a": 1})
		if want := `{"a":1,"b":2,"c":3}`; err != nil || string(got) != want {
			t.Errorf("Marshal = %s, %v; want %s", got, err, want)
		}
		got, err = c.Marshal(detValue([]int{2, 0, 1}).Inner)
		if want := `{"Tags":{"a":0,"b":1,"c":2},"IDs":{"0":"","14":"xx","7":"x"}}`; err != nil || string(got) != want {
			t.Errorf("Marshal = %s, %v; want %s", got, err, want)
		}
	})

	t.Run("no HTML escaping, no newline", func(t *testing.T) {
		got, err := c.Marshal("<a href=\"x\">&</a>")
		if want := `"<a href=\"x\">&</a>"`; err != nil || string(got) != want {
			t.Errorf("Marshal = %s, %v; want %s", got, err, want)
		}
		for _, v := range []any{1, "s", struct{}{}, []int{1}, map[string]bool{"k": true}, nil} {
			got, err := c.Marshal(v)
			std, _ := json.Marshal(v)
			if err != nil || bytes.HasSuffix(got, []byte("\n")) || !bytes.Equal(got, std) {
				t.Errorf("Marshal(%#v) = %q, %v; want %q", v, got, err, std)
			}
		}
	})

	t.Run("errors unchanged", func(t *testing.T) {
		for _, v := range []any{math.NaN(), math.Inf(1), make(chan int), failing{}, []any{func() {}}} {
			got, err := c.Marshal(v)
			_, stdErr := json.Marshal(v)
			if got != nil || err == nil || stdErr == nil || err.Error() != stdErr.Error() ||
				reflect.TypeOf(err) != reflect.TypeOf(stdErr) {
				t.Errorf("Marshal(%T) = %q, %v (%T); want nil, %v (%T)", v, got, err, err, stdErr, stdErr)
			}
		}
		var uve *json.UnsupportedValueError
		if _, err := c.Marshal(math.NaN()); !errors.As(err, &uve) {
			t.Errorf("Marshal(NaN) error %T, want *json.UnsupportedValueError", err)
		}
		if _, err := c.Marshal(failing{}); !errors.Is(err, errFailing) {
			t.Errorf("Marshal(failing) error %v does not wrap the MarshalJSON error", err)
		}
	})

	t.Run("unmarshal", func(t *testing.T) {
		in := detValue([]int{3, 1, 2})
		data, err := c.Marshal(in)
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		var out detMsg
		if err := c.Unmarshal(data, &out); err != nil || !reflect.DeepEqual(out, in) {
			t.Errorf("round trip = %+v, %v; want %+v", out, err, in)
		}
		var p struct{ N int }
		if err := c.Unmarshal([]byte(`{"N":4,"Extra":[1,2]}`), &p); err != nil || p.N != 4 {
			t.Errorf("unknown field: %+v, %v", p, err)
		}
		for _, data := range []string{`{"N":`, `{"N":"x"}`} {
			err := c.Unmarshal([]byte(data), &p)
			stdErr := json.Unmarshal([]byte(data), &p)
			if err == nil || stdErr == nil || err.Error() != stdErr.Error() || reflect.TypeOf(err) != reflect.TypeOf(stdErr) {
				t.Errorf("Unmarshal(%s) = %v (%T); want %v (%T)", data, err, err, stdErr, stdErr)
			}
		}
	})
}

type identityMsg struct {
	Addr   lockstep.Address
	PID    lockstep.PID[identityMsg]
	Reply  lockstep.ReplyTo[int64]
	Ptr    *lockstep.Address
	ByAddr map[lockstep.Address]int
	ByPID  map[lockstep.PID[identityMsg]]string
	ByRT   map[lockstep.ReplyTo[int64]]bool
	Zero   lockstep.Address
}

// fromText sets *v from text, failing the test on error.
func fromText(t *testing.T, v interface{ UnmarshalText([]byte) error }, text string) {
	t.Helper()
	if err := v.UnmarshalText([]byte(text)); err != nil {
		t.Fatalf("UnmarshalText(%q): %v", text, err)
	}
}

// AT-COD-11, COD-130: identity values encode as JSON strings of their text forms, as values and as
// map keys, round trip, and are supported.
func TestATCOD11IdentityJSON(t *testing.T) {
	const (
		addrText  = "lockstep://n1/user/worker#1f"
		pidText   = "lockstep://n2/user/orders/1#0"
		replyText = "lockstep-reply://n1/temp/3#2a"
	)
	var (
		addr  lockstep.Address
		pid   lockstep.PID[identityMsg]
		reply lockstep.ReplyTo[int64]
	)
	fromText(t, &addr, addrText)
	fromText(t, &pid, pidText)
	fromText(t, &reply, replyText)
	c := codec.JSON{}

	for _, tt := range []struct {
		v    any
		want string
	}{
		{addr, `"` + addrText + `"`},
		{pid, `"` + pidText + `"`},
		{reply, `"` + replyText + `"`},
		{lockstep.Address{}, `""`},
		{lockstep.PIDAt[int]("n2", "/user/orders/1"), `"` + pidText + `"`},
		{map[lockstep.Address]int{addr: 1}, `{"` + addrText + `":1}`},
		{map[lockstep.PID[identityMsg]]int{pid: 2}, `{"` + pidText + `":2}`},
		{map[lockstep.ReplyTo[int64]]int{reply: 3}, `{"` + replyText + `":3}`},
	} {
		got, err := c.Marshal(tt.v)
		if err != nil || string(got) != tt.want {
			t.Errorf("Marshal(%T) = %s, %v; want %s", tt.v, got, err, tt.want)
		}
	}

	in := identityMsg{
		Addr:   addr,
		PID:    pid,
		Reply:  reply,
		Ptr:    &addr,
		ByAddr: map[lockstep.Address]int{addr: 1, pid.Address(): 2},
		ByPID:  map[lockstep.PID[identityMsg]]string{pid: "p"},
		ByRT:   map[lockstep.ReplyTo[int64]]bool{reply: true},
	}
	data, err := c.Marshal(in)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	want := `{"Addr":"` + addrText + `","PID":"` + pidText + `","Reply":"` + replyText + `","Ptr":"` + addrText +
		`","ByAddr":{"` + addrText + `":1,"` + pidText + `":2},"ByPID":{"` + pidText + `":"p"},"ByRT":{"` +
		replyText + `":true},"Zero":""}`
	if string(data) != want {
		t.Errorf("Marshal = %s\nwant      %s", data, want)
	}
	var out identityMsg
	if err := c.Unmarshal(data, &out); err != nil || !reflect.DeepEqual(out, in) {
		t.Errorf("round trip = %+v, %v; want %+v", out, err, in)
	}
	if err := c.Unmarshal([]byte(`{"Addr":"lockstep://bad"}`), &out); err == nil ||
		!strings.Contains(err.Error(), `lockstep: invalid address "lockstep://bad"`) {
		t.Errorf("Unmarshal of an invalid address: %v", err)
	}

	for _, typ := range []reflect.Type{
		reflect.TypeFor[lockstep.Address](),
		reflect.TypeFor[lockstep.PID[identityMsg]](),
		reflect.TypeFor[lockstep.ReplyTo[int64]](),
		reflect.TypeFor[*lockstep.PID[int]](),
		reflect.TypeFor[identityMsg](),
	} {
		if err := c.Supports(typ); err != nil {
			t.Errorf("Supports(%v) = %v, want nil", typ, err)
		}
	}
}
