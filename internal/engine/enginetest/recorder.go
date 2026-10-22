// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package enginetest

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"github.com/hmdsefi/lockstep/internal/engine"
)

// Recorder is an enabled Tracer that keeps every record in order. It is safe for concurrent use,
// so it also serves as the tracer of a local engine in tests.
type Recorder struct {
	mu      sync.Mutex
	records []engine.Record
}

// NewRecorder returns an empty Recorder.
func NewRecorder() *Recorder {
	return &Recorder{}
}

// Enabled returns true.
func (r *Recorder) Enabled() bool {
	return true
}

// Record appends one record with a copy of attrs. It panics if kind does not start with
// "lockstep." or an attribute key is empty (ENG-550).
func (r *Recorder) Record(kind string, attrs ...engine.Attr) {
	if !strings.HasPrefix(kind, "lockstep.") {
		panic(fmt.Sprintf("enginetest: record kind %q does not start with \"lockstep.\"", kind))
	}
	for _, a := range attrs {
		if a.Key == "" {
			panic(fmt.Sprintf("enginetest: empty attribute key in %q", kind))
		}
	}
	rec := engine.Record{Kind: kind, Attrs: append([]engine.Attr(nil), attrs...)}
	r.mu.Lock()
	r.records = append(r.records, rec)
	r.mu.Unlock()
}

// Describe returns a description of v that is the same in every run (ENG-550): "<nil>" for nil,
// v.Describe() if v has that method, then String, Error, a quoted string, a bool, the decimal
// value of an integer, and otherwise the type name.
func (r *Recorder) Describe(v any) string {
	if v == nil {
		return "<nil>"
	}
	if d, ok := v.(interface{ Describe() string }); ok {
		return d.Describe()
	}
	if s, ok := v.(fmt.Stringer); ok {
		return s.String()
	}
	if err, ok := v.(error); ok {
		return err.Error()
	}
	switch x := v.(type) {
	case string:
		return strconv.Quote(x)
	case bool:
		return strconv.FormatBool(x)
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(rv.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return strconv.FormatUint(rv.Uint(), 10)
	}
	return fmt.Sprintf("%T", v)
}

// Records returns a copy of the records so far, in order.
func (r *Recorder) Records() []engine.Record {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]engine.Record, len(r.records))
	for i, rec := range r.records {
		out[i] = engine.Record{Kind: rec.Kind, Attrs: append([]engine.Attr(nil), rec.Attrs...)}
	}
	return out
}
