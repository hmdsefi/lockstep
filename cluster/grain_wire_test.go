// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package cluster

import (
	"reflect"
	"strings"
	"testing"

	"github.com/hmdsefi/lockstep/codec"
)

// AT-VAC-27, VAC-700: the three grain wire types are registered in the default registry under
// their wire names, with the fields VAC-700 lists, round-trip through codec.JSON and are accepted
// by JSON.Supports.
func TestATVAC27GrainWireTypes(t *testing.T) {
	cases := []struct {
		name   string
		val    any
		fields []string
	}{
		{"lockstep.cluster.grain", grainEnvelope{Kind: "counter", Key: "a%2Fb", Hops: 2, Name: "app.inc", Data: []byte(`{"By":3}`)},
			[]string{"Kind", "Key", "Hops", "Name", "Data"}},
		{"lockstep.cluster.handoff", handoffReq{Kind: "counter", Key: "a%2Fb", From: "n1", Seq: 1 << 63},
			[]string{"Kind", "Key", "From", "Seq"}},
		{"lockstep.cluster.handoffack", handoffAck{Kind: "counter", Key: "k0", From: "n2", Seq: 1<<64 - 1},
			[]string{"Kind", "Key", "From", "Seq"}},
	}
	reg := codec.Default()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			typ := reflect.TypeOf(tc.val)
			if got, err := reg.TypeOf(tc.name); err != nil || got != typ {
				t.Fatalf("TypeOf(%q) = %v, %v, want %v", tc.name, got, err, typ)
			}
			if got, err := reg.NameOf(tc.val); err != nil || got != tc.name {
				t.Fatalf("NameOf = %q, %v, want %q", got, err, tc.name)
			}
			if err := (codec.JSON{}).Supports(typ); err != nil {
				t.Fatalf("Supports: %v", err)
			}
			var fields []string
			for i := range typ.NumField() {
				fields = append(fields, typ.Field(i).Name)
			}
			if !reflect.DeepEqual(fields, tc.fields) {
				t.Fatalf("fields %v, want %v", fields, tc.fields)
			}
			name, data, err := reg.Encode(codec.JSON{}, tc.val)
			if err != nil || name != tc.name {
				t.Fatalf("Encode = %q, %v, want %q", name, err, tc.name)
			}
			got, err := reg.Decode(codec.JSON{}, name, data)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if !reflect.DeepEqual(got, tc.val) {
				t.Fatalf("round trip = %+v, want %+v", got, tc.val)
			}
		})
	}
	if err := reg.Check(codec.JSON{}); err != nil {
		t.Fatalf("Check: %v", err)
	}
	if wireGrain != "lockstep.cluster.grain" || wireHandoff != "lockstep.cluster.handoff" || wireHandoffAck != "lockstep.cluster.handoffack" {
		t.Error("the wire names are part of the protocol")
	}
	for _, n := range []string{wireGrain, wireHandoff, wireHandoffAck} {
		if !strings.HasPrefix(n, recordPrefix) {
			t.Errorf("wire name %q lacks the prefix %q", n, recordPrefix)
		}
	}
}
