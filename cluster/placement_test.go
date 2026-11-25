// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package cluster

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/hmdsefi/tenure/core"
)

// fnv1a64 is an independent FNV-1a 64-bit hash (VAC-100).
func fnv1a64(data []byte) uint64 {
	h := uint64(14695981039346656037)
	for _, b := range data {
		h ^= uint64(b)
		h *= 1099511628211
	}
	return h
}

// fmix64 is MurmurHash3's 64-bit finalizer (VAC-100).
func fmix64(k uint64) uint64 {
	k ^= k >> 33
	k *= 0xff51afd7ed558ccd
	k ^= k >> 33
	k *= 0xc4ceb9fe1a85ec53
	k ^= k >> 33
	return k
}

// refScore computes Score from the definition: fmix64(fnv1a64(kind 0x00 key 0x00 node)).
func refScore(node, kind, key string) uint64 {
	return fmix64(fnv1a64([]byte(kind + "\x00" + key + "\x00" + node)))
}

func TestATVAC01Score(t *testing.T) {
	cases := []struct{ node, kind, key string }{
		{"n1", "counter", "k0"},
		{"n2", "counter", "k0"},
		{"n3", "counter", "k3"},
		{"n4", "counter", "k8"},
		{"n1", "session", "alice"},
		{"node-a.example", "user.profile", "a/b c%d"},
		{"", "", ""},
		{"n1", "", "k"},
		{"n1", "counter", ""},
		{"", "counter", "k0"},
		{"né", "känd", "日本語"},
		{"n1", "counter", string([]byte{0, 1, 0xfe, 0xff})},
		{"n1", "counter", strings.Repeat("x", 256)},
	}
	for _, c := range cases {
		got := Score(c.node, c.kind, c.key)
		if want := core.Score(c.kind, c.key, c.node); got != want {
			t.Errorf("Score(%q, %q, %q) = %#x, core.Score = %#x", c.node, c.kind, c.key, got, want)
		}
		if want := refScore(c.node, c.kind, c.key); got != want {
			t.Errorf("Score(%q, %q, %q) = %#x, FNV-1a and fmix64 give %#x", c.node, c.kind, c.key, got, want)
		}
	}

	// Fixed constants: the placement of every grain depends on them, so they never change.
	fixed := []struct {
		node, kind, key string
		want            uint64
	}{
		{"n1", "counter", "k0", 0x4a5c48e4a8d01ff3},
		{"n2", "counter", "k0", 0x0cc559d4a39312db},
		{"n3", "counter", "k0", 0x0b36557667fd0ab6},
		{"n4", "counter", "k0", 0xa984ebeeaf5a12a6},
		{"", "", "", 0xdb01c3104e99566d},
		{"n1", "session", "alice", 0x9d7de71f591ba5c6},
	}
	for _, c := range fixed {
		if got := Score(c.node, c.kind, c.key); got != c.want {
			t.Errorf("Score(%q, %q, %q) = %#x, want %#x", c.node, c.kind, c.key, got, c.want)
		}
	}

	// The three parts are separated: moving a byte between them changes the score.
	if Score("bc", "a", "x") == Score("c", "ab", "x") || Score("n", "k", "ab") == Score("n", "ka", "b") {
		t.Error("scores do not tell kind, key and node apart")
	}
}

// hostView returns a view of members "node:state:kind,kind" (state a, s, d or l) with IDs 1, 2, ...
func hostView(specs ...string) *View {
	v := &View{Version: 1}
	for i, spec := range specs {
		parts := strings.SplitN(spec, ":", 3)
		m := Member{Node: parts[0], ID: uint32(i + 1), State: map[string]MemberState{
			"a": Alive, "s": Suspect, "d": Dead, "l": Left}[parts[1]]}
		if len(parts) == 3 && parts[2] != "" {
			m.Kinds = strings.Split(parts[2], ",")
		}
		v.Members = append(v.Members, m)
	}
	slices.SortFunc(v.Members, func(a, b Member) int { return strings.Compare(a.Node, b.Node) })
	v.Self = v.Members[0]
	return v
}

// refRank ranks the nodes by score descending and name ascending, from the definition.
func refRank(nodes []string, kind, key string) []string {
	out := slices.Clone(nodes)
	slices.SortFunc(out, func(a, b string) int {
		sa, sb := refScore(a, kind, key), refScore(b, kind, key)
		switch {
		case sa > sb:
			return -1
		case sa < sb:
			return 1
		}
		return strings.Compare(a, b)
	})
	return out
}

func TestATVAC02Owner(t *testing.T) {
	all := hostView("n1:a:counter", "n2:a:counter", "n3:a:counter", "n4:a:counter")

	// The rankings of the design: n1 to n4 hosting "counter".
	for _, c := range []struct {
		key  string
		want []string
	}{
		{"k0", []string{"n4", "n1", "n2", "n3"}},
		{"k3", []string{"n1", "n3", "n4", "n2"}},
		{"k8", []string{"n4", "n3", "n1", "n2"}},
	} {
		if got := rank(all, "counter", c.key, 4); !slices.Equal(got, c.want) {
			t.Errorf("rank(%s) = %v, want %v", c.key, got, c.want)
		}
		if got := rank(all, "counter", c.key, 10); !slices.Equal(got, c.want) {
			t.Errorf("rank(%s, 10) = %v, want %v", c.key, got, c.want)
		}
		if got, ok := Owner(all, "counter", c.key); !ok || got != c.want[0] {
			t.Errorf("Owner(%s) = %q, %v, want %q", c.key, got, ok, c.want[0])
		}
	}

	tests := []struct {
		name string
		v    *View
		kind string
		key  string
		want string // "" for no owner
	}{
		{"no members", &View{Version: 1}, "counter", "k0", ""},
		{"nobody hosts the kind", hostView("n1:a:other", "n2:a", "n3:a:counters"), "counter", "k0", ""},
		{"the kind is not a prefix match", hostView("n1:a:count", "n2:a:counter.x"), "counter", "k0", ""},
		{"one host", hostView("n1:a:other", "n2:a:counter,other", "n3:a"), "counter", "k0", "n2"},
		{"n4 is best for k0", all, "counter", "k0", "n4"},
		{"a dead member is skipped", hostView("n1:a:counter", "n2:a:counter", "n3:a:counter", "n4:d:counter"), "counter", "k0", "n1"},
		{"a left member is skipped", hostView("n1:a:counter", "n2:a:counter", "n3:a:counter", "n4:l:counter"), "counter", "k0", "n1"},
		{"a suspect member counts", hostView("n1:a:counter", "n2:a:counter", "n3:a:counter", "n4:s:counter"), "counter", "k0", "n4"},
		{"all down", hostView("n1:d:counter", "n2:l:counter"), "counter", "k0", ""},
		{"a member without the kind is skipped", hostView("n1:a:counter", "n2:a:counter", "n3:a:counter", "n4:a:other"), "counter", "k0", "n1"},
		{"a client-only member is skipped", hostView("n1:a:counter", "n2:a:counter", "n3:a:counter", "n4:a"), "counter", "k0", "n1"},
		{"the kind is one of several", hostView("n1:a:a,counter,z", "n2:a:counter", "n3:a:counter", "n4:a:b,counter"), "counter", "k3", "n1"},
	}
	for _, tc := range tests {
		got, ok := Owner(tc.v, tc.kind, tc.key)
		if got != tc.want || ok != (tc.want != "") {
			t.Errorf("%s: Owner = %q, %v, want %q", tc.name, got, ok, tc.want)
		}
	}

	// IDs and incarnations play no part: a restarted member keeps its place.
	moved := hostView("n1:a:counter", "n2:a:counter", "n3:a:counter", "n4:a:counter")
	for i := range moved.Members {
		moved.Members[i].ID += 100
		moved.Members[i].Incarnation = uint64(i + 7)
	}
	for _, key := range []string{"k0", "k3", "k8", "other"} {
		if got, want := rank(moved, "counter", key, 4), rank(all, "counter", key, 4); !slices.Equal(got, want) {
			t.Errorf("rank(%s) with other IDs = %v, want %v", key, got, want)
		}
	}

	// Many keys: the ranking is the definition (score descending, then name), and a member that
	// stops hosting the kind takes only its own keys with it.
	nodes := []string{"n1", "n2", "n3", "n4", "n5", "n6", "n7"}
	var specs []string
	for _, n := range nodes {
		specs = append(specs, n+":a:counter")
	}
	big := hostView(specs...)
	without := hostView(specs[:3]...)
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("key-%d", i)
		want := refRank(nodes, "counter", key)
		if got := rank(big, "counter", key, len(nodes)); !slices.Equal(got, want) {
			t.Fatalf("rank(%s) = %v, want %v", key, got, want)
		}
		if got := rank(big, "counter", key, 3); !slices.Equal(got, want[:3]) {
			t.Fatalf("rank(%s, 3) = %v, want %v", key, got, want[:3])
		}
		if got, _ := Owner(without, "counter", key); got != refRank(nodes[:3], "counter", key)[0] {
			t.Fatalf("Owner(%s) over n1 to n3 = %q", key, got)
		}
		if o, _ := Owner(big, "counter", key); o != want[0] {
			t.Fatalf("Owner(%s) = %q, want %q", key, o, want[0])
		}
	}
}

func TestATVAC03Keys(t *testing.T) {
	for _, tc := range []struct {
		key   string
		valid bool
	}{
		{"", false},
		{"a", true},
		{"\x00", true},
		{"\xff", true},
		{strings.Repeat("a", 255), true},
		{strings.Repeat("a", 256), true},
		{strings.Repeat("a", 257), false},
		{strings.Repeat("\xff", 256), true},
		{strings.Repeat("\xff", 257), false},
		{strings.Repeat("日", 85), true},  // 255 bytes
		{strings.Repeat("日", 86), false}, // 258 bytes
	} {
		if got := validKey(tc.key); got != tc.valid {
			t.Errorf("validKey(%d bytes %.12q) = %v, want %v", len(tc.key), tc.key, got, tc.valid)
		}
	}

	table := []struct{ key, want string }{
		{"a", "a"},
		{"abcXYZ019", "abcXYZ019"},
		{"-._", "-._"},
		{"a.b", "a.b"},
		{".a", ".a"},
		{"a..", "a.."},
		{"...", "..."},
		{".", "%2E"},
		{"..", "%2E%2E"},
		{"%", "%25"},
		{"%41", "%2541"},
		{"a b", "a%20b"},
		{"a/b", "a%2Fb"},
		{"user@example.com", "user%40example.com"},
		{"\x00", "%00"},
		{"\x7f", "%7F"},
		{"\x80", "%80"},
		{"\xff", "%FF"},
		{"\xc3\xa9", "%C3%A9"}, // e with an acute accent, as UTF-8
		{"日", "%E6%97%A5"},
		{"a\nb", "a%0Ab"},
		{"~", "%7E"},
		{"\xfe\xff", "%FE%FF"},
	}
	for _, tc := range table {
		got := escapeKey(tc.key)
		if got != tc.want {
			t.Errorf("escapeKey(%q) = %q, want %q", tc.key, got, tc.want)
		}
		if back, ok := unescapeKey(tc.want); !ok || back != tc.key {
			t.Errorf("unescapeKey(%q) = %q, %v, want %q", tc.want, back, ok, tc.key)
		}
	}

	// The longest key escapes to 768 bytes, ACT's limit for a name.
	if got := escapeKey(strings.Repeat("\xff", maxKeyLen)); len(got) != 768 {
		t.Errorf("a 256-byte key escapes to %d bytes, want 768", len(got))
	}

	// Every single byte, and every pair of bytes, escapes to its own name that only has kept bytes
	// and '%XX' groups, and unescapes to the key.
	seen := map[string]string{}
	check := func(key string) {
		esc := escapeKey(key)
		if other, dup := seen[esc]; dup && other != key {
			t.Fatalf("keys %q and %q both escape to %q", other, key, esc)
		}
		seen[esc] = key
		for i := 0; i < len(esc); i++ {
			c := esc[i]
			if c == '%' {
				if i+2 >= len(esc) || !strings.ContainsRune("0123456789ABCDEF", rune(esc[i+1])) ||
					!strings.ContainsRune("0123456789ABCDEF", rune(esc[i+2])) {
					t.Fatalf("escapeKey(%q) = %q has a bad %%XX at %d", key, esc, i)
				}
				i += 2
			} else if !keepByte(c) {
				t.Fatalf("escapeKey(%q) = %q has the byte %#x", key, esc, c)
			}
		}
		if back, ok := unescapeKey(esc); !ok || back != key {
			t.Fatalf("unescapeKey(escapeKey(%q)) = %q, %v", key, back, ok)
		}
	}
	for a := 0; a < 256; a++ {
		check(string([]byte{byte(a)}))
	}
	for a := 0; a < 256; a++ {
		for b := 0; b < 256; b++ {
			check(string([]byte{byte(a), byte(b)}))
		}
	}
	for _, k := range []string{"..", "...", ".a", "a.", "%2E", "%252E"} {
		check(k)
	}

	// unescapeKey rejects what escapeKey cannot produce.
	for _, bad := range []string{
		"",       // no key
		"%",      // truncated
		"%2",     // truncated
		"a%",     // truncated
		"a%4",    // truncated
		"%zz",    // not hex
		"%2g",    // not hex
		"%2e",    // lowercase hex
		"%c3%a9", // lowercase hex
		"%41",    // escapes a byte that is kept
		"%2D",    // escapes a byte that is kept
		"%5F",    // escapes a byte that is kept
		".",      // not an actor name, escapeKey writes %2E
		"..",     // idem
		"%2E.",   // "..": every dot is escaped
		".%2E",   // idem
		"a b",    // a byte that is not kept
		"a/b",    // idem
		"a%b",    // a '%' without two digits
		"\xff",   // a raw byte
		"日",
		"+",
		"~",
		strings.Repeat("a", 257),   // key too long
		strings.Repeat("%FF", 257), // key too long
		"%00%2g",                   // bad group after a good one
		"%%41",                     // percent sign inside a group
		" %41",                     // leading space
	} {
		if got, ok := unescapeKey(bad); ok {
			t.Errorf("unescapeKey(%q) = %q, true, want a rejection", bad, got)
		}
	}
	if got, ok := unescapeKey(strings.Repeat("a", 256)); !ok || len(got) != 256 {
		t.Errorf("unescapeKey of 256 bytes = %d bytes, %v", len(got), ok)
	}
	if got, ok := unescapeKey(strings.Repeat("%FF", 256)); !ok || got != strings.Repeat("\xff", 256) {
		t.Errorf("unescapeKey of 256 escaped bytes = %d bytes, %v", len(got), ok)
	}
}

func TestRunnerUp(t *testing.T) {
	all := hostView("n1:a:counter", "n2:a:counter", "n3:a:counter", "n4:a:counter")
	for _, c := range []struct{ key, owner, runner string }{
		{"k0", "n4", "n1"},
		{"k3", "n1", "n3"},
		{"k8", "n4", "n3"},
	} {
		if got, ok := runnerUp(all, "counter", c.key); !ok || got != c.runner {
			t.Errorf("runnerUp(%s) = %q, %v, want %q", c.key, got, ok, c.runner)
		}
		if got, _ := Owner(all, "counter", c.key); got != c.owner {
			t.Errorf("Owner(%s) = %q, want %q", c.key, got, c.owner)
		}
	}

	// The runner-up is the second of the ranking among the up hosting members only.
	if got, ok := runnerUp(hostView("n1:a:counter", "n2:a:counter", "n3:a:counter", "n4:d:counter"), "counter", "k0"); !ok || got != "n2" {
		t.Errorf("runnerUp(k0) without n4 = %q, %v, want n2", got, ok)
	}
	if got, ok := runnerUp(hostView("n1:a:counter", "n2:a:counter", "n3:a:counter", "n4:s:counter"), "counter", "k0"); !ok || got != "n1" {
		t.Errorf("runnerUp(k0) with a suspect n4 = %q, %v, want n1", got, ok)
	}

	// Only the members that host the kind take part.
	mixed := hostView("n1:a:counter", "n2:a:other", "n3:a:other", "n4:a:counter,other")
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("m%d", i)
		want := refRank([]string{"n2", "n3", "n4"}, "other", key)
		if got, ok := runnerUp(mixed, "other", key); !ok || got != want[1] {
			t.Fatalf("runnerUp(other, %s) = %q, %v, want %q", key, got, ok, want[1])
		}
	}

	// With fewer than two hosting members there is none.
	for name, v := range map[string]*View{
		"no members":    {Version: 1},
		"one member":    hostView("n1:a:counter", "n2:a:other"),
		"one up member": hostView("n1:a:counter", "n2:d:counter", "n3:l:counter"),
	} {
		if got, ok := runnerUp(v, "counter", "k0"); ok || got != "" {
			t.Errorf("%s: runnerUp = %q, %v, want none", name, got, ok)
		}
	}

	// The runner-up differs from the owner, and drops out when the owner leaves: then it is owner.
	nodes := []string{"n1", "n2", "n3", "n4", "n5"}
	var specs []string
	for _, n := range nodes {
		specs = append(specs, n+":a:counter")
	}
	big := hostView(specs...)
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("g%d", i)
		owner, _ := Owner(big, "counter", key)
		runner, ok := runnerUp(big, "counter", key)
		if !ok || runner == owner {
			t.Fatalf("key %s: owner %q, runner-up %q, %v", key, owner, runner, ok)
		}
		var rest []string
		for _, s := range specs {
			if !strings.HasPrefix(s, owner+":") {
				rest = append(rest, s)
			}
		}
		if next, _ := Owner(hostView(rest...), "counter", key); next != runner {
			t.Fatalf("key %s: owner %q left, the new owner is %q, want the runner-up %q", key, owner, next, runner)
		}
	}
}

// TestHostingLookup covers the helpers hosts use for lookups that do not allocate.
func TestHostingLookup(t *testing.T) {
	v := hostView("n1:a:counter", "n2:s:counter,other", "n3:d:counter", "n4:a:other", "n5:a", "n6:a:counter")
	ms := hosting(v.Members, "counter")
	var ids []string
	for _, m := range ms {
		ids = append(ids, m.ID)
	}
	if want := []string{"n1", "n2", "n6"}; !slices.Equal(ids, want) {
		t.Fatalf("hosting counter = %v, want %v", ids, want)
	}
	if got := hosting(v.Members, "none"); len(got) != 0 {
		t.Errorf("hosting none = %v, want no members", got)
	}

	var sink int
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("key%d", i)
		owner, runner := lookup(ms, "counter", key)
		want := rank(v, "counter", key, 2)
		if owner < 0 || runner < 0 || ms[owner].ID != want[0] || ms[runner].ID != want[1] {
			t.Fatalf("lookup(%s) = %d, %d, want %v", key, owner, runner, want)
		}
		sink += owner
	}
	if n := testing.AllocsPerRun(100, func() { o, r := lookup(ms, "counter", "key7"); sink += o + r }); n != 0 {
		t.Errorf("lookup allocates %v times, want 0", n)
	}

	// Another kind ranks over the members that host it.
	others := hosting(v.Members, "other")
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("o%d", i)
		owner, runner := lookup(others, "other", key)
		want := rank(v, "other", key, 2)
		if owner < 0 || runner < 0 || others[owner].ID != want[0] || others[runner].ID != want[1] {
			t.Fatalf("lookup(other, %s) = %d, %d, want %v", key, owner, runner, want)
		}
	}

	// Fewer than two hosting members.
	if o, r := lookup(ms[:1], "counter", "k"); o != 0 || r != -1 {
		t.Errorf("lookup over one member = %d, %d, want 0, -1", o, r)
	}
	if o, r := lookup(nil, "counter", "k"); o != -1 || r != -1 {
		t.Errorf("lookup over none = %d, %d, want -1, -1", o, r)
	}
	_ = sink
}
