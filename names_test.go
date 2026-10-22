// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"strings"
	"testing"
)

// AT-ACT-15, ACT-310.
func TestValidName(t *testing.T) {
	valid := []string{"a", "o-17", "A.b_c%2Fd-e", strings.Repeat("a", 768), "...", ".a", "a.", "-", "_", "%", "0", "Z9"}
	invalid := []string{"", ".", "..", "a/b", "a b", "$1", "é", "a~b", "a:b", "a@b", "a+b", strings.Repeat("a", 769),
		"a\x00b", "a\nb", "/", "aé", "a*", "a,b"}
	for _, s := range valid {
		if !validName(s) {
			t.Errorf("validName(%q) = false, want true", short(s))
		}
	}
	for _, s := range invalid {
		if validName(s) {
			t.Errorf("validName(%q) = true, want false", short(s))
		}
	}
}

// AT-ACT-22, ACT-510.
func TestValidPath(t *testing.T) {
	valid := []string{"/user/a", "/user/$1", "/user/a/b", "/user/a/$12/b", "/a", "/user/$10", "/user/$0",
		"/user/" + strings.Repeat("a", 768), "/user/a.b/c_d/e%2F"}
	invalid := []string{"", "user/a", "/user/", "/user//a", "/user/$01", "/user/a b", "/", "//", "/user/a/",
		"/$", "/user/$", "/user/$a", "/user/$1a", "/user/$-1", "/user/$1$", "/user/.", "/user/..", "/user/a/./b",
		"/user/" + strings.Repeat("a", 769), "/user/é", "user", "/user/$ 1", "/user/a?b"}
	for _, s := range valid {
		if !validPath(s) {
			t.Errorf("validPath(%q) = false, want true", short(s))
		}
	}
	for _, s := range invalid {
		if validPath(s) {
			t.Errorf("validPath(%q) = true, want false", short(s))
		}
	}
}

// ACT-020: ^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$.
func TestValidNode(t *testing.T) {
	valid := []string{"local", "n1", "A", "0", "a.b_c-d", "9z", strings.Repeat("a", 64)}
	invalid := []string{"", "-x", ".a", "_a", "a b", "a/b", "a:b", "é", "a%b", "a$", strings.Repeat("a", 65), "a\n", "node\x00"}
	for _, s := range valid {
		if !validNode(s) {
			t.Errorf("validNode(%q) = false, want true", short(s))
		}
	}
	for _, s := range invalid {
		if validNode(s) {
			t.Errorf("validNode(%q) = true, want false", short(s))
		}
	}
}

func short(s string) string {
	if len(s) > 20 {
		return s[:10] + "..." + s[len(s)-5:]
	}
	return s
}
