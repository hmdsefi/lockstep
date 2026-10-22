// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

const (
	maxNodeLen = 64  // ACT-020
	maxNameLen = 768 // ACT-310
)

// validNode reports whether s is a valid node name: 1 to 64 bytes from [A-Za-z0-9._-], starting
// with a letter or digit (ACT-020).
func validNode(s string) bool {
	if len(s) == 0 || len(s) > maxNodeLen || !isAlnum(s[0]) {
		return false
	}
	for i := 1; i < len(s); i++ {
		if c := s[i]; !isAlnum(c) && c != '.' && c != '_' && c != '-' {
			return false
		}
	}
	return true
}

// validName reports whether s is a valid actor name: 1 to 768 bytes from [A-Za-z0-9._%-], neither
// "." nor ".." (ACT-310). The "$" of generated names is outside the set.
func validName(s string) bool {
	if len(s) == 0 || len(s) > maxNameLen || s == "." || s == ".." {
		return false
	}
	for i := 0; i < len(s); i++ {
		if c := s[i]; !isAlnum(c) && c != '.' && c != '_' && c != '%' && c != '-' {
			return false
		}
	}
	return true
}

// validPath reports whether s is a well-formed actor path (ACT-510): "/" followed by one or more
// segments separated by single "/", with no trailing "/", where each segment is a valid name or
// "$" followed by a decimal number without leading zeros.
func validPath(s string) bool {
	if len(s) < 2 || s[0] != '/' {
		return false
	}
	rest := s[1:]
	for {
		seg, more, ok := cutSegment(rest)
		if !validName(seg) && !validGenerated(seg) {
			return false
		}
		if !ok {
			return true
		}
		rest = more
	}
}

// cutSegment splits s at its first "/". ok reports whether there was one; after is what follows.
func cutSegment(s string) (seg, after string, ok bool) {
	for i := 0; i < len(s); i++ {
		if s[i] == '/' {
			return s[:i], s[i+1:], true
		}
	}
	return s, "", false
}

// validGenerated reports whether s is "$" followed by a decimal number without leading zeros, the
// form of a generated name (ACT-320).
func validGenerated(s string) bool {
	if len(s) < 2 || s[0] != '$' {
		return false
	}
	digits := s[1:]
	if digits[0] == '0' && len(digits) > 1 {
		return false
	}
	for i := 0; i < len(digits); i++ {
		if digits[i] < '0' || digits[i] > '9' {
			return false
		}
	}
	return true
}

func isAlnum(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
}
