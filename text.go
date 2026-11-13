// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"fmt"
	"strconv"
)

const (
	addressScheme = "lockstep://"
	replyScheme   = "lockstep-reply://"
	askPrefix     = "/temp/"
)

// MarshalText returns the text form of a: "lockstep://<node><path>#<uid>", with the uid in
// lowercase hexadecimal without leading zeros ("0" for uid 0). The zero Address marshals to empty
// text. The error is always nil.
func (a Address) MarshalText() ([]byte, error) {
	if a.IsZero() {
		return []byte{}, nil
	}
	return []byte(a.String()), nil
}

// UnmarshalText sets a from text that MarshalText produced, or from empty text, which gives the
// zero Address. The node must be a valid node name and the path a well-formed actor path. Any other
// text returns an error reading `lockstep: invalid address "<text>"` and leaves a unchanged.
func (a *Address) UnmarshalText(text []byte) error {
	if len(text) == 0 {
		*a = Address{}
		return nil
	}
	node, path, uid, ok := splitAddress(string(text), addressScheme)
	if !ok || !validNode(node) || !validPath(path) {
		return fmt.Errorf("lockstep: invalid address %q", text)
	}
	*a = Address{node: node, path: path, uid: uid}
	return nil
}

// MarshalText returns p's address in the text form of Address.MarshalText.
func (p PID[M]) MarshalText() ([]byte, error) { return p.addr.MarshalText() }

// UnmarshalText sets p from the text form of an Address, or from empty text, which gives the zero
// PID. It does not check M. Invalid text returns the error of Address.UnmarshalText and leaves p
// unchanged.
func (p *PID[M]) UnmarshalText(text []byte) error {
	var a Address
	if err := a.UnmarshalText(text); err != nil {
		return err
	}
	p.addr = a
	return nil
}

// MarshalText returns the text form of rt: "lockstep-reply://<node>/temp/<n>#<uid>", where n is
// the ask number in decimal and the uid is in lowercase hexadecimal. The zero ReplyTo marshals to
// empty text. A ReplyTo whose address has another form returns an error reading
// `lockstep: invalid reply address "<address>"`.
func (rt ReplyTo[R]) MarshalText() ([]byte, error) {
	a := rt.addr
	if a.IsZero() {
		return []byte{}, nil
	}
	if a.uid == 0 || !validAskPath(a.path) || !validNode(a.node) {
		return nil, fmt.Errorf("lockstep: invalid reply address %q", a.String())
	}
	return []byte(replyScheme + a.node + a.path + "#" + strconv.FormatUint(a.uid, 16)), nil
}

// UnmarshalText sets rt from text that MarshalText produced, or from empty text, which gives the
// zero ReplyTo. Any other text returns an error reading `lockstep: invalid reply address "<text>"`
// and leaves rt unchanged.
func (rt *ReplyTo[R]) UnmarshalText(text []byte) error {
	if len(text) == 0 {
		rt.addr = Address{}
		return nil
	}
	node, path, uid, ok := splitAddress(string(text), replyScheme)
	if !ok || uid == 0 || !validNode(node) || !validAskPath(path) {
		return fmt.Errorf("lockstep: invalid reply address %q", text)
	}
	rt.addr = Address{node: node, path: path, uid: uid}
	return nil
}

// splitAddress splits s, which must start with scheme, into node, path and uid. The node ends at
// the first "/" after the scheme and the path at the last "#". It checks the uid's form (lowercase
// hexadecimal, no leading zeros, at most 64 bits) but not the node or the path.
func splitAddress(s, scheme string) (node, path string, uid uint64, ok bool) {
	if len(s) < len(scheme) || s[:len(scheme)] != scheme {
		return "", "", 0, false
	}
	rest := s[len(scheme):]
	slash := -1
	for i := 0; i < len(rest); i++ {
		if rest[i] == '/' {
			slash = i
			break
		}
	}
	if slash < 0 {
		return "", "", 0, false
	}
	hash := -1
	for i := len(rest) - 1; i > slash; i-- {
		if rest[i] == '#' {
			hash = i
			break
		}
	}
	if hash < 0 {
		return "", "", 0, false
	}
	uid, ok = parseHexUID(rest[hash+1:])
	if !ok {
		return "", "", 0, false
	}
	return rest[:slash], rest[slash:hash], uid, true
}

// parseHexUID parses s as the output of strconv.FormatUint(uid, 16): lowercase hexadecimal digits
// without leading zeros, "0" for zero.
func parseHexUID(s string) (uint64, bool) {
	if len(s) == 0 || len(s) > 16 || s[0] == '0' && len(s) > 1 {
		return 0, false
	}
	for i := 0; i < len(s); i++ {
		if c := s[i]; c < '0' || c > '9' && c < 'a' || c > 'f' {
			return 0, false
		}
	}
	uid, err := strconv.ParseUint(s, 16, 64)
	return uid, err == nil
}

// validAskPath reports whether s is "/temp/<n>" with n a decimal number of at least 1, without
// leading zeros, that fits in 64 bits: the path of an ask address.
func validAskPath(s string) bool {
	if len(s) <= len(askPrefix) || s[:len(askPrefix)] != askPrefix {
		return false
	}
	digits := s[len(askPrefix):]
	if digits[0] == '0' || len(digits) > 20 {
		return false
	}
	for i := 0; i < len(digits); i++ {
		if digits[i] < '0' || digits[i] > '9' {
			return false
		}
	}
	_, err := strconv.ParseUint(digits, 10, 64)
	return err == nil
}
