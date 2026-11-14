// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package remoting

import (
	"encoding/binary"
	"strings"
	"unicode/utf8"

	"github.com/hmdsefi/lockstep/internal/engine"
)

// Limits of the hello payload.
const (
	MaxCodecName = 64  // bytes of the codec name; it has at least one
	MaxAdvertise = 255 // bytes of the advertise address; it may be empty
)

// maxNodeName is the longest valid node name (ACT-020).
const maxNodeName = 64

// Hello is the payload of HELLO and WELCOME frames.
type Hello struct {
	Sender      string // the node that writes the frame
	Receiver    string // HELLO: the node the dialer expects to reach; WELCOME: the dialer
	Incarnation uint32 // the sender's incarnation, never 0
	FrameLimit  uint32 // the largest frame the sender accepts, from MinFrameLimit to MaxFrameLimit
	Codec       string // the sender's codec name, 1 to MaxCodecName bytes
	Advertise   string // the address the sender can be dialed at, 0 to MaxAdvertise bytes
}

// AppendHello appends the encoding of h to dst and returns the extended slice. It encodes h as it
// is; ParseHello checks the rules.
func AppendHello(dst []byte, h *Hello) []byte {
	dst = appendString(dst, h.Sender)
	dst = appendString(dst, h.Receiver)
	dst = binary.BigEndian.AppendUint32(dst, h.Incarnation)
	dst = binary.BigEndian.AppendUint32(dst, h.FrameLimit)
	dst = appendString(dst, h.Codec)
	return appendString(dst, h.Advertise)
}

// ParseHello decodes a HELLO or WELCOME payload. Both names must be valid node names, the
// incarnation not 0, the frame limit within [MinFrameLimit, MaxFrameLimit], the codec name 1 to
// MaxCodecName bytes and the advertise address at most MaxAdvertise bytes, and no bytes may follow
// it. A broken rule returns a *ProtocolError about FieldPayload.
func ParseHello(b []byte) (Hello, error) {
	var h Hello
	var v []byte
	var err error
	for _, name := range [...]struct {
		dst  *string
		role string
	}{{&h.Sender, "sender"}, {&h.Receiver, "receiver"}} {
		if v, b, err = lengthPrefixed(b, FieldPayload); err != nil {
			return Hello{}, err
		}
		// The length is checked first so that a peer's long name is neither copied nor quoted.
		if len(v) > maxNodeName {
			return Hello{}, protoErr(FieldPayload, "hello %s of %d bytes is longer than a node name", name.role, len(v))
		}
		if !engine.ValidNodeName(string(v)) {
			return Hello{}, protoErr(FieldPayload, "hello %s %q is not a valid node name", name.role, v)
		}
		*name.dst = string(v)
	}
	if len(b) < 8 {
		return Hello{}, protoErr(FieldPayload, "hello incarnation and frame limit run past the end")
	}
	h.Incarnation = binary.BigEndian.Uint32(b)
	h.FrameLimit = binary.BigEndian.Uint32(b[4:])
	b = b[8:]
	if h.Incarnation == 0 {
		return Hello{}, protoErr(FieldPayload, "hello incarnation is 0")
	}
	if h.FrameLimit < MinFrameLimit || h.FrameLimit > MaxFrameLimit {
		return Hello{}, protoErr(FieldPayload, "hello frame limit %d is not in [%d, %d]", h.FrameLimit, MinFrameLimit, MaxFrameLimit)
	}
	if v, b, err = lengthPrefixed(b, FieldPayload); err != nil {
		return Hello{}, err
	}
	if len(v) == 0 || len(v) > MaxCodecName {
		return Hello{}, protoErr(FieldPayload, "hello codec name of %d bytes is not 1 to %d", len(v), MaxCodecName)
	}
	h.Codec = string(v)
	if v, b, err = lengthPrefixed(b, FieldPayload); err != nil {
		return Hello{}, err
	}
	if len(v) > MaxAdvertise {
		return Hello{}, protoErr(FieldPayload, "hello advertise address of %d bytes exceeds %d", len(v), MaxAdvertise)
	}
	h.Advertise = string(v)
	if len(b) > 0 {
		return Hello{}, protoErr(FieldPayload, "%d bytes follow the hello payload", len(b))
	}
	return h, nil
}

// AppendAsk appends an ASK payload to dst, the ask address text followed by the codec bytes of
// the request, and returns the extended slice.
func AppendAsk(dst []byte, askAddr string, body []byte) []byte {
	return append(appendString(dst, askAddr), body...)
}

// ParseAsk splits an ASK payload into the ask address text and the codec bytes that follow it. A
// length that runs past the payload returns a *ProtocolError about FieldPayload. The body aliases
// payload; it is nil when empty.
func ParseAsk(payload []byte) (askAddr string, body []byte, err error) {
	v, rest, err := lengthPrefixed(payload, FieldPayload)
	if err != nil {
		return "", nil, err
	}
	if len(rest) == 0 {
		rest = nil
	}
	return string(v), rest, nil
}

// Failure is the payload of a FAIL frame, and the failure of a TERMINATED frame whose actor failed.
// Code is the wire byte of the failure code; a byte that names no code is kept as it is.
type Failure struct {
	Code    uint8
	Message string
}

// AppendFailure appends the encoding of f to dst and returns the extended slice. Bytes of the
// message that are not valid UTF-8 are replaced with U+FFFD.
func AppendFailure(dst []byte, f *Failure) []byte {
	dst = append(dst, f.Code)
	return append(dst, strings.ToValidUTF8(f.Message, "�")...)
}

// ParseFailure decodes a failure: the code byte, then the message as the rest of b, which must be
// valid UTF-8. A broken rule returns a *ProtocolError about FieldPayload.
func ParseFailure(b []byte) (Failure, error) {
	if len(b) == 0 {
		return Failure{}, protoErr(FieldPayload, "failure has no code byte")
	}
	if !utf8.Valid(b[1:]) {
		return Failure{}, protoErr(FieldPayload, "failure message is not valid UTF-8")
	}
	return Failure{Code: b[0], Message: string(b[1:])}, nil
}

// AppendWatch appends the payload of a WATCH or UNWATCH frame to dst, the watcher's watch number n
// as a uvarint, and returns the extended slice. It encodes n as it is; ParseWatch rejects 0.
func AppendWatch(dst []byte, n uint64) []byte {
	return binary.AppendUvarint(dst, n)
}

// ParseWatch decodes the payload of a WATCH or UNWATCH frame: one uvarint watch number, not 0, and
// nothing after it. A broken rule returns a *ProtocolError about FieldPayload.
func ParseWatch(b []byte) (uint64, error) {
	n, rest, err := uvarint(b, FieldPayload)
	switch {
	case err != nil:
		return 0, err
	case n == 0:
		return 0, protoErr(FieldPayload, "watch number is 0")
	case len(rest) > 0:
		return 0, protoErr(FieldPayload, "%d bytes follow the watch number", len(rest))
	}
	return n, nil
}

// StopKind says why a watched actor stopped, as a TERMINATED frame carries it.
type StopKind uint8

// The stop kinds.
const (
	StopNormal         StopKind = 0 // the actor stopped normally
	StopFailed         StopKind = 1 // the actor failed; Terminated.Failure holds the error
	StopParentStopping StopKind = 2 // the actor's parent stopped
	StopShutdown       StopKind = 3 // the actor's system shut down
	StopNoActor        StopKind = 4 // no such actor was alive when the watch arrived
)

// Terminated is the payload of a TERMINATED frame.
type Terminated struct {
	// Watch is the watcher's number for the watch that ended, which its WATCH carried; never 0.
	Watch   uint64
	Kind    StopKind
	Failure Failure // set only when Kind is StopFailed
}

// AppendTerminated appends the encoding of t to dst and returns the extended slice: the watch
// number as a uvarint, the stop kind, and the failure, which is written only when t.Kind is
// StopFailed. It encodes t as it is; ParseTerminated rejects watch number 0.
func AppendTerminated(dst []byte, t *Terminated) []byte {
	dst = binary.AppendUvarint(dst, t.Watch)
	dst = append(dst, byte(t.Kind))
	if t.Kind == StopFailed {
		dst = AppendFailure(dst, &t.Failure)
	}
	return dst
}

// ParseTerminated decodes a TERMINATED payload: a uvarint watch number, not 0, then a stop kind from
// StopNormal to StopNoActor, followed by a failure when it is StopFailed and by nothing otherwise.
// A broken rule returns a *ProtocolError about FieldPayload.
func ParseTerminated(b []byte) (Terminated, error) {
	n, b, err := uvarint(b, FieldPayload)
	switch {
	case err != nil:
		return Terminated{}, err
	case n == 0:
		return Terminated{}, protoErr(FieldPayload, "terminated watch number is 0")
	case len(b) == 0:
		return Terminated{}, protoErr(FieldPayload, "terminated payload has no stop kind")
	}
	t := Terminated{Watch: n, Kind: StopKind(b[0])}
	switch {
	case t.Kind == StopFailed:
		f, err := ParseFailure(b[1:])
		if err != nil {
			return Terminated{}, err
		}
		t.Failure = f
	case t.Kind > StopNoActor:
		return Terminated{}, protoErr(FieldPayload, "unknown stop kind %d", b[0])
	case len(b) > 1:
		return Terminated{}, protoErr(FieldPayload, "%d bytes follow stop kind %d", len(b)-1, b[0])
	}
	return t, nil
}
