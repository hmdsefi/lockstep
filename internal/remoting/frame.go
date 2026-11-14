// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package remoting

import (
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/hmdsefi/lockstep/internal/engine"
)

// Version is the protocol version byte of every frame.
const Version = 1

// Frame lengths. A frame's length L counts the bytes after its 4-byte length prefix.
const (
	MinFrameLen   = 16       // the smallest L: the fixed part and four empty length prefixes
	MinFrameLimit = 4096     // the smallest frame limit a HELLO or WELCOME may announce
	MaxFrameLimit = 64 << 20 // the largest frame limit a HELLO or WELCOME may announce
)

// Header limits that a received frame must keep.
const (
	MaxHeaders     = 32   // headers per frame
	MaxHeaderKey   = 256  // bytes per key; a key has at least one byte
	MaxHeaderValue = 8192 // bytes per value
)

// MaxReason is the longest REJECT or CLOSE reason text, in bytes.
const MaxReason = 64

// The REJECT reasons.
const (
	RejectVersion   = "version"
	RejectKind      = "kind"
	RejectPayload   = "payload"
	RejectSelf      = "self"
	RejectWrongNode = "wrong-node"
	RejectCodec     = "codec"
)

// The CLOSE reasons.
const (
	CloseShutdown          = "shutdown"
	CloseReplaced          = "replaced"
	CloseIdle              = "idle"
	CloseProtocol          = "protocol"
	CloseUnknownConnection = "unknown-connection"
)

// prefixLen is the size of the length prefix; fixedLen the size of version, kind, flags and seq.
const (
	prefixLen = 4
	fixedLen  = 12
)

// Kind is a frame's kind byte. Kinds below 16 are control frames, which carry sequence number 0;
// the others are data frames, which carry a sequence number above 0.
type Kind uint8

// The frame kinds.
const (
	KindHello      Kind = 1  // dialer's handshake; payload: hello
	KindWelcome    Kind = 2  // acceptor's handshake answer; payload: hello
	KindReject     Kind = 3  // acceptor refuses a HELLO; payload: reason text
	KindPing       Kind = 4  // dialer's heartbeat
	KindPong       Kind = 5  // acceptor's heartbeat answer
	KindClose      Kind = 6  // either side ends the session; payload: reason text
	KindMsg        Kind = 16 // a message; payload: codec bytes
	KindAsk        Kind = 17 // a request; payload: ask address, then codec bytes
	KindReply      Kind = 18 // an answer to an ask; payload: codec bytes
	KindFail       Kind = 19 // a failed ask; payload: failure
	KindWatch      Kind = 20 // starts watching an actor
	KindUnwatch    Kind = 21 // stops watching an actor
	KindTerminated Kind = 22 // a watched actor stopped; payload: terminated
	KindLookup     Kind = 23 // looks an actor up by path
)

// payloadRule says what a kind's payload must hold.
type payloadRule uint8

const (
	payloadEmpty payloadRule = iota
	payloadAny
	payloadHello
	payloadReason
	payloadAsk
	payloadFailure
	payloadTerminated
)

// kindInfo is one row of the kind table (REM-060): which fields may be non-empty.
type kindInfo struct {
	name                                string
	data                                bool
	headers, sender, recipient, typeKey bool
	payload                             payloadRule
}

// kinds is the kind table (REM-060), indexed by kind byte; a row without a name is not a kind.
// Headers ride only on MSG, ASK and REPLY (R25); LOOKUP's type may be empty.
var kinds = [...]kindInfo{
	KindHello:      {name: "HELLO", payload: payloadHello},
	KindWelcome:    {name: "WELCOME", payload: payloadHello},
	KindReject:     {name: "REJECT", payload: payloadReason},
	KindPing:       {name: "PING", payload: payloadEmpty},
	KindPong:       {name: "PONG", payload: payloadEmpty},
	KindClose:      {name: "CLOSE", payload: payloadReason},
	KindMsg:        {name: "MSG", data: true, headers: true, sender: true, recipient: true, typeKey: true, payload: payloadAny},
	KindAsk:        {name: "ASK", data: true, headers: true, sender: true, recipient: true, typeKey: true, payload: payloadAsk},
	KindReply:      {name: "REPLY", data: true, headers: true, sender: true, recipient: true, typeKey: true, payload: payloadAny},
	KindFail:       {name: "FAIL", data: true, sender: true, recipient: true, payload: payloadFailure},
	KindWatch:      {name: "WATCH", data: true, sender: true, recipient: true, payload: payloadEmpty},
	KindUnwatch:    {name: "UNWATCH", data: true, sender: true, recipient: true, payload: payloadEmpty},
	KindTerminated: {name: "TERMINATED", data: true, sender: true, recipient: true, payload: payloadTerminated},
	KindLookup:     {name: "LOOKUP", data: true, sender: true, recipient: true, typeKey: true, payload: payloadEmpty},
}

func (k Kind) info() (kindInfo, bool) {
	if int(k) < len(kinds) && kinds[k].name != "" {
		return kinds[k], true
	}
	return kindInfo{}, false
}

// Known reports whether k is one of the frame kinds.
func (k Kind) Known() bool {
	_, ok := k.info()
	return ok
}

// Data reports whether k is a data frame kind, one whose frames carry a sequence number above 0.
func (k Kind) Data() bool {
	in, _ := k.info()
	return in.data
}

// String returns the kind's name, such as "HELLO", or "Kind(<n>)" for a byte that is not a kind.
func (k Kind) String() string {
	if in, ok := k.info(); ok {
		return in.name
	}
	return "Kind(" + strconv.Itoa(int(k)) + ")"
}

// Frame is one decoded frame. Which fields a frame may fill depends on its Kind: control frames
// carry no headers, sender, recipient or type, and only MSG, ASK and REPLY carry headers.
type Frame struct {
	Kind      Kind
	Seq       uint64          // 0 for control frames, above 0 for data frames
	Headers   []engine.Header // nil when there are none
	Sender    string          // address text, or empty
	Recipient string          // address text, or empty
	Type      string          // wire name or type key, or empty
	Payload   []byte          // the rest of the frame; nil when empty
}

// AppendFrame appends the encoding of f, length prefix included, to dst and returns the extended
// slice. It encodes f as it is: checking f against the kind table and the frame limits is the
// caller's job, and a frame of 4 GiB or more cannot be encoded.
func AppendFrame(dst []byte, f *Frame) []byte {
	start := len(dst)
	dst = append(dst, 0, 0, 0, 0, Version, byte(f.Kind), 0, 0)
	dst = binary.BigEndian.AppendUint64(dst, f.Seq)
	dst = binary.AppendUvarint(dst, uint64(len(f.Headers)))
	for _, h := range f.Headers {
		dst = appendString(dst, h.Key)
		dst = appendString(dst, h.Value)
	}
	dst = appendString(dst, f.Sender)
	dst = appendString(dst, f.Recipient)
	dst = appendString(dst, f.Type)
	dst = append(dst, f.Payload...)
	binary.BigEndian.PutUint32(dst[start:], uint32(len(dst)-start-prefixLen)) //nolint:gosec // G115: frames of 4 GiB or more are documented as not encodable
	return dst
}

// ParseFrame decodes b, which holds exactly one frame with its length prefix, and checks it
// against every rule of the protocol: the length against MinFrameLen and maxFrame, the version,
// the kind, zero flags, the sequence number's class, the header limits, the fields the kind
// leaves empty, and the kind's payload layout. A broken rule returns a *ProtocolError. The
// returned Payload aliases b; the strings are copies.
func ParseFrame(b []byte, maxFrame int) (Frame, error) {
	if len(b) < prefixLen {
		return Frame{}, protoErr(FieldLength, "%d bytes hold no length prefix", len(b))
	}
	l, err := frameLen(b, maxFrame)
	if err != nil {
		return Frame{}, err
	}
	if len(b)-prefixLen != l {
		return Frame{}, protoErr(FieldLength, "length %d but %d bytes follow the prefix", l, len(b)-prefixLen)
	}
	b = b[prefixLen:]
	if b[0] != Version {
		return Frame{}, protoErr(FieldVersion, "version %d is not %d", b[0], Version)
	}
	f := Frame{Kind: Kind(b[1])}
	in, ok := f.Kind.info()
	if !ok {
		return Frame{}, protoErr(FieldKind, "unknown kind %d", b[1])
	}
	if flags := binary.BigEndian.Uint16(b[2:]); flags != 0 {
		return Frame{}, protoErr(FieldFlags, "flags %#04x are not 0", flags)
	}
	f.Seq = binary.BigEndian.Uint64(b[4:])
	switch {
	case in.data && f.Seq == 0:
		return Frame{}, protoErr(FieldSeq, "%s frame has sequence number 0", f.Kind)
	case !in.data && f.Seq != 0:
		return Frame{}, protoErr(FieldSeq, "%s frame has sequence number %d, not 0", f.Kind, f.Seq)
	}
	p := b[fixedLen:]

	if f.Headers, p, err = parseHeaders(p, f.Kind, in.headers); err != nil {
		return Frame{}, err
	}
	for _, fd := range [...]struct {
		dst     *string
		field   Field
		allowed bool
	}{
		{&f.Sender, FieldSender, in.sender},
		{&f.Recipient, FieldRecipient, in.recipient},
		{&f.Type, FieldType, in.typeKey},
	} {
		var v []byte
		if v, p, err = lengthPrefixed(p, fd.field); err != nil {
			return Frame{}, err
		}
		if len(v) > 0 && !fd.allowed {
			return Frame{}, protoErr(fd.field, "%s frame must leave it empty", f.Kind)
		}
		*fd.dst = string(v)
	}

	if len(p) > 0 {
		f.Payload = p
	}
	if err := checkPayload(f.Kind, in.payload, p); err != nil {
		return Frame{}, err
	}
	return f, nil
}

// SplitFrame cuts the first frame off a byte stream. It returns the frame with its length prefix
// and the bytes after it, or a nil frame and buf unchanged when buf does not yet hold a whole
// frame. A length prefix outside [MinFrameLen, maxFrame] is a *ProtocolError as soon as its four
// bytes have arrived. The frame is only cut, not checked; pass it to ParseFrame.
func SplitFrame(buf []byte, maxFrame int) (frame, rest []byte, err error) {
	if len(buf) < prefixLen {
		return nil, buf, nil
	}
	l, err := frameLen(buf, maxFrame)
	if err != nil {
		return nil, buf, err
	}
	n := prefixLen + l
	if len(buf) < n {
		return nil, buf, nil
	}
	return buf[:n:n], buf[n:], nil
}

// ValidHeaders reports whether h keeps the header limits that a receiver checks: at most
// MaxHeaders headers, keys of 1 to MaxHeaderKey bytes and values of at most MaxHeaderValue bytes.
func ValidHeaders(h []engine.Header) bool {
	if len(h) > MaxHeaders {
		return false
	}
	for _, x := range h {
		if len(x.Key) == 0 || len(x.Key) > MaxHeaderKey || len(x.Value) > MaxHeaderValue {
			return false
		}
	}
	return true
}

// frameLen reads the length prefix of b, which has at least four bytes, and checks its range.
func frameLen(b []byte, maxFrame int) (int, error) {
	l := int64(binary.BigEndian.Uint32(b))
	if l < MinFrameLen {
		return 0, protoErr(FieldLength, "length %d is below %d", l, MinFrameLen)
	}
	if l > int64(maxFrame) {
		return 0, protoErr(FieldLength, "length %d exceeds the frame limit %d", l, maxFrame)
	}
	return int(l), nil
}

// parseHeaders reads the headers section (REM-050) and checks OTL-052's limits on receipt.
func parseHeaders(p []byte, k Kind, allowed bool) ([]engine.Header, []byte, error) {
	n, p, err := uvarint(p, FieldHeaders)
	if err != nil {
		return nil, nil, err
	}
	if n == 0 {
		return nil, p, nil
	}
	if !allowed {
		return nil, nil, protoErr(FieldHeaders, "%s frame may carry no headers; it carries %d", k, n)
	}
	if n > MaxHeaders {
		return nil, nil, protoErr(FieldHeaders, "%d headers exceed %d", n, MaxHeaders)
	}
	hs := make([]engine.Header, n)
	for i := range hs {
		var key, value []byte
		if key, p, err = lengthPrefixed(p, FieldHeaders); err != nil {
			return nil, nil, err
		}
		if len(key) == 0 || len(key) > MaxHeaderKey {
			return nil, nil, protoErr(FieldHeaders, "header %d: key of %d bytes is not 1 to %d", i, len(key), MaxHeaderKey)
		}
		if value, p, err = lengthPrefixed(p, FieldHeaders); err != nil {
			return nil, nil, err
		}
		if len(value) > MaxHeaderValue {
			return nil, nil, protoErr(FieldHeaders, "header %d: value of %d bytes exceeds %d", i, len(value), MaxHeaderValue)
		}
		hs[i] = engine.Header{Key: string(key), Value: string(value)}
	}
	return hs, p, nil
}

// checkPayload checks p against the payload layout of kind k (REM-060, REM-065, REM-068).
func checkPayload(k Kind, r payloadRule, p []byte) error {
	var err error
	switch r {
	case payloadEmpty:
		if len(p) > 0 {
			err = protoErr(FieldPayload, "%s frame may carry no payload; it carries %d bytes", k, len(p))
		}
	case payloadHello:
		_, err = ParseHello(p)
	case payloadReason:
		err = checkReason(p)
	case payloadAsk:
		_, _, err = ParseAsk(p)
	case payloadFailure:
		_, err = ParseFailure(p)
	case payloadTerminated:
		_, err = ParseTerminated(p)
	case payloadAny:
	}
	return err
}

// checkReason checks a REJECT or CLOSE reason: 1 to MaxReason printable ASCII bytes.
func checkReason(p []byte) error {
	if len(p) == 0 || len(p) > MaxReason {
		return protoErr(FieldPayload, "reason of %d bytes is not 1 to %d", len(p), MaxReason)
	}
	for i, c := range p {
		if c < 0x20 || c > 0x7e {
			return protoErr(FieldPayload, "reason byte %d is %#02x, not printable ASCII", i, c)
		}
	}
	return nil
}

func appendString(dst []byte, s string) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(s)))
	return append(dst, s...)
}

// uvarint reads one uvarint from p.
func uvarint(p []byte, field Field) (uint64, []byte, error) {
	v, n := binary.Uvarint(p)
	switch {
	case n == 0:
		return 0, nil, protoErr(field, "length runs past the end of the frame")
	case n < 0:
		return 0, nil, protoErr(field, "length overflows 64 bits")
	}
	return v, p[n:], nil
}

// lengthPrefixed reads a uvarint length and that many bytes from p.
func lengthPrefixed(p []byte, field Field) (v, rest []byte, err error) {
	n, p, err := uvarint(p, field)
	if err != nil {
		return nil, nil, err
	}
	if n > uint64(len(p)) {
		return nil, nil, protoErr(field, "length %d runs past the end of the frame (%d bytes left)", n, len(p))
	}
	return p[:n], p[n:], nil
}

// Field names the part of a frame that a ProtocolError is about.
type Field uint8

// The parts of a frame.
const (
	FieldLength    Field = iota + 1 // the length prefix, or the frame's size as a whole
	FieldVersion                    // the version byte
	FieldKind                       // the kind byte
	FieldFlags                      // the flags
	FieldSeq                        // the sequence number
	FieldHeaders                    // the headers section
	FieldSender                     // the sender field
	FieldRecipient                  // the recipient field
	FieldType                       // the type field
	FieldPayload                    // the payload
)

var fieldNames = [...]string{
	FieldLength:    "length",
	FieldVersion:   "version",
	FieldKind:      "kind",
	FieldFlags:     "flags",
	FieldSeq:       "seq",
	FieldHeaders:   "headers",
	FieldSender:    "sender",
	FieldRecipient: "recipient",
	FieldType:      "type",
	FieldPayload:   "payload",
}

// String returns the field's name, such as "payload", or "Field(<n>)" for an unknown value.
func (f Field) String() string {
	if int(f) < len(fieldNames) && fieldNames[f] != "" {
		return fieldNames[f]
	}
	return "Field(" + strconv.Itoa(int(f)) + ")"
}

// ProtocolError reports a received frame or payload that breaks the protocol. Field says which
// part of the frame is wrong and Detail how.
type ProtocolError struct {
	Field  Field
	Detail string
}

// Error returns "remoting: protocol error: <Field>: <Detail>".
func (e *ProtocolError) Error() string {
	return "remoting: protocol error: " + e.Field.String() + ": " + e.Detail
}

func protoErr(field Field, format string, args ...any) *ProtocolError {
	return &ProtocolError{Field: field, Detail: fmt.Sprintf(format, args...)}
}
