package slsk

import (
	"encoding/binary"
	"errors"
	"fmt"
)

var le = binary.LittleEndian

// ---------------------------------------------------------------------------
// MessageBuilder — server/peer messages: [4-byte LE length][4-byte LE code][payload]
// Length counts code + payload (NOT itself).
// ---------------------------------------------------------------------------

// MessageBuilder assembles a server/peer protocol message.
type MessageBuilder struct {
	code uint32
	buf  []byte
}

// NewBuilder creates a MessageBuilder for the given 4-byte message code.
func NewBuilder(code uint32) *MessageBuilder {
	return &MessageBuilder{code: code}
}

func (b *MessageBuilder) WriteUint8(v uint8) {
	b.buf = append(b.buf, v)
}

func (b *MessageBuilder) WriteUint32(v uint32) {
	tmp := make([]byte, 4)
	le.PutUint32(tmp, v)
	b.buf = append(b.buf, tmp...)
}

func (b *MessageBuilder) WriteUint64(v uint64) {
	tmp := make([]byte, 8)
	le.PutUint64(tmp, v)
	b.buf = append(b.buf, tmp...)
}

func (b *MessageBuilder) WriteString(s string) {
	b.WriteUint32(uint32(len(s)))
	b.buf = append(b.buf, s...)
}

func (b *MessageBuilder) WriteBytes(data []byte) {
	b.buf = append(b.buf, data...)
}

func (b *MessageBuilder) WriteBool(v bool) {
	if v {
		b.buf = append(b.buf, 1)
	} else {
		b.buf = append(b.buf, 0)
	}
}

// Build returns the complete wire message: [length][code][payload].
func (b *MessageBuilder) Build() []byte {
	payloadLen := 4 + len(b.buf) // 4 for code + payload
	msg := make([]byte, 4+payloadLen)
	le.PutUint32(msg[0:4], uint32(payloadLen))
	le.PutUint32(msg[4:8], b.code)
	copy(msg[8:], b.buf)
	return msg
}

// ---------------------------------------------------------------------------
// MessageReader — server/peer messages
// ---------------------------------------------------------------------------

// MessageReader reads fields from a server/peer protocol message.
type MessageReader struct {
	code uint32
	data []byte // payload after the code
	pos  int
}

// NewReader creates a MessageReader from a full wire message (including length prefix).
// The message must be at least 8 bytes (4 length + 4 code).
func NewReader(msg []byte) (*MessageReader, error) {
	if len(msg) < 8 {
		return nil, errors.New("message too short: need at least 8 bytes")
	}
	code := le.Uint32(msg[4:8])
	return &MessageReader{
		code: code,
		data: msg[8:],
	}, nil
}

// Code returns the 4-byte message code.
func (r *MessageReader) Code() uint32 { return r.code }

// HasMore returns true if there are unread bytes remaining.
func (r *MessageReader) HasMore() bool { return r.pos < len(r.data) }

// Remaining returns the number of unread bytes. Parsers use it to reject
// element counts that cannot fit in what is left of the message.
func (r *MessageReader) Remaining() int { return len(r.data) - r.pos }

func (r *MessageReader) ReadUint8() (uint8, error) {
	if r.pos+1 > len(r.data) {
		return 0, fmt.Errorf("read uint8: need 1 byte, have %d", len(r.data)-r.pos)
	}
	v := r.data[r.pos]
	r.pos++
	return v, nil
}

func (r *MessageReader) ReadUint32() (uint32, error) {
	if r.pos+4 > len(r.data) {
		return 0, fmt.Errorf("read uint32: need 4 bytes, have %d", len(r.data)-r.pos)
	}
	v := le.Uint32(r.data[r.pos : r.pos+4])
	r.pos += 4
	return v, nil
}

func (r *MessageReader) ReadUint64() (uint64, error) {
	if r.pos+8 > len(r.data) {
		return 0, fmt.Errorf("read uint64: need 8 bytes, have %d", len(r.data)-r.pos)
	}
	v := le.Uint64(r.data[r.pos : r.pos+8])
	r.pos += 8
	return v, nil
}

func (r *MessageReader) ReadString() (string, error) {
	slen, err := r.ReadUint32()
	if err != nil {
		return "", fmt.Errorf("read string length: %w", err)
	}
	if r.pos+int(slen) > len(r.data) {
		return "", fmt.Errorf("read string: need %d bytes, have %d", slen, len(r.data)-r.pos)
	}
	s := string(r.data[r.pos : r.pos+int(slen)])
	r.pos += int(slen)
	return s, nil
}

func (r *MessageReader) ReadBytes(n int) ([]byte, error) {
	if r.pos+n > len(r.data) {
		return nil, fmt.Errorf("read bytes: need %d bytes, have %d", n, len(r.data)-r.pos)
	}
	b := make([]byte, n)
	copy(b, r.data[r.pos:r.pos+n])
	r.pos += n
	return b, nil
}

func (r *MessageReader) ReadBool() (bool, error) {
	v, err := r.ReadUint8()
	if err != nil {
		return false, fmt.Errorf("read bool: %w", err)
	}
	return v != 0, nil
}

// ---------------------------------------------------------------------------
// InitMessageBuilder — init/distributed messages: [4-byte LE length][1-byte code][payload]
// Length counts code + payload (NOT itself).
// ---------------------------------------------------------------------------

// InitMessageBuilder assembles an init or distributed protocol message.
type InitMessageBuilder struct {
	code uint8
	buf  []byte
}

// NewInitBuilder creates an InitMessageBuilder for the given 1-byte message code.
func NewInitBuilder(code uint8) *InitMessageBuilder {
	return &InitMessageBuilder{code: code}
}

func (b *InitMessageBuilder) WriteUint8(v uint8) {
	b.buf = append(b.buf, v)
}

func (b *InitMessageBuilder) WriteUint32(v uint32) {
	tmp := make([]byte, 4)
	le.PutUint32(tmp, v)
	b.buf = append(b.buf, tmp...)
}

func (b *InitMessageBuilder) WriteUint64(v uint64) {
	tmp := make([]byte, 8)
	le.PutUint64(tmp, v)
	b.buf = append(b.buf, tmp...)
}

func (b *InitMessageBuilder) WriteString(s string) {
	b.WriteUint32(uint32(len(s)))
	b.buf = append(b.buf, s...)
}

// Build returns the complete wire message: [length][1-byte code][payload].
func (b *InitMessageBuilder) Build() []byte {
	payloadLen := 1 + len(b.buf) // 1 for code + payload
	msg := make([]byte, 4+payloadLen)
	le.PutUint32(msg[0:4], uint32(payloadLen))
	msg[4] = b.code
	copy(msg[5:], b.buf)
	return msg
}

// ---------------------------------------------------------------------------
// InitMessageReader — init/distributed messages
// ---------------------------------------------------------------------------

// InitMessageReader reads fields from an init or distributed protocol message.
type InitMessageReader struct {
	code uint8
	data []byte // payload after the code
	pos  int
}

// NewInitReader creates an InitMessageReader from a full wire message (including length prefix).
// The message must be at least 5 bytes (4 length + 1 code).
func NewInitReader(msg []byte) (*InitMessageReader, error) {
	if len(msg) < 5 {
		return nil, errors.New("init message too short: need at least 5 bytes")
	}
	return &InitMessageReader{
		code: msg[4],
		data: msg[5:],
	}, nil
}

// Code returns the 1-byte message code.
func (r *InitMessageReader) Code() uint8 { return r.code }

// HasMore returns true if there are unread bytes remaining.
func (r *InitMessageReader) HasMore() bool { return r.pos < len(r.data) }

func (r *InitMessageReader) ReadUint32() (uint32, error) {
	if r.pos+4 > len(r.data) {
		return 0, fmt.Errorf("read uint32: need 4 bytes, have %d", len(r.data)-r.pos)
	}
	v := le.Uint32(r.data[r.pos : r.pos+4])
	r.pos += 4
	return v, nil
}

func (r *InitMessageReader) ReadString() (string, error) {
	slen, err := r.ReadUint32()
	if err != nil {
		return "", fmt.Errorf("read string length: %w", err)
	}
	if r.pos+int(slen) > len(r.data) {
		return "", fmt.Errorf("read string: need %d bytes, have %d", slen, len(r.data)-r.pos)
	}
	s := string(r.data[r.pos : r.pos+int(slen)])
	r.pos += int(slen)
	return s, nil
}
