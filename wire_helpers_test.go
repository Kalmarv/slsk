package slsk

import "encoding/binary"

// buildMsg constructs a raw wire message from code and payload bytes.
// Format: [4-byte LE length][4-byte LE code][payload]
func buildMsg(code uint32, payload []byte) []byte {
	msgLen := 4 + len(payload)
	msg := make([]byte, 4+msgLen)
	binary.LittleEndian.PutUint32(msg[0:4], uint32(msgLen))
	binary.LittleEndian.PutUint32(msg[4:8], code)
	copy(msg[8:], payload)
	return msg
}

// appendStr appends a length-prefixed string to a byte slice.
func appendStr(buf []byte, s string) []byte {
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(s)))
	return append(buf, s...)
}

// reversedIP returns the 4 wire bytes for an IP address (reversed order).
func reversedIP(a, b, c, d byte) []byte {
	return []byte{d, c, b, a}
}

// u32le encodes v as 4 little-endian bytes.
func u32le(v uint32) []byte {
	return binary.LittleEndian.AppendUint32(nil, v)
}
