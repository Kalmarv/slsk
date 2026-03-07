package slsk

import (
	"fmt"
	"io"
	"net"
	"sync"
)

// maxMessageSize is the largest payload we accept (100 MB).
// Messages claiming a larger size are rejected as corrupt or malicious.
const maxMessageSize = 100 * 1024 * 1024 // 100 MB

// Conn wraps a net.Conn with Soulseek message framing.
// Every message on the wire is prefixed with a 4-byte little-endian length.
type Conn struct {
	raw net.Conn
	mu  sync.Mutex // protects writes
}

// WrapConn wraps an existing net.Conn with Soulseek message framing.
func WrapConn(c net.Conn) *Conn {
	return &Conn{raw: c}
}

// ReadMessage reads a single framed message from the connection.
// It returns the complete message including the 4-byte length prefix,
// so the result can be passed directly to NewReader or NewInitReader.
func (c *Conn) ReadMessage() ([]byte, error) {
	// 1. Read the 4-byte length prefix.
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(c.raw, lenBuf); err != nil {
		return nil, err
	}

	// 2. Parse the payload size.
	payloadLen := le.Uint32(lenBuf)

	// 3. Sanity check: reject oversized messages.
	if payloadLen > maxMessageSize {
		return nil, fmt.Errorf("message too large: %d bytes (max %d)", payloadLen, maxMessageSize)
	}

	// 4. Read exactly payloadLen bytes.
	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(c.raw, payload); err != nil {
		return nil, err
	}

	// 5. Return the full message: length prefix + payload.
	msg := make([]byte, 4+payloadLen)
	copy(msg[:4], lenBuf)
	copy(msg[4:], payload)
	return msg, nil
}

// WriteMessage writes a complete framed message to the connection.
// The msg must already include the 4-byte length prefix (as returned by Build()).
// WriteMessage is safe for concurrent use.
func (c *Conn) WriteMessage(msg []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	_, err := c.raw.Write(msg)
	return err
}

// Raw returns the underlying net.Conn for unframed I/O (e.g., file transfers).
func (c *Conn) Raw() net.Conn {
	return c.raw
}

// Close closes the underlying connection.
func (c *Conn) Close() error {
	return c.raw.Close()
}

// RemoteAddr returns the remote network address of the underlying connection.
func (c *Conn) RemoteAddr() net.Addr {
	return c.raw.RemoteAddr()
}
