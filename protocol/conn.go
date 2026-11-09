package protocol

import (
	"fmt"
	"io"
	"net"
	"slices"
	"sync"
	"time"
)

// Largest payloads we accept. Messages claiming a larger size are rejected as
// corrupt or malicious. The server sends big lists (rooms, privileged users);
// peers only send small control messages and compressed search responses
// (mantis never requests a browse), so their limit is much lower.
const (
	maxMessageSize     = 100 * 1024 * 1024 // server connection
	maxPeerMessageSize = 4 * 1024 * 1024   // peer, transfer and distributed connections
)

// Conn wraps a net.Conn with Soulseek message framing.
// Every message on the wire is prefixed with a 4-byte little-endian length.
type Conn struct {
	raw          net.Conn
	maxSize      uint32        // largest payload ReadMessage accepts
	writeTimeout time.Duration // bound on one WriteMessage
	mu           sync.Mutex    // protects writes
}

// writeTimeout is how long one framed write may block. Control messages are
// small, so a peer that does not drain its socket for this long is gone; without
// a bound it would hold the write lock (and the calling goroutine) forever.
const writeTimeout = 30 * time.Second

// WrapConn wraps an existing server net.Conn with Soulseek message framing.
func WrapConn(c net.Conn) *Conn {
	return &Conn{raw: c, maxSize: maxMessageSize, writeTimeout: writeTimeout}
}

// WrapPeerConn is WrapConn for connections to other peers, which accept only
// small messages.
func WrapPeerConn(c net.Conn) *Conn {
	return &Conn{raw: c, maxSize: maxPeerMessageSize, writeTimeout: writeTimeout}
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
	if payloadLen > c.maxSize {
		return nil, fmt.Errorf("message too large: %d bytes (max %d)", payloadLen, c.maxSize)
	}

	// 4. Read exactly payloadLen bytes, growing the buffer as data arrives so a
	// peer that claims a large message but sends nothing cannot force a large
	// upfront allocation.
	const readChunk = 64 * 1024
	total := 4 + int(payloadLen)
	msg := make([]byte, 4, min(total, 4+readChunk))
	copy(msg, lenBuf)
	for len(msg) < total {
		n := min(readChunk, total-len(msg))
		msg = slices.Grow(msg, n)[:len(msg)+n]
		if _, err := io.ReadFull(c.raw, msg[len(msg)-n:]); err != nil {
			return nil, err
		}
	}
	return msg, nil
}

// WriteMessage writes a complete framed message to the connection.
// The msg must already include the 4-byte length prefix (as returned by Build()).
// A write that does not complete within writeTimeout fails; the connection is
// then in an undefined framing state and should be closed.
// WriteMessage is safe for concurrent use.
func (c *Conn) WriteMessage(msg []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.raw.SetWriteDeadline(time.Now().Add(c.writeTimeout)); err != nil {
		return err
	}
	_, err := c.raw.Write(msg)
	// Clear the deadline so raw writes by the owner (file transfers) are not
	// cut off by a stale one.
	_ = c.raw.SetWriteDeadline(time.Time{})
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
