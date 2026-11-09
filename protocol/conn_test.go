package protocol

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// helper: start a TCP server on a random port, return the listener
func startTestServer(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })
	return ln
}

// helper: dial the test server and return a wrapped Conn
func dialTestServer(t *testing.T, ln net.Listener) *Conn {
	t.Helper()
	raw, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { raw.Close() })
	return WrapConn(raw)
}

func TestConn_SendAndReceive(t *testing.T) {
	ln := startTestServer(t)

	// Server goroutine: accept one connection, read a message, echo it back.
	serverDone := make(chan error, 1)
	go func() {
		raw, err := ln.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer raw.Close()

		sc := WrapConn(raw)
		msg, err := sc.ReadMessage()
		if err != nil {
			serverDone <- err
			return
		}

		// Echo the message back
		if err := sc.WriteMessage(msg); err != nil {
			serverDone <- err
			return
		}
		serverDone <- nil
	}()

	// Client: send a login request, read the echo
	client := dialTestServer(t, ln)
	loginMsg := BuildLoginRequest("testuser", "testpass", 181, 1)

	if err := client.WriteMessage(loginMsg); err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}

	echo, err := client.ReadMessage()
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}

	// Verify the echoed message can be parsed and has the right code
	r, err := NewReader(echo)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if r.Code() != ServerLogin {
		t.Fatalf("code = %d, want %d (ServerLogin)", r.Code(), ServerLogin)
	}

	if err := <-serverDone; err != nil {
		t.Fatalf("server error: %v", err)
	}
}

func TestConn_MultipleMessages(t *testing.T) {
	ln := startTestServer(t)

	// Build 3 distinct messages
	messages := [][]byte{
		BuildLoginRequest("alice", "pass1", 181, 1),
		BuildPing(),
		BuildSetListenPort(2234),
	}
	expectedCodes := []uint32{ServerLogin, ServerPing, ServerSetListenPort}

	// Server: read 3 messages, echo each one back
	serverDone := make(chan error, 1)
	go func() {
		raw, err := ln.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer raw.Close()

		sc := WrapConn(raw)
		for i := 0; i < 3; i++ {
			msg, err := sc.ReadMessage()
			if err != nil {
				serverDone <- err
				return
			}
			if err := sc.WriteMessage(msg); err != nil {
				serverDone <- err
				return
			}
		}
		serverDone <- nil
	}()

	client := dialTestServer(t, ln)

	// Send all 3
	for i, msg := range messages {
		if err := client.WriteMessage(msg); err != nil {
			t.Fatalf("WriteMessage[%d]: %v", i, err)
		}
	}

	// Read all 3 echoes and verify codes in order
	for i, wantCode := range expectedCodes {
		echo, err := client.ReadMessage()
		if err != nil {
			t.Fatalf("ReadMessage[%d]: %v", i, err)
		}
		r, err := NewReader(echo)
		if err != nil {
			t.Fatalf("NewReader[%d]: %v", i, err)
		}
		if r.Code() != wantCode {
			t.Fatalf("message[%d] code = %d, want %d", i, r.Code(), wantCode)
		}
	}

	if err := <-serverDone; err != nil {
		t.Fatalf("server error: %v", err)
	}
}

func TestConn_ConcurrentWrites(t *testing.T) {
	ln := startTestServer(t)
	const numWriters = 10

	// Server: read exactly 10 messages
	serverGot := make(chan []byte, numWriters)
	serverDone := make(chan error, 1)
	go func() {
		raw, err := ln.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer raw.Close()

		sc := WrapConn(raw)
		for i := 0; i < numWriters; i++ {
			msg, err := sc.ReadMessage()
			if err != nil {
				serverDone <- err
				return
			}
			serverGot <- msg
		}
		serverDone <- nil
	}()

	client := dialTestServer(t, ln)

	// Launch 10 goroutines, each writing a ping message concurrently
	var wg sync.WaitGroup
	wg.Add(numWriters)
	for i := 0; i < numWriters; i++ {
		go func() {
			defer wg.Done()
			msg := BuildPing()
			if err := client.WriteMessage(msg); err != nil {
				t.Errorf("WriteMessage: %v", err)
			}
		}()
	}
	wg.Wait()

	if err := <-serverDone; err != nil {
		t.Fatalf("server error: %v", err)
	}

	// Verify all 10 messages were received and are valid
	close(serverGot)
	count := 0
	for msg := range serverGot {
		r, err := NewReader(msg)
		if err != nil {
			t.Fatalf("NewReader: %v", err)
		}
		if r.Code() != ServerPing {
			t.Fatalf("code = %d, want %d", r.Code(), ServerPing)
		}
		count++
	}
	if count != numWriters {
		t.Fatalf("received %d messages, want %d", count, numWriters)
	}
}

func TestConn_ReadFromClosedConn(t *testing.T) {
	ln := startTestServer(t)

	// Server: accept and immediately close
	go func() {
		raw, err := ln.Accept()
		if err != nil {
			return
		}
		raw.Close()
	}()

	client := dialTestServer(t, ln)

	// Reading from a closed remote should return an error (EOF or similar)
	_, err := client.ReadMessage()
	if err == nil {
		t.Fatal("expected error reading from closed connection, got nil")
	}
	// The error should ultimately be io.EOF or io.ErrUnexpectedEOF
	if err != io.EOF && err != io.ErrUnexpectedEOF {
		// Accept any error — the connection is closed, so any read error is valid.
		// But log it for visibility.
		t.Logf("got error (acceptable): %v", err)
	}
}

func TestConn_OversizedMessage(t *testing.T) {
	ln := startTestServer(t)

	// Server: send a message with a length prefix claiming 200MB
	serverDone := make(chan error, 1)
	go func() {
		raw, err := ln.Accept()
		if err != nil {
			serverDone <- err
			return
		}
		defer raw.Close()

		// Write a 4-byte length prefix claiming 200MB (200 * 1024 * 1024)
		lenBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(lenBuf, 200*1024*1024)
		_, err = raw.Write(lenBuf)
		serverDone <- err
	}()

	client := dialTestServer(t, ln)

	_, err := client.ReadMessage()
	if err == nil {
		t.Fatal("expected error for oversized message, got nil")
	}

	// The error should mention the size limit
	t.Logf("got expected error: %v", err)

	if err := <-serverDone; err != nil {
		t.Fatalf("server error: %v", err)
	}
}

func TestConn_RemoteAddr(t *testing.T) {
	ln := startTestServer(t)

	go func() {
		raw, _ := ln.Accept()
		if raw != nil {
			raw.Close()
		}
	}()

	client := dialTestServer(t, ln)
	addr := client.RemoteAddr()
	if addr == nil {
		t.Fatal("RemoteAddr returned nil")
	}
	if addr.String() != ln.Addr().String() {
		t.Fatalf("RemoteAddr = %s, want %s", addr.String(), ln.Addr().String())
	}
}

func TestWrapPeerConn_RejectsMessagesOverPeerLimit(t *testing.T) {
	lenBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenBuf, maxPeerMessageSize+1)

	c := WrapPeerConn(&streamConn{r: bytes.NewReader(lenBuf)})
	if _, err := c.ReadMessage(); err == nil || !strings.Contains(err.Error(), "too large") {
		t.Fatalf("peer conn: err = %v, want message too large", err)
	}

	// The same length is acceptable on the server connection (it fails later
	// on EOF, not on the size check).
	s := WrapConn(&streamConn{r: bytes.NewReader(lenBuf)})
	if _, err := s.ReadMessage(); err == nil || strings.Contains(err.Error(), "too large") {
		t.Fatalf("server conn: err = %v, want read error", err)
	}
}

// A peer that never drains its socket must not block WriteMessage forever.
func TestConn_WriteMessageTimesOut(t *testing.T) {
	a, b := net.Pipe() // unbuffered: Write blocks until b reads
	defer a.Close()
	defer b.Close()
	c := WrapPeerConn(a)
	c.writeTimeout = 50 * time.Millisecond

	done := make(chan error, 1)
	go func() { done <- c.WriteMessage(BuildQueueDownload("x")) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected a timeout error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("WriteMessage blocked past its write deadline")
	}
}

// The write deadline must not outlive the write: a later raw write by the
// owner of the connection (file transfers) is unaffected.
func TestConn_WriteMessageClearsDeadline(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	c := WrapPeerConn(a)
	c.writeTimeout = 50 * time.Millisecond

	go io.Copy(io.Discard, b)
	if err := c.WriteMessage(BuildQueueDownload("x")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if _, err := c.Raw().Write([]byte("raw")); err != nil {
		t.Fatalf("raw write after WriteMessage: %v", err)
	}
}
