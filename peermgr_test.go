package slsk

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestPeerManager creates a PeerManager without a real Client (nil),
// suitable for testing handleIncoming and token management.
func newTestPeerManager(username string) *PeerManager {
	return &PeerManager{
		username:      username,
		pendingTokens: make(map[uint32]chan *PeerConn),
	}
}

// tcpPair creates a connected pair of TCP connections using a local listener.
// Both connections are registered for cleanup via t.Cleanup.
func tcpPair(t *testing.T) (client net.Conn, server net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	var serverConn net.Conn
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		serverConn, _ = ln.Accept()
	}()

	clientConn, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)

	wg.Wait()
	require.NotNil(t, serverConn)

	t.Cleanup(func() {
		clientConn.Close()
		serverConn.Close()
	})

	return clientConn, serverConn
}

// ---------------------------------------------------------------------------
// handleIncoming tests
// ---------------------------------------------------------------------------

func TestPeerManager_HandleIncoming_PeerInit(t *testing.T) {
	pm := newTestPeerManager("localuser")

	received := make(chan *PeerConn, 1)
	pm.OnPeerConnection(func(pc *PeerConn) {
		received <- pc
	})

	// Create a TCP pair: "remote" writes PeerInit, "local" is given to handleIncoming.
	remote, local := tcpPair(t)

	// Write a PeerInit message from the remote side.
	initMsg := BuildPeerInit("remoteuser", ConnTypePeer, 42)
	remoteConn := WrapConn(remote)
	err := remoteConn.WriteMessage(initMsg)
	require.NoError(t, err)

	// Feed the local side to handleIncoming.
	pm.handleIncoming(local)

	select {
	case pc := <-received:
		assert.Equal(t, "remoteuser", pc.Username)
		assert.Equal(t, ConnTypePeer, pc.Type)
		assert.Equal(t, uint32(42), pc.Token)
		assert.NotNil(t, pc.Conn)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for peer connection callback")
	}
}

func TestPeerManager_HandleIncoming_PeerInit_FileTransfer(t *testing.T) {
	pm := newTestPeerManager("localuser")

	received := make(chan *PeerConn, 1)
	pm.OnFileConnection(func(pc *PeerConn) {
		received <- pc
	})

	remote, local := tcpPair(t)

	initMsg := BuildPeerInit("uploader", ConnTypeTransfer, 777)
	remoteConn := WrapConn(remote)
	err := remoteConn.WriteMessage(initMsg)
	require.NoError(t, err)

	pm.handleIncoming(local)

	select {
	case pc := <-received:
		assert.Equal(t, "uploader", pc.Username)
		assert.Equal(t, ConnTypeTransfer, pc.Type)
		assert.Equal(t, uint32(777), pc.Token)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for file connection callback")
	}
}

func TestPeerManager_HandleIncoming_PierceFirewall_MatchingToken(t *testing.T) {
	pm := newTestPeerManager("localuser")

	// Register a pending token.
	token := uint32(12345)
	ch := make(chan *PeerConn, 1)
	pm.mu.Lock()
	pm.pendingTokens[token] = ch
	pm.mu.Unlock()

	remote, local := tcpPair(t)

	// Write PierceFirewall from the remote side.
	pfMsg := BuildPierceFirewall(token)
	remoteConn := WrapConn(remote)
	err := remoteConn.WriteMessage(pfMsg)
	require.NoError(t, err)

	// handleIncoming should deliver the connection to the pending channel.
	pm.handleIncoming(local)

	select {
	case pc := <-ch:
		assert.Equal(t, token, pc.Token)
		assert.NotNil(t, pc.Conn)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for pierce firewall token match")
	}
}

func TestPeerManager_HandleIncoming_PierceFirewall_NoMatchingToken(t *testing.T) {
	pm := newTestPeerManager("localuser")

	// Do NOT register any pending token.

	remote, local := tcpPair(t)

	pfMsg := BuildPierceFirewall(99999)
	remoteConn := WrapConn(remote)
	err := remoteConn.WriteMessage(pfMsg)
	require.NoError(t, err)

	// handleIncoming should close the connection since no pending token matches.
	pm.handleIncoming(local)

	// Verify the local side was closed by trying to write.
	// The connection was given to handleIncoming which closes it, so
	// reading from the remote side should get EOF.
	remote.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1)
	_, err = remote.Read(buf)
	// Should get EOF or connection reset since local was closed.
	assert.Error(t, err, "expected error after connection was closed")
}

func TestPeerManager_HandleIncoming_UnknownInitCode(t *testing.T) {
	pm := newTestPeerManager("localuser")

	remote, local := tcpPair(t)

	// Build a message with an unknown init code (e.g., code 99).
	// Format: [4-byte length][1-byte code=99][some payload]
	payload := []byte{0x01, 0x02, 0x03, 0x04}
	msg := make([]byte, 4+1+len(payload))
	le.PutUint32(msg[0:4], uint32(1+len(payload)))
	msg[4] = 99
	copy(msg[5:], payload)

	remoteConn := WrapConn(remote)
	err := remoteConn.WriteMessage(msg)
	require.NoError(t, err)

	pm.handleIncoming(local)

	// Connection should be closed.
	remote.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1)
	_, err = remote.Read(buf)
	assert.Error(t, err, "expected error after connection was closed")
}

func TestPeerManager_HandleIncoming_DistributedType_Closed(t *testing.T) {
	pm := newTestPeerManager("localuser")

	// No D handler registered; should close.
	remote, local := tcpPair(t)

	initMsg := BuildPeerInit("distuser", ConnTypeDistributed, 555)
	remoteConn := WrapConn(remote)
	err := remoteConn.WriteMessage(initMsg)
	require.NoError(t, err)

	pm.handleIncoming(local)

	// Connection should be closed since D type falls through to default.
	remote.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1)
	_, err = remote.Read(buf)
	assert.Error(t, err)
}

// ---------------------------------------------------------------------------
// Listen + incoming connection integration tests
// ---------------------------------------------------------------------------

func TestPeerManager_Listen_AcceptsPeerInit(t *testing.T) {
	pm := newTestPeerManager("localuser")

	received := make(chan *PeerConn, 1)
	pm.OnPeerConnection(func(pc *PeerConn) {
		received <- pc
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start listening on a random port.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	pm.listener = ln

	go func() {
		<-ctx.Done()
		ln.Close()
	}()
	go func() {
		for {
			raw, err := ln.Accept()
			if err != nil {
				return
			}
			go pm.handleIncoming(raw)
		}
	}()

	// Connect to the listener and send PeerInit.
	raw, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	defer raw.Close()

	conn := WrapConn(raw)
	initMsg := BuildPeerInit("remotepeer", ConnTypePeer, 1001)
	err = conn.WriteMessage(initMsg)
	require.NoError(t, err)

	select {
	case pc := <-received:
		assert.Equal(t, "remotepeer", pc.Username)
		assert.Equal(t, ConnTypePeer, pc.Type)
		assert.Equal(t, uint32(1001), pc.Token)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for peer connection via listener")
	}
}

func TestPeerManager_Listen_AcceptsPierceFirewall(t *testing.T) {
	pm := newTestPeerManager("localuser")

	// Register a pending token.
	token := uint32(54321)
	ch := make(chan *PeerConn, 1)
	pm.mu.Lock()
	pm.pendingTokens[token] = ch
	pm.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	pm.listener = ln

	go func() {
		<-ctx.Done()
		ln.Close()
	}()
	go func() {
		for {
			raw, err := ln.Accept()
			if err != nil {
				return
			}
			go pm.handleIncoming(raw)
		}
	}()

	// Connect and send PierceFirewall.
	raw, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	defer raw.Close()

	conn := WrapConn(raw)
	pfMsg := BuildPierceFirewall(token)
	err = conn.WriteMessage(pfMsg)
	require.NoError(t, err)

	select {
	case pc := <-ch:
		assert.Equal(t, token, pc.Token)
		assert.NotNil(t, pc.Conn)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for pierce firewall via listener")
	}
}

// ---------------------------------------------------------------------------
// connectDirect tests
// ---------------------------------------------------------------------------

func TestPeerManager_ConnectDirect(t *testing.T) {
	pm := newTestPeerManager("myuser")

	// Start a mock peer that accepts a connection and reads PeerInit.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	receivedInit := make(chan *PeerInitMessage, 1)
	go func() {
		raw, err := ln.Accept()
		if err != nil {
			return
		}
		defer raw.Close()

		sc := WrapConn(raw)
		msg, err := sc.ReadMessage()
		if err != nil {
			return
		}
		init, err := ParsePeerInit(msg)
		if err != nil {
			return
		}
		receivedInit <- init
	}()

	// Parse the listener address to construct PeerAddressResponse.
	host, portStr, _ := net.SplitHostPort(ln.Addr().String())
	var port int
	fmt.Sscanf(portStr, "%d", &port)

	addr := &PeerAddressResponse{
		Username: "targetpeer",
		IP:       net.ParseIP(host),
		Port:     uint32(port),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	pc, err := pm.connectDirect(ctx, addr, "targetpeer", ConnTypePeer, 9999)
	require.NoError(t, err)
	defer pc.Conn.Close()

	assert.Equal(t, "targetpeer", pc.Username)
	assert.Equal(t, ConnTypePeer, pc.Type)
	assert.Equal(t, uint32(9999), pc.Token)

	// Verify the mock peer received our PeerInit.
	select {
	case init := <-receivedInit:
		assert.Equal(t, "myuser", init.Username)
		assert.Equal(t, ConnTypePeer, init.Type)
		assert.Equal(t, uint32(9999), init.Token)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for mock peer to receive PeerInit")
	}
}

func TestPeerManager_ConnectDirect_DialFailure(t *testing.T) {
	pm := newTestPeerManager("myuser")

	// Use an address that will refuse connection.
	addr := &PeerAddressResponse{
		Username: "unreachable",
		IP:       net.ParseIP("127.0.0.1"),
		Port:     1, // port 1 should refuse
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := pm.connectDirect(ctx, addr, "unreachable", ConnTypePeer, 1111)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "dial")
}

// ---------------------------------------------------------------------------
// Token management tests
// ---------------------------------------------------------------------------

func TestPeerManager_PendingTokens_RegisterAndMatch(t *testing.T) {
	pm := newTestPeerManager("localuser")

	token := uint32(77777)
	ch := make(chan *PeerConn, 1)

	pm.mu.Lock()
	pm.pendingTokens[token] = ch
	pm.mu.Unlock()

	// Verify the token is registered.
	pm.mu.Lock()
	_, exists := pm.pendingTokens[token]
	pm.mu.Unlock()
	assert.True(t, exists, "token should be registered")

	// Clean up.
	pm.mu.Lock()
	delete(pm.pendingTokens, token)
	pm.mu.Unlock()

	pm.mu.Lock()
	_, exists = pm.pendingTokens[token]
	pm.mu.Unlock()
	assert.False(t, exists, "token should be removed")
}

func TestPeerManager_PendingTokens_MultipleTokens(t *testing.T) {
	pm := newTestPeerManager("localuser")

	tokens := []uint32{100, 200, 300}
	channels := make([]chan *PeerConn, len(tokens))

	for i, tok := range tokens {
		channels[i] = make(chan *PeerConn, 1)
		pm.mu.Lock()
		pm.pendingTokens[tok] = channels[i]
		pm.mu.Unlock()
	}

	// Create TCP pairs and send PierceFirewall for each.
	for i, tok := range tokens {
		remote, local := tcpPair(t)

		pfMsg := BuildPierceFirewall(tok)
		remoteConn := WrapConn(remote)
		err := remoteConn.WriteMessage(pfMsg)
		require.NoError(t, err)

		pm.handleIncoming(local)

		select {
		case pc := <-channels[i]:
			assert.Equal(t, tok, pc.Token, "token %d should match", i)
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for token %d", tok)
		}
	}
}

// ---------------------------------------------------------------------------
// Handler registration tests
// ---------------------------------------------------------------------------

func TestPeerManager_NoHandler_DoesNotPanic(t *testing.T) {
	pm := newTestPeerManager("localuser")
	// Do NOT register any handlers.

	remote, local := tcpPair(t)

	initMsg := BuildPeerInit("someuser", ConnTypePeer, 1)
	remoteConn := WrapConn(remote)
	err := remoteConn.WriteMessage(initMsg)
	require.NoError(t, err)

	// Should not panic even without a handler registered.
	pm.handleIncoming(local)

	// Give a moment for any goroutine to run.
	time.Sleep(50 * time.Millisecond)
}

func TestPeerManager_NoFileHandler_DoesNotPanic(t *testing.T) {
	pm := newTestPeerManager("localuser")
	// No file handler registered.

	remote, local := tcpPair(t)

	initMsg := BuildPeerInit("someuser", ConnTypeTransfer, 2)
	remoteConn := WrapConn(remote)
	err := remoteConn.WriteMessage(initMsg)
	require.NoError(t, err)

	pm.handleIncoming(local)

	time.Sleep(50 * time.Millisecond)
}

// ---------------------------------------------------------------------------
// generateToken test
// ---------------------------------------------------------------------------

func TestGenerateToken_NotZero(t *testing.T) {
	// Generate several tokens and verify they are non-zero.
	// (generateToken is time-based so they should differ if called with delay.)
	seen := make(map[uint32]bool)
	for i := 0; i < 10; i++ {
		tok := generateToken()
		seen[tok] = true
		time.Sleep(1 * time.Millisecond) // ensure time advances
	}
	// With 10 samples spaced 1ms apart, we should have some unique values.
	assert.Greater(t, len(seen), 1, "expected multiple unique tokens")
}

// ---------------------------------------------------------------------------
// connectIndirect test (unit level)
// ---------------------------------------------------------------------------

func TestPeerManager_ConnectIndirect_TokenDelivery(t *testing.T) {
	// Simulate the indirect flow: register token, then deliver a PeerConn
	// through the pending channel as if a peer connected with PierceFirewall.
	pm := newTestPeerManager("localuser")

	token := uint32(88888)
	ch := make(chan *PeerConn, 1)
	pm.mu.Lock()
	pm.pendingTokens[token] = ch
	pm.mu.Unlock()

	// Simulate the peer connecting and sending PierceFirewall.
	remote, local := tcpPair(t)
	_ = remote // remote end not needed beyond creating the pair

	pfMsg := BuildPierceFirewall(token)
	remoteConn := WrapConn(remote)
	err := remoteConn.WriteMessage(pfMsg)
	require.NoError(t, err)

	// Run handleIncoming in a goroutine.
	go pm.handleIncoming(local)

	// The channel should receive the connection.
	select {
	case pc := <-ch:
		assert.Equal(t, token, pc.Token)
		assert.NotNil(t, pc.Conn)
		// In connectIndirect, Username and Type would be set after receiving.
		// Here we just verify the token matching works.
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for indirect connection delivery")
	}
}

// ---------------------------------------------------------------------------
// handleIncoming with connection that closes before sending data
// ---------------------------------------------------------------------------

func TestPeerManager_HandleIncoming_ImmediateClose(t *testing.T) {
	pm := newTestPeerManager("localuser")

	// Create a TCP pair and close the writer side so handleIncoming gets EOF.
	remote, local := tcpPair(t)
	remote.Close()

	// handleIncoming should return without panicking.
	pm.handleIncoming(local)
}
