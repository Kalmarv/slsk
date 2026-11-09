package slsk

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/kalmarv/slsk/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestPeerManager creates a PeerManager without a real Client (nil),
// suitable for testing handleIncoming and token management.
func newTestPeerManager(username string) *peerManager {
	return &peerManager{
		log:           testLogger,
		username:      username,
		client:        &serverConn{log: testLogger}, // never logged in: server writes fail
		pendingTokens: make(map[uint32]chan connectResult),
		pendingAddrs:  make(map[string][]chan addrResult),
		msgHandlers:   make(map[uint32]peerMessageHandler),
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

	// Register a message handler for a test message code (code 9 = PeerSearchResponse).
	received := make(chan *peerMessage, 1)
	pm.OnPeerMessage(protocol.PeerSearchResponse, func(msg *peerMessage) {
		received <- msg
	})

	// Create a TCP pair: "remote" writes PeerInit + a peer message.
	remote, local := tcpPair(t)
	remoteConn := protocol.WrapConn(remote)

	// Send PeerInit handshake.
	initMsg := protocol.BuildPeerInit("remoteuser", protocol.ConnTypePeer, 42)
	err := remoteConn.WriteMessage(initMsg)
	require.NoError(t, err)

	// Send a peer message after handshake (dummy search response-shaped message).
	testMsg := protocol.NewBuilder(protocol.PeerSearchResponse)
	testMsg.WriteUint32(0) // dummy payload
	err = remoteConn.WriteMessage(testMsg.Build())
	require.NoError(t, err)

	// Feed the local side to handleIncoming (starts routePeerMessages).
	go pm.handleIncoming(local)

	select {
	case msg := <-received:
		assert.Equal(t, "remoteuser", msg.PeerConn.Username)
		assert.Equal(t, protocol.PeerSearchResponse, msg.Code)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for peer message callback")
	}
}

func TestPeerManager_HandleIncoming_PeerInit_FileTransfer(t *testing.T) {
	pm := newTestPeerManager("localuser")

	received := make(chan *peerConn, 1)
	pm.OnFileConnection(func(pc *peerConn) {
		received <- pc
	})

	remote, local := tcpPair(t)

	initMsg := protocol.BuildPeerInit("uploader", protocol.ConnTypeTransfer, 777)
	remoteConn := protocol.WrapConn(remote)
	err := remoteConn.WriteMessage(initMsg)
	require.NoError(t, err)

	pm.handleIncoming(local)

	select {
	case pc := <-received:
		assert.Equal(t, "uploader", pc.Username)
		assert.Equal(t, protocol.ConnTypeTransfer, pc.Type)
		assert.Equal(t, uint32(777), pc.Token)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for file connection callback")
	}
}

func TestPeerManager_HandleIncoming_PierceFirewall_MatchingToken(t *testing.T) {
	pm := newTestPeerManager("localuser")

	// Register a pending token.
	token := uint32(12345)
	ch := make(chan connectResult, 1)
	pm.mu.Lock()
	pm.pendingTokens[token] = ch
	pm.mu.Unlock()

	remote, local := tcpPair(t)

	// Write PierceFirewall from the remote side.
	pfMsg := protocol.BuildPierceFirewall(token)
	remoteConn := protocol.WrapConn(remote)
	err := remoteConn.WriteMessage(pfMsg)
	require.NoError(t, err)

	// handleIncoming should deliver the connection to the pending channel.
	pm.handleIncoming(local)

	select {
	case result := <-ch:
		require.NoError(t, result.err)
		assert.Equal(t, token, result.pc.Token)
		assert.NotNil(t, result.pc.Conn)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for pierce firewall token match")
	}
}

func TestPeerManager_HandleIncoming_PierceFirewall_NoMatchingToken(t *testing.T) {
	pm := newTestPeerManager("localuser")

	// Do NOT register any pending token.

	remote, local := tcpPair(t)

	pfMsg := protocol.BuildPierceFirewall(99999)
	remoteConn := protocol.WrapConn(remote)
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
	binary.LittleEndian.PutUint32(msg[0:4], uint32(1+len(payload)))
	msg[4] = 99
	copy(msg[5:], payload)

	remoteConn := protocol.WrapConn(remote)
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

	initMsg := protocol.BuildPeerInit("distuser", protocol.ConnTypeDistributed, 555)
	remoteConn := protocol.WrapConn(remote)
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

	// Register a message handler.
	received := make(chan *peerMessage, 1)
	pm.OnPeerMessage(protocol.PeerSearchResponse, func(msg *peerMessage) {
		received <- msg
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

	// Connect to the listener and send PeerInit + a peer message.
	raw, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	defer raw.Close()

	conn := protocol.WrapConn(raw)
	initMsg := protocol.BuildPeerInit("remotepeer", protocol.ConnTypePeer, 1001)
	err = conn.WriteMessage(initMsg)
	require.NoError(t, err)

	testMsg := protocol.NewBuilder(protocol.PeerSearchResponse)
	testMsg.WriteUint32(0)
	err = conn.WriteMessage(testMsg.Build())
	require.NoError(t, err)

	select {
	case msg := <-received:
		assert.Equal(t, "remotepeer", msg.PeerConn.Username)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for peer message via listener")
	}
}

func TestPeerManager_Listen_AcceptsPierceFirewall(t *testing.T) {
	pm := newTestPeerManager("localuser")

	// Register a pending token.
	token := uint32(54321)
	ch := make(chan connectResult, 1)
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

	conn := protocol.WrapConn(raw)
	pfMsg := protocol.BuildPierceFirewall(token)
	err = conn.WriteMessage(pfMsg)
	require.NoError(t, err)

	select {
	case result := <-ch:
		require.NoError(t, result.err)
		assert.Equal(t, token, result.pc.Token)
		assert.NotNil(t, result.pc.Conn)
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

	receivedInit := make(chan *protocol.PeerInitMessage, 1)
	go func() {
		raw, err := ln.Accept()
		if err != nil {
			return
		}
		defer raw.Close()

		sc := protocol.WrapConn(raw)
		msg, err := sc.ReadMessage()
		if err != nil {
			return
		}
		init, err := protocol.ParsePeerInit(msg)
		if err != nil {
			return
		}
		receivedInit <- init
	}()

	// Parse the listener address to construct PeerAddressResponse.
	host, portStr, _ := net.SplitHostPort(ln.Addr().String())
	var port int
	fmt.Sscanf(portStr, "%d", &port)

	addr := &protocol.PeerAddressResponse{
		Username: "targetpeer",
		IP:       net.ParseIP(host),
		Port:     uint32(port),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	pc, err := pm.connectDirect(ctx, addr, "targetpeer", protocol.ConnTypePeer, 9999)
	require.NoError(t, err)
	defer pc.Conn.Close()

	assert.Equal(t, "targetpeer", pc.Username)
	assert.Equal(t, protocol.ConnTypePeer, pc.Type)
	assert.Equal(t, uint32(9999), pc.Token)

	// Verify the mock peer received our PeerInit.
	select {
	case init := <-receivedInit:
		assert.Equal(t, "myuser", init.Username)
		assert.Equal(t, protocol.ConnTypePeer, init.Type)
		assert.Equal(t, uint32(9999), init.Token)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for mock peer to receive PeerInit")
	}
}

func TestPeerManager_ConnectDirect_DialFailure(t *testing.T) {
	pm := newTestPeerManager("myuser")

	// Use an address that will refuse connection.
	addr := &protocol.PeerAddressResponse{
		Username: "unreachable",
		IP:       net.ParseIP("127.0.0.1"),
		Port:     1, // port 1 should refuse
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := pm.connectDirect(ctx, addr, "unreachable", protocol.ConnTypePeer, 1111)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "dial")
}

// ---------------------------------------------------------------------------
// Token management tests
// ---------------------------------------------------------------------------

func TestPeerManager_PendingTokens_RegisterAndMatch(t *testing.T) {
	pm := newTestPeerManager("localuser")

	token := uint32(77777)
	ch := make(chan connectResult, 1)

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
	channels := make([]chan connectResult, len(tokens))

	for i, tok := range tokens {
		channels[i] = make(chan connectResult, 1)
		pm.mu.Lock()
		pm.pendingTokens[tok] = channels[i]
		pm.mu.Unlock()
	}

	// Create TCP pairs and send PierceFirewall for each.
	for i, tok := range tokens {
		remote, local := tcpPair(t)

		pfMsg := protocol.BuildPierceFirewall(tok)
		remoteConn := protocol.WrapConn(remote)
		err := remoteConn.WriteMessage(pfMsg)
		require.NoError(t, err)

		pm.handleIncoming(local)

		select {
		case result := <-channels[i]:
			require.NoError(t, result.err)
			assert.Equal(t, tok, result.pc.Token, "token %d should match", i)
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

	initMsg := protocol.BuildPeerInit("someuser", protocol.ConnTypePeer, 1)
	remoteConn := protocol.WrapConn(remote)
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

	initMsg := protocol.BuildPeerInit("someuser", protocol.ConnTypeTransfer, 2)
	remoteConn := protocol.WrapConn(remote)
	err := remoteConn.WriteMessage(initMsg)
	require.NoError(t, err)

	pm.handleIncoming(local)

	time.Sleep(50 * time.Millisecond)
}

// ---------------------------------------------------------------------------
// generateToken test
// ---------------------------------------------------------------------------

func TestGenerateToken_Unique(t *testing.T) {
	// Generate tokens concurrently and verify no collisions.
	const n = 100
	tokens := make(chan uint32, n)
	for i := 0; i < n; i++ {
		go func() { tokens <- generateToken() }()
	}
	seen := make(map[uint32]bool)
	for i := 0; i < n; i++ {
		tok := <-tokens
		seen[tok] = true
	}
	// With random uint32, 100 tokens should all be unique.
	assert.Equal(t, n, len(seen), "expected all unique tokens")
}

// ---------------------------------------------------------------------------
// connectIndirect test (unit level)
// ---------------------------------------------------------------------------

func TestPeerManager_ConnectIndirect_TokenDelivery(t *testing.T) {
	// Simulate the indirect flow: register token, then deliver a PeerConn
	// through the pending channel as if a peer connected with PierceFirewall.
	pm := newTestPeerManager("localuser")

	token := uint32(88888)
	ch := make(chan connectResult, 1)
	pm.mu.Lock()
	pm.pendingTokens[token] = ch
	pm.mu.Unlock()

	// Simulate the peer connecting and sending PierceFirewall.
	remote, local := tcpPair(t)
	_ = remote // remote end not needed beyond creating the pair

	pfMsg := protocol.BuildPierceFirewall(token)
	remoteConn := protocol.WrapConn(remote)
	err := remoteConn.WriteMessage(pfMsg)
	require.NoError(t, err)

	// Run handleIncoming in a goroutine.
	go pm.handleIncoming(local)

	// The channel should receive the connection.
	select {
	case result := <-ch:
		require.NoError(t, result.err)
		assert.Equal(t, token, result.pc.Token)
		assert.NotNil(t, result.pc.Conn)
		// In connectIndirect, Username and Type would be set after receiving.
		// Here we just verify the token matching works.
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for indirect connection delivery")
	}
}

// ---------------------------------------------------------------------------
// handleConnectToPeer (reverse/indirect connection initiated by server)
// ---------------------------------------------------------------------------

func TestPeerManager_HandleConnectToPeer_PeerType(t *testing.T) {
	pm := newTestPeerManager("localuser")

	// Register a message handler.
	received := make(chan *peerMessage, 1)
	pm.OnPeerMessage(protocol.PeerSearchResponse, func(msg *peerMessage) {
		received <- msg
	})

	// Start a listener simulating the remote peer waiting for us.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	var peerConn net.Conn
	accepted := make(chan struct{})
	go func() {
		peerConn, _ = ln.Accept()
		close(accepted)
	}()

	addr := ln.Addr().(*net.TCPAddr)

	// Simulate server sending us ConnectToPeer.
	resp := &protocol.ConnectToPeerResponse{
		Username: "remoteuser",
		Type:     protocol.ConnTypePeer,
		IP:       net.ParseIP("127.0.0.1"),
		Port:     uint32(addr.Port),
		Token:    12345,
	}

	go pm.handleConnectToPeer(resp)

	// Verify the peer side received a PierceFirewall message.
	<-accepted
	require.NotNil(t, peerConn)
	defer peerConn.Close()

	peerWrapped := protocol.WrapConn(peerConn)
	msg, err := peerWrapped.ReadMessage()
	require.NoError(t, err)

	token, err := protocol.ParsePierceFirewall(msg)
	require.NoError(t, err)
	assert.Equal(t, uint32(12345), token)

	// Send a peer message from the remote side so routePeerMessages dispatches it.
	testMsg := protocol.NewBuilder(protocol.PeerSearchResponse)
	testMsg.WriteUint32(0)
	err = peerWrapped.WriteMessage(testMsg.Build())
	require.NoError(t, err)

	// Verify the message handler fired.
	select {
	case m := <-received:
		assert.Equal(t, "remoteuser", m.PeerConn.Username)
		assert.Equal(t, protocol.PeerSearchResponse, m.Code)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for peer message callback")
	}
}

func TestPeerManager_HandleConnectToPeer_FileType(t *testing.T) {
	pm := newTestPeerManager("localuser")

	received := make(chan *peerConn, 1)
	pm.OnFileConnection(func(pc *peerConn) {
		received <- pc
	})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	accepted := make(chan net.Conn, 1)
	go func() {
		c, _ := ln.Accept()
		accepted <- c
	}()

	addr := ln.Addr().(*net.TCPAddr)

	resp := &protocol.ConnectToPeerResponse{
		Username: "uploader",
		Type:     protocol.ConnTypeTransfer,
		IP:       net.ParseIP("127.0.0.1"),
		Port:     uint32(addr.Port),
		Token:    99999,
	}

	pm.handleConnectToPeer(resp)

	peerConn := <-accepted
	require.NotNil(t, peerConn)
	defer peerConn.Close()

	// Verify PierceFirewall was sent.
	peerWrapped := protocol.WrapConn(peerConn)
	msg, err := peerWrapped.ReadMessage()
	require.NoError(t, err)

	token, err := protocol.ParsePierceFirewall(msg)
	require.NoError(t, err)
	assert.Equal(t, uint32(99999), token)

	select {
	case pc := <-received:
		assert.Equal(t, "uploader", pc.Username)
		assert.Equal(t, protocol.ConnTypeTransfer, pc.Type)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for file connection callback")
	}
}

func TestPeerManager_HandleConnectToPeer_UnreachablePeer(t *testing.T) {
	pm := newTestPeerManager("localuser")

	// Use a port that's not listening — should fail gracefully.
	resp := &protocol.ConnectToPeerResponse{
		Username: "ghost",
		Type:     protocol.ConnTypePeer,
		IP:       net.ParseIP("127.0.0.1"),
		Port:     1, // unlikely to be open
		Token:    55555,
	}

	// Should not panic, just log.
	pm.handleConnectToPeer(resp)
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

// ---------------------------------------------------------------------------
// Address validation tests
// ---------------------------------------------------------------------------

func TestIsValidAddr(t *testing.T) {
	tests := []struct {
		name  string
		addr  *protocol.PeerAddressResponse
		valid bool
	}{
		{"normal address", &protocol.PeerAddressResponse{IP: net.ParseIP("192.168.1.1"), Port: 2234}, true},
		{"zero IP", &protocol.PeerAddressResponse{IP: net.ParseIP("0.0.0.0"), Port: 2234}, false},
		{"zero port", &protocol.PeerAddressResponse{IP: net.ParseIP("192.168.1.1"), Port: 0}, false},
		{"both zero", &protocol.PeerAddressResponse{IP: net.ParseIP("0.0.0.0"), Port: 0}, false},
		{"nil IP", &protocol.PeerAddressResponse{IP: nil, Port: 2234}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.valid, isValidAddr(tt.addr))
		})
	}
}

func TestPeerManager_HandleConnectToPeer_InvalidAddr(t *testing.T) {
	pm := newTestPeerManager("localuser")

	// Should return immediately without dialing.
	resp := &protocol.ConnectToPeerResponse{
		Username: "ghost",
		Type:     protocol.ConnTypePeer,
		IP:       net.ParseIP("0.0.0.0"),
		Port:     0,
		Token:    55555,
	}

	// Should not panic or block.
	pm.handleConnectToPeer(resp)
}

// ---------------------------------------------------------------------------
// CannotConnect fast-fail test
// ---------------------------------------------------------------------------

func TestPeerManager_CannotConnect_FailsFast(t *testing.T) {
	pm := newTestPeerManager("localuser")

	// Register a pending token as connectIndirect would.
	token := uint32(44444)
	ch := make(chan connectResult, 1)
	pm.mu.Lock()
	pm.pendingTokens[token] = ch
	pm.mu.Unlock()

	// Simulate CannotConnect arriving.
	pm.mu.Lock()
	resultCh, ok := pm.pendingTokens[token]
	pm.mu.Unlock()
	require.True(t, ok)

	resultCh <- connectResult{err: fmt.Errorf("server reported peer cannot connect (token %d)", token)}

	// Should receive the error immediately.
	select {
	case result := <-ch:
		assert.Error(t, result.err)
		assert.Contains(t, result.err.Error(), "cannot connect")
	case <-time.After(1 * time.Second):
		t.Fatal("CannotConnect should have unblocked immediately")
	}
}

// ---------------------------------------------------------------------------
// getPeerAddress failure propagation
// ---------------------------------------------------------------------------

func TestGetPeerAddressFailsFastForAllWaiters(t *testing.T) {
	// A client that is not logged in makes every GetPeerAddress request fail
	// immediately, which is exactly the "request write failed" path.
	c, err := newServerConn(serverConfig{ServerAddr: "127.0.0.1:1", Username: "u", Password: "p"})
	require.NoError(t, err)
	pm := newPeerManager("u", c)

	const n = 3
	errs := make(chan error, n)
	var started sync.WaitGroup
	started.Add(n)
	for range n {
		go func() {
			started.Done()
			_, err := pm.getPeerAddress(context.Background(), "peerx")
			errs <- err
		}()
	}
	started.Wait()

	for range n {
		select {
		case err := <-errs:
			assert.Error(t, err)
		case <-time.After(2 * time.Second):
			t.Fatal("waiter hung instead of failing fast")
		}
	}

	pm.mu.Lock()
	_, exists := pm.pendingAddrs["peerx"]
	pm.mu.Unlock()
	assert.False(t, exists, "no waiters should be left behind")
}

func TestCountedConnReleasesOnce(t *testing.T) {
	pm := newTestPeerManager("u")
	pm.conns = 1

	c1, c2 := net.Pipe()
	defer c1.Close()
	cc := &countedConn{Conn: c2, release: pm.releaseConn}

	require.NoError(t, cc.Close())
	require.NoError(t, cc.Close()) // double close must not double-release
	assert.Equal(t, 0, pm.conns)

	pm.releaseConn() // must not go negative
	assert.Equal(t, 0, pm.conns)
}

// ---------------------------------------------------------------------------
// pendingAddrs cleanup test
// ---------------------------------------------------------------------------

func TestPeerManager_RemoveAddrWaiter(t *testing.T) {
	pm := newTestPeerManager("localuser")

	ch1 := make(chan addrResult, 1)
	ch2 := make(chan addrResult, 1)

	pm.mu.Lock()
	pm.pendingAddrs["user1"] = append(pm.pendingAddrs["user1"], ch1)
	pm.pendingAddrs["user1"] = append(pm.pendingAddrs["user1"], ch2)
	pm.mu.Unlock()

	// Remove first waiter.
	pm.removeAddrWaiter("user1", ch1)

	pm.mu.Lock()
	remaining := len(pm.pendingAddrs["user1"])
	pm.mu.Unlock()
	assert.Equal(t, 1, remaining, "should have 1 waiter remaining")

	// Remove second waiter — should delete the key entirely.
	pm.removeAddrWaiter("user1", ch2)

	pm.mu.Lock()
	_, exists := pm.pendingAddrs["user1"]
	pm.mu.Unlock()
	assert.False(t, exists, "key should be deleted when no waiters remain")
}

// ---------------------------------------------------------------------------
// CantConnectToPeer
// ---------------------------------------------------------------------------

// readCantConnect returns the (token, username) of the next code-1001 message
// the mock server receives.
func readCantConnect(t *testing.T, sc *protocol.Conn) (uint32, string) {
	t.Helper()
	sc.Raw().SetReadDeadline(time.Now().Add(3 * time.Second))
	for {
		msg, err := sc.ReadMessage()
		require.NoError(t, err, "no CantConnectToPeer reached the server")
		r, err := protocol.NewReader(msg)
		require.NoError(t, err)
		if r.Code() != protocol.ServerCannotConnect {
			continue
		}
		token, err := r.ReadUint32()
		require.NoError(t, err)
		user, err := r.ReadString()
		require.NoError(t, err)
		return token, user
	}
}

func TestPeerManager_HandleConnectToPeer_ReportsCantConnect(t *testing.T) {
	// A closed local port: the dial is refused immediately.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	closedPort := uint32(ln.Addr().(*net.TCPAddr).Port)
	ln.Close()

	cases := []struct {
		name string
		ip   string
		port uint32
	}{
		{"dial failure", "127.0.0.1", closedPort},
		{"invalid address", "0.0.0.0", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client, serverSide := connectedClient(t)
			sc := <-serverSide
			defer sc.Close()
			pm := newTestPeerManager("me")
			pm.client = client

			go pm.handleConnectToPeer(&protocol.ConnectToPeerResponse{
				Username: "ghost", Type: protocol.ConnTypePeer, IP: net.ParseIP(tc.ip), Port: tc.port, Token: 4242,
			})

			token, user := readCantConnect(t, sc)
			assert.Equal(t, uint32(4242), token)
			assert.Equal(t, "ghost", user)
		})
	}
}

func TestBuildCantConnectToPeer(t *testing.T) {
	r, err := protocol.NewReader(protocol.BuildCantConnectToPeer(7, "bob"))
	require.NoError(t, err)
	assert.Equal(t, protocol.ServerCannotConnect, r.Code())
	token, _ := r.ReadUint32()
	user, _ := r.ReadString()
	assert.Equal(t, uint32(7), token)
	assert.Equal(t, "bob", user)
	assert.False(t, r.HasMore())
}

// ---------------------------------------------------------------------------
// Racing direct and indirect connects
// ---------------------------------------------------------------------------

// racingPM returns a PeerManager on a logged-in client plus the mock server's
// connection, so SendConnectToPeer really reaches a (fake) server.
func racingPM(t *testing.T) (*peerManager, *protocol.Conn) {
	t.Helper()
	client, serverSide := connectedClient(t)
	sc := <-serverSide
	t.Cleanup(func() { sc.Close() })
	pm := newPeerManager("me", client)
	return pm, sc
}

var testAddr = &protocol.PeerAddressResponse{IP: net.ParseIP("127.0.0.1"), Port: 2234}

// deliverIndirect plays the part of handleIncoming matching a PierceFirewall:
// once the token is pending it hands over a connection and returns its far end.
func deliverIndirect(t *testing.T, pm *peerManager, token uint32) net.Conn {
	t.Helper()
	far, near := tcpPair(t)
	require.Eventually(t, func() bool {
		pm.mu.Lock()
		defer pm.mu.Unlock()
		ch, ok := pm.pendingTokens[token]
		if ok {
			ch <- connectResult{pc: &peerConn{Conn: protocol.WrapPeerConn(near), Token: token}}
		}
		return ok
	}, 2*time.Second, 5*time.Millisecond)
	return far
}

func TestConnectRacing_IndirectWinsWhileDirectBlocks(t *testing.T) {
	pm, _ := racingPM(t)
	dialCancelled := make(chan struct{})
	pm.dial = func(ctx context.Context, _ string) (net.Conn, error) {
		<-ctx.Done() // a black-holed address
		close(dialCancelled)
		return nil, ctx.Err()
	}

	go deliverIndirect(t, pm, 77)
	start := time.Now()
	pc, err := pm.connectRacing(context.Background(), testAddr, "peer", protocol.ConnTypePeer, 77)
	require.NoError(t, err)
	assert.Less(t, time.Since(start), directConnectTimeout/2, "indirect must not wait for the direct leg")
	assert.Equal(t, "peer", pc.Username)
	assert.Equal(t, protocol.ConnTypePeer, pc.Type)
	pc.Conn.Close()

	select {
	case <-dialCancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("losing direct dial was not cancelled")
	}
	pm.mu.Lock()
	assert.Empty(t, pm.pendingTokens)
	pm.mu.Unlock()
}

func TestConnectRacing_DirectWinsAndLateIndirectIsClosed(t *testing.T) {
	pm, sc := racingPM(t)
	far, near := tcpPair(t)
	pm.dial = func(context.Context, string) (net.Conn, error) { return near, nil }

	pc, err := pm.connectRacing(context.Background(), testAddr, "peer", protocol.ConnTypePeer, 78)
	require.NoError(t, err)
	assert.Same(t, near, pc.Conn.Raw())

	// The direct leg sent PeerInit and the server was asked to relay too.
	msg, err := protocol.WrapPeerConn(far).ReadMessage()
	require.NoError(t, err)
	init, err := protocol.ParsePeerInit(msg)
	require.NoError(t, err)
	assert.Equal(t, "me", init.Username)
	sc.Raw().SetReadDeadline(time.Now().Add(2 * time.Second))
	for {
		m, err := sc.ReadMessage()
		require.NoError(t, err, "indirect ConnectToPeer request never reached the server")
		if r, _ := protocol.NewReader(m); r != nil && r.Code() == protocol.ServerConnectToPeer {
			break
		}
	}

	// The token is released, so the peer's later PierceFirewall is unmatched.
	pm.mu.Lock()
	assert.Empty(t, pm.pendingTokens)
	pm.mu.Unlock()
}

func TestConnectRacing_DirectFailureFallsBackToIndirect(t *testing.T) {
	pm, _ := racingPM(t)
	pm.dial = func(context.Context, string) (net.Conn, error) { return nil, errors.New("refused") }

	go deliverIndirect(t, pm, 79)
	pc, err := pm.connectRacing(context.Background(), testAddr, "peer", protocol.ConnTypePeer, 79)
	require.NoError(t, err)
	pc.Conn.Close()
}

func TestConnectRacing_BothFail(t *testing.T) {
	pm, _ := racingPM(t)
	pm.dial = func(context.Context, string) (net.Conn, error) { return nil, errors.New("refused") }

	go func() {
		require.Eventually(t, func() bool {
			pm.mu.Lock()
			defer pm.mu.Unlock()
			ch, ok := pm.pendingTokens[80]
			if ok {
				ch <- connectResult{err: errors.New("server reported peer cannot connect")}
			}
			return ok
		}, 2*time.Second, 5*time.Millisecond)
	}()
	_, err := pm.connectRacing(context.Background(), testAddr, "peer", protocol.ConnTypePeer, 80)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "refused")
	assert.Contains(t, err.Error(), "cannot connect")
}

// answerPeerAddress fulfils the next GetPeerAddress wait for username.
func answerPeerAddress(t *testing.T, pm *peerManager, resp *protocol.PeerAddressResponse) {
	t.Helper()
	require.Eventually(t, func() bool {
		pm.mu.Lock()
		n := len(pm.pendingAddrs[resp.Username])
		pm.mu.Unlock()
		return n > 0
	}, 2*time.Second, 5*time.Millisecond)
	pm.client.onPeerAddress(resp)
}

func TestConnectToPeer_OfflinePeerFailsFast(t *testing.T) {
	pm, sc := racingPM(t)
	pm.dial = func(context.Context, string) (net.Conn, error) {
		t.Error("must not dial an offline peer")
		return nil, errors.New("unreachable")
	}

	go answerPeerAddress(t, pm, &protocol.PeerAddressResponse{Username: "gone", IP: net.IPv4zero, Port: 0})
	start := time.Now()
	_, err := pm.ConnectToPeer(context.Background(), "gone", protocol.ConnTypePeer)
	require.ErrorIs(t, err, ErrPeerOffline)
	assert.Less(t, time.Since(start), 2*time.Second)

	// No ConnectToPeer was relayed for an offline peer.
	sc.Raw().SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	for {
		m, err := sc.ReadMessage()
		if err != nil {
			break
		}
		if r, _ := protocol.NewReader(m); r != nil {
			assert.NotEqual(t, protocol.ServerConnectToPeer, r.Code())
		}
	}
}

// Replies on a P connection we opened reach the registered handlers, exactly
// as on one the peer opened.
func TestConnectToPeer_OutboundPConnectionIsRouted(t *testing.T) {
	pm, _ := racingPM(t)
	far, near := tcpPair(t)
	pm.dial = func(context.Context, string) (net.Conn, error) { return near, nil }

	got := make(chan *peerMessage, 1)
	pm.OnPeerMessage(protocol.PeerTransferResponse, func(m *peerMessage) { got <- m })

	go answerPeerAddress(t, pm, &protocol.PeerAddressResponse{Username: "peer", IP: net.ParseIP("127.0.0.1"), Port: 2234})
	pc, err := pm.ConnectToPeer(context.Background(), "peer", protocol.ConnTypePeer)
	require.NoError(t, err)
	defer pc.Conn.Close()

	require.NoError(t, protocol.WrapPeerConn(far).WriteMessage(protocol.BuildTransferResponseAllowed(5)))
	select {
	case m := <-got:
		assert.Equal(t, "peer", m.PeerConn.Username)
		assert.Same(t, pc, m.PeerConn)
		resp, err := protocol.ParseTransferResponse(m.Raw)
		require.NoError(t, err)
		assert.Equal(t, uint32(5), resp.Token)
	case <-time.After(2 * time.Second):
		t.Fatal("reply on outbound P connection never reached the handler")
	}
}

// F connections carry raw file data and must not be read by the manager.
func TestConnectToPeer_FileConnectionIsNotRouted(t *testing.T) {
	pm, _ := racingPM(t)
	far, near := tcpPair(t)
	pm.dial = func(context.Context, string) (net.Conn, error) { return near, nil }
	pm.OnPeerMessage(protocol.PeerTransferResponse, func(*peerMessage) { t.Error("F connection was routed") })

	go answerPeerAddress(t, pm, &protocol.PeerAddressResponse{Username: "peer", IP: net.ParseIP("127.0.0.1"), Port: 2234})
	pc, err := pm.ConnectToPeer(context.Background(), "peer", protocol.ConnTypeTransfer)
	require.NoError(t, err)
	defer pc.Conn.Close()

	_, err = far.Write([]byte("rawbytes0123"))
	require.NoError(t, err)
	// PeerInit first, then the raw bytes: the caller reads them.
	buf := make([]byte, 64)
	pc.Conn.Raw().SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err := pc.Conn.Raw().Read(buf)
	require.NoError(t, err)
	assert.Equal(t, "rawbytes0123", string(buf[:n]))
}

// ---------------------------------------------------------------------------
// Listener bind retry
// ---------------------------------------------------------------------------

func TestPeerManager_Listen_RetriesUntilPortFree(t *testing.T) {
	// Hold a port so the first bind fails.
	hold, err := net.Listen("tcp", ":0")
	require.NoError(t, err)
	port := hold.Addr().(*net.TCPAddr).Port

	pm := newTestPeerManager("me")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pm.Listen(ctx, port)

	require.Eventually(t, func() bool {
		listening, lastErr := pm.ListenStatus()
		return !listening && lastErr != ""
	}, 2*time.Second, 10*time.Millisecond, "bind failure must be reported")

	hold.Close()
	require.Eventually(t, func() bool {
		listening, lastErr := pm.ListenStatus()
		return listening && lastErr == ""
	}, 5*time.Second, 20*time.Millisecond, "listener must bind once the port is free")

	// And it really accepts.
	raw, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	require.NoError(t, err)
	raw.Close()
}

func TestPeerManager_Listen_StopsRetryingOnCancel(t *testing.T) {
	hold, err := net.Listen("tcp", ":0")
	require.NoError(t, err)
	defer hold.Close()

	pm := newTestPeerManager("me")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { pm.Listen(ctx, hold.Addr().(*net.TCPAddr).Port); close(done) }()
	require.Eventually(t, func() bool { _, e := pm.ListenStatus(); return e != "" }, 2*time.Second, 10*time.Millisecond)

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Listen kept retrying after cancellation")
	}
}
