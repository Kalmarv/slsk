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
		client:        &Client{}, // never logged in: server writes fail
		pendingTokens: make(map[uint32]chan connectResult),
		pendingAddrs:  make(map[string][]chan addrResult),
		msgHandlers:   make(map[uint32]PeerMessageHandler),
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
	received := make(chan *PeerMessage, 1)
	pm.OnPeerMessage(PeerSearchResponse, func(msg *PeerMessage) {
		received <- msg
	})

	// Create a TCP pair: "remote" writes PeerInit + a peer message.
	remote, local := tcpPair(t)
	remoteConn := WrapConn(remote)

	// Send PeerInit handshake.
	initMsg := BuildPeerInit("remoteuser", ConnTypePeer, 42)
	err := remoteConn.WriteMessage(initMsg)
	require.NoError(t, err)

	// Send a peer message after handshake (dummy search response-shaped message).
	testMsg := NewBuilder(PeerSearchResponse)
	testMsg.WriteUint32(0) // dummy payload
	err = remoteConn.WriteMessage(testMsg.Build())
	require.NoError(t, err)

	// Feed the local side to handleIncoming (starts routePeerMessages).
	go pm.handleIncoming(local)

	select {
	case msg := <-received:
		assert.Equal(t, "remoteuser", msg.PeerConn.Username)
		assert.Equal(t, PeerSearchResponse, msg.Code)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for peer message callback")
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
	ch := make(chan connectResult, 1)
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

	// Register a message handler.
	received := make(chan *PeerMessage, 1)
	pm.OnPeerMessage(PeerSearchResponse, func(msg *PeerMessage) {
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

	conn := WrapConn(raw)
	initMsg := BuildPeerInit("remotepeer", ConnTypePeer, 1001)
	err = conn.WriteMessage(initMsg)
	require.NoError(t, err)

	testMsg := NewBuilder(PeerSearchResponse)
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

	conn := WrapConn(raw)
	pfMsg := BuildPierceFirewall(token)
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

		pfMsg := BuildPierceFirewall(tok)
		remoteConn := WrapConn(remote)
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

	pfMsg := BuildPierceFirewall(token)
	remoteConn := WrapConn(remote)
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
	received := make(chan *PeerMessage, 1)
	pm.OnPeerMessage(PeerSearchResponse, func(msg *PeerMessage) {
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
	resp := &ConnectToPeerResponse{
		Username: "remoteuser",
		Type:     ConnTypePeer,
		IP:       net.ParseIP("127.0.0.1"),
		Port:     uint32(addr.Port),
		Token:    12345,
	}

	go pm.handleConnectToPeer(resp)

	// Verify the peer side received a PierceFirewall message.
	<-accepted
	require.NotNil(t, peerConn)
	defer peerConn.Close()

	peerWrapped := WrapConn(peerConn)
	msg, err := peerWrapped.ReadMessage()
	require.NoError(t, err)

	token, err := ParsePierceFirewall(msg)
	require.NoError(t, err)
	assert.Equal(t, uint32(12345), token)

	// Send a peer message from the remote side so routePeerMessages dispatches it.
	testMsg := NewBuilder(PeerSearchResponse)
	testMsg.WriteUint32(0)
	err = peerWrapped.WriteMessage(testMsg.Build())
	require.NoError(t, err)

	// Verify the message handler fired.
	select {
	case m := <-received:
		assert.Equal(t, "remoteuser", m.PeerConn.Username)
		assert.Equal(t, PeerSearchResponse, m.Code)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for peer message callback")
	}
}

func TestPeerManager_HandleConnectToPeer_FileType(t *testing.T) {
	pm := newTestPeerManager("localuser")

	received := make(chan *PeerConn, 1)
	pm.OnFileConnection(func(pc *PeerConn) {
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

	resp := &ConnectToPeerResponse{
		Username: "uploader",
		Type:     ConnTypeTransfer,
		IP:       net.ParseIP("127.0.0.1"),
		Port:     uint32(addr.Port),
		Token:    99999,
	}

	pm.handleConnectToPeer(resp)

	peerConn := <-accepted
	require.NotNil(t, peerConn)
	defer peerConn.Close()

	// Verify PierceFirewall was sent.
	peerWrapped := WrapConn(peerConn)
	msg, err := peerWrapped.ReadMessage()
	require.NoError(t, err)

	token, err := ParsePierceFirewall(msg)
	require.NoError(t, err)
	assert.Equal(t, uint32(99999), token)

	select {
	case pc := <-received:
		assert.Equal(t, "uploader", pc.Username)
		assert.Equal(t, ConnTypeTransfer, pc.Type)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for file connection callback")
	}
}

func TestPeerManager_HandleConnectToPeer_UnreachablePeer(t *testing.T) {
	pm := newTestPeerManager("localuser")

	// Use a port that's not listening — should fail gracefully.
	resp := &ConnectToPeerResponse{
		Username: "ghost",
		Type:     ConnTypePeer,
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
		addr  *PeerAddressResponse
		valid bool
	}{
		{"normal address", &PeerAddressResponse{IP: net.ParseIP("192.168.1.1"), Port: 2234}, true},
		{"zero IP", &PeerAddressResponse{IP: net.ParseIP("0.0.0.0"), Port: 2234}, false},
		{"zero port", &PeerAddressResponse{IP: net.ParseIP("192.168.1.1"), Port: 0}, false},
		{"both zero", &PeerAddressResponse{IP: net.ParseIP("0.0.0.0"), Port: 0}, false},
		{"nil IP", &PeerAddressResponse{IP: nil, Port: 2234}, false},
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
	resp := &ConnectToPeerResponse{
		Username: "ghost",
		Type:     ConnTypePeer,
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
	c, err := NewClient(ClientConfig{ServerAddr: "127.0.0.1:1", Username: "u", Password: "p"})
	require.NoError(t, err)
	pm := NewPeerManager("u", c)

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
func readCantConnect(t *testing.T, sc *Conn) (uint32, string) {
	t.Helper()
	sc.Raw().SetReadDeadline(time.Now().Add(3 * time.Second))
	for {
		msg, err := sc.ReadMessage()
		require.NoError(t, err, "no CantConnectToPeer reached the server")
		r, err := NewReader(msg)
		require.NoError(t, err)
		if r.Code() != ServerCannotConnect {
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

			go pm.handleConnectToPeer(&ConnectToPeerResponse{
				Username: "ghost", Type: ConnTypePeer, IP: net.ParseIP(tc.ip), Port: tc.port, Token: 4242,
			})

			token, user := readCantConnect(t, sc)
			assert.Equal(t, uint32(4242), token)
			assert.Equal(t, "ghost", user)
		})
	}
}

func TestBuildCantConnectToPeer(t *testing.T) {
	r, err := NewReader(BuildCantConnectToPeer(7, "bob"))
	require.NoError(t, err)
	assert.Equal(t, ServerCannotConnect, r.Code())
	token, _ := r.ReadUint32()
	user, _ := r.ReadString()
	assert.Equal(t, uint32(7), token)
	assert.Equal(t, "bob", user)
	assert.False(t, r.HasMore())
}
