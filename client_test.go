package slsk

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/kalmarv/slsk/protocol"
)

// startMockServer starts a TCP listener on a random port, runs the handler
// in a goroutine for the first accepted connection, and returns the address.
func startMockServer(t *testing.T, handler func(*protocol.Conn)) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { listener.Close() })
	go func() {
		raw, err := listener.Accept()
		if err != nil {
			return
		}
		handler(protocol.WrapConn(raw))
	}()
	return listener.Addr().String()
}

// buildLoginSuccessResponse builds a raw wire login success response message.
func buildLoginSuccessResponse() []byte {
	var payload []byte
	payload = append(payload, 1) // success = true
	payload = appendStr(payload, "Welcome!")
	payload = append(payload, reversedIP(127, 0, 0, 1)...)
	payload = appendStr(payload, "somehash")
	payload = append(payload, 0) // not supporter

	return buildMsg(protocol.ServerLogin, payload)
}

// buildLoginFailureResponse builds a raw wire login failure response message.
func buildLoginFailureResponse(reason string) []byte {
	var payload []byte
	payload = append(payload, 0) // success = false
	payload = appendStr(payload, reason)
	return buildMsg(protocol.ServerLogin, payload)
}

// drainInitMessages reads and discards all post-login init messages
// (CheckPrivileges, SetListenPort, SetStatus, HaveNoParent, BranchRoot,
// BranchLevel, AcceptChildren). Uses a short deadline to avoid blocking.
func drainInitMessages(sc *protocol.Conn) {
	sc.Raw().SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	for {
		if _, err := sc.ReadMessage(); err != nil {
			break
		}
	}
	sc.Raw().SetReadDeadline(time.Time{}) // clear deadline
}

// handleLogin reads a login request and sends the given response.
// Returns the login message for inspection, or nil on error.
func handleLogin(t *testing.T, sc *protocol.Conn, response []byte) []byte {
	t.Helper()
	msg, err := sc.ReadMessage()
	if err != nil {
		t.Errorf("mock server: read login: %v", err)
		return nil
	}
	if err := sc.WriteMessage(response); err != nil {
		t.Errorf("mock server: write login response: %v", err)
		return nil
	}
	return msg
}

func TestClient_LoginSuccess(t *testing.T) {
	// Channel receives the first post-login message from the client.
	setListenPortMsg := make(chan []byte, 1)

	addr := startMockServer(t, func(sc *protocol.Conn) {
		defer sc.Close()
		// Read login request, send success.
		handleLogin(t, sc, buildLoginSuccessResponse())

		// Read messages until we find SetListenPort (code 2).
		// Other init messages (CheckPrivileges, etc.) come first.
		for {
			msg, err := sc.ReadMessage()
			if err != nil {
				t.Errorf("mock server: read after login: %v", err)
				return
			}
			r, err := protocol.NewReader(msg)
			if err != nil {
				continue
			}
			if r.Code() == protocol.ServerSetListenPort {
				setListenPortMsg <- msg
				break
			}
		}

		// Drain remaining init messages.
		drainInitMessages(sc)
	})

	cfg := ClientConfig{
		ServerAddr: addr,
		Username:   "testuser",
		Password:   "testpass",
		ListenPort: 2235,
	}
	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err = client.Connect(ctx)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer client.Close()

	if !client.LoggedIn() {
		t.Fatal("expected LoggedIn() == true after successful login")
	}

	// Verify the client sent SetListenPort.
	select {
	case msg := <-setListenPortMsg:
		r, err := protocol.NewReader(msg)
		if err != nil {
			t.Fatalf("NewReader: %v", err)
		}
		if r.Code() != protocol.ServerSetListenPort {
			t.Fatalf("code = %d, want %d (ServerSetListenPort)", r.Code(), protocol.ServerSetListenPort)
		}
		port, err := r.ReadUint32()
		if err != nil {
			t.Fatalf("read port from SetListenPort: %v", err)
		}
		if port != 2235 {
			t.Fatalf("SetListenPort port = %d, want 2235", port)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for SetListenPort message")
	}
}

func TestClient_LoginFailure(t *testing.T) {
	addr := startMockServer(t, func(sc *protocol.Conn) {
		defer sc.Close()
		handleLogin(t, sc, buildLoginFailureResponse("INVALIDPASS"))
	})

	cfg := ClientConfig{
		ServerAddr: addr,
		Username:   "baduser",
		Password:   "badpass",
	}
	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err = client.Connect(ctx)
	if err == nil {
		t.Fatal("expected Connect to return error on login failure")
	}

	// Error should contain the reason string.
	if err.Error() == "" {
		t.Fatal("expected non-empty error message")
	}
	t.Logf("got expected error: %v", err)

	if client.LoggedIn() {
		t.Fatal("expected LoggedIn() == false after failed login")
	}
}

func TestClient_Search(t *testing.T) {
	searchReceived := make(chan []byte, 1)

	addr := startMockServer(t, func(sc *protocol.Conn) {
		defer sc.Close()
		handleLogin(t, sc, buildLoginSuccessResponse())

		// Read messages until we get a FileSearch.
		for {
			msg, err := sc.ReadMessage()
			if err != nil {
				return
			}
			r, err := protocol.NewReader(msg)
			if err != nil {
				continue
			}
			if r.Code() == protocol.ServerFileSearch {
				searchReceived <- msg
				return
			}
		}
	})

	cfg := ClientConfig{
		ServerAddr: addr,
		Username:   "testuser",
		Password:   "testpass",
	}
	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer client.Close()

	// Send a search
	token := uint32(0xBEEF)
	query := "led zeppelin"
	if err := client.Search(token, query); err != nil {
		t.Fatalf("Search: %v", err)
	}

	select {
	case msg := <-searchReceived:
		r, err := protocol.NewReader(msg)
		if err != nil {
			t.Fatalf("NewReader: %v", err)
		}
		if r.Code() != protocol.ServerFileSearch {
			t.Fatalf("code = %d, want %d", r.Code(), protocol.ServerFileSearch)
		}
		tk, err := r.ReadUint32()
		if err != nil {
			t.Fatalf("read token: %v", err)
		}
		if tk != token {
			t.Fatalf("token = %d, want %d", tk, token)
		}
		q, err := r.ReadString()
		if err != nil {
			t.Fatalf("read query: %v", err)
		}
		if q != query {
			t.Fatalf("query = %q, want %q", q, query)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for search message")
	}
}

func TestClient_PingResponse(t *testing.T) {
	pongReceived := make(chan struct{}, 1)

	addr := startMockServer(t, func(sc *protocol.Conn) {
		defer sc.Close()
		handleLogin(t, sc, buildLoginSuccessResponse())

		// Drain post-login init messages.
		drainInitMessages(sc)

		// Send a ping to the client.
		ping := protocol.BuildPing()
		if err := sc.WriteMessage(ping); err != nil {
			t.Errorf("mock server: write ping: %v", err)
			return
		}

		// Read the pong response.
		for {
			msg, err := sc.ReadMessage()
			if err != nil {
				return
			}
			r, err := protocol.NewReader(msg)
			if err != nil {
				continue
			}
			if r.Code() == protocol.ServerPing {
				pongReceived <- struct{}{}
				return
			}
		}
	})

	cfg := ClientConfig{
		ServerAddr: addr,
		Username:   "testuser",
		Password:   "testpass",
	}
	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer client.Close()

	select {
	case <-pongReceived:
		// success
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for pong response")
	}
}

func TestClient_OnSearchRequest(t *testing.T) {
	callbackFired := make(chan *protocol.IncomingSearchRequest, 1)

	addr := startMockServer(t, func(sc *protocol.Conn) {
		defer sc.Close()
		handleLogin(t, sc, buildLoginSuccessResponse())

		// Drain post-login init messages.
		drainInitMessages(sc)

		// Send an incoming search request (code 93 = ServerEmbeddedMessage).
		var payload []byte
		payload = append(payload, byte(protocol.DistribSearchRequest)) // distrib code = 3
		payload = append(payload, u32le(49)...)                        // identifier
		payload = appendStr(payload, "searchuser")
		payload = append(payload, u32le(42)...) // token
		payload = appendStr(payload, "pink floyd")
		msg := buildMsg(protocol.ServerEmbeddedMessage, payload)
		if err := sc.WriteMessage(msg); err != nil {
			t.Errorf("mock server: write search: %v", err)
			return
		}

		// Keep connection open briefly so the client can process.
		time.Sleep(500 * time.Millisecond)
	})

	cfg := ClientConfig{
		ServerAddr: addr,
		Username:   "testuser",
		Password:   "testpass",
	}
	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	client.OnSearchRequest(func(req *protocol.IncomingSearchRequest) {
		callbackFired <- req
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer client.Close()

	select {
	case req := <-callbackFired:
		if req.Username != "searchuser" {
			t.Fatalf("username = %q, want searchuser", req.Username)
		}
		if req.Token != 42 {
			t.Fatalf("token = %d, want 42", req.Token)
		}
		if req.Query != "pink floyd" {
			t.Fatalf("query = %q, want 'pink floyd'", req.Query)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for search request callback")
	}
}

func TestClient_OnConnectToPeer(t *testing.T) {
	callbackFired := make(chan *protocol.ConnectToPeerResponse, 1)

	addr := startMockServer(t, func(sc *protocol.Conn) {
		defer sc.Close()
		handleLogin(t, sc, buildLoginSuccessResponse())

		// Drain post-login init messages.
		drainInitMessages(sc)

		// Send a ConnectToPeer notification (code 18).
		var payload []byte
		payload = appendStr(payload, "peeruser")
		payload = appendStr(payload, "P")
		payload = append(payload, reversedIP(10, 0, 0, 5)...)
		payload = append(payload, u32le(2234)...)  // port
		payload = append(payload, u32le(99999)...) // token
		payload = append(payload, 1)               // privileged
		msg := buildMsg(protocol.ServerConnectToPeer, payload)
		if err := sc.WriteMessage(msg); err != nil {
			t.Errorf("mock server: write connect to peer: %v", err)
			return
		}

		time.Sleep(500 * time.Millisecond)
	})

	cfg := ClientConfig{
		ServerAddr: addr,
		Username:   "testuser",
		Password:   "testpass",
	}
	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	client.OnConnectToPeer(func(resp *protocol.ConnectToPeerResponse) {
		callbackFired <- resp
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer client.Close()

	select {
	case resp := <-callbackFired:
		if resp.Username != "peeruser" {
			t.Fatalf("username = %q, want peeruser", resp.Username)
		}
		if resp.Type != "P" {
			t.Fatalf("type = %q, want P", resp.Type)
		}
		expectedIP := net.IPv4(10, 0, 0, 5).To4()
		if !resp.IP.Equal(expectedIP) {
			t.Fatalf("IP = %v, want %v", resp.IP, expectedIP)
		}
		if resp.Port != 2234 {
			t.Fatalf("port = %d, want 2234", resp.Port)
		}
		if resp.Token != 99999 {
			t.Fatalf("token = %d, want 99999", resp.Token)
		}
		if !resp.Privileged {
			t.Fatal("expected privileged=true")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for connect to peer callback")
	}
}

func TestClient_Close(t *testing.T) {
	addr := startMockServer(t, func(sc *protocol.Conn) {
		defer sc.Close()
		handleLogin(t, sc, buildLoginSuccessResponse())
		// Keep connection alive until the client closes.
		for {
			_, err := sc.ReadMessage()
			if err != nil {
				return
			}
		}
	})

	cfg := ClientConfig{
		ServerAddr: addr,
		Username:   "testuser",
		Password:   "testpass",
	}
	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	if !client.LoggedIn() {
		t.Fatal("expected LoggedIn() == true before Close")
	}

	if err := client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if client.LoggedIn() {
		t.Fatal("expected LoggedIn() == false after Close")
	}

	// Subsequent operations should fail.
	if err := client.Search(1, "test"); err == nil {
		t.Fatal("expected Search to fail after Close")
	}
}

// connectedClient logs a Client in to a mock server and hands the server side
// of the connection to the test through the returned channel.
func connectedClient(t *testing.T) (*Client, <-chan *protocol.Conn) {
	t.Helper()
	serverSide := make(chan *protocol.Conn, 1)
	addr := startMockServer(t, func(sc *protocol.Conn) {
		handleLogin(t, sc, buildLoginSuccessResponse())
		drainInitMessages(sc)
		serverSide <- sc
	})
	client, err := NewClient(ClientConfig{ServerAddr: addr, Username: "u", Password: "p"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { client.Close() })
	return client, serverSide
}

func TestClient_Relogged(t *testing.T) {
	client, serverSide := connectedClient(t)
	sc := <-serverSide
	defer sc.Close()

	if client.DisconnectReason() != nil {
		t.Fatalf("DisconnectReason = %v while connected", client.DisconnectReason())
	}
	if err := sc.WriteMessage(protocol.NewBuilder(protocol.ServerRelogged).Build()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-client.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("client did not drop the connection on Relogged")
	}
	if !errors.Is(client.DisconnectReason(), ErrRelogged) {
		t.Fatalf("DisconnectReason = %v, want ErrRelogged", client.DisconnectReason())
	}
	if client.LoggedIn() {
		t.Fatal("client still logged in after Relogged")
	}
}

func TestClient_DisconnectReasonOnDrop(t *testing.T) {
	client, serverSide := connectedClient(t)
	(<-serverSide).Close()
	select {
	case <-client.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("client did not notice the dropped connection")
	}
	if err := client.DisconnectReason(); err == nil || errors.Is(err, ErrRelogged) {
		t.Fatalf("DisconnectReason = %v, want the read error", err)
	}
}
