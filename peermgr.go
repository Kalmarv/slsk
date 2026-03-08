package slsk

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

// PeerConn represents an established peer connection.
type PeerConn struct {
	Conn     *Conn
	Username string
	Type     string // "P", "F"
	Token    uint32
}

// PeerHandler is called when a peer connection is fully established.
type PeerHandler func(pc *PeerConn)

// PeerManager manages peer connections: listening for incoming,
// establishing outgoing, and handling the init handshake.
type PeerManager struct {
	username string
	client   *Client
	listener net.Listener

	mu            sync.Mutex
	pendingTokens map[uint32]chan *PeerConn // tokens we're waiting for (indirect)

	onPeerConn PeerHandler // called for incoming P connections
	onFileConn PeerHandler // called for incoming F connections
}

// NewPeerManager creates a new PeerManager with the given username and client.
// It registers for ConnectToPeer server messages to handle indirect peer connections
// (NAT traversal: peer asks server to tell us to connect to them).
func NewPeerManager(username string, client *Client) *PeerManager {
	pm := &PeerManager{
		username:      username,
		client:        client,
		pendingTokens: make(map[uint32]chan *PeerConn),
	}

	// Handle ConnectToPeer from server: a peer wants us to connect to them.
	client.OnConnectToPeer(func(resp *ConnectToPeerResponse) {
		go pm.handleConnectToPeer(resp)
	})

	return pm
}

// OnPeerConnection registers handler for incoming P-type connections.
func (pm *PeerManager) OnPeerConnection(fn PeerHandler) {
	pm.onPeerConn = fn
}

// OnFileConnection registers handler for incoming F-type connections.
func (pm *PeerManager) OnFileConnection(fn PeerHandler) {
	pm.onFileConn = fn
}

// Listen starts accepting incoming peer connections on the given port.
func (pm *PeerManager) Listen(ctx context.Context, port int) error {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	pm.listener = ln

	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	for {
		raw, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil // context cancelled, clean shutdown
			}
			log.Printf("peer accept error: %v", err)
			continue
		}
		go pm.handleIncoming(raw)
	}
}

// handleIncoming reads the init message from an incoming connection.
func (pm *PeerManager) handleIncoming(raw net.Conn) {
	conn := WrapConn(raw)

	// Set a deadline for the handshake.
	raw.SetReadDeadline(time.Now().Add(10 * time.Second))

	msg, err := conn.ReadMessage()
	if err != nil {
		conn.Close()
		return
	}

	// Reset deadline.
	raw.SetReadDeadline(time.Time{})

	// Need at least 5 bytes: [4-byte length][1-byte init code].
	if len(msg) < 5 {
		conn.Close()
		return
	}

	initCode := msg[4] // 1-byte code after 4-byte length

	if initCode == InitPierceFirewall {
		token, err := ParsePierceFirewall(msg)
		if err != nil {
			conn.Close()
			return
		}

		pm.mu.Lock()
		ch, ok := pm.pendingTokens[token]
		pm.mu.Unlock()

		if ok {
			ch <- &PeerConn{Conn: conn, Token: token}
		} else {
			conn.Close()
		}
		return
	}

	if initCode == InitPeerInit {
		init, err := ParsePeerInit(msg)
		if err != nil {
			conn.Close()
			return
		}

		pc := &PeerConn{
			Conn:     conn,
			Username: init.Username,
			Type:     init.Type,
			Token:    init.Token,
		}

		switch init.Type {
		case ConnTypePeer:
			if pm.onPeerConn != nil {
				go pm.onPeerConn(pc)
			}
		case ConnTypeTransfer:
			if pm.onFileConn != nil {
				go pm.onFileConn(pc)
			}
		default:
			conn.Close()
		}
		return
	}

	conn.Close()
}

// ConnectToPeer establishes a peer connection, trying direct first then indirect.
// Returns a connected PeerConn ready for message exchange.
func (pm *PeerManager) ConnectToPeer(ctx context.Context, username string, connType string) (*PeerConn, error) {
	// 1. Get peer address from server.
	addrCh := make(chan *PeerAddressResponse, 1)

	// Register one-time callback.
	pm.client.OnPeerAddress(func(resp *PeerAddressResponse) {
		if resp.Username == username {
			addrCh <- resp
		}
	})

	if err := pm.client.GetPeerAddress(username); err != nil {
		return nil, fmt.Errorf("get peer address: %w", err)
	}

	// Wait for address response.
	var addr *PeerAddressResponse
	select {
	case addr = <-addrCh:
	case <-time.After(10 * time.Second):
		return nil, fmt.Errorf("timeout waiting for peer address of %s", username)
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	// Generate a token for this connection.
	token := generateToken()

	// 2. Try direct connection.
	directCtx, directCancel := context.WithTimeout(ctx, 5*time.Second)
	defer directCancel()

	pc, err := pm.connectDirect(directCtx, addr, username, connType, token)
	if err == nil {
		return pc, nil
	}
	log.Printf("direct connection to %s failed: %v, trying indirect", username, err)

	// 3. Try indirect (NAT traversal via server).
	return pm.connectIndirect(ctx, username, connType, token)
}

// connectDirect dials the peer directly and sends PeerInit.
func (pm *PeerManager) connectDirect(ctx context.Context, addr *PeerAddressResponse, username, connType string, token uint32) (*PeerConn, error) {
	target := fmt.Sprintf("%s:%d", addr.IP.String(), addr.Port)

	var d net.Dialer
	raw, err := d.DialContext(ctx, "tcp", target)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", target, err)
	}

	conn := WrapConn(raw)

	// Send PeerInit.
	initMsg := BuildPeerInit(pm.username, connType, token)
	if err := conn.WriteMessage(initMsg); err != nil {
		conn.Close()
		return nil, fmt.Errorf("send peer init: %w", err)
	}

	return &PeerConn{
		Conn:     conn,
		Username: username,
		Type:     connType,
		Token:    token,
	}, nil
}

// connectIndirect asks the server to tell the peer to connect to us.
func (pm *PeerManager) connectIndirect(ctx context.Context, username, connType string, token uint32) (*PeerConn, error) {
	// Register a pending token so we can match the incoming PierceFirewall.
	ch := make(chan *PeerConn, 1)
	pm.mu.Lock()
	pm.pendingTokens[token] = ch
	pm.mu.Unlock()

	defer func() {
		pm.mu.Lock()
		delete(pm.pendingTokens, token)
		pm.mu.Unlock()
	}()

	// Ask server to tell peer to connect to us.
	if err := pm.client.SendConnectToPeer(token, username, connType); err != nil {
		return nil, fmt.Errorf("send connect to peer: %w", err)
	}

	// Wait for the peer to connect back.
	select {
	case pc := <-ch:
		pc.Username = username
		pc.Type = connType
		return pc, nil
	case <-time.After(15 * time.Second):
		return nil, fmt.Errorf("timeout waiting for indirect connection from %s", username)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// handleConnectToPeer handles the server telling us that a peer wants us to
// connect to them (reverse/indirect connection). We dial the peer and send
// PierceFirewall with their token, then dispatch based on connection type.
func (pm *PeerManager) handleConnectToPeer(resp *ConnectToPeerResponse) {
	target := fmt.Sprintf("%s:%d", resp.IP.String(), resp.Port)
	log.Printf("ConnectToPeer from %s type=%s addr=%s token=%d", resp.Username, resp.Type, target, resp.Token)

	var d net.Dialer
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	raw, err := d.DialContext(ctx, "tcp", target)
	if err != nil {
		log.Printf("failed to connect to peer %s at %s: %v", resp.Username, target, err)
		return
	}

	conn := WrapConn(raw)

	// Send PierceFirewall with the token from the server message.
	pfMsg := BuildPierceFirewall(resp.Token)
	if err := conn.WriteMessage(pfMsg); err != nil {
		conn.Close()
		log.Printf("failed to send PierceFirewall to %s: %v", resp.Username, err)
		return
	}

	pc := &PeerConn{
		Conn:     conn,
		Username: resp.Username,
		Type:     resp.Type,
		Token:    resp.Token,
	}

	switch resp.Type {
	case ConnTypePeer:
		if pm.onPeerConn != nil {
			go pm.onPeerConn(pc)
		} else {
			conn.Close()
		}
	case ConnTypeTransfer:
		if pm.onFileConn != nil {
			go pm.onFileConn(pc)
		} else {
			conn.Close()
		}
	default:
		conn.Close()
	}
}

// generateToken returns a random-ish uint32 token.
func generateToken() uint32 {
	return uint32(time.Now().UnixNano() & 0xFFFFFFFF)
}
