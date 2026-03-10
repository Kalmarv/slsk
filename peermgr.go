package slsk

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"math/rand"
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

// PeerMessage represents a message received on a peer connection.
type PeerMessage struct {
	PeerConn *PeerConn
	Code     uint32
	Raw      []byte // full message including length+code prefix
}

// PeerMessageHandler handles a specific peer message type.
type PeerMessageHandler func(msg *PeerMessage)

// connectResult carries either a successful peer connection or an error
// through the pendingTokens channel, enabling fast failure on CannotConnect.
type connectResult struct {
	pc  *PeerConn
	err error
}

// PeerManager manages peer connections: listening for incoming,
// establishing outgoing, and handling the init handshake.
type PeerManager struct {
	username string
	client   *Client
	listener net.Listener

	mu            sync.Mutex
	pendingTokens map[uint32]chan connectResult           // tokens we're waiting for (indirect)
	pendingAddrs  map[string][]chan *PeerAddressResponse  // username -> waiters for address
	msgHandlers   map[uint32]PeerMessageHandler          // message code -> handler

	onFileConn PeerHandler // called for incoming F connections
}

// NewPeerManager creates a new PeerManager with the given username and client.
// It registers for ConnectToPeer server messages to handle indirect peer connections
// (NAT traversal: peer asks server to tell us to connect to them).
func NewPeerManager(username string, client *Client) *PeerManager {
	pm := &PeerManager{
		username:      username,
		client:        client,
		pendingTokens: make(map[uint32]chan connectResult),
		pendingAddrs:  make(map[string][]chan *PeerAddressResponse),
		msgHandlers:   make(map[uint32]PeerMessageHandler),
	}

	// Handle ConnectToPeer from server: a peer wants us to connect to them.
	client.OnConnectToPeer(func(resp *ConnectToPeerResponse) {
		go pm.handleConnectToPeer(resp)
	})

	// Handle PeerAddress responses: fan out to all goroutines waiting for this peer's address.
	client.OnPeerAddress(func(resp *PeerAddressResponse) {
		pm.mu.Lock()
		waiters := pm.pendingAddrs[resp.Username]
		delete(pm.pendingAddrs, resp.Username)
		pm.mu.Unlock()

		for _, ch := range waiters {
			ch <- resp
		}
	})

	// Handle CannotConnect: server tells us a peer can't reach us.
	// Signal the pending indirect connection to fail fast instead of timing out.
	client.OnCannotConnect(func(token uint32) {
		pm.mu.Lock()
		ch, ok := pm.pendingTokens[token]
		pm.mu.Unlock()

		if ok {
			log.Printf("[peermgr] CannotConnect for token %d — failing fast", token)
			select {
			case ch <- connectResult{err: fmt.Errorf("server reported peer cannot connect (token %d)", token)}:
			default:
			}
		}
	})

	return pm
}

// OnPeerMessage registers a handler for a specific peer message code.
// When a P-type connection receives a message with this code, the handler
// is called with the message and the connection it arrived on.
func (pm *PeerManager) OnPeerMessage(code uint32, fn PeerMessageHandler) {
	pm.mu.Lock()
	pm.msgHandlers[code] = fn
	pm.mu.Unlock()
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
			log.Printf("[peermgr] incoming PierceFirewall parse error: %v", err)
			conn.Close()
			return
		}

		pm.mu.Lock()
		ch, ok := pm.pendingTokens[token]
		pm.mu.Unlock()

		if ok {
			log.Printf("[peermgr] incoming PierceFirewall matched token %d", token)
			ch <- connectResult{pc: &PeerConn{Conn: conn, Token: token}}
		} else {
			log.Printf("[peermgr] incoming PierceFirewall unmatched token %d (no pending)", token)
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
			go pm.routePeerMessages(pc)
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
	return pm.ConnectToPeerWithToken(ctx, username, connType, generateToken())
}

// ConnectToPeerWithToken is like ConnectToPeer but uses the specified token
// instead of generating a random one. Used for F-connections where the token
// must match the transfer token.
func (pm *PeerManager) ConnectToPeerWithToken(ctx context.Context, username string, connType string, token uint32) (*PeerConn, error) {
	log.Printf("[peermgr] ConnectToPeerWithToken %s type=%s token=%d: getting address", username, connType, token)

	addr, err := pm.getPeerAddress(ctx, username)
	if err != nil {
		return nil, err
	}

	if !isValidAddr(addr) {
		log.Printf("[peermgr] ConnectToPeerWithToken %s: invalid address %s:%d, trying indirect only", username, addr.IP, addr.Port)
	} else {
		directCtx, directCancel := context.WithTimeout(ctx, 5*time.Second)
		pc, err := pm.connectDirect(directCtx, addr, username, connType, token)
		directCancel()
		if err == nil {
			return pc, nil
		}
	}

	pc, err := pm.connectIndirect(ctx, username, connType, token)
	if err != nil {
		return nil, err
	}
	return pc, nil
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
	ch := make(chan connectResult, 1)
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

	// Wait for the peer to connect back, or CannotConnect to fail fast.
	select {
	case result := <-ch:
		if result.err != nil {
			return nil, result.err
		}
		result.pc.Username = username
		result.pc.Type = connType
		return result.pc, nil
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

	if resp.IP.IsUnspecified() || resp.Port == 0 {
		log.Printf("[peermgr] ignoring ConnectToPeer from %s: invalid address %s", resp.Username, target)
		return
	}

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
		go pm.routePeerMessages(pc)
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

// routePeerMessages reads messages from a P-type connection and dispatches
// them to registered handlers by message code. The connection is closed when
// the peer disconnects or an error occurs.
func (pm *PeerManager) routePeerMessages(pc *PeerConn) {
	defer pc.Conn.Close()

	for {
		msg, err := pc.Conn.ReadMessage()
		if err != nil {
			return // connection closed
		}

		if len(msg) < 8 {
			continue
		}

		code := binary.LittleEndian.Uint32(msg[4:8])

		pm.mu.Lock()
		handler, ok := pm.msgHandlers[code]
		pm.mu.Unlock()

		if ok {
			handler(&PeerMessage{
				PeerConn: pc,
				Code:     code,
				Raw:      msg,
			})
		} else {
			log.Printf("[peermgr] unhandled peer message code %d from %s (%d bytes)", code, pc.Username, len(msg))
		}
	}
}

// generateToken returns a random uint32 token.
func generateToken() uint32 {
	return rand.Uint32()
}

// isValidAddr returns true if the peer address is usable for a direct connection.
// The Soulseek server returns 0.0.0.0:0 for offline or unreachable peers.
func isValidAddr(addr *PeerAddressResponse) bool {
	return addr.IP != nil && !addr.IP.IsUnspecified() && addr.Port != 0
}

// getPeerAddress requests a peer's address from the server and waits for the response.
// Deduplicates concurrent requests for the same username. Cleans up on timeout/cancel.
func (pm *PeerManager) getPeerAddress(ctx context.Context, username string) (*PeerAddressResponse, error) {
	addrCh := make(chan *PeerAddressResponse, 1)

	pm.mu.Lock()
	pm.pendingAddrs[username] = append(pm.pendingAddrs[username], addrCh)
	needsRequest := len(pm.pendingAddrs[username]) == 1
	pm.mu.Unlock()

	if needsRequest {
		if err := pm.client.GetPeerAddress(username); err != nil {
			pm.removeAddrWaiter(username, addrCh)
			return nil, fmt.Errorf("get peer address: %w", err)
		}
	}

	select {
	case addr := <-addrCh:
		return addr, nil
	case <-time.After(10 * time.Second):
		pm.removeAddrWaiter(username, addrCh)
		return nil, fmt.Errorf("timeout waiting for peer address of %s", username)
	case <-ctx.Done():
		pm.removeAddrWaiter(username, addrCh)
		return nil, ctx.Err()
	}
}

// removeAddrWaiter removes a specific channel from pendingAddrs to prevent leaks.
func (pm *PeerManager) removeAddrWaiter(username string, ch chan *PeerAddressResponse) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	waiters := pm.pendingAddrs[username]
	for i, w := range waiters {
		if w == ch {
			pm.pendingAddrs[username] = append(waiters[:i], waiters[i+1:]...)
			break
		}
	}
	if len(pm.pendingAddrs[username]) == 0 {
		delete(pm.pendingAddrs, username)
	}
}
