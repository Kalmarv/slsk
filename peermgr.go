package slsk

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
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

// addrResult carries a peer address or lookup error to address waiters.
type addrResult struct {
	addr *PeerAddressResponse
	err  error
}

// Peer connection limits.
const (
	// maxPeerConns caps concurrent inbound peer connections so a peer flood
	// cannot exhaust file descriptors and goroutines.
	maxPeerConns = 1000
	// peerIdleTimeout closes P-type connections that go quiet. Transfers run on
	// separate F connections, so this only reclaims abandoned signaling links.
	peerIdleTimeout = 15 * time.Minute
)

// PeerManager manages peer connections: listening for incoming,
// establishing outgoing, and handling the init handshake.
type PeerManager struct {
	username string
	client   *Client
	listener net.Listener

	mu            sync.Mutex
	pendingTokens map[uint32]chan connectResult // tokens we're waiting for (indirect)
	pendingAddrs  map[string][]chan addrResult  // username -> waiters for address
	msgHandlers   map[uint32]PeerMessageHandler // message code -> handler
	conns         int                           // open inbound peer connections

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
		pendingAddrs:  make(map[string][]chan addrResult),
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
			select {
			case ch <- addrResult{addr: resp}:
			default:
			}
		}
	})

	// Handle CannotConnect: server tells us a peer can't reach us.
	// Signal the pending indirect connection to fail fast instead of timing out.
	client.OnCannotConnect(func(token uint32) {
		pm.mu.Lock()
		ch, ok := pm.pendingTokens[token]
		pm.mu.Unlock()

		if ok {
			slog.Debug("CannotConnect failing fast", "component", "peermgr", "token", token)
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
			slog.Error("peer accept error", "component", "peermgr", "err", err)
			// Back off so a persistent error (e.g. fd exhaustion) cannot spin
			// the accept loop at 100% CPU.
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(time.Second):
			}
			continue
		}

		pm.mu.Lock()
		if pm.conns >= maxPeerConns {
			pm.mu.Unlock()
			slog.Warn("peer connection limit reached, rejecting", "component", "peermgr", "limit", maxPeerConns)
			raw.Close()
			continue
		}
		pm.conns++
		pm.mu.Unlock()

		go pm.handleIncoming(&countedConn{Conn: raw, release: pm.releaseConn})
	}
}

// countedConn releases a peer connection slot exactly once, on Close.
type countedConn struct {
	net.Conn
	once    sync.Once
	release func()
}

func (c *countedConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.release)
	return err
}

// releaseConn decrements the open inbound peer connection count.
func (pm *PeerManager) releaseConn() {
	pm.mu.Lock()
	if pm.conns > 0 {
		pm.conns--
	}
	pm.mu.Unlock()
}

// handleIncoming reads the init message from an incoming connection.
func (pm *PeerManager) handleIncoming(raw net.Conn) {
	conn := WrapPeerConn(raw)

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
			slog.Warn("PierceFirewall parse error", "component", "peermgr", "err", err)
			conn.Close()
			return
		}

		// Deliver while holding the lock so a waiter that already gave up
		// cannot leave this connection parked unread in its channel.
		pm.mu.Lock()
		ch, ok := pm.pendingTokens[token]
		if ok {
			select {
			case ch <- connectResult{pc: &PeerConn{Conn: conn, Token: token}}:
			default:
				ok = false
			}
		}
		pm.mu.Unlock()

		if ok {
			slog.Debug("PierceFirewall matched", "component", "peermgr", "token", token)
		} else {
			slog.Debug("PierceFirewall unmatched", "component", "peermgr", "token", token)
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
			} else {
				conn.Close()
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
	slog.Debug("getting peer address", "component", "peermgr", "user", username, "type", connType, "token", token)

	addr, err := pm.getPeerAddress(ctx, username)
	if err != nil {
		return nil, err
	}

	if !isValidAddr(addr) {
		slog.Debug("invalid address, trying indirect", "component", "peermgr", "user", username, "ip", addr.IP, "port", addr.Port)
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

	conn := WrapPeerConn(raw)

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
		// A connection that arrived after we gave up is parked in ch; close it
		// so its socket and goroutine are not leaked.
		var orphan *PeerConn
		select {
		case res := <-ch:
			orphan = res.pc
		default:
		}
		pm.mu.Unlock()
		if orphan != nil {
			orphan.Conn.Close()
		}
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
	slog.Debug("ConnectToPeer request", "component", "peermgr", "user", resp.Username, "type", resp.Type, "addr", target, "token", resp.Token)

	if resp.IP.IsUnspecified() || resp.Port == 0 {
		slog.Debug("ignoring ConnectToPeer, invalid address", "component", "peermgr", "user", resp.Username, "addr", target)
		return
	}

	var d net.Dialer
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	raw, err := d.DialContext(ctx, "tcp", target)
	if err != nil {
		slog.Debug("failed to connect to peer", "component", "peermgr", "user", resp.Username, "addr", target, "err", err)
		return
	}

	conn := WrapPeerConn(raw)

	// Send PierceFirewall with the token from the server message.
	pfMsg := BuildPierceFirewall(resp.Token)
	if err := conn.WriteMessage(pfMsg); err != nil {
		conn.Close()
		slog.Debug("failed to send PierceFirewall", "component", "peermgr", "user", resp.Username, "err", err)
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
		// Reclaim connections that go quiet; file transfers use separate F
		// connections, so the idle timeout only affects signaling.
		_ = pc.Conn.Raw().SetReadDeadline(time.Now().Add(peerIdleTimeout))
		msg, err := pc.Conn.ReadMessage()
		if err != nil {
			return // connection closed or idle
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
			slog.Debug("unhandled peer message", "component", "peermgr", "code", code, "user", pc.Username, "bytes", len(msg))
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
	addrCh := make(chan addrResult, 1)

	pm.mu.Lock()
	pm.pendingAddrs[username] = append(pm.pendingAddrs[username], addrCh)
	needsRequest := len(pm.pendingAddrs[username]) == 1
	pm.mu.Unlock()

	if needsRequest {
		if err := pm.client.GetPeerAddress(username); err != nil {
			// Fail every waiter instead of leaving the rest to time out.
			pm.failAddrWaiters(username, fmt.Errorf("get peer address: %w", err))
			return nil, fmt.Errorf("get peer address: %w", err)
		}
	}

	select {
	case res := <-addrCh:
		if res.err != nil {
			return nil, res.err
		}
		return res.addr, nil
	case <-time.After(10 * time.Second):
		pm.removeAddrWaiter(username, addrCh)
		if needsRequest {
			// Other waiters may have joined after us; re-issue so they don't
			// all hang on a request nobody will answer.
			pm.reissueAddrRequest(username)
		}
		return nil, fmt.Errorf("timeout waiting for peer address of %s", username)
	case <-ctx.Done():
		pm.removeAddrWaiter(username, addrCh)
		return nil, ctx.Err()
	}
}

// failAddrWaiters removes all waiters for username and delivers err to each.
func (pm *PeerManager) failAddrWaiters(username string, err error) {
	pm.mu.Lock()
	waiters := pm.pendingAddrs[username]
	delete(pm.pendingAddrs, username)
	pm.mu.Unlock()

	for _, ch := range waiters {
		select {
		case ch <- addrResult{err: err}:
		default:
		}
	}
}

// reissueAddrRequest re-sends a GetPeerAddress request when waiters remain.
func (pm *PeerManager) reissueAddrRequest(username string) {
	pm.mu.Lock()
	remaining := len(pm.pendingAddrs[username])
	pm.mu.Unlock()
	if remaining == 0 {
		return
	}
	if err := pm.client.GetPeerAddress(username); err != nil {
		pm.failAddrWaiters(username, fmt.Errorf("get peer address: %w", err))
	}
}

// removeAddrWaiter removes a specific channel from pendingAddrs to prevent leaks.
func (pm *PeerManager) removeAddrWaiter(username string, ch chan addrResult) {
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
