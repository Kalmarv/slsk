package slsk

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/kalmarv/slsk/protocol"
)

// peerConn represents an established peer connection.
type peerConn struct {
	Conn     *protocol.Conn
	Username string
	Type     string // "P", "F"
	Token    uint32
}

// peerHandler is called when a peer connection is fully established.
type peerHandler func(pc *peerConn)

// peerMessage represents a message received on a peer connection.
type peerMessage struct {
	PeerConn *peerConn
	Code     uint32
	Raw      []byte // full message including length+code prefix
}

// peerMessageHandler handles a specific peer message type.
type peerMessageHandler func(msg *peerMessage)

// connectResult carries either a successful peer connection or an error
// through the pendingTokens channel, enabling fast failure on CannotConnect.
type connectResult struct {
	pc  *peerConn
	err error
}

// addrResult carries a peer address or lookup error to address waiters.
type addrResult struct {
	addr *protocol.PeerAddressResponse
	err  error
}

// Peer connection limits.
const (
	// maxPeerConns caps concurrent inbound peer connections so a peer flood
	// cannot exhaust file descriptors and goroutines.
	maxPeerConns = 1000
	// directConnectTimeout bounds the direct dial of an outgoing connection;
	// the indirect leg runs concurrently and has indirectConnectTimeout.
	directConnectTimeout   = 5 * time.Second
	indirectConnectTimeout = 15 * time.Second
	// peerIdleTimeout closes P-type connections that go quiet. Transfers run on
	// separate F connections, so this only reclaims abandoned signaling links.
	peerIdleTimeout = 15 * time.Minute
)

// peerManager manages peer connections: listening for incoming,
// establishing outgoing, and handling the init handshake.
type peerManager struct {
	log      *slog.Logger
	username string
	client   *serverConn
	listener net.Listener

	mu            sync.Mutex
	pendingTokens map[uint32]chan connectResult // tokens we're waiting for (indirect)
	pendingAddrs  map[string][]chan addrResult  // username -> waiters for address
	msgHandlers   map[uint32]peerMessageHandler // message code -> handler
	conns         int                           // open inbound peer connections
	listening     bool                          // listener bound and accepting
	listenErr     string                        // last bind failure, "" once bound

	onFileConn peerHandler // called for incoming F connections

	// dial opens direct outgoing connections; nil means a plain net.Dialer.
	// Tests replace it.
	dial func(ctx context.Context, addr string) (net.Conn, error)
}

// ErrPeerOffline is returned when the server reports no address for a peer
// (0.0.0.0:0), which it does for users that are offline.
var ErrPeerOffline = errors.New("peer is offline")

// newPeerManager creates a new PeerManager with the given username and client.
// It registers for ConnectToPeer server messages to handle indirect peer connections
// (NAT traversal: peer asks server to tell us to connect to them).
func newPeerManager(username string, client *serverConn) *peerManager {
	pm := &peerManager{
		log:           client.log,
		username:      username,
		client:        client,
		pendingTokens: make(map[uint32]chan connectResult),
		pendingAddrs:  make(map[string][]chan addrResult),
		msgHandlers:   make(map[uint32]peerMessageHandler),
	}

	// Handle ConnectToPeer from server: a peer wants us to connect to them.
	client.OnConnectToPeer(func(resp *protocol.ConnectToPeerResponse) {
		go pm.handleConnectToPeer(resp)
	})

	// Handle PeerAddress responses: fan out to all goroutines waiting for this peer's address.
	client.OnPeerAddress(func(resp *protocol.PeerAddressResponse) {
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
			pm.log.Debug("CannotConnect failing fast", "component", "peermgr", "token", token)
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
func (pm *peerManager) OnPeerMessage(code uint32, fn peerMessageHandler) {
	pm.mu.Lock()
	pm.msgHandlers[code] = fn
	pm.mu.Unlock()
}

// OnFileConnection registers handler for incoming F-type connections.
func (pm *peerManager) OnFileConnection(fn peerHandler) {
	pm.onFileConn = fn
}

// Listen accepts incoming peer connections on the given port until ctx is
// cancelled. If the port cannot be bound (still held by a previous process,
// say) it keeps retrying with backoff rather than leaving inbound connections
// silently dead; ListenStatus reports the state.
func (pm *peerManager) Listen(ctx context.Context, port int) {
	ln := pm.bind(ctx, port)
	if ln == nil {
		return // cancelled while waiting to bind
	}
	pm.mu.Lock()
	pm.listener = ln
	pm.mu.Unlock()

	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	for {
		raw, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return // context cancelled, clean shutdown
			}
			pm.log.Error("peer accept error", "component", "peermgr", "err", err)
			// Back off so a persistent error (e.g. fd exhaustion) cannot spin
			// the accept loop at 100% CPU.
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}

		pm.mu.Lock()
		if pm.conns >= maxPeerConns {
			pm.mu.Unlock()
			pm.log.Warn("peer connection limit reached, rejecting", "component", "peermgr", "limit", maxPeerConns)
			raw.Close()
			continue
		}
		pm.conns++
		pm.mu.Unlock()

		go pm.handleIncoming(&countedConn{Conn: raw, release: pm.releaseConn})
	}
}

// Bind retry schedule for Listen.
const (
	bindRetryInitial = time.Second
	bindRetryMax     = 30 * time.Second
)

// bind opens the listening socket, retrying until it succeeds or ctx ends (in
// which case it returns nil).
func (pm *peerManager) bind(ctx context.Context, port int) net.Listener {
	delay := bindRetryInitial
	for {
		ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
		if err == nil {
			pm.setListenState(true, "")
			pm.log.Info("peer listener started", "component", "peermgr", "port", port)
			return ln
		}
		pm.setListenState(false, err.Error())
		pm.log.Error("peer listener cannot bind, will retry", "component", "peermgr", "port", port, "err", err, "retryIn", delay)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(delay):
		}
		delay = min(delay*2, bindRetryMax)
	}
}

func (pm *peerManager) setListenState(listening bool, errMsg string) {
	pm.mu.Lock()
	pm.listening, pm.listenErr = listening, errMsg
	pm.mu.Unlock()
}

// ListenStatus reports whether the peer listener is accepting connections
// and, if not, why the last bind attempt failed.
func (pm *peerManager) ListenStatus() (listening bool, lastErr string) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	return pm.listening, pm.listenErr
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
func (pm *peerManager) releaseConn() {
	pm.mu.Lock()
	if pm.conns > 0 {
		pm.conns--
	}
	pm.mu.Unlock()
}

// handleIncoming reads the init message from an incoming connection.
func (pm *peerManager) handleIncoming(raw net.Conn) {
	conn := protocol.WrapPeerConn(raw)

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

	if initCode == protocol.InitPierceFirewall {
		token, err := protocol.ParsePierceFirewall(msg)
		if err != nil {
			pm.log.Warn("PierceFirewall parse error", "component", "peermgr", "err", err)
			conn.Close()
			return
		}

		// Deliver while holding the lock so a waiter that already gave up
		// cannot leave this connection parked unread in its channel.
		pm.mu.Lock()
		ch, ok := pm.pendingTokens[token]
		if ok {
			select {
			case ch <- connectResult{pc: &peerConn{Conn: conn, Token: token}}:
			default:
				ok = false
			}
		}
		pm.mu.Unlock()

		if ok {
			pm.log.Debug("PierceFirewall matched", "component", "peermgr", "token", token)
		} else {
			pm.log.Debug("PierceFirewall unmatched", "component", "peermgr", "token", token)
			conn.Close()
		}
		return
	}

	if initCode == protocol.InitPeerInit {
		init, err := protocol.ParsePeerInit(msg)
		if err != nil {
			conn.Close()
			return
		}

		pc := &peerConn{
			Conn:     conn,
			Username: init.Username,
			Type:     init.Type,
			Token:    init.Token,
		}

		switch init.Type {
		case protocol.ConnTypePeer:
			go pm.routePeerMessages(pc)
		case protocol.ConnTypeTransfer:
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

// ConnectToPeer establishes a peer connection by dialing the peer and, at the
// same time, asking the server to make the peer connect back to us; the first
// connection to succeed wins and the other is closed. It fails fast with
// ErrPeerOffline if the server has no address for the peer.
//
// Connections of type "P" are owned by the PeerManager from the moment they
// are returned: every message the peer sends on them is dispatched to the
// handlers registered with OnPeerMessage, exactly as on connections the peer
// opened to us. Callers must only write to a P connection (and may Close it
// when done) and must not read from it; replies arrive at the handlers. "F"
// connections carry raw file data and stay owned by the caller.
func (pm *peerManager) ConnectToPeer(ctx context.Context, username string, connType string) (*peerConn, error) {
	return pm.ConnectToPeerWithToken(ctx, username, connType, generateToken())
}

// ConnectToPeerWithToken is like ConnectToPeer but uses the specified token
// instead of generating a random one. Used for F-connections where the token
// must match the transfer token.
func (pm *peerManager) ConnectToPeerWithToken(ctx context.Context, username string, connType string, token uint32) (*peerConn, error) {
	pm.log.Debug("getting peer address", "component", "peermgr", "user", username, "type", connType, "token", token)

	addr, err := pm.getPeerAddress(ctx, username)
	if err != nil {
		return nil, err
	}
	if !isValidAddr(addr) {
		return nil, fmt.Errorf("connect to %s: %w", username, ErrPeerOffline)
	}

	pc, err := pm.connectRacing(ctx, addr, username, connType, token)
	if err != nil {
		return nil, err
	}
	if connType == protocol.ConnTypePeer {
		go pm.routePeerMessages(pc)
	}
	return pc, nil
}

// connectRacing runs the direct dial and the indirect (server-relayed)
// request concurrently and returns the first connection that is established.
// The token is registered before either leg starts so a PierceFirewall that
// arrives early cannot be missed. The losing leg is cancelled and any
// connection it produced is closed.
func (pm *peerManager) connectRacing(ctx context.Context, addr *protocol.PeerAddressResponse, username, connType string, token uint32) (*peerConn, error) {
	indirect := pm.registerToken(token)
	defer pm.unregisterToken(token, indirect)

	directCtx, cancelDirect := context.WithTimeout(ctx, directConnectTimeout)
	defer cancelDirect()
	directDone := make(chan connectResult, 1)
	go func() {
		pc, err := pm.connectDirect(directCtx, addr, username, connType, token)
		directDone <- connectResult{pc: pc, err: err}
	}()
	direct := directDone // nil once consumed
	// Whatever the direct leg produces after we return is closed.
	defer func() {
		if direct == nil {
			return
		}
		cancelDirect()
		go func() {
			if res := <-directDone; res.pc != nil {
				res.pc.Conn.Close()
			}
		}()
	}()

	var directErr, indirectErr error
	if err := pm.client.SendConnectToPeer(token, username, connType); err != nil {
		indirectErr = fmt.Errorf("send connect to peer: %w", err)
		indirect = nil
	}
	timeout := time.NewTimer(indirectConnectTimeout)
	defer timeout.Stop()

	for direct != nil || indirect != nil {
		select {
		case res := <-direct:
			direct = nil
			if res.err == nil {
				return res.pc, nil
			}
			directErr = res.err
		case res := <-indirect:
			indirect = nil
			if res.err == nil {
				res.pc.Username = username
				res.pc.Type = connType
				return res.pc, nil
			}
			indirectErr = res.err
		case <-timeout.C:
			indirect = nil
			indirectErr = fmt.Errorf("timeout waiting for indirect connection from %s", username)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, fmt.Errorf("connect to %s: %w", username, errors.Join(directErr, indirectErr))
}

// registerToken registers a pending indirect connection and returns the
// channel its PierceFirewall (or a CannotConnect error) will be delivered on.
func (pm *peerManager) registerToken(token uint32) chan connectResult {
	ch := make(chan connectResult, 1)
	pm.mu.Lock()
	pm.pendingTokens[token] = ch
	pm.mu.Unlock()
	return ch
}

// unregisterToken removes a pending token. A connection that arrived after
// the waiter gave up is parked in ch; it is closed so its socket and
// goroutine are not leaked.
func (pm *peerManager) unregisterToken(token uint32, ch chan connectResult) {
	pm.mu.Lock()
	delete(pm.pendingTokens, token)
	var orphan *peerConn
	select {
	case res := <-ch:
		orphan = res.pc
	default:
	}
	pm.mu.Unlock()
	if orphan != nil {
		orphan.Conn.Close()
	}
}

func (pm *peerManager) dialContext(ctx context.Context, addr string) (net.Conn, error) {
	if pm.dial != nil {
		return pm.dial(ctx, addr)
	}
	var d net.Dialer
	return d.DialContext(ctx, "tcp", addr)
}

// connectDirect dials the peer directly and sends PeerInit.
func (pm *peerManager) connectDirect(ctx context.Context, addr *protocol.PeerAddressResponse, username, connType string, token uint32) (*peerConn, error) {
	target := fmt.Sprintf("%s:%d", addr.IP.String(), addr.Port)

	raw, err := pm.dialContext(ctx, target)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", target, err)
	}

	conn := protocol.WrapPeerConn(raw)

	// Send PeerInit.
	initMsg := protocol.BuildPeerInit(pm.username, connType, token)
	if err := conn.WriteMessage(initMsg); err != nil {
		conn.Close()
		return nil, fmt.Errorf("send peer init: %w", err)
	}

	return &peerConn{
		Conn:     conn,
		Username: username,
		Type:     connType,
		Token:    token,
	}, nil
}

// handleConnectToPeer handles the server telling us that a peer wants us to
// connect to them (reverse/indirect connection). We dial the peer and send
// PierceFirewall with their token, then dispatch based on connection type.
func (pm *peerManager) handleConnectToPeer(resp *protocol.ConnectToPeerResponse) {
	target := fmt.Sprintf("%s:%d", resp.IP.String(), resp.Port)
	pm.log.Debug("ConnectToPeer request", "component", "peermgr", "user", resp.Username, "type", resp.Type, "addr", target, "token", resp.Token)

	if resp.IP.IsUnspecified() || resp.Port == 0 {
		pm.log.Debug("cannot honour ConnectToPeer, invalid address", "component", "peermgr", "user", resp.Username, "addr", target)
		pm.reportCantConnect(resp)
		return
	}

	var d net.Dialer
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	raw, err := d.DialContext(ctx, "tcp", target)
	if err != nil {
		pm.log.Debug("failed to connect to peer", "component", "peermgr", "user", resp.Username, "addr", target, "err", err)
		pm.reportCantConnect(resp)
		return
	}

	conn := protocol.WrapPeerConn(raw)

	// Send PierceFirewall with the token from the server message.
	pfMsg := protocol.BuildPierceFirewall(resp.Token)
	if err := conn.WriteMessage(pfMsg); err != nil {
		conn.Close()
		pm.log.Debug("failed to send PierceFirewall", "component", "peermgr", "user", resp.Username, "err", err)
		pm.reportCantConnect(resp)
		return
	}

	pc := &peerConn{
		Conn:     conn,
		Username: resp.Username,
		Type:     resp.Type,
		Token:    resp.Token,
	}

	switch resp.Type {
	case protocol.ConnTypePeer:
		go pm.routePeerMessages(pc)
	case protocol.ConnTypeTransfer:
		if pm.onFileConn != nil {
			go pm.onFileConn(pc)
		} else {
			conn.Close()
		}
	default:
		conn.Close()
	}
}

// reportCantConnect tells the server an indirect connection it asked us to
// make could not be made, so the peer that wanted it fails fast.
func (pm *peerManager) reportCantConnect(resp *protocol.ConnectToPeerResponse) {
	if err := pm.client.SendCantConnectToPeer(resp.Token, resp.Username); err != nil {
		pm.log.Debug("failed to send CantConnectToPeer", "component", "peermgr", "user", resp.Username, "err", err)
	}
}

// routePeerMessages reads messages from a P-type connection and dispatches
// them to registered handlers by message code. The connection is closed when
// the peer disconnects or an error occurs.
func (pm *peerManager) routePeerMessages(pc *peerConn) {
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
			handler(&peerMessage{
				PeerConn: pc,
				Code:     code,
				Raw:      msg,
			})
		} else {
			pm.log.Debug("unhandled peer message", "component", "peermgr", "code", code, "user", pc.Username, "bytes", len(msg))
		}
	}
}

// generateToken returns a random uint32 token.
func generateToken() uint32 {
	return rand.Uint32()
}

// isValidAddr returns true if the peer address is usable for a direct connection.
// The Soulseek server returns 0.0.0.0:0 for offline or unreachable peers.
func isValidAddr(addr *protocol.PeerAddressResponse) bool {
	return addr.IP != nil && !addr.IP.IsUnspecified() && addr.Port != 0
}

// getPeerAddress requests a peer's address from the server and waits for the response.
// Deduplicates concurrent requests for the same username. Cleans up on timeout/cancel.
func (pm *peerManager) getPeerAddress(ctx context.Context, username string) (*protocol.PeerAddressResponse, error) {
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
func (pm *peerManager) failAddrWaiters(username string, err error) {
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
func (pm *peerManager) reissueAddrRequest(username string) {
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
func (pm *peerManager) removeAddrWaiter(username string, ch chan addrResult) {
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
