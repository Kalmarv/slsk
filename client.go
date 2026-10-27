package slsk

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// ClientConfig holds the configuration for connecting to the Soulseek server.
type ClientConfig struct {
	ServerAddr   string // e.g. "server.slsknet.org:2242"
	Username     string
	Password     string
	ListenPort   uint32 // default 2235
	MajorVersion uint32 // default 170
	MinorVersion uint32 // default 1
}

// Client manages a connection to the Soulseek server, handling login,
// message dispatch, and outgoing requests.
type Client struct {
	cfg      ClientConfig
	conn     *Conn
	done     chan struct{} // closed when the current connection's message loop exits
	loggedIn atomic.Bool
	mu       sync.Mutex // protects conn, done and lost
	lost     error      // why the last connection ended; nil while connected

	// Event callbacks
	onSearchRequest func(*IncomingSearchRequest)
	onConnectToPeer func(*ConnectToPeerResponse)
	onPeerAddress   func(*PeerAddressResponse)
	onUserStatus    func(*UserStatusResponse)
	onCannotConnect func(token uint32)

	distribMgr *DistribManager
	distribMu  sync.Mutex // serializes distributed-state updates to the server

	excludedPhrases atomic.Pointer[[]string] // from the server's ExcludedSearchPhrases
}

// NewClient creates a new Client with the given configuration, applying
// defaults for zero-value fields.
func NewClient(cfg ClientConfig) (*Client, error) {
	if cfg.ServerAddr == "" {
		return nil, errors.New("ServerAddr is required")
	}
	if cfg.Username == "" {
		return nil, errors.New("Username is required")
	}
	if cfg.Password == "" {
		return nil, errors.New("Password is required")
	}

	if cfg.ListenPort == 0 {
		cfg.ListenPort = 2235
	}
	if cfg.MajorVersion == 0 {
		cfg.MajorVersion = 170
	}
	if cfg.MinorVersion == 0 {
		cfg.MinorVersion = 1
	}

	return &Client{cfg: cfg}, nil
}

var errNotLoggedIn = errors.New("not logged in")

// ErrRelogged is the reason a connection ended when the server reported
// (code 41) that another client logged in to the same account. Reconnecting
// at once would just kick that client off in turn.
var ErrRelogged = errors.New("logged in elsewhere (server sent Relogged)")

// loginTimeout bounds the TCP dial plus login handshake.
const loginTimeout = 30 * time.Second

// write sends a framed message to the server when logged in.
// It is safe for concurrent use and during a reconnect.
func (c *Client) write(msg []byte) error {
	if !c.loggedIn.Load() {
		return errNotLoggedIn
	}
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()
	if conn == nil {
		return errNotLoggedIn
	}
	return conn.WriteMessage(msg)
}

// Done returns a channel closed when the current server connection's message
// loop exits (connection lost or Close called). A fresh channel is created by
// each successful Connect; it is nil before the first Connect.
func (c *Client) Done() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.done
}

// DisconnectReason returns why the most recent connection ended: ErrRelogged,
// or the read error that dropped it. It is nil while connected and before the
// first connection.
func (c *Client) DisconnectReason() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lost
}

// setLost records why the connection ended, keeping the first reason.
func (c *Client) setLost(err error) {
	c.mu.Lock()
	if c.lost == nil {
		c.lost = err
	}
	c.mu.Unlock()
}

// Connect dials the Soulseek server, performs login, and on success sends
// SetListenPort and starts the background message loop.
func (c *Client) Connect(ctx context.Context) error {
	// Dial TCP with context deadline.
	var d net.Dialer
	raw, err := d.DialContext(ctx, "tcp", c.cfg.ServerAddr)
	if err != nil {
		return fmt.Errorf("dial %s: %w", c.cfg.ServerAddr, err)
	}

	conn := WrapConn(raw)
	c.mu.Lock()
	c.conn = conn
	c.lost = nil
	c.mu.Unlock()

	// Bound the login handshake so a half-open connection cannot block the
	// caller forever. Cleared once the connection is fully established.
	if err := raw.SetDeadline(time.Now().Add(loginTimeout)); err != nil {
		conn.Close()
		return fmt.Errorf("set login deadline: %w", err)
	}

	// Send login request.
	loginMsg := BuildLoginRequest(c.cfg.Username, c.cfg.Password, c.cfg.MajorVersion, c.cfg.MinorVersion)
	if err := conn.WriteMessage(loginMsg); err != nil {
		conn.Close()
		return fmt.Errorf("send login: %w", err)
	}

	// Read login response.
	respMsg, err := conn.ReadMessage()
	if err != nil {
		conn.Close()
		return fmt.Errorf("read login response: %w", err)
	}

	resp, err := ParseLoginResponse(respMsg)
	if err != nil {
		conn.Close()
		return fmt.Errorf("parse login response: %w", err)
	}

	if !resp.Success {
		conn.Close()
		return fmt.Errorf("login failed: %s", resp.Reason)
	}

	c.loggedIn.Store(true)

	// Post-login initialization: matches soul/Nicotine+ sequence.
	write := func(msg []byte) func() error { return func() error { return conn.WriteMessage(msg) } }
	initMsgs := []struct {
		name string
		send func() error
	}{
		{"CheckPrivileges", write(BuildCheckPrivileges())},
		{"SetListenPort", write(BuildSetListenPort(c.cfg.ListenPort))},
		{"SetStatus", write(BuildSetStatus(2))},
		// The distributed manager keeps its parent across server reconnects,
		// so report its actual state rather than "no parent".
		{"DistribState", c.syncDistrib},
		// We do not implement the child side of the distributed tree, so do
		// not advertise that we accept children (the server would otherwise
		// assign us peers we immediately reject).
		{"AcceptChildren", write(BuildAcceptChildren(false))},
	}
	for _, m := range initMsgs {
		if err := m.send(); err != nil {
			c.loggedIn.Store(false)
			conn.Close()
			return fmt.Errorf("send %s: %w", m.name, err)
		}
	}

	// Start background message loop for this connection.
	if err := raw.SetDeadline(time.Time{}); err != nil {
		c.loggedIn.Store(false)
		conn.Close()
		return fmt.Errorf("clear login deadline: %w", err)
	}
	done := make(chan struct{})
	c.mu.Lock()
	c.done = done
	c.mu.Unlock()
	go c.messageLoop(conn, done)

	return nil
}

// Close closes the connection and marks the client as logged out.
func (c *Client) Close() error {
	c.loggedIn.Store(false)
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()

	if conn != nil {
		return conn.Close()
	}
	return nil
}

// LoggedIn returns whether the client is currently logged in.
func (c *Client) LoggedIn() bool {
	return c.loggedIn.Load()
}

// ListenPort returns the configured listen port for incoming peer connections.
func (c *Client) ListenPort() uint32 {
	return c.cfg.ListenPort
}

// Search sends a file search request to the server.
func (c *Client) Search(token uint32, query string) error {
	return c.write(BuildFileSearchRequest(token, query))
}

// GetPeerAddress sends a GetPeerAddress request to the server.
func (c *Client) GetPeerAddress(username string) error {
	return c.write(BuildGetPeerAddressRequest(username))
}

// SendConnectToPeer sends a ConnectToPeer request to the server.
func (c *Client) SendConnectToPeer(token uint32, username, connType string) error {
	return c.write(BuildConnectToPeerRequest(token, username, connType))
}

// SendCantConnectToPeer tells the server we could not connect to the peer of
// an indirect ConnectToPeer request it relayed to us.
func (c *Client) SendCantConnectToPeer(token uint32, username string) error {
	return c.write(BuildCantConnectToPeer(token, username))
}

// SendSharedFoldersFiles reports share counts to the server.
func (c *Client) SendSharedFoldersFiles(dirs, files uint32) error {
	return c.write(BuildSharedFoldersFiles(dirs, files))
}

// OnSearchRequest registers a callback for incoming search requests.
func (c *Client) OnSearchRequest(fn func(*IncomingSearchRequest)) {
	c.onSearchRequest = fn
}

// OnConnectToPeer registers a callback for ConnectToPeer notifications.
func (c *Client) OnConnectToPeer(fn func(*ConnectToPeerResponse)) {
	c.onConnectToPeer = fn
}

// OnPeerAddress registers a callback for PeerAddress responses.
func (c *Client) OnPeerAddress(fn func(*PeerAddressResponse)) {
	c.onPeerAddress = fn
}

// GetUserStatus sends a GetUserStatus request (code 7) for the given username.
func (c *Client) GetUserStatus(username string) error {
	return c.write(BuildGetUserStatusRequest(username))
}

// WatchUser sends a WatchUser request (code 5) for the given username.
func (c *Client) WatchUser(username string) error {
	return c.write(BuildWatchUserRequest(username))
}

// OnUserStatus registers a callback for GetUserStatus responses (code 7).
// This fires both for explicit GetUserStatus requests and for server-pushed
// status changes after WatchUser subscriptions.
func (c *Client) OnUserStatus(fn func(*UserStatusResponse)) {
	c.onUserStatus = fn
}

// OnCannotConnect registers a callback for CannotConnect (code 1001).
// The server sends this when a peer we asked to connect to us cannot reach us,
// carrying the token from our original ConnectToPeer request.
func (c *Client) OnCannotConnect(fn func(token uint32)) {
	c.onCannotConnect = fn
}

// SetDistribManager sets the distributed network manager. When set, the client
// will parse NetInfo (code 102) messages and pass them to the manager, and keep
// the server informed of the manager's parent, branch level and branch root
// (on login and whenever they change).
func (c *Client) SetDistribManager(dm *DistribManager) {
	c.distribMgr = dm
	dm.OnChange = func() {
		if err := c.syncDistrib(); err != nil && !errors.Is(err, errNotLoggedIn) {
			slog.Error("send distributed state", "component", "slsk", "err", err)
		}
	}
}

// syncDistrib tells the server our current place in the distributed tree
// (HaveNoParent, BranchRoot, BranchLevel). The state is read under distribMu,
// so racing updates are sent in the order their state was read and the last
// message always reflects the latest state. Errors while logged out are
// expected: Connect re-sends the state after each login.
func (c *Client) syncDistrib() error {
	c.distribMu.Lock()
	defer c.distribMu.Unlock()

	st := DistribState{BranchRoot: c.cfg.Username}
	if c.distribMgr != nil {
		st = c.distribMgr.State()
	}
	for _, m := range [][]byte{
		BuildHaveNoParent(!st.HasParent),
		BuildBranchRoot(st.BranchRoot),
		BuildBranchLevel(st.BranchLevel),
	} {
		if err := c.write(m); err != nil {
			return err
		}
	}
	slog.Debug("distrib state sent", "component", "slsk", "hasParent", st.HasParent, "level", st.BranchLevel, "root", st.BranchRoot)
	return nil
}

// ExcludedSearchPhrases returns the phrases the server drops
// searches for, as last sent by the server (nil before the first login).
func (c *Client) ExcludedSearchPhrases() []string {
	if p := c.excludedPhrases.Load(); p != nil {
		return *p
	}
	return nil
}

// messageLoop reads messages from the server in a loop and dispatches
// them by code. It runs in a goroutine, owns conn for reads, and closes done
// when the connection drops.
func (c *Client) messageLoop(conn *Conn, done chan struct{}) {
	defer close(done)
	for {
		msg, err := conn.ReadMessage()
		if err != nil {
			// Connection closed or error — exit the loop.
			slog.Warn("slsk server connection lost", "component", "slsk", "err", err)
			c.setLost(err)
			c.loggedIn.Store(false)
			_ = conn.Close()
			return
		}

		r, err := NewReader(msg)
		if err != nil {
			// Malformed message — skip.
			continue
		}

		switch r.Code() {
		case ServerPing:
			// Respond with a ping.
			pong := BuildPing()
			if err := conn.WriteMessage(pong); err != nil {
				slog.Error("failed to send pong", "component", "slsk", "err", err)
			}

		case ServerRelogged:
			slog.Warn("slsk server reports another client logged in to this account", "component", "slsk")
			c.setLost(ErrRelogged)
			c.loggedIn.Store(false)
			_ = conn.Close()
			return

		case ServerEmbeddedMessage:
			// Incoming search request (code 93).
			slog.Debug("received EmbeddedMessage", "component", "slsk", "bytes", len(msg))
			req, err := ParseServerSearchRequest(msg)
			if err != nil {
				slog.Warn("failed to parse search request", "component", "slsk", "err", err)
				continue
			}
			if c.onSearchRequest != nil {
				c.onSearchRequest(req)
			}

		case ServerConnectToPeer:
			resp, err := ParseConnectToPeerResponse(msg)
			if err != nil {
				slog.Warn("failed to parse connect to peer", "component", "slsk", "err", err)
				continue
			}
			if c.onConnectToPeer != nil {
				c.onConnectToPeer(resp)
			}

		case ServerGetPeerAddress:
			resp, err := ParseGetPeerAddressResponse(msg)
			if err != nil {
				slog.Warn("failed to parse peer address", "component", "slsk", "err", err)
				continue
			}
			if c.onPeerAddress != nil {
				c.onPeerAddress(resp)
			}

		case ServerWatchUser:
			resp, err := ParseWatchUserResponse(msg)
			if err != nil {
				slog.Warn("failed to parse watch user response", "component", "slsk", "err", err)
				continue
			}
			if resp.Exists && c.onUserStatus != nil {
				c.onUserStatus(&UserStatusResponse{
					Username: resp.Username,
					Status:   resp.Status,
				})
			}

		case ServerGetStatus:
			resp, err := ParseGetUserStatusResponse(msg)
			if err != nil {
				slog.Warn("failed to parse user status", "component", "slsk", "err", err)
				continue
			}
			if c.onUserStatus != nil {
				c.onUserStatus(resp)
			}

		case ServerCannotConnect:
			token, err := ParseCannotConnect(msg)
			if err != nil {
				slog.Warn("failed to parse CannotConnect", "component", "slsk", "err", err)
				continue
			}
			slog.Debug("CannotConnect", "component", "slsk", "token", token)
			if c.onCannotConnect != nil {
				c.onCannotConnect(token)
			}

		case ServerNetInfo:
			parents, err := ParseNetInfo(msg)
			if err != nil {
				slog.Warn("failed to parse net info", "component", "slsk", "err", err)
				continue
			}
			if c.distribMgr != nil && len(parents) > 0 {
				slog.Debug("received potential parents", "component", "slsk", "count", len(parents))
				c.distribMgr.HandleNetInfo(parents)
			}

		case ServerCheckPrivileges, ServerRoomList, ServerPrivilegedUsers,
			ServerParentMinSpeed, ServerParentSpeedRatio,
			ServerWishlistInterval:
			// Known server messages we don't need to handle.

		case ServerExcludedPhrases:
			phrases, err := ParseExcludedSearchPhrases(msg)
			if err != nil {
				slog.Warn("failed to parse excluded search phrases", "component", "slsk", "err", err)
				continue
			}
			c.excludedPhrases.Store(&phrases)
			slog.Info("received excluded search phrases", "component", "slsk", "count", len(phrases))

		default:
			slog.Debug("unhandled server message", "component", "slsk", "code", r.Code(), "bytes", len(msg))
		}
	}
}
