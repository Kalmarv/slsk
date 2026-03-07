package slsk

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
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
	loggedIn atomic.Bool
	mu       sync.Mutex // protects conn field

	// Event callbacks
	onSearchRequest func(*IncomingSearchRequest)
	onConnectToPeer func(*ConnectToPeerResponse)
	onPeerAddress   func(*PeerAddressResponse)
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
	c.mu.Unlock()

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

	// Send SetListenPort.
	portMsg := BuildSetListenPort(c.cfg.ListenPort)
	if err := conn.WriteMessage(portMsg); err != nil {
		c.loggedIn.Store(false)
		conn.Close()
		return fmt.Errorf("send SetListenPort: %w", err)
	}

	// Start background message loop.
	go c.messageLoop()

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

// Search sends a file search request to the server.
func (c *Client) Search(token uint32, query string) error {
	if !c.loggedIn.Load() {
		return errors.New("not logged in")
	}
	msg := BuildFileSearchRequest(token, query)
	return c.conn.WriteMessage(msg)
}

// GetPeerAddress sends a GetPeerAddress request to the server.
func (c *Client) GetPeerAddress(username string) error {
	if !c.loggedIn.Load() {
		return errors.New("not logged in")
	}
	msg := BuildGetPeerAddressRequest(username)
	return c.conn.WriteMessage(msg)
}

// SendConnectToPeer sends a ConnectToPeer request to the server.
func (c *Client) SendConnectToPeer(token uint32, username, connType string) error {
	if !c.loggedIn.Load() {
		return errors.New("not logged in")
	}
	msg := BuildConnectToPeerRequest(token, username, connType)
	return c.conn.WriteMessage(msg)
}

// SendSharedFoldersFiles reports share counts to the server.
func (c *Client) SendSharedFoldersFiles(dirs, files uint32) error {
	if !c.loggedIn.Load() {
		return errors.New("not logged in")
	}
	msg := BuildSharedFoldersFiles(dirs, files)
	return c.conn.WriteMessage(msg)
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

// messageLoop reads messages from the server in a loop and dispatches
// them by code. It runs in a goroutine and exits on read error.
func (c *Client) messageLoop() {
	for {
		msg, err := c.conn.ReadMessage()
		if err != nil {
			// Connection closed or error — exit the loop.
			c.loggedIn.Store(false)
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
			if err := c.conn.WriteMessage(pong); err != nil {
				log.Printf("slsk: failed to send pong: %v", err)
			}

		case ServerEmbeddedMessage:
			// Incoming search request (code 93).
			req, err := ParseServerSearchRequest(msg)
			if err != nil {
				log.Printf("slsk: failed to parse search request: %v", err)
				continue
			}
			if c.onSearchRequest != nil {
				c.onSearchRequest(req)
			}

		case ServerConnectToPeer:
			resp, err := ParseConnectToPeerResponse(msg)
			if err != nil {
				log.Printf("slsk: failed to parse connect to peer: %v", err)
				continue
			}
			if c.onConnectToPeer != nil {
				c.onConnectToPeer(resp)
			}

		case ServerGetPeerAddress:
			resp, err := ParseGetPeerAddressResponse(msg)
			if err != nil {
				log.Printf("slsk: failed to parse peer address: %v", err)
				continue
			}
			if c.onPeerAddress != nil {
				c.onPeerAddress(resp)
			}

		case ServerCannotConnect:
			// Log and ignore for now.

		default:
			// Unknown code — skip silently.
		}
	}
}
