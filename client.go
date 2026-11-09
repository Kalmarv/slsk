package slsk

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kalmarv/slsk/protocol"
)

// DefaultServerAddr is the address of the official Soulseek server.
const DefaultServerAddr = "server.slsknet.org:2242"

// DefaultListenPort is the port other users connect to when Config.ListenPort
// is zero. Forward it on your router: peers that cannot accept connections
// themselves can only reach you through it.
const DefaultListenPort = 2235

// Config configures a Client. Username and Password are required; every other
// field has a usable zero value.
type Config struct {
	Username string
	Password string

	// ServerAddr is the Soulseek server to log in to; DefaultServerAddr if empty.
	ServerAddr string
	// ListenPort is the TCP port peers connect to; DefaultListenPort if zero.
	ListenPort int

	// Logger receives the client's logs; slog.Default() if nil.
	Logger *slog.Logger

	// Shares answers other users' searches. Without it the client shares
	// nothing: it reports no files to the server and does not join the
	// distributed search network.
	Shares ShareIndex
	// Uploads decides what happens when a user asks to download one of our
	// files. Without it every request is denied as not shared.
	Uploads UploadQueue
	// UploadSpeed is the upload speed in bytes per second advertised in our
	// search results. Clients rank results by it, so a zero value puts ours
	// last.
	UploadSpeed uint32

	// SearchTimeouts bounds outgoing searches; zero fields take the defaults
	// described on SearchTimeouts.
	SearchTimeouts SearchTimeouts

	// OnLogin, if set, is called after every successful login, once the
	// client has re-sent its share counts and watched users. A login follows
	// every reconnect, so this is where an application resumes work that
	// needs the server.
	OnLogin func()
	// OnUserStatus, if set, is called whenever the server reports a user's
	// status: in answer to WatchUser, and each time a watched user's status
	// changes. It runs on the server connection's read loop and must not
	// block.
	OnUserStatus func(username string, status UserStatus)
}

// UserStatus is a user's presence on the network.
type UserStatus uint32

// User statuses, as the server reports them.
const (
	StatusOffline UserStatus = 0
	StatusAway    UserStatus = 1
	StatusOnline  UserStatus = 2
)

func (s UserStatus) String() string {
	switch s {
	case StatusOffline:
		return "offline"
	case StatusAway:
		return "away"
	case StatusOnline:
		return "online"
	}
	return fmt.Sprintf("status(%d)", uint32(s))
}

// Status describes the client's connection to the network.
type Status struct {
	// LoggedIn is true while the server session is up.
	LoggedIn bool
	// Listening is true once the peer listener accepts connections.
	Listening bool
	// ListenError is why the listener could not bind, if it can't.
	ListenError string
	// LastError is why the last login attempt or session failed.
	LastError string
	// RetryAt is when the next login attempt happens; zero unless waiting.
	RetryAt time.Time
	// Relogged is true when another client logged in to this account and
	// took over the session. The client waits before logging in again, so
	// the two do not keep knocking each other off.
	Relogged bool
}

// Client is a Soulseek client: it keeps a session with the server, connects to
// peers, searches, downloads and, given a ShareIndex and an UploadQueue, shares
// files. Create one with New and start it with Run; every other method is safe
// for concurrent use once Run has started.
type Client struct {
	cfg Config
	log *slog.Logger

	server    *serverConn
	peers     *peerManager
	searches  *searches
	downloads *downloads
	uploads   *uploads
	sharing   *sharing

	running atomic.Bool

	stateMu sync.Mutex
	state   Status // LoggedIn, LastError, RetryAt, Relogged; listener fields are live

	watchMu sync.Mutex
	watched map[string]struct{}
}

// New returns a Client for cfg. It does not connect: call Run.
func New(cfg Config) (*Client, error) {
	if cfg.Username == "" {
		return nil, errors.New("slsk: Username is required")
	}
	if cfg.Password == "" {
		return nil, errors.New("slsk: Password is required")
	}
	if cfg.ServerAddr == "" {
		cfg.ServerAddr = DefaultServerAddr
	}
	if cfg.ListenPort == 0 {
		cfg.ListenPort = DefaultListenPort
	}
	if cfg.ListenPort < 1 || cfg.ListenPort > 65535 {
		return nil, fmt.Errorf("slsk: ListenPort %d out of range", cfg.ListenPort)
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	cfg.SearchTimeouts = cfg.SearchTimeouts.withDefaults()

	server, err := newServerConn(serverConfig{
		ServerAddr: cfg.ServerAddr,
		Username:   cfg.Username,
		Password:   cfg.Password,
		ListenPort: uint32(cfg.ListenPort),
		Logger:     cfg.Logger,
	})
	if err != nil {
		return nil, err
	}

	c := &Client{
		cfg:     cfg,
		log:     cfg.Logger,
		server:  server,
		watched: make(map[string]struct{}),
	}
	c.peers = newPeerManager(cfg.Username, server)
	c.searches = newSearches(c.log, server, c.peers, cfg.SearchTimeouts)
	c.downloads = newDownloads(c.log, c.peers)
	c.uploads = newUploads(c.log, c.peers)
	c.sharing = newSharing(c.log, cfg, c.peers)

	server.OnUserStatus(func(resp *protocol.UserStatusResponse) {
		if c.cfg.OnUserStatus != nil {
			c.cfg.OnUserStatus(resp.Username, UserStatus(resp.Status))
		}
	})
	return c, nil
}

// Run connects to the network and keeps the client online until ctx is
// cancelled: it listens for peers, logs in, and logs in again with backoff
// whenever a login fails or the server drops the session. It returns nil
// after ctx is cancelled and the connections are closed. Run may be called
// only once.
func (c *Client) Run(ctx context.Context) error {
	if !c.running.CompareAndSwap(false, true) {
		return errors.New("slsk: Run called twice")
	}
	if c.cfg.Shares != nil {
		// Searches arrive on the server connection and the distributed
		// parent connection; both hand them to a bounded worker pool so a
		// slow share search never stalls their read loops.
		workers := newSearchWorkers(ctx, searchWorkerCount, searchQueueDepth, func(req *protocol.IncomingSearchRequest) {
			c.sharing.answer(ctx, req)
		})
		c.server.OnSearchRequest(workers.Submit)
		distrib := newDistribManager(c.cfg.Username, c.log, workers.Submit)
		c.server.SetDistribManager(distrib)
		defer distrib.Close()
		go c.sharing.logStats(ctx, workers)
	}

	go c.peers.Listen(ctx, c.cfg.ListenPort)
	supervise(ctx, c.log, c.server, c.afterLogin, c.setState, defaultSupervisorTiming)
	c.server.Close()
	return nil
}

// afterLogin re-establishes what the server forgets between sessions.
func (c *Client) afterLogin() {
	if err := c.reportShares(); err != nil {
		c.log.Error("report shares", "component", "slsk", "err", err)
	}
	c.watchMu.Lock()
	users := make([]string, 0, len(c.watched))
	for u := range c.watched {
		users = append(users, u)
	}
	c.watchMu.Unlock()
	for _, u := range users {
		if err := c.server.WatchUser(u); err != nil {
			c.log.Error("watch user", "component", "slsk", "user", u, "err", err)
		}
	}
	if c.cfg.OnLogin != nil {
		c.cfg.OnLogin()
	}
}

// Status reports the state of the server session and the peer listener.
func (c *Client) Status() Status {
	c.stateMu.Lock()
	st := c.state
	c.stateMu.Unlock()
	st.Listening, st.ListenError = c.peers.ListenStatus()
	return st
}

func (c *Client) setState(f func(*Status)) {
	c.stateMu.Lock()
	f(&c.state)
	c.stateMu.Unlock()
}

// LoggedIn reports whether the server session is up.
func (c *Client) LoggedIn() bool { return c.server.LoggedIn() }

// Username returns the account the client logs in as.
func (c *Client) Username() string { return c.cfg.Username }

// WatchUser subscribes to a user's status: Config.OnUserStatus is called with
// it now and whenever it changes. Subscriptions are renewed after every
// login, so a user only needs to be watched once.
func (c *Client) WatchUser(username string) error {
	c.watchMu.Lock()
	c.watched[username] = struct{}{}
	c.watchMu.Unlock()
	if !c.server.LoggedIn() {
		return nil // sent after the next login
	}
	return c.server.WatchUser(username)
}

// SharesChanged reports the current share counts (Config.Shares.Counts) to
// the server; call it after the shared files change. The server routes
// searches only to users who share files. It does nothing while logged out:
// the counts are sent at every login.
func (c *Client) SharesChanged() error {
	if !c.server.LoggedIn() {
		return nil
	}
	return c.reportShares()
}

func (c *Client) reportShares() error {
	var folders, files int
	if c.cfg.Shares != nil {
		folders, files = c.cfg.Shares.Counts()
	}
	if err := c.server.SendSharedFoldersFiles(uint32(folders), uint32(files)); err != nil {
		return err
	}
	c.log.Info("reported shares to server", "component", "slsk", "folders", folders, "files", files)
	return nil
}

// ExcludedSearchPhrases returns the phrases the server silently drops
// searches for, as it sent them at the last login. Search rewrites queries
// around them.
func (c *Client) ExcludedSearchPhrases() []string {
	return c.server.ExcludedSearchPhrases()
}
