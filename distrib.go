package slsk

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"
)

// DistribManager manages the distributed search network parent connection.
// It connects to candidate parents from NetInfo (code 102), adopts the first
// one that sends BranchLevel + BranchRoot + a search request, and forwards
// all received searches to a callback.
type DistribManager struct {
	username string
	onSearch func(*IncomingSearchRequest)

	mu         sync.Mutex
	parentConn *Conn
	parentUser string
	branchLvl  uint32
	branchRoot string
	hasParent  bool
	closed     bool

	// OnParentChange is called when the parent state changes.
	// hasParent=true means we adopted a parent; false means we lost it.
	// The caller should send HaveNoParent(!hasParent), BranchLevel, BranchRoot to the server.
	OnParentChange func(hasParent bool, branchLevel uint32, branchRoot string)
}

// NewDistribManager creates a new DistribManager.
func NewDistribManager(username string, onSearch func(*IncomingSearchRequest)) *DistribManager {
	return &DistribManager{
		username: username,
		onSearch: onSearch,
	}
}

// HandleNetInfo processes a list of potential parents from the server (code 102).
// If we already have a parent, the list is ignored. Otherwise, we try each
// candidate in order and adopt the first one that completes the handshake.
func (dm *DistribManager) HandleNetInfo(parents []PotentialParent) {
	dm.mu.Lock()
	if dm.hasParent || dm.closed {
		dm.mu.Unlock()
		return
	}
	dm.mu.Unlock()

	go dm.tryParents(parents)
}

// tryParents attempts to connect to each candidate parent in order.
func (dm *DistribManager) tryParents(parents []PotentialParent) {
	for _, p := range parents {
		dm.mu.Lock()
		if dm.hasParent || dm.closed {
			dm.mu.Unlock()
			return
		}
		dm.mu.Unlock()

		if dm.tryOneParent(p) {
			return
		}
	}
}

// tryOneParent attempts to connect to a single candidate parent.
// Returns true if the parent was adopted.
func (dm *DistribManager) tryOneParent(p PotentialParent) bool {
	target := fmt.Sprintf("%s:%d", p.IP.String(), p.Port)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var d net.Dialer
	raw, err := d.DialContext(ctx, "tcp", target)
	if err != nil {
		slog.Debug("failed to connect to parent", "component", "distrib", "user", p.Username, "addr", target, "err", err)
		return false
	}

	conn := WrapConn(raw)

	// Send PeerInit with type "D" and token 0.
	initMsg := BuildPeerInit(dm.username, ConnTypeDistributed, 0)
	if err := conn.WriteMessage(initMsg); err != nil {
		conn.Close()
		slog.Debug("failed to send PeerInit", "component", "distrib", "user", p.Username, "err", err)
		return false
	}

	// Read messages from the parent. We need BranchLevel + BranchRoot before
	// we can consider this parent valid. The first search request confirms adoption.
	raw.SetReadDeadline(time.Now().Add(10 * time.Second))

	var gotLevel, gotRoot bool
	var level uint32
	var root string

	for {
		msg, err := conn.ReadMessage()
		if err != nil {
			conn.Close()
			return false
		}

		if len(msg) < 5 {
			continue
		}

		code := msg[4] // 1-byte init/distrib code

		switch code {
		case DistribBranchLevel:
			level, err = ParseDistribBranchLevel(msg)
			if err != nil {
				slog.Warn("parse branch level", "component", "distrib", "user", p.Username, "err", err)
				conn.Close()
				return false
			}
			gotLevel = true

		case DistribBranchRoot:
			root, err = ParseDistribBranchRoot(msg)
			if err != nil {
				slog.Warn("parse branch root", "component", "distrib", "user", p.Username, "err", err)
				conn.Close()
				return false
			}
			gotRoot = true

		case DistribSearchRequest:
			if !gotLevel || !gotRoot {
				continue
			}

			// Clear deadline and adopt.
			raw.SetReadDeadline(time.Time{})

			dm.mu.Lock()
			if dm.hasParent {
				dm.mu.Unlock()
				conn.Close()
				return false
			}
			dm.parentConn = conn
			dm.parentUser = p.Username
			dm.branchLvl = level + 1
			dm.branchRoot = root
			dm.hasParent = true
			dm.mu.Unlock()

			slog.Info("adopted parent", "component", "distrib", "user", p.Username, "level", level, "root", root)

			if dm.OnParentChange != nil {
				dm.OnParentChange(true, level+1, root)
			}

			// Deliver the first search that triggered adoption.
			search, err := ParseDistribSearchRequest(msg)
			if err == nil && dm.onSearch != nil {
				dm.onSearch(search)
			}

			go dm.readLoop(conn, p.Username)
			return true
		}
	}
}

// readLoop reads distributed messages from the parent connection
// and forwards search requests to the callback.
func (dm *DistribManager) readLoop(conn *Conn, parentUsername string) {
	defer func() {
		conn.Close()
		dm.mu.Lock()
		dm.parentConn = nil
		dm.parentUser = ""
		dm.hasParent = false
		dm.branchLvl = 0
		dm.branchRoot = ""
		dm.mu.Unlock()

		slog.Info("parent disconnected", "component", "distrib", "user", parentUsername)

		if dm.OnParentChange != nil {
			dm.OnParentChange(false, 0, dm.username)
		}
	}()

	for {
		msg, err := conn.ReadMessage()
		if err != nil {
			return
		}

		if len(msg) < 5 {
			continue
		}

		code := msg[4]

		switch code {
		case DistribSearchRequest:
			search, err := ParseDistribSearchRequest(msg)
			if err != nil {
				slog.Warn("parse search", "component", "distrib", "user", parentUsername, "err", err)
				continue
			}
			if dm.onSearch != nil {
				dm.onSearch(search)
			}

		case DistribBranchLevel:
			level, err := ParseDistribBranchLevel(msg)
			if err == nil {
				dm.mu.Lock()
				dm.branchLvl = level + 1
				dm.mu.Unlock()
			}

		case DistribBranchRoot:
			root, err := ParseDistribBranchRoot(msg)
			if err == nil {
				dm.mu.Lock()
				dm.branchRoot = root
				dm.mu.Unlock()
			}
		}
	}
}

// HasParent returns whether we currently have an adopted distributed parent.
func (dm *DistribManager) HasParent() bool {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	return dm.hasParent
}

// ParentUsername returns the username of the current parent, or "" if none.
func (dm *DistribManager) ParentUsername() string {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	return dm.parentUser
}

// BranchLevel returns our current branch level (parent's level + 1).
func (dm *DistribManager) BranchLevel() uint32 {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	return dm.branchLvl
}

// BranchRoot returns the root username of our branch.
func (dm *DistribManager) BranchRoot() string {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	return dm.branchRoot
}

// Close shuts down the distributed manager and disconnects from any parent.
func (dm *DistribManager) Close() {
	dm.mu.Lock()
	dm.closed = true
	conn := dm.parentConn
	dm.mu.Unlock()

	if conn != nil {
		conn.Close()
	}
}
