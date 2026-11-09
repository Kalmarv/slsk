package slsk

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/kalmarv/slsk/protocol"
)

// distribManager manages the distributed search network parent connection.
// It connects to candidate parents from NetInfo (code 102), adopts the first
// one that sends BranchLevel + BranchRoot + a search request, and forwards
// all received searches to a callback.
type distribManager struct {
	log      *slog.Logger
	username string
	onSearch func(*protocol.IncomingSearchRequest)

	mu         sync.Mutex
	parentConn *protocol.Conn
	parentUser string
	branchLvl  uint32
	branchRoot string
	hasParent  bool
	closed     bool

	// OnChange is called after the distributed state changes: a parent was
	// adopted or lost, or the parent reported a new branch level or root. It
	// carries no state; the callee reads State(), so concurrent changes
	// cannot be applied out of order. It is invoked with no locks held.
	OnChange func()
}

// distribState is a consistent snapshot of what the server must be told about
// our place in the distributed tree.
type distribState struct {
	HasParent   bool
	BranchLevel uint32 // 0 without a parent
	BranchRoot  string // our own username without a parent
}

// State returns the current distributed state.
func (dm *distribManager) State() distribState {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	if !dm.hasParent {
		return distribState{BranchRoot: dm.username}
	}
	return distribState{HasParent: true, BranchLevel: dm.branchLvl, BranchRoot: dm.branchRoot}
}

func (dm *distribManager) changed() {
	if dm.OnChange != nil {
		dm.OnChange()
	}
}

// newDistribManager creates a new DistribManager.
func newDistribManager(username string, log *slog.Logger, onSearch func(*protocol.IncomingSearchRequest)) *distribManager {
	return &distribManager{
		log:      log,
		username: username,
		onSearch: onSearch,
	}
}

// HandleNetInfo processes a list of potential parents from the server (code 102).
// If we already have a parent, the list is ignored. Otherwise, we try each
// candidate in order and adopt the first one that completes the handshake.
func (dm *distribManager) HandleNetInfo(parents []protocol.PotentialParent) {
	dm.mu.Lock()
	if dm.hasParent || dm.closed {
		dm.mu.Unlock()
		return
	}
	dm.mu.Unlock()

	go dm.tryParents(parents)
}

// tryParents attempts to connect to each candidate parent in order.
func (dm *distribManager) tryParents(parents []protocol.PotentialParent) {
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
func (dm *distribManager) tryOneParent(p protocol.PotentialParent) bool {
	target := fmt.Sprintf("%s:%d", p.IP.String(), p.Port)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var d net.Dialer
	raw, err := d.DialContext(ctx, "tcp", target)
	if err != nil {
		dm.log.Debug("failed to connect to parent", "component", "distrib", "user", p.Username, "addr", target, "err", err)
		return false
	}

	conn := protocol.WrapPeerConn(raw)

	// Send PeerInit with type "D" and token 0.
	initMsg := protocol.BuildPeerInit(dm.username, protocol.ConnTypeDistributed, 0)
	if err := conn.WriteMessage(initMsg); err != nil {
		conn.Close()
		dm.log.Debug("failed to send PeerInit", "component", "distrib", "user", p.Username, "err", err)
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
		case protocol.DistribBranchLevel:
			level, err = protocol.ParseDistribBranchLevel(msg)
			if err != nil {
				dm.log.Warn("parse branch level", "component", "distrib", "user", p.Username, "err", err)
				conn.Close()
				return false
			}
			gotLevel = true

		case protocol.DistribBranchRoot:
			root, err = protocol.ParseDistribBranchRoot(msg)
			if err != nil {
				dm.log.Warn("parse branch root", "component", "distrib", "user", p.Username, "err", err)
				conn.Close()
				return false
			}
			gotRoot = true

		case protocol.DistribSearchRequest:
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

			dm.log.Info("adopted parent", "component", "distrib", "user", p.Username, "level", level, "root", root)

			dm.changed()

			// Deliver the first search that triggered adoption.
			search, err := protocol.ParseDistribSearchRequest(msg)
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
func (dm *distribManager) readLoop(conn *protocol.Conn, parentUsername string) {
	defer func() {
		conn.Close()
		dm.mu.Lock()
		dm.parentConn = nil
		dm.parentUser = ""
		dm.hasParent = false
		dm.branchLvl = 0
		dm.branchRoot = ""
		dm.mu.Unlock()

		dm.log.Info("parent disconnected", "component", "distrib", "user", parentUsername)

		dm.changed()
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
		case protocol.DistribSearchRequest:
			search, err := protocol.ParseDistribSearchRequest(msg)
			if err != nil {
				dm.log.Warn("parse search", "component", "distrib", "user", parentUsername, "err", err)
				continue
			}
			if dm.onSearch != nil {
				dm.onSearch(search)
			}

		case protocol.DistribBranchLevel:
			level, err := protocol.ParseDistribBranchLevel(msg)
			if err == nil {
				dm.mu.Lock()
				updated := dm.branchLvl != level+1
				dm.branchLvl = level + 1
				dm.mu.Unlock()
				if updated {
					dm.changed()
				}
			}

		case protocol.DistribBranchRoot:
			root, err := protocol.ParseDistribBranchRoot(msg)
			if err == nil {
				dm.mu.Lock()
				updated := dm.branchRoot != root
				dm.branchRoot = root
				dm.mu.Unlock()
				if updated {
					dm.changed()
				}
			}
		}
	}
}

// HasParent returns whether we currently have an adopted distributed parent.
func (dm *distribManager) HasParent() bool {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	return dm.hasParent
}

// ParentUsername returns the username of the current parent, or "" if none.
func (dm *distribManager) ParentUsername() string {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	return dm.parentUser
}

// BranchLevel returns our current branch level (parent's level + 1).
func (dm *distribManager) BranchLevel() uint32 {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	return dm.branchLvl
}

// BranchRoot returns the root username of our branch.
func (dm *distribManager) BranchRoot() string {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	return dm.branchRoot
}

// Close shuts down the distributed manager and disconnects from any parent.
func (dm *distribManager) Close() {
	dm.mu.Lock()
	dm.closed = true
	conn := dm.parentConn
	dm.mu.Unlock()

	if conn != nil {
		conn.Close()
	}
}
