package slsk

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// startDistribParent starts a TCP listener that simulates a distributed parent.
// It accepts one connection, sends BranchLevel, BranchRoot, and a search request,
// then keeps the connection open. Returns the listener address.
func startDistribParent(t *testing.T, branchLevel uint32, branchRoot string, search *IncomingSearchRequest) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })

	go func() {
		raw, err := ln.Accept()
		if err != nil {
			return
		}
		conn := WrapConn(raw)

		// Read the PeerInit from the connecting client.
		conn.ReadMessage()

		// Send BranchLevel.
		if err := conn.WriteMessage(BuildDistribBranchLevel(branchLevel)); err != nil {
			return
		}

		// Send BranchRoot.
		if err := conn.WriteMessage(BuildDistribBranchRoot(branchRoot)); err != nil {
			return
		}

		// Send a search request.
		if search != nil {
			b := NewInitBuilder(DistribSearchRequest)
			b.WriteUint32(distribSearchIdentifier)
			b.WriteString(search.Username)
			b.WriteUint32(search.Token)
			b.WriteString(search.Query)
			if err := conn.WriteMessage(b.Build()); err != nil {
				return
			}
		}

		// Keep connection alive until test cleanup closes the listener.
		for {
			_, err := conn.ReadMessage()
			if err != nil {
				return
			}
		}
	}()

	return ln.Addr().String()
}

func TestDistribManager_ConnectsToParent(t *testing.T) {
	search := &IncomingSearchRequest{
		Username: "searcher",
		Token:    12345,
		Query:    "test query",
	}

	addr := startDistribParent(t, 0, "rootuser", search)
	host, portStr, _ := net.SplitHostPort(addr)
	port := 0
	fmt.Sscanf(portStr, "%d", &port)
	ip := net.ParseIP(host).To4()

	searchReceived := make(chan *IncomingSearchRequest, 1)
	dm := NewDistribManager("testuser", func(s *IncomingSearchRequest) {
		searchReceived <- s
	})

	parents := []PotentialParent{
		{Username: "rootuser", IP: ip, Port: uint32(port)},
	}

	dm.HandleNetInfo(parents)

	select {
	case s := <-searchReceived:
		if s.Username != "searcher" {
			t.Fatalf("search username = %q, want searcher", s.Username)
		}
		if s.Token != 12345 {
			t.Fatalf("search token = %d, want 12345", s.Token)
		}
		if s.Query != "test query" {
			t.Fatalf("search query = %q, want 'test query'", s.Query)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for search from distributed parent")
	}

	// Should have adopted parent.
	if dm.ParentUsername() != "rootuser" {
		t.Fatalf("parent = %q, want rootuser", dm.ParentUsername())
	}
	if dm.BranchLevel() != 1 { // parent is level 0, we are level 1
		t.Fatalf("branch level = %d, want 1", dm.BranchLevel())
	}
	if dm.BranchRoot() != "rootuser" {
		t.Fatalf("branch root = %q, want rootuser", dm.BranchRoot())
	}

	dm.Close()
}

func TestDistribManager_AdoptsFirstValidParent(t *testing.T) {
	// First parent: unreachable.
	// Second parent: valid.
	search := &IncomingSearchRequest{
		Username: "searcher",
		Token:    42,
		Query:    "music",
	}

	addr := startDistribParent(t, 2, "deeproot", search)
	host, portStr, _ := net.SplitHostPort(addr)
	port := 0
	fmt.Sscanf(portStr, "%d", &port)
	ip := net.ParseIP(host).To4()

	searchReceived := make(chan *IncomingSearchRequest, 1)
	dm := NewDistribManager("testuser", func(s *IncomingSearchRequest) {
		searchReceived <- s
	})

	parents := []PotentialParent{
		{Username: "unreachable", IP: net.IPv4(192, 0, 2, 1).To4(), Port: 9999}, // RFC 5737 TEST-NET
		{Username: "goodparent", IP: ip, Port: uint32(port)},
	}

	dm.HandleNetInfo(parents)

	select {
	case s := <-searchReceived:
		if s.Query != "music" {
			t.Fatalf("search query = %q, want music", s.Query)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for search from second parent")
	}

	if dm.ParentUsername() != "goodparent" {
		t.Fatalf("parent = %q, want goodparent", dm.ParentUsername())
	}
	// Parent level is 2, so we are 3.
	if dm.BranchLevel() != 3 {
		t.Fatalf("branch level = %d, want 3", dm.BranchLevel())
	}
	if dm.BranchRoot() != "deeproot" {
		t.Fatalf("branch root = %q, want deeproot", dm.BranchRoot())
	}

	dm.Close()
}

func TestDistribManager_ForwardsMultipleSearches(t *testing.T) {
	// Start a parent that sends multiple searches.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })

	go func() {
		raw, err := ln.Accept()
		if err != nil {
			return
		}
		conn := WrapConn(raw)
		conn.ReadMessage() // PeerInit

		conn.WriteMessage(BuildDistribBranchLevel(0))
		conn.WriteMessage(BuildDistribBranchRoot("root"))

		// Send 3 search requests.
		for i := uint32(1); i <= 3; i++ {
			b := NewInitBuilder(DistribSearchRequest)
			b.WriteUint32(distribSearchIdentifier)
			b.WriteString("user")
			b.WriteUint32(i)
			b.WriteString("query")
			conn.WriteMessage(b.Build())
			time.Sleep(10 * time.Millisecond)
		}

		// Keep alive.
		for {
			if _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()

	addr := ln.Addr().String()
	host, portStr, _ := net.SplitHostPort(addr)
	port := 0
	fmt.Sscanf(portStr, "%d", &port)
	ip := net.ParseIP(host).To4()

	var count atomic.Int32
	dm := NewDistribManager("testuser", func(s *IncomingSearchRequest) {
		count.Add(1)
	})

	dm.HandleNetInfo([]PotentialParent{
		{Username: "root", IP: ip, Port: uint32(port)},
	})

	// Wait for all 3 searches.
	deadline := time.After(3 * time.Second)
	for {
		if count.Load() >= 3 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("got %d searches, want 3", count.Load())
		case <-time.After(10 * time.Millisecond):
		}
	}

	dm.Close()
}

func TestDistribManager_ParentDisconnectResetsState(t *testing.T) {
	// Start a parent that will disconnect after sending initial messages.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })

	go func() {
		raw, err := ln.Accept()
		if err != nil {
			return
		}
		conn := WrapConn(raw)
		conn.ReadMessage() // PeerInit

		conn.WriteMessage(BuildDistribBranchLevel(0))
		conn.WriteMessage(BuildDistribBranchRoot("root"))

		b := NewInitBuilder(DistribSearchRequest)
		b.WriteUint32(distribSearchIdentifier)
		b.WriteString("user")
		b.WriteUint32(1)
		b.WriteString("query")
		conn.WriteMessage(b.Build())

		// Disconnect after a brief pause.
		time.Sleep(100 * time.Millisecond)
		conn.Close()
	}()

	addr := ln.Addr().String()
	host, portStr, _ := net.SplitHostPort(addr)
	port := 0
	fmt.Sscanf(portStr, "%d", &port)
	ip := net.ParseIP(host).To4()

	searchReceived := make(chan struct{}, 1)
	dm := NewDistribManager("testuser", func(s *IncomingSearchRequest) {
		select {
		case searchReceived <- struct{}{}:
		default:
		}
	})

	dm.HandleNetInfo([]PotentialParent{
		{Username: "root", IP: ip, Port: uint32(port)},
	})

	// Wait for at least one search to confirm connection established.
	select {
	case <-searchReceived:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for initial search")
	}

	// Wait for parent disconnect to be detected.
	time.Sleep(300 * time.Millisecond)

	if dm.ParentUsername() != "" {
		t.Fatalf("parent should be empty after disconnect, got %q", dm.ParentUsername())
	}
	if dm.HasParent() {
		t.Fatal("HasParent should be false after disconnect")
	}

	dm.Close()
}

func TestDistribManager_IgnoresNetInfoWhenHasParent(t *testing.T) {
	search := &IncomingSearchRequest{
		Username: "searcher",
		Token:    1,
		Query:    "query",
	}

	addr := startDistribParent(t, 0, "root", search)
	host, portStr, _ := net.SplitHostPort(addr)
	port := 0
	fmt.Sscanf(portStr, "%d", &port)
	ip := net.ParseIP(host).To4()

	searchReceived := make(chan struct{}, 1)
	dm := NewDistribManager("testuser", func(s *IncomingSearchRequest) {
		select {
		case searchReceived <- struct{}{}:
		default:
		}
	})

	parents := []PotentialParent{
		{Username: "root", IP: ip, Port: uint32(port)},
	}

	dm.HandleNetInfo(parents)

	// Wait for adoption.
	select {
	case <-searchReceived:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for first search")
	}

	if !dm.HasParent() {
		t.Fatal("should have parent")
	}

	// Send another NetInfo — should be ignored since we already have a parent.
	dm.HandleNetInfo([]PotentialParent{
		{Username: "other", IP: net.IPv4(192, 0, 2, 1).To4(), Port: 9999},
	})

	// Parent should still be the original.
	if dm.ParentUsername() != "root" {
		t.Fatalf("parent = %q, want root (should not have changed)", dm.ParentUsername())
	}

	dm.Close()
}

// distribUpdates collects the server messages that describe the distributed
// state (HaveNoParent, BranchRoot, BranchLevel) as readable strings.
type distribUpdates struct {
	ch chan string
}

func describeDistribMsg(msg []byte) string {
	r, err := NewReader(msg)
	if err != nil {
		return ""
	}
	switch r.Code() {
	case ServerHaveNoParent:
		v, _ := r.ReadBool()
		return fmt.Sprintf("HaveNoParent=%v", v)
	case ServerBranchRoot:
		v, _ := r.ReadString()
		return "BranchRoot=" + v
	case ServerBranchLevel:
		v, _ := r.ReadUint32()
		return fmt.Sprintf("BranchLevel=%d", v)
	}
	return ""
}

// loginWithDistrib logs a client with the given manager into a mock server and
// returns the distributed-state messages the server saw, in order.
func loginWithDistrib(t *testing.T, dm *DistribManager) (*Client, *distribUpdates) {
	t.Helper()
	u := &distribUpdates{ch: make(chan string, 64)}
	addr := startMockServer(t, func(sc *Conn) {
		handleLogin(t, sc, buildLoginSuccessResponse())
		for {
			msg, err := sc.ReadMessage()
			if err != nil {
				return
			}
			if s := describeDistribMsg(msg); s != "" {
				u.ch <- s
			}
		}
	})
	c, err := NewClient(ClientConfig{ServerAddr: addr, Username: "me", Password: "p"})
	if err != nil {
		t.Fatal(err)
	}
	if dm != nil {
		c.SetDistribManager(dm)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c, u
}

func (u *distribUpdates) expect(t *testing.T, want ...string) {
	t.Helper()
	for _, w := range want {
		select {
		case got := <-u.ch:
			if got != w {
				t.Fatalf("server saw %q, want %q", got, w)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for %q", w)
		}
	}
}

func TestClient_LoginWithoutParentReportsNoParent(t *testing.T) {
	dm := NewDistribManager("me", nil)
	_, u := loginWithDistrib(t, dm)
	u.expect(t, "HaveNoParent=true", "BranchRoot=me", "BranchLevel=0")
}

// The manager keeps its parent when the server connection drops, so the next
// login must report that parent instead of claiming to have none.
func TestClient_LoginResyncsExistingParent(t *testing.T) {
	dm := NewDistribManager("me", nil)
	dm.hasParent, dm.branchLvl, dm.branchRoot = true, 3, "root"
	_, u := loginWithDistrib(t, dm)
	u.expect(t, "HaveNoParent=false", "BranchRoot=root", "BranchLevel=3")
}

// Branch level/root changes announced by the parent are forwarded to the server.
func TestClient_ForwardsParentBranchChanges(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	update := make(chan struct{})
	go func() {
		raw, err := ln.Accept()
		if err != nil {
			return
		}
		conn := WrapConn(raw)
		conn.ReadMessage() // PeerInit
		conn.WriteMessage(BuildDistribBranchLevel(0))
		conn.WriteMessage(BuildDistribBranchRoot("root"))
		b := NewInitBuilder(DistribSearchRequest)
		b.WriteUint32(distribSearchIdentifier)
		b.WriteString("s")
		b.WriteUint32(1)
		b.WriteString("q")
		conn.WriteMessage(b.Build())
		<-update
		conn.WriteMessage(BuildDistribBranchLevel(4))
		conn.WriteMessage(BuildDistribBranchRoot("newroot"))
		for {
			if _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()

	dm := NewDistribManager("me", func(*IncomingSearchRequest) {})
	_, u := loginWithDistrib(t, dm)
	u.expect(t, "HaveNoParent=true", "BranchRoot=me", "BranchLevel=0")

	host, portStr, _ := net.SplitHostPort(ln.Addr().String())
	var port uint32
	fmt.Sscanf(portStr, "%d", &port)
	dm.HandleNetInfo([]PotentialParent{{Username: "root", IP: net.ParseIP(host).To4(), Port: port}})

	u.expect(t, "HaveNoParent=false", "BranchRoot=root", "BranchLevel=1")
	close(update)
	u.expect(t, "HaveNoParent=false", "BranchRoot=root", "BranchLevel=5")
	u.expect(t, "HaveNoParent=false", "BranchRoot=newroot", "BranchLevel=5")
	dm.Close()
}

// Concurrent state changes must not be reordered on the wire: whatever the
// interleaving, the last state the server hears is the manager's final state.
func TestClient_ConcurrentParentChangesEndInFinalState(t *testing.T) {
	dm := NewDistribManager("me", nil)
	_, u := loginWithDistrib(t, dm)
	u.expect(t, "HaveNoParent=true", "BranchRoot=me", "BranchLevel=0")

	var wg sync.WaitGroup
	for i := 1; i <= 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			dm.mu.Lock()
			dm.hasParent, dm.branchLvl, dm.branchRoot = true, uint32(i), fmt.Sprintf("root%d", i)
			dm.mu.Unlock()
			dm.changed()
		}(i)
	}
	wg.Wait()
	final := dm.State()
	want := []string{"HaveNoParent=false", "BranchRoot=" + final.BranchRoot, fmt.Sprintf("BranchLevel=%d", final.BranchLevel)}

	var last []string
	deadline := time.After(2 * time.Second)
	for len(last) < 3 || last[0] != want[0] || last[1] != want[1] || last[2] != want[2] {
		select {
		case s := <-u.ch:
			last = append(last, s)
			if len(last) > 3 {
				last = last[1:]
			}
		case <-deadline:
			t.Fatalf("server's last state %v, want %v", last, want)
		}
	}
}
