package slsk

import (
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// startDistribParent starts a TCP listener that simulates a distributed parent.
// It accepts one connection, sends BranchLevel, BranchRoot, and a search request,
// then keeps the connection open. Returns the listener address.
func startDistribParent(t *testing.T, branchLevel uint32, branchRoot string, search *DistribSearch) string {
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
	search := &DistribSearch{
		Username: "searcher",
		Token:    12345,
		Query:    "test query",
	}

	addr := startDistribParent(t, 0, "rootuser", search)
	host, portStr, _ := net.SplitHostPort(addr)
	port := 0
	fmt.Sscanf(portStr, "%d", &port)
	ip := net.ParseIP(host).To4()

	searchReceived := make(chan *DistribSearch, 1)
	dm := NewDistribManager("testuser", func(s *DistribSearch) {
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
	search := &DistribSearch{
		Username: "searcher",
		Token:    42,
		Query:    "music",
	}

	addr := startDistribParent(t, 2, "deeproot", search)
	host, portStr, _ := net.SplitHostPort(addr)
	port := 0
	fmt.Sscanf(portStr, "%d", &port)
	ip := net.ParseIP(host).To4()

	searchReceived := make(chan *DistribSearch, 1)
	dm := NewDistribManager("testuser", func(s *DistribSearch) {
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
	dm := NewDistribManager("testuser", func(s *DistribSearch) {
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
	dm := NewDistribManager("testuser", func(s *DistribSearch) {
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
	search := &DistribSearch{
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
	dm := NewDistribManager("testuser", func(s *DistribSearch) {
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
