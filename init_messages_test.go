package slsk

import (
	"testing"
)

// ---------------------------------------------------------------------------
// PeerInit round-trip tests
// ---------------------------------------------------------------------------

func TestPeerInitRoundTrip(t *testing.T) {
	msg := BuildPeerInit("testuser", ConnTypePeer, 12345)
	parsed, err := ParsePeerInit(msg)
	if err != nil {
		t.Fatalf("ParsePeerInit: %v", err)
	}

	if parsed.Username != "testuser" {
		t.Fatalf("username = %q, want %q", parsed.Username, "testuser")
	}
	if parsed.Type != ConnTypePeer {
		t.Fatalf("type = %q, want %q", parsed.Type, ConnTypePeer)
	}
	if parsed.Token != 12345 {
		t.Fatalf("token = %d, want 12345", parsed.Token)
	}
}

func TestPeerInitConnTypeP(t *testing.T) {
	msg := BuildPeerInit("alice", ConnTypePeer, 1)
	parsed, err := ParsePeerInit(msg)
	if err != nil {
		t.Fatalf("ParsePeerInit: %v", err)
	}
	if parsed.Type != "P" {
		t.Fatalf("type = %q, want %q", parsed.Type, "P")
	}
}

func TestPeerInitConnTypeF(t *testing.T) {
	msg := BuildPeerInit("bob", ConnTypeTransfer, 2)
	parsed, err := ParsePeerInit(msg)
	if err != nil {
		t.Fatalf("ParsePeerInit: %v", err)
	}
	if parsed.Type != "F" {
		t.Fatalf("type = %q, want %q", parsed.Type, "F")
	}
}

func TestPeerInitConnTypeD(t *testing.T) {
	msg := BuildPeerInit("charlie", ConnTypeDistributed, 3)
	parsed, err := ParsePeerInit(msg)
	if err != nil {
		t.Fatalf("ParsePeerInit: %v", err)
	}
	if parsed.Type != "D" {
		t.Fatalf("type = %q, want %q", parsed.Type, "D")
	}
}

func TestPeerInitEmptyUsername(t *testing.T) {
	msg := BuildPeerInit("", ConnTypePeer, 42)
	parsed, err := ParsePeerInit(msg)
	if err != nil {
		t.Fatalf("ParsePeerInit: %v", err)
	}
	if parsed.Username != "" {
		t.Fatalf("username = %q, want empty", parsed.Username)
	}
	if parsed.Token != 42 {
		t.Fatalf("token = %d, want 42", parsed.Token)
	}
}

func TestPeerInitWireCode(t *testing.T) {
	msg := BuildPeerInit("u", ConnTypePeer, 1)
	// Init messages: [4-byte length][1-byte code][payload]
	// Code is at byte 4.
	if msg[4] != InitPeerInit {
		t.Fatalf("wire code = %d, want %d", msg[4], InitPeerInit)
	}
}

func TestPeerInitTruncatedMessage(t *testing.T) {
	msg := BuildPeerInit("testuser", ConnTypePeer, 12345)
	// Truncate the message to cut off the token.
	truncated := msg[:len(msg)-2]
	_, err := ParsePeerInit(truncated)
	if err == nil {
		t.Fatal("expected error for truncated message")
	}
}

func TestPeerInitTooShort(t *testing.T) {
	_, err := ParsePeerInit([]byte{1, 2, 3})
	if err == nil {
		t.Fatal("expected error for short message")
	}
}

func TestPeerInitBadCode(t *testing.T) {
	msg := BuildPeerInit("u", ConnTypePeer, 1)
	// Tamper the code byte.
	msg[4] = 99
	_, err := ParsePeerInit(msg)
	if err == nil {
		t.Fatal("expected error for wrong code")
	}
}

// ---------------------------------------------------------------------------
// PierceFirewall round-trip tests
// ---------------------------------------------------------------------------

func TestPierceFirewallRoundTrip(t *testing.T) {
	msg := BuildPierceFirewall(98765)
	token, err := ParsePierceFirewall(msg)
	if err != nil {
		t.Fatalf("ParsePierceFirewall: %v", err)
	}
	if token != 98765 {
		t.Fatalf("token = %d, want 98765", token)
	}
}

func TestPierceFirewallWireCode(t *testing.T) {
	msg := BuildPierceFirewall(1)
	if msg[4] != InitPierceFirewall {
		t.Fatalf("wire code = %d, want %d", msg[4], InitPierceFirewall)
	}
}

func TestPierceFirewallTooShort(t *testing.T) {
	_, err := ParsePierceFirewall([]byte{1, 2, 3})
	if err == nil {
		t.Fatal("expected error for short message")
	}
}

func TestPierceFirewallBadCode(t *testing.T) {
	msg := BuildPierceFirewall(1)
	msg[4] = 99
	_, err := ParsePierceFirewall(msg)
	if err == nil {
		t.Fatal("expected error for wrong code")
	}
}

func TestPierceFirewallTruncated(t *testing.T) {
	msg := BuildPierceFirewall(12345)
	// Truncate so the token is incomplete.
	truncated := msg[:len(msg)-2]
	_, err := ParsePierceFirewall(truncated)
	if err == nil {
		t.Fatal("expected error for truncated message")
	}
}
