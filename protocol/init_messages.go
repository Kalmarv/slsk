package protocol

import "fmt"

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

// PeerInitMessage represents a PeerInit handshake message (init code 1).
type PeerInitMessage struct {
	Username string
	Type     string // "P", "F", or "D"
	Token    uint32
}

// ---------------------------------------------------------------------------
// PeerInit (init code 1)
// ---------------------------------------------------------------------------

// BuildPeerInit builds a PeerInit message (init code 1).
// Wire format: [4-byte length][1-byte code=1][string username][string connType][uint32 token]
func BuildPeerInit(username, connType string, token uint32) []byte {
	b := NewInitBuilder(InitPeerInit)
	b.WriteString(username)
	b.WriteString(connType)
	b.WriteUint32(token)
	return b.Build()
}

// ParsePeerInit parses a PeerInit message (init code 1).
func ParsePeerInit(msg []byte) (*PeerInitMessage, error) {
	r, err := NewInitReader(msg)
	if err != nil {
		return nil, fmt.Errorf("peer init: %w", err)
	}
	if r.Code() != InitPeerInit {
		return nil, fmt.Errorf("expected code %d, got %d", InitPeerInit, r.Code())
	}

	result := &PeerInitMessage{}
	result.Username, err = r.ReadString()
	if err != nil {
		return nil, fmt.Errorf("peer init username: %w", err)
	}
	result.Type, err = r.ReadString()
	if err != nil {
		return nil, fmt.Errorf("peer init type: %w", err)
	}
	result.Token, err = r.ReadUint32()
	if err != nil {
		return nil, fmt.Errorf("peer init token: %w", err)
	}
	return result, nil
}

// ---------------------------------------------------------------------------
// PierceFirewall (init code 0)
// ---------------------------------------------------------------------------

// BuildPierceFirewall builds a PierceFirewall message (init code 0).
// Wire format: [4-byte length][1-byte code=0][uint32 token]
func BuildPierceFirewall(token uint32) []byte {
	b := NewInitBuilder(InitPierceFirewall)
	b.WriteUint32(token)
	return b.Build()
}

// ParsePierceFirewall parses a PierceFirewall message (init code 0) and returns the token.
func ParsePierceFirewall(msg []byte) (uint32, error) {
	r, err := NewInitReader(msg)
	if err != nil {
		return 0, fmt.Errorf("pierce firewall: %w", err)
	}
	if r.Code() != InitPierceFirewall {
		return 0, fmt.Errorf("expected code %d, got %d", InitPierceFirewall, r.Code())
	}

	token, err := r.ReadUint32()
	if err != nil {
		return 0, fmt.Errorf("pierce firewall token: %w", err)
	}
	return token, nil
}
