package slsk

import "fmt"

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

// DistribSearch represents a distributed search request message (distrib code 3).
type DistribSearch struct {
	Username string
	Token    uint32
	Query    string
}

// distribSearchIdentifier is the required identifier value in distributed search
// request messages. It must equal 49 (ASCII '1').
const distribSearchIdentifier uint32 = 49

// ---------------------------------------------------------------------------
// Distributed Search Request (distrib code 3)
// ---------------------------------------------------------------------------

// ParseDistribSearchRequest parses a distributed search request (distrib code 3).
// Wire format: [4-byte length][1-byte code=3][uint32 identifier=49][string username][uint32 token][string query]
// Returns an error if the identifier is not 49.
func ParseDistribSearchRequest(msg []byte) (*DistribSearch, error) {
	r, err := NewInitReader(msg)
	if err != nil {
		return nil, fmt.Errorf("distrib search request: %w", err)
	}
	if r.Code() != DistribSearchRequest {
		return nil, fmt.Errorf("expected code %d, got %d", DistribSearchRequest, r.Code())
	}

	identifier, err := r.ReadUint32()
	if err != nil {
		return nil, fmt.Errorf("distrib search request identifier: %w", err)
	}
	if identifier != distribSearchIdentifier {
		return nil, fmt.Errorf("distrib search request: invalid identifier %d, expected %d", identifier, distribSearchIdentifier)
	}

	result := &DistribSearch{}
	result.Username, err = r.ReadString()
	if err != nil {
		return nil, fmt.Errorf("distrib search request username: %w", err)
	}
	result.Token, err = r.ReadUint32()
	if err != nil {
		return nil, fmt.Errorf("distrib search request token: %w", err)
	}
	result.Query, err = r.ReadString()
	if err != nil {
		return nil, fmt.Errorf("distrib search request query: %w", err)
	}
	return result, nil
}
