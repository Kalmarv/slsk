package protocol

import "fmt"

// distribSearchIdentifier is the required identifier value in distributed search
// request messages. It must equal 49 (ASCII '1').
const distribSearchIdentifier uint32 = 49

// ---------------------------------------------------------------------------
// DistribBranchLevel (distrib code 4)
// ---------------------------------------------------------------------------

// BuildDistribBranchLevel builds a DistribBranchLevel message (distrib code 4).
// Level 0 = branch root, children increment by 1.
func BuildDistribBranchLevel(level uint32) []byte {
	b := NewInitBuilder(DistribBranchLevel)
	b.WriteUint32(level)
	return b.Build()
}

// ParseDistribBranchLevel parses a DistribBranchLevel message (distrib code 4).
func ParseDistribBranchLevel(msg []byte) (uint32, error) {
	r, err := NewInitReader(msg)
	if err != nil {
		return 0, fmt.Errorf("distrib branch level: %w", err)
	}
	if r.Code() != DistribBranchLevel {
		return 0, fmt.Errorf("distrib branch level: expected code %d, got %d", DistribBranchLevel, r.Code())
	}
	level, err := r.ReadUint32()
	if err != nil {
		return 0, fmt.Errorf("distrib branch level: %w", err)
	}
	return level, nil
}

// ---------------------------------------------------------------------------
// DistribBranchRoot (distrib code 5)
// ---------------------------------------------------------------------------

// BuildDistribBranchRoot builds a DistribBranchRoot message (distrib code 5).
func BuildDistribBranchRoot(username string) []byte {
	b := NewInitBuilder(DistribBranchRoot)
	b.WriteString(username)
	return b.Build()
}

// ParseDistribBranchRoot parses a DistribBranchRoot message (distrib code 5).
func ParseDistribBranchRoot(msg []byte) (string, error) {
	r, err := NewInitReader(msg)
	if err != nil {
		return "", fmt.Errorf("distrib branch root: %w", err)
	}
	if r.Code() != DistribBranchRoot {
		return "", fmt.Errorf("distrib branch root: expected code %d, got %d", DistribBranchRoot, r.Code())
	}
	username, err := r.ReadString()
	if err != nil {
		return "", fmt.Errorf("distrib branch root: %w", err)
	}
	return username, nil
}

// ---------------------------------------------------------------------------
// Distributed Search Request (distrib code 3)
// ---------------------------------------------------------------------------

// BuildDistribSearchRequest builds a distributed search request (distrib code
// 3), as a parent forwards it to its children.
func BuildDistribSearchRequest(username string, token uint32, query string) []byte {
	b := NewInitBuilder(DistribSearchRequest)
	b.WriteUint32(distribSearchIdentifier)
	b.WriteString(username)
	b.WriteUint32(token)
	b.WriteString(query)
	return b.Build()
}

// ParseDistribSearchRequest parses a distributed search request (distrib code 3).
// Wire format: [4-byte length][1-byte code=3][uint32 identifier=49][string username][uint32 token][string query]
// Returns an error if the identifier is not 49.
func ParseDistribSearchRequest(msg []byte) (*IncomingSearchRequest, error) {
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

	result := &IncomingSearchRequest{}
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
