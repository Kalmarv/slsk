package protocol

import (
	"testing"
)

// ---------------------------------------------------------------------------
// DistribSearchRequest tests
// ---------------------------------------------------------------------------

func TestDistribSearchRequestValid(t *testing.T) {
	// Build a valid DistribSearchRequest manually using InitMessageBuilder.
	// Wire format: [4-byte length][1-byte code=3][uint32 identifier=49][string username][uint32 token][string query]
	b := NewInitBuilder(DistribSearchRequest)
	b.WriteUint32(49) // identifier must be 49
	b.WriteString("searchuser")
	b.WriteUint32(77777)
	b.WriteString("artist album")
	msg := b.Build()

	parsed, err := ParseDistribSearchRequest(msg)
	if err != nil {
		t.Fatalf("ParseDistribSearchRequest: %v", err)
	}

	if parsed.Username != "searchuser" {
		t.Fatalf("username = %q, want %q", parsed.Username, "searchuser")
	}
	if parsed.Token != 77777 {
		t.Fatalf("token = %d, want 77777", parsed.Token)
	}
	if parsed.Query != "artist album" {
		t.Fatalf("query = %q, want %q", parsed.Query, "artist album")
	}
}

func TestDistribSearchRequestInvalidIdentifier(t *testing.T) {
	b := NewInitBuilder(DistribSearchRequest)
	b.WriteUint32(50) // wrong identifier
	b.WriteString("user")
	b.WriteUint32(1)
	b.WriteString("query")
	msg := b.Build()

	_, err := ParseDistribSearchRequest(msg)
	if err == nil {
		t.Fatal("expected error for invalid identifier")
	}
}

func TestDistribSearchRequestIdentifierZero(t *testing.T) {
	b := NewInitBuilder(DistribSearchRequest)
	b.WriteUint32(0) // wrong identifier
	b.WriteString("user")
	b.WriteUint32(1)
	b.WriteString("query")
	msg := b.Build()

	_, err := ParseDistribSearchRequest(msg)
	if err == nil {
		t.Fatal("expected error for identifier 0")
	}
}

func TestDistribSearchRequestEmptyUsername(t *testing.T) {
	b := NewInitBuilder(DistribSearchRequest)
	b.WriteUint32(49)
	b.WriteString("")
	b.WriteUint32(100)
	b.WriteString("some query")
	msg := b.Build()

	parsed, err := ParseDistribSearchRequest(msg)
	if err != nil {
		t.Fatalf("ParseDistribSearchRequest: %v", err)
	}
	if parsed.Username != "" {
		t.Fatalf("username = %q, want empty", parsed.Username)
	}
	if parsed.Token != 100 {
		t.Fatalf("token = %d, want 100", parsed.Token)
	}
	if parsed.Query != "some query" {
		t.Fatalf("query = %q, want %q", parsed.Query, "some query")
	}
}

func TestDistribSearchRequestEmptyQuery(t *testing.T) {
	b := NewInitBuilder(DistribSearchRequest)
	b.WriteUint32(49)
	b.WriteString("user")
	b.WriteUint32(1)
	b.WriteString("")
	msg := b.Build()

	parsed, err := ParseDistribSearchRequest(msg)
	if err != nil {
		t.Fatalf("ParseDistribSearchRequest: %v", err)
	}
	if parsed.Query != "" {
		t.Fatalf("query = %q, want empty", parsed.Query)
	}
}

func TestDistribSearchRequestTooShort(t *testing.T) {
	_, err := ParseDistribSearchRequest([]byte{1, 2, 3})
	if err == nil {
		t.Fatal("expected error for short message")
	}
}

func TestDistribSearchRequestBadCode(t *testing.T) {
	b := NewInitBuilder(99) // wrong code
	b.WriteUint32(49)
	b.WriteString("user")
	b.WriteUint32(1)
	b.WriteString("query")
	msg := b.Build()

	_, err := ParseDistribSearchRequest(msg)
	if err == nil {
		t.Fatal("expected error for wrong code")
	}
}

func TestDistribSearchRequestTruncated(t *testing.T) {
	b := NewInitBuilder(DistribSearchRequest)
	b.WriteUint32(49)
	b.WriteString("user")
	// Missing token and query — truncate.
	msg := b.Build()
	truncated := msg[:len(msg)-1]

	_, err := ParseDistribSearchRequest(truncated)
	if err == nil {
		t.Fatal("expected error for truncated message")
	}
}

// ---------------------------------------------------------------------------
// DistribBranchLevel (distrib code 4) tests
// ---------------------------------------------------------------------------

func TestBuildDistribBranchLevel(t *testing.T) {
	msg := BuildDistribBranchLevel(3)

	r, err := NewInitReader(msg)
	if err != nil {
		t.Fatalf("NewInitReader: %v", err)
	}
	if r.Code() != DistribBranchLevel {
		t.Fatalf("code = %d, want %d", r.Code(), DistribBranchLevel)
	}
	level, err := r.ReadUint32()
	if err != nil {
		t.Fatalf("read level: %v", err)
	}
	if level != 3 {
		t.Fatalf("level = %d, want 3", level)
	}
}

func TestParseDistribBranchLevel(t *testing.T) {
	b := NewInitBuilder(DistribBranchLevel)
	b.WriteUint32(5)
	msg := b.Build()

	level, err := ParseDistribBranchLevel(msg)
	if err != nil {
		t.Fatalf("ParseDistribBranchLevel: %v", err)
	}
	if level != 5 {
		t.Fatalf("level = %d, want 5", level)
	}
}

func TestParseDistribBranchLevelZero(t *testing.T) {
	b := NewInitBuilder(DistribBranchLevel)
	b.WriteUint32(0)
	msg := b.Build()

	level, err := ParseDistribBranchLevel(msg)
	if err != nil {
		t.Fatalf("ParseDistribBranchLevel: %v", err)
	}
	if level != 0 {
		t.Fatalf("level = %d, want 0", level)
	}
}

func TestParseDistribBranchLevelBadCode(t *testing.T) {
	b := NewInitBuilder(99)
	b.WriteUint32(0)
	msg := b.Build()

	_, err := ParseDistribBranchLevel(msg)
	if err == nil {
		t.Fatal("expected error for wrong code")
	}
}

func TestParseDistribBranchLevelTruncated(t *testing.T) {
	_, err := ParseDistribBranchLevel([]byte{1, 2})
	if err == nil {
		t.Fatal("expected error for truncated message")
	}
}

// ---------------------------------------------------------------------------
// DistribBranchRoot (distrib code 5) tests
// ---------------------------------------------------------------------------

func TestBuildDistribBranchRoot(t *testing.T) {
	msg := BuildDistribBranchRoot("rootuser")

	r, err := NewInitReader(msg)
	if err != nil {
		t.Fatalf("NewInitReader: %v", err)
	}
	if r.Code() != DistribBranchRoot {
		t.Fatalf("code = %d, want %d", r.Code(), DistribBranchRoot)
	}
	username, err := r.ReadString()
	if err != nil {
		t.Fatalf("read username: %v", err)
	}
	if username != "rootuser" {
		t.Fatalf("username = %q, want rootuser", username)
	}
}

func TestParseDistribBranchRoot(t *testing.T) {
	b := NewInitBuilder(DistribBranchRoot)
	b.WriteString("therootnode")
	msg := b.Build()

	username, err := ParseDistribBranchRoot(msg)
	if err != nil {
		t.Fatalf("ParseDistribBranchRoot: %v", err)
	}
	if username != "therootnode" {
		t.Fatalf("username = %q, want therootnode", username)
	}
}

func TestParseDistribBranchRootEmpty(t *testing.T) {
	b := NewInitBuilder(DistribBranchRoot)
	b.WriteString("")
	msg := b.Build()

	username, err := ParseDistribBranchRoot(msg)
	if err != nil {
		t.Fatalf("ParseDistribBranchRoot: %v", err)
	}
	if username != "" {
		t.Fatalf("username = %q, want empty", username)
	}
}

func TestParseDistribBranchRootBadCode(t *testing.T) {
	b := NewInitBuilder(99)
	b.WriteString("user")
	msg := b.Build()

	_, err := ParseDistribBranchRoot(msg)
	if err == nil {
		t.Fatal("expected error for wrong code")
	}
}

func TestParseDistribBranchRootTruncated(t *testing.T) {
	_, err := ParseDistribBranchRoot([]byte{1, 2})
	if err == nil {
		t.Fatal("expected error for truncated message")
	}
}
