package slsk

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"net"
	"testing"
)

// ---------- helpers ----------

// buildMsg constructs a raw wire message from code and payload bytes
// for use in parse tests. Format: [4-byte LE length][4-byte LE code][payload]
func buildMsg(code uint32, payload []byte) []byte {
	msgLen := 4 + len(payload)
	msg := make([]byte, 4+msgLen)
	binary.LittleEndian.PutUint32(msg[0:4], uint32(msgLen))
	binary.LittleEndian.PutUint32(msg[4:8], code)
	copy(msg[8:], payload)
	return msg
}

// appendStr appends a length-prefixed string to a byte slice.
func appendStr(buf []byte, s string) []byte {
	buf = append(buf, u32le(uint32(len(s)))...)
	buf = append(buf, s...)
	return buf
}

// reversedIP returns the 4 wire bytes for an IP address (reversed order).
// e.g. 192.168.1.1 -> [1, 1, 168, 192]
func reversedIP(a, b, c, d byte) []byte {
	return []byte{d, c, b, a}
}

// ---------- BuildLoginRequest ----------

func TestBuildLoginRequest(t *testing.T) {
	username := "testuser"
	password := "testpass"
	major := uint32(170)
	minor := uint32(1)

	msg := BuildLoginRequest(username, password, major, minor)

	r, err := NewReader(msg)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	if r.Code() != ServerLogin {
		t.Fatalf("code = %d, want %d", r.Code(), ServerLogin)
	}

	// username
	u, err := r.ReadString()
	if err != nil {
		t.Fatal(err)
	}
	if u != username {
		t.Fatalf("username = %q, want %q", u, username)
	}

	// password
	p, err := r.ReadString()
	if err != nil {
		t.Fatal(err)
	}
	if p != password {
		t.Fatalf("password = %q, want %q", p, password)
	}

	// version (major)
	v, err := r.ReadUint32()
	if err != nil {
		t.Fatal(err)
	}
	if v != major {
		t.Fatalf("version = %d, want %d", v, major)
	}

	// hash
	expectedHash := fmt.Sprintf("%x", md5.Sum([]byte(username+password)))
	h, err := r.ReadString()
	if err != nil {
		t.Fatal(err)
	}
	if h != expectedHash {
		t.Fatalf("hash = %q, want %q", h, expectedHash)
	}

	// minor version
	mv, err := r.ReadUint32()
	if err != nil {
		t.Fatal(err)
	}
	if mv != minor {
		t.Fatalf("minorVersion = %d, want %d", mv, minor)
	}

	if r.HasMore() {
		t.Fatal("unexpected trailing data")
	}
}

func TestBuildLoginRequestHash(t *testing.T) {
	// Verify the MD5 hash is correct for known input
	msg := BuildLoginRequest("alice", "secret", 170, 0)

	r, err := NewReader(msg)
	if err != nil {
		t.Fatal(err)
	}

	r.ReadString() // username
	r.ReadString() // password
	r.ReadUint32() // version

	hash, err := r.ReadString()
	if err != nil {
		t.Fatal(err)
	}

	expected := fmt.Sprintf("%x", md5.Sum([]byte("alicesecret")))
	if hash != expected {
		t.Fatalf("hash = %q, want %q", hash, expected)
	}
}

// ---------- ParseLoginResponse ----------

func TestParseLoginResponseSuccess(t *testing.T) {
	// Build a success response: bool(1) + greeting + IP(reversed) + hash + isSupporter
	var payload []byte
	payload = append(payload, 1) // success = true
	payload = appendStr(payload, "Welcome to Soulseek!")
	payload = append(payload, reversedIP(192, 168, 1, 1)...)
	payload = appendStr(payload, "abcdef1234567890")
	payload = append(payload, 1) // isSupporter = true

	msg := buildMsg(ServerLogin, payload)

	resp, err := ParseLoginResponse(msg)
	if err != nil {
		t.Fatalf("ParseLoginResponse: %v", err)
	}

	if !resp.Success {
		t.Fatal("expected success=true")
	}
	if resp.Greeting != "Welcome to Soulseek!" {
		t.Fatalf("greeting = %q", resp.Greeting)
	}
	expectedIP := net.IPv4(192, 168, 1, 1).To4()
	if !resp.IP.Equal(expectedIP) {
		t.Fatalf("IP = %v, want %v", resp.IP, expectedIP)
	}
	if resp.Hash != "abcdef1234567890" {
		t.Fatalf("hash = %q", resp.Hash)
	}
	if !resp.IsSupporter {
		t.Fatal("expected isSupporter=true")
	}
	if resp.Reason != "" {
		t.Fatalf("reason should be empty on success, got %q", resp.Reason)
	}
}

func TestParseLoginResponseFailure(t *testing.T) {
	// Build a failure response: bool(0) + reason
	var payload []byte
	payload = append(payload, 0) // success = false
	payload = appendStr(payload, "INVALIDPASS")

	msg := buildMsg(ServerLogin, payload)

	resp, err := ParseLoginResponse(msg)
	if err != nil {
		t.Fatalf("ParseLoginResponse: %v", err)
	}

	if resp.Success {
		t.Fatal("expected success=false")
	}
	if resp.Reason != "INVALIDPASS" {
		t.Fatalf("reason = %q, want INVALIDPASS", resp.Reason)
	}
	if resp.Greeting != "" {
		t.Fatalf("greeting should be empty on failure, got %q", resp.Greeting)
	}
}

func TestParseLoginResponseIPReversed(t *testing.T) {
	// Wire bytes [10, 0, 0, 127] should decode to 127.0.0.10
	var payload []byte
	payload = append(payload, 1) // success
	payload = appendStr(payload, "hi")
	payload = append(payload, 10, 0, 0, 127) // reversed IP
	payload = appendStr(payload, "hash")
	payload = append(payload, 0) // not supporter

	msg := buildMsg(ServerLogin, payload)

	resp, err := ParseLoginResponse(msg)
	if err != nil {
		t.Fatal(err)
	}

	expectedIP := net.IPv4(127, 0, 0, 10).To4()
	if !resp.IP.Equal(expectedIP) {
		t.Fatalf("IP = %v, want %v", resp.IP, expectedIP)
	}
}

func TestParseLoginResponseTruncated(t *testing.T) {
	// success=true but no greeting or anything after
	var payload []byte
	payload = append(payload, 1)
	msg := buildMsg(ServerLogin, payload)

	_, err := ParseLoginResponse(msg)
	if err == nil {
		t.Fatal("expected error for truncated success response")
	}
}

// ---------- BuildFileSearchRequest ----------

func TestBuildFileSearchRequest(t *testing.T) {
	token := uint32(0xDEAD)
	query := "artist album"

	msg := BuildFileSearchRequest(token, query)

	r, err := NewReader(msg)
	if err != nil {
		t.Fatal(err)
	}

	if r.Code() != ServerFileSearch {
		t.Fatalf("code = %d, want %d", r.Code(), ServerFileSearch)
	}

	tk, err := r.ReadUint32()
	if err != nil {
		t.Fatal(err)
	}
	if tk != token {
		t.Fatalf("token = %d, want %d", tk, token)
	}

	q, err := r.ReadString()
	if err != nil {
		t.Fatal(err)
	}
	if q != query {
		t.Fatalf("query = %q, want %q", q, query)
	}

	if r.HasMore() {
		t.Fatal("unexpected trailing data")
	}
}

// ---------- BuildGetPeerAddressRequest ----------

func TestBuildGetPeerAddressRequest(t *testing.T) {
	msg := BuildGetPeerAddressRequest("someuser")

	r, err := NewReader(msg)
	if err != nil {
		t.Fatal(err)
	}

	if r.Code() != ServerGetPeerAddress {
		t.Fatalf("code = %d, want %d", r.Code(), ServerGetPeerAddress)
	}

	u, err := r.ReadString()
	if err != nil {
		t.Fatal(err)
	}
	if u != "someuser" {
		t.Fatalf("username = %q, want someuser", u)
	}

	if r.HasMore() {
		t.Fatal("unexpected trailing data")
	}
}

// ---------- ParseGetPeerAddressResponse ----------

func TestParseGetPeerAddressResponse(t *testing.T) {
	var payload []byte
	payload = appendStr(payload, "peeruser")
	payload = append(payload, reversedIP(10, 0, 1, 50)...)
	payload = append(payload, u32le(2234)...) // port

	msg := buildMsg(ServerGetPeerAddress, payload)

	resp, err := ParseGetPeerAddressResponse(msg)
	if err != nil {
		t.Fatalf("ParseGetPeerAddressResponse: %v", err)
	}

	if resp.Username != "peeruser" {
		t.Fatalf("username = %q", resp.Username)
	}

	expectedIP := net.IPv4(10, 0, 1, 50).To4()
	if !resp.IP.Equal(expectedIP) {
		t.Fatalf("IP = %v, want %v", resp.IP, expectedIP)
	}

	if resp.Port != 2234 {
		t.Fatalf("port = %d, want 2234", resp.Port)
	}
}

func TestParseGetPeerAddressResponseTruncated(t *testing.T) {
	// Only username, no IP or port
	var payload []byte
	payload = appendStr(payload, "user")
	msg := buildMsg(ServerGetPeerAddress, payload)

	_, err := ParseGetPeerAddressResponse(msg)
	if err == nil {
		t.Fatal("expected error for truncated response")
	}
}

// ---------- BuildSetListenPort ----------

func TestBuildSetListenPort(t *testing.T) {
	msg := BuildSetListenPort(2242)

	r, err := NewReader(msg)
	if err != nil {
		t.Fatal(err)
	}

	if r.Code() != ServerSetListenPort {
		t.Fatalf("code = %d, want %d", r.Code(), ServerSetListenPort)
	}

	p, err := r.ReadUint32()
	if err != nil {
		t.Fatal(err)
	}
	if p != 2242 {
		t.Fatalf("port = %d, want 2242", p)
	}

	if r.HasMore() {
		t.Fatal("unexpected trailing data")
	}
}

// ---------- BuildSharedFoldersFiles ----------

func TestBuildSharedFoldersFiles(t *testing.T) {
	msg := BuildSharedFoldersFiles(10, 500)

	r, err := NewReader(msg)
	if err != nil {
		t.Fatal(err)
	}

	if r.Code() != ServerSharedFoldersFiles {
		t.Fatalf("code = %d, want %d", r.Code(), ServerSharedFoldersFiles)
	}

	d, err := r.ReadUint32()
	if err != nil {
		t.Fatal(err)
	}
	if d != 10 {
		t.Fatalf("dirs = %d, want 10", d)
	}

	f, err := r.ReadUint32()
	if err != nil {
		t.Fatal(err)
	}
	if f != 500 {
		t.Fatalf("files = %d, want 500", f)
	}

	if r.HasMore() {
		t.Fatal("unexpected trailing data")
	}
}

// ---------- BuildPing ----------

func TestBuildPing(t *testing.T) {
	msg := BuildPing()

	r, err := NewReader(msg)
	if err != nil {
		t.Fatal(err)
	}

	if r.Code() != ServerPing {
		t.Fatalf("code = %d, want %d", r.Code(), ServerPing)
	}

	if r.HasMore() {
		t.Fatal("expected no payload")
	}

	// Verify wire format: length=4 (just the code), code=32
	expected := bytes.Join([][]byte{
		u32le(4),  // length
		u32le(32), // code = ServerPing
	}, nil)
	if !bytes.Equal(msg, expected) {
		t.Fatalf("wire:\ngot  %v\nwant %v", msg, expected)
	}
}

// ---------- BuildConnectToPeerRequest ----------

func TestBuildConnectToPeerRequest(t *testing.T) {
	msg := BuildConnectToPeerRequest(0xCAFE, "targetuser", ConnTypePeer)

	r, err := NewReader(msg)
	if err != nil {
		t.Fatal(err)
	}

	if r.Code() != ServerConnectToPeer {
		t.Fatalf("code = %d, want %d", r.Code(), ServerConnectToPeer)
	}

	tk, err := r.ReadUint32()
	if err != nil {
		t.Fatal(err)
	}
	if tk != 0xCAFE {
		t.Fatalf("token = %#x, want 0xCAFE", tk)
	}

	u, err := r.ReadString()
	if err != nil {
		t.Fatal(err)
	}
	if u != "targetuser" {
		t.Fatalf("username = %q", u)
	}

	ct, err := r.ReadString()
	if err != nil {
		t.Fatal(err)
	}
	if ct != ConnTypePeer {
		t.Fatalf("connType = %q, want %q", ct, ConnTypePeer)
	}

	if r.HasMore() {
		t.Fatal("unexpected trailing data")
	}
}

// ---------- ParseConnectToPeerResponse ----------

func TestParseConnectToPeerResponse(t *testing.T) {
	var payload []byte
	payload = appendStr(payload, "otheruser")
	payload = appendStr(payload, "P")
	payload = append(payload, reversedIP(172, 16, 0, 5)...)
	payload = append(payload, u32le(6789)...)  // port
	payload = append(payload, u32le(54321)...) // token
	payload = append(payload, 1)               // privileged

	msg := buildMsg(ServerConnectToPeer, payload)

	resp, err := ParseConnectToPeerResponse(msg)
	if err != nil {
		t.Fatalf("ParseConnectToPeerResponse: %v", err)
	}

	if resp.Username != "otheruser" {
		t.Fatalf("username = %q", resp.Username)
	}
	if resp.Type != "P" {
		t.Fatalf("type = %q", resp.Type)
	}

	expectedIP := net.IPv4(172, 16, 0, 5).To4()
	if !resp.IP.Equal(expectedIP) {
		t.Fatalf("IP = %v, want %v", resp.IP, expectedIP)
	}

	if resp.Port != 6789 {
		t.Fatalf("port = %d, want 6789", resp.Port)
	}
	if resp.Token != 54321 {
		t.Fatalf("token = %d, want 54321", resp.Token)
	}
	if !resp.Privileged {
		t.Fatal("expected privileged=true")
	}
}

func TestParseConnectToPeerResponseNotPrivileged(t *testing.T) {
	var payload []byte
	payload = appendStr(payload, "user2")
	payload = appendStr(payload, "F")
	payload = append(payload, reversedIP(1, 2, 3, 4)...)
	payload = append(payload, u32le(1111)...)
	payload = append(payload, u32le(2222)...)
	payload = append(payload, 0) // not privileged

	msg := buildMsg(ServerConnectToPeer, payload)

	resp, err := ParseConnectToPeerResponse(msg)
	if err != nil {
		t.Fatal(err)
	}

	if resp.Privileged {
		t.Fatal("expected privileged=false")
	}
	if resp.Type != "F" {
		t.Fatalf("type = %q, want F", resp.Type)
	}
	expectedIP := net.IPv4(1, 2, 3, 4).To4()
	if !resp.IP.Equal(expectedIP) {
		t.Fatalf("IP = %v, want %v", resp.IP, expectedIP)
	}
}

func TestParseConnectToPeerResponseTruncated(t *testing.T) {
	var payload []byte
	payload = appendStr(payload, "user")
	msg := buildMsg(ServerConnectToPeer, payload)

	_, err := ParseConnectToPeerResponse(msg)
	if err == nil {
		t.Fatal("expected error for truncated response")
	}
}

// ---------- ParseServerSearchRequest ----------

func TestParseServerSearchRequest(t *testing.T) {
	var payload []byte
	payload = appendStr(payload, "searchuser")
	payload = append(payload, u32le(99999)...) // token
	payload = appendStr(payload, "some music query")

	msg := buildMsg(ServerEmbeddedMessage, payload)

	resp, err := ParseServerSearchRequest(msg)
	if err != nil {
		t.Fatalf("ParseServerSearchRequest: %v", err)
	}

	if resp.Username != "searchuser" {
		t.Fatalf("username = %q", resp.Username)
	}
	if resp.Token != 99999 {
		t.Fatalf("token = %d, want 99999", resp.Token)
	}
	if resp.Query != "some music query" {
		t.Fatalf("query = %q", resp.Query)
	}
}

func TestParseServerSearchRequestTruncated(t *testing.T) {
	var payload []byte
	payload = appendStr(payload, "user")
	msg := buildMsg(ServerEmbeddedMessage, payload)

	_, err := ParseServerSearchRequest(msg)
	if err == nil {
		t.Fatal("expected error for truncated search request")
	}
}

// ---------- IP reversal edge cases ----------

func TestIPReversalAllZeroes(t *testing.T) {
	var payload []byte
	payload = append(payload, 1) // success
	payload = appendStr(payload, "hi")
	payload = append(payload, 0, 0, 0, 0) // 0.0.0.0 reversed is still 0.0.0.0
	payload = appendStr(payload, "hash")
	payload = append(payload, 0)

	msg := buildMsg(ServerLogin, payload)

	resp, err := ParseLoginResponse(msg)
	if err != nil {
		t.Fatal(err)
	}

	expectedIP := net.IPv4(0, 0, 0, 0).To4()
	if !resp.IP.Equal(expectedIP) {
		t.Fatalf("IP = %v, want %v", resp.IP, expectedIP)
	}
}

func TestIPReversalMaxValues(t *testing.T) {
	var payload []byte
	payload = append(payload, 1)
	payload = appendStr(payload, "hi")
	payload = append(payload, 255, 255, 255, 255) // 255.255.255.255
	payload = appendStr(payload, "hash")
	payload = append(payload, 0)

	msg := buildMsg(ServerLogin, payload)

	resp, err := ParseLoginResponse(msg)
	if err != nil {
		t.Fatal(err)
	}

	expectedIP := net.IPv4(255, 255, 255, 255).To4()
	if !resp.IP.Equal(expectedIP) {
		t.Fatalf("IP = %v, want %v", resp.IP, expectedIP)
	}
}
