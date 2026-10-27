package slsk

import (
	"crypto/md5"
	"fmt"
	"net"
)

// ---------------------------------------------------------------------------
// Response structs
// ---------------------------------------------------------------------------

// LoginResponse is the parsed response to a login request.
type LoginResponse struct {
	Success     bool
	Greeting    string
	IP          net.IP
	Hash        string
	IsSupporter bool
	Reason      string // populated only on failure
}

// PeerAddressResponse is the parsed response to a GetPeerAddress request.
type PeerAddressResponse struct {
	Username string
	IP       net.IP
	Port     uint32
}

// ConnectToPeerResponse is the parsed response/notification for ConnectToPeer.
type ConnectToPeerResponse struct {
	Username   string
	Type       string
	IP         net.IP
	Port       uint32
	Token      uint32
	Privileged bool
}

// UserStatusResponse is the parsed response to a GetUserStatus request (code 7).
// Status values: 0=offline, 1=away, 2=online.
type UserStatusResponse struct {
	Username   string
	Status     uint32
	Privileged bool
}

// WatchUserResponse is the parsed response to a WatchUser request (code 5).
// If Exists is false, only that field is populated. If Exists is true, all
// remaining fields are present: Username, Status (0=offline, 1=away, 2=online),
// AvgSpeed, UploadNum, Files, Dirs.
type WatchUserResponse struct {
	Exists    bool
	Username  string
	Status    uint32
	AvgSpeed  uint32
	UploadNum uint64
	Files     uint32
	Dirs      uint32
}

// IncomingSearchRequest is a search request distributed by the server (code 93).
type IncomingSearchRequest struct {
	Username string
	Token    uint32
	Query    string
}

// PotentialParent is a candidate distributed parent from NetInfo (code 102).
type PotentialParent struct {
	Username string
	IP       net.IP
	Port     uint32
}

// ---------------------------------------------------------------------------
// IP helpers
// ---------------------------------------------------------------------------

// readReversedIP reads 4 bytes from the reader and returns them as a net.IP
// with the byte order reversed. On the wire, 192.168.1.1 is sent as [1,1,168,192].
func readReversedIP(r *MessageReader) (net.IP, error) {
	raw, err := r.ReadBytes(4)
	if err != nil {
		return nil, fmt.Errorf("read IP: %w", err)
	}
	return net.IPv4(raw[3], raw[2], raw[1], raw[0]).To4(), nil
}

// ---------------------------------------------------------------------------
// Build functions — client → server
// ---------------------------------------------------------------------------

// BuildLoginRequest builds a login request message (code 1).
// Fields: username, password, version (major), MD5 hash of username+password, minorVersion.
func BuildLoginRequest(username, password string, majorVersion, minorVersion uint32) []byte {
	hash := fmt.Sprintf("%x", md5.Sum([]byte(username+password)))

	b := NewBuilder(ServerLogin)
	b.WriteString(username)
	b.WriteString(password)
	b.WriteUint32(majorVersion)
	b.WriteString(hash)
	b.WriteUint32(minorVersion)
	return b.Build()
}

// BuildFileSearchRequest builds a file search request (code 26).
// Fields: token, query.
func BuildFileSearchRequest(token uint32, query string) []byte {
	b := NewBuilder(ServerFileSearch)
	b.WriteUint32(token)
	b.WriteString(query)
	return b.Build()
}

// BuildGetPeerAddressRequest builds a GetPeerAddress request (code 3).
// Fields: username.
func BuildGetPeerAddressRequest(username string) []byte {
	b := NewBuilder(ServerGetPeerAddress)
	b.WriteString(username)
	return b.Build()
}

// BuildWatchUserRequest builds a WatchUser request (code 5).
// Fields: username.
func BuildWatchUserRequest(username string) []byte {
	b := NewBuilder(ServerWatchUser)
	b.WriteString(username)
	return b.Build()
}

// BuildGetUserStatusRequest builds a GetUserStatus request (code 7).
// Fields: username.
func BuildGetUserStatusRequest(username string) []byte {
	b := NewBuilder(ServerGetStatus)
	b.WriteString(username)
	return b.Build()
}

// BuildSetListenPort builds a SetListenPort message (code 2).
// Fields: port.
func BuildSetListenPort(port uint32) []byte {
	b := NewBuilder(ServerSetListenPort)
	b.WriteUint32(port)
	return b.Build()
}

// BuildSharedFoldersFiles builds a SharedFoldersFiles message (code 35).
// Fields: dirs, files.
func BuildSharedFoldersFiles(dirs, files uint32) []byte {
	b := NewBuilder(ServerSharedFoldersFiles)
	b.WriteUint32(dirs)
	b.WriteUint32(files)
	return b.Build()
}

// BuildCheckPrivileges builds a CheckPrivileges message (code 92).
func BuildCheckPrivileges() []byte {
	b := NewBuilder(ServerCheckPrivileges)
	return b.Build()
}

// BuildSetStatus builds a SetStatus message (code 28).
// Status values: 1 = Away, 2 = Online.
func BuildSetStatus(status uint32) []byte {
	b := NewBuilder(ServerSetStatus)
	b.WriteUint32(status)
	return b.Build()
}

// BuildHaveNoParent builds a HaveNoParent message (code 71).
// Tells the server this client has no parent in the distributed search tree.
// The server will then send search requests directly via EmbeddedMessage (code 93).
func BuildHaveNoParent(noParent bool) []byte {
	b := NewBuilder(ServerHaveNoParent)
	b.WriteBool(noParent)
	return b.Build()
}

// BuildBranchLevel builds a BranchLevel message (code 126).
func BuildBranchLevel(level uint32) []byte {
	b := NewBuilder(ServerBranchLevel)
	b.WriteUint32(level)
	return b.Build()
}

// BuildBranchRoot builds a BranchRoot message (code 127).
func BuildBranchRoot(username string) []byte {
	b := NewBuilder(ServerBranchRoot)
	b.WriteString(username)
	return b.Build()
}

// BuildAcceptChildren builds an AcceptChildren message (code 100).
func BuildAcceptChildren(accept bool) []byte {
	b := NewBuilder(ServerAcceptChildren)
	b.WriteBool(accept)
	return b.Build()
}

// BuildPing builds a ping message (code 32) with no payload.
func BuildPing() []byte {
	b := NewBuilder(ServerPing)
	return b.Build()
}

// BuildConnectToPeerRequest builds a ConnectToPeer request (code 18).
// Fields: token, username, connType.
func BuildConnectToPeerRequest(token uint32, username, connType string) []byte {
	b := NewBuilder(ServerConnectToPeer)
	b.WriteUint32(token)
	b.WriteString(username)
	b.WriteString(connType)
	return b.Build()
}

// ---------------------------------------------------------------------------
// Parse functions — server → client
// ---------------------------------------------------------------------------

// ParseLoginResponse parses a login response from the server (code 1).
// On success (bool=1): greeting, IP (reversed), hash, isSupporter.
// On failure (bool=0): reason string.
func ParseLoginResponse(msg []byte) (*LoginResponse, error) {
	r, err := NewReader(msg)
	if err != nil {
		return nil, fmt.Errorf("parse login response: %w", err)
	}

	success, err := r.ReadBool()
	if err != nil {
		return nil, fmt.Errorf("parse login response: read success: %w", err)
	}

	resp := &LoginResponse{Success: success}

	if !success {
		reason, err := r.ReadString()
		if err != nil {
			return nil, fmt.Errorf("parse login response: read reason: %w", err)
		}
		resp.Reason = reason
		return resp, nil
	}

	// Success path
	greeting, err := r.ReadString()
	if err != nil {
		return nil, fmt.Errorf("parse login response: read greeting: %w", err)
	}
	resp.Greeting = greeting

	ip, err := readReversedIP(r)
	if err != nil {
		return nil, fmt.Errorf("parse login response: %w", err)
	}
	resp.IP = ip

	hash, err := r.ReadString()
	if err != nil {
		return nil, fmt.Errorf("parse login response: read hash: %w", err)
	}
	resp.Hash = hash

	isSupporter, err := r.ReadBool()
	if err != nil {
		return nil, fmt.Errorf("parse login response: read isSupporter: %w", err)
	}
	resp.IsSupporter = isSupporter

	return resp, nil
}

// ParseGetPeerAddressResponse parses a GetPeerAddress response (code 3).
// Fields: username, IP (reversed), port.
func ParseGetPeerAddressResponse(msg []byte) (*PeerAddressResponse, error) {
	r, err := NewReader(msg)
	if err != nil {
		return nil, fmt.Errorf("parse peer address: %w", err)
	}

	username, err := r.ReadString()
	if err != nil {
		return nil, fmt.Errorf("parse peer address: read username: %w", err)
	}

	ip, err := readReversedIP(r)
	if err != nil {
		return nil, fmt.Errorf("parse peer address: %w", err)
	}

	port, err := r.ReadUint32()
	if err != nil {
		return nil, fmt.Errorf("parse peer address: read port: %w", err)
	}

	return &PeerAddressResponse{
		Username: username,
		IP:       ip,
		Port:     port,
	}, nil
}

// ParseGetUserStatusResponse parses a GetUserStatus response (code 7).
// Fields: username, status (uint32), privileged (bool).
func ParseGetUserStatusResponse(msg []byte) (*UserStatusResponse, error) {
	r, err := NewReader(msg)
	if err != nil {
		return nil, fmt.Errorf("parse user status: %w", err)
	}

	username, err := r.ReadString()
	if err != nil {
		return nil, fmt.Errorf("parse user status: read username: %w", err)
	}

	status, err := r.ReadUint32()
	if err != nil {
		return nil, fmt.Errorf("parse user status: read status: %w", err)
	}

	privileged, err := r.ReadBool()
	if err != nil {
		return nil, fmt.Errorf("parse user status: read privileged: %w", err)
	}

	return &UserStatusResponse{
		Username:   username,
		Status:     status,
		Privileged: privileged,
	}, nil
}

// ParseWatchUserResponse parses a WatchUser response (code 5).
// If exists=false, returns early with just Exists=false.
// If exists=true, reads: username, status, avgspeed, uploadnum, files, dirs.
func ParseWatchUserResponse(msg []byte) (*WatchUserResponse, error) {
	r, err := NewReader(msg)
	if err != nil {
		return nil, fmt.Errorf("parse watch user: %w", err)
	}

	// Server echoes username first, then exists flag.
	username, err := r.ReadString()
	if err != nil {
		return nil, fmt.Errorf("parse watch user: read username: %w", err)
	}

	exists, err := r.ReadBool()
	if err != nil {
		return nil, fmt.Errorf("parse watch user: read exists: %w", err)
	}

	resp := &WatchUserResponse{Username: username, Exists: exists}

	if !exists {
		return resp, nil
	}

	status, err := r.ReadUint32()
	if err != nil {
		return nil, fmt.Errorf("parse watch user: read status: %w", err)
	}
	resp.Status = status

	avgSpeed, err := r.ReadUint32()
	if err != nil {
		return nil, fmt.Errorf("parse watch user: read avgspeed: %w", err)
	}
	resp.AvgSpeed = avgSpeed

	uploadNum, err := r.ReadUint32()
	if err != nil {
		return nil, fmt.Errorf("parse watch user: read uploadnum: %w", err)
	}
	resp.UploadNum = uint64(uploadNum)

	// Reserved/unknown uint32 that follows uploadnum on the wire.
	if _, err := r.ReadUint32(); err != nil {
		return nil, fmt.Errorf("parse watch user: read reserved: %w", err)
	}

	files, err := r.ReadUint32()
	if err != nil {
		return nil, fmt.Errorf("parse watch user: read files: %w", err)
	}
	resp.Files = files

	dirs, err := r.ReadUint32()
	if err != nil {
		return nil, fmt.Errorf("parse watch user: read dirs: %w", err)
	}
	resp.Dirs = dirs

	return resp, nil
}

// ParseConnectToPeerResponse parses a ConnectToPeer response (code 18).
// Fields: username, type, IP (reversed), port, token, privileged.
func ParseConnectToPeerResponse(msg []byte) (*ConnectToPeerResponse, error) {
	r, err := NewReader(msg)
	if err != nil {
		return nil, fmt.Errorf("parse connect to peer: %w", err)
	}

	username, err := r.ReadString()
	if err != nil {
		return nil, fmt.Errorf("parse connect to peer: read username: %w", err)
	}

	connType, err := r.ReadString()
	if err != nil {
		return nil, fmt.Errorf("parse connect to peer: read type: %w", err)
	}

	ip, err := readReversedIP(r)
	if err != nil {
		return nil, fmt.Errorf("parse connect to peer: %w", err)
	}

	port, err := r.ReadUint32()
	if err != nil {
		return nil, fmt.Errorf("parse connect to peer: read port: %w", err)
	}

	token, err := r.ReadUint32()
	if err != nil {
		return nil, fmt.Errorf("parse connect to peer: read token: %w", err)
	}

	privileged, err := r.ReadBool()
	if err != nil {
		return nil, fmt.Errorf("parse connect to peer: read privileged: %w", err)
	}

	return &ConnectToPeerResponse{
		Username:   username,
		Type:       connType,
		IP:         ip,
		Port:       port,
		Token:      token,
		Privileged: privileged,
	}, nil
}

// ParseServerSearchRequest parses an incoming distributed search request
// from the server (code 93 = EmbeddedMessage).
// Wire format: [server header][uint8 distrib_code=3][uint32 identifier=49][string username][uint32 token][string query]
func ParseServerSearchRequest(msg []byte) (*IncomingSearchRequest, error) {
	r, err := NewReader(msg)
	if err != nil {
		return nil, fmt.Errorf("parse server search: %w", err)
	}

	// Read and validate the distributed message code (should be 3 = DistribSearchRequest).
	distribCode, err := r.ReadUint8()
	if err != nil {
		return nil, fmt.Errorf("parse server search: read distrib code: %w", err)
	}
	if distribCode != DistribSearchRequest {
		return nil, fmt.Errorf("parse server search: unexpected distrib code %d, want %d", distribCode, DistribSearchRequest)
	}

	// Read and validate the identifier (should be 49).
	identifier, err := r.ReadUint32()
	if err != nil {
		return nil, fmt.Errorf("parse server search: read identifier: %w", err)
	}
	if identifier != distribSearchIdentifier {
		return nil, fmt.Errorf("parse server search: unexpected identifier %d, want %d", identifier, distribSearchIdentifier)
	}

	username, err := r.ReadString()
	if err != nil {
		return nil, fmt.Errorf("parse server search: read username: %w", err)
	}

	token, err := r.ReadUint32()
	if err != nil {
		return nil, fmt.Errorf("parse server search: read token: %w", err)
	}

	query, err := r.ReadString()
	if err != nil {
		return nil, fmt.Errorf("parse server search: read query: %w", err)
	}

	return &IncomingSearchRequest{
		Username: username,
		Token:    token,
		Query:    query,
	}, nil
}

// ParseNetInfo parses a NetInfo/PossibleParents response (code 102).
// Wire format: [uint32 count][for each: string username, uint32 IP (reversed), uint32 port]
func ParseNetInfo(msg []byte) ([]PotentialParent, error) {
	r, err := NewReader(msg)
	if err != nil {
		return nil, fmt.Errorf("parse net info: %w", err)
	}

	count, err := r.ReadUint32()
	if err != nil {
		return nil, fmt.Errorf("parse net info: read count: %w", err)
	}

	parents := make([]PotentialParent, 0, min(int(count), 16))
	for i := uint32(0); i < count; i++ {
		username, err := r.ReadString()
		if err != nil {
			return nil, fmt.Errorf("parse net info: read username[%d]: %w", i, err)
		}
		ip, err := readReversedIP(r)
		if err != nil {
			return nil, fmt.Errorf("parse net info: read IP[%d]: %w", i, err)
		}
		port, err := r.ReadUint32()
		if err != nil {
			return nil, fmt.Errorf("parse net info: read port[%d]: %w", i, err)
		}
		parents = append(parents, PotentialParent{
			Username: username,
			IP:       ip,
			Port:     port,
		})
	}

	return parents, nil
}

// ParseCannotConnect parses a CannotConnect message (code 1001).
// Wire format: [4-byte length][4-byte code=1001][4-byte token]
// The token identifies which ConnectToPeer request failed.
func ParseCannotConnect(msg []byte) (uint32, error) {
	r, err := NewReader(msg)
	if err != nil {
		return 0, fmt.Errorf("parse cannot connect: %w", err)
	}

	token, err := r.ReadUint32()
	if err != nil {
		return 0, fmt.Errorf("parse cannot connect: read token: %w", err)
	}
	return token, nil
}

// BuildCantConnectToPeer builds a CantConnectToPeer message (code 1001) telling
// the server we could not fulfil its ConnectToPeer request with this token, so
// it can fail the other side's attempt at once instead of timing out.
func BuildCantConnectToPeer(token uint32, username string) []byte {
	b := NewBuilder(ServerCannotConnect)
	b.WriteUint32(token)
	b.WriteString(username)
	return b.Build()
}

// ParseExcludedSearchPhrases parses an ExcludedSearchPhrases message (code 160),
// sent after login. The server drops searches containing any of these phrases.
// Wire format: [uint32 count][for each: string phrase]
func ParseExcludedSearchPhrases(msg []byte) ([]string, error) {
	r, err := NewReader(msg)
	if err != nil {
		return nil, fmt.Errorf("parse excluded phrases: %w", err)
	}

	count, err := r.ReadUint32()
	if err != nil {
		return nil, fmt.Errorf("parse excluded phrases: read count: %w", err)
	}

	phrases := make([]string, 0, min(int(count), 1024))
	for i := uint32(0); i < count; i++ {
		phrase, err := r.ReadString()
		if err != nil {
			return nil, fmt.Errorf("parse excluded phrases: phrase[%d]: %w", i, err)
		}
		phrases = append(phrases, phrase)
	}
	return phrases, nil
}
