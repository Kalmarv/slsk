package slsk

// Server message codes (4-byte LE code in server↔client messages).
const (
	ServerLogin              uint32 = 1
	ServerSetListenPort      uint32 = 2
	ServerGetPeerAddress     uint32 = 3
	ServerWatchUser          uint32 = 5
	ServerGetStatus          uint32 = 7
	ServerConnectToPeer      uint32 = 18
	ServerFileSearch         uint32 = 26
	ServerSetStatus          uint32 = 28
	ServerPing               uint32 = 32
	ServerSharedFoldersFiles uint32 = 35
	ServerEmbeddedMessage    uint32 = 93
	ServerRoomList           uint32 = 64
	ServerPrivilegedUsers    uint32 = 69
	ServerHaveNoParent       uint32 = 71
	ServerCheckPrivileges    uint32 = 92
	ServerParentMinSpeed     uint32 = 83
	ServerParentSpeedRatio   uint32 = 84
	ServerAcceptChildren     uint32 = 100
	ServerNetInfo            uint32 = 102
	ServerBranchLevel        uint32 = 126
	ServerBranchRoot         uint32 = 127
	ServerWishlistInterval   uint32 = 104
	ServerExcludedPhrases    uint32 = 160
	ServerCannotConnect      uint32 = 1001
)

// Peer message codes (4-byte LE code in peer↔peer messages).
const (
	PeerSearchResponse       uint32 = 9
	PeerTransferRequest      uint32 = 40
	PeerTransferResponse     uint32 = 41
	PeerQueueDownload        uint32 = 43
	PeerPlaceInQueueResponse uint32 = 44
	PeerUploadFailed         uint32 = 46
	PeerUploadDenied         uint32 = 50
	PeerPlaceInQueueRequest  uint32 = 51
)

// Distributed message codes (1-byte code).
const (
	DistribSearchRequest uint8 = 3
	DistribBranchLevel   uint8 = 4
	DistribBranchRoot    uint8 = 5
)

// Init message codes (1-byte code, used during connection handshake).
const (
	InitPierceFirewall uint8 = 0
	InitPeerInit       uint8 = 1
)

// Connection type strings sent in PeerInit messages.
const (
	ConnTypePeer        = "P"
	ConnTypeTransfer    = "F"
	ConnTypeDistributed = "D"
)

// Transfer direction constants.
const (
	TransferDownload uint32 = 0
	TransferUpload   uint32 = 1
)

// File attribute codes used in search results and browse responses.
const (
	AttrBitRate    uint32 = 0
	AttrDuration   uint32 = 1
	AttrVBR        uint32 = 2
	AttrSampleRate uint32 = 4
	AttrBitDepth   uint32 = 5
)
