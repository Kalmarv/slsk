package slsk

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/kalmarv/slsk/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeShares struct{ files []File }

func (f *fakeShares) Search(query string) []File {
	q := ParseQuery(query)
	var out []File
	for _, file := range f.files {
		if !q.Empty() && q.Match(Words(file.Name)) {
			out = append(out, file)
		}
	}
	return out
}

func (f *fakeShares) Counts() (int, int) { return 1, len(f.files) }

type fakeQueue struct {
	mu       sync.Mutex
	requests []UploadRequest
	position int
	err      error
	free     bool
	queued   int
}

func (f *fakeQueue) Enqueue(req UploadRequest) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, req)
	return f.position, f.err
}

func (f *fakeQueue) Position(string, string) int { return f.position }

func (f *fakeQueue) SlotState() (bool, int) { return f.free, f.queued }

// tcpPeerConn returns a peerConn over loopback TCP (so RemoteAddr is a real
// address) and the peer's end.
func tcpPeerConn(t *testing.T, username string) (*peerConn, *protocol.Conn) {
	t.Helper()
	local, remote := tcpPair(t)
	return &peerConn{Conn: protocol.WrapPeerConn(local), Username: username, Type: protocol.ConnTypePeer},
		protocol.WrapPeerConn(remote)
}

func readMsg(t *testing.T, c *protocol.Conn) []byte {
	t.Helper()
	_ = c.Raw().SetReadDeadline(time.Now().Add(2 * time.Second))
	msg, err := c.ReadMessage()
	require.NoError(t, err)
	return msg
}

func newTestSharing(cfg Config, peers peerConnector) *sharing {
	s := &sharing{
		log:       testLogger,
		username:  "me",
		shares:    cfg.Shares,
		queue:     cfg.Uploads,
		speed:     cfg.UploadSpeed,
		peers:     peers,
		sendSlots: make(chan struct{}, maxSearchReplies),
	}
	return s
}

func TestBuildSearchReply_RoundTripsFilesAndSlotState(t *testing.T) {
	files := []File{
		{Name: `Music\A\01.flac`, Size: 30_000_000, Bitrate: 900, Duration: 200, SampleRate: 44100, BitDepth: 16},
		{Name: `Music\A\02.mp3`, Size: 5_000_000, Bitrate: 245, VBR: true},
	}
	msg, err := buildSearchReply("me", 7, files, false, 3, 1<<20)
	require.NoError(t, err)

	got, err := protocol.ParseSearchResponse(msg)
	require.NoError(t, err)
	assert.Equal(t, "me", got.Username)
	assert.Equal(t, uint32(7), got.Token)
	assert.False(t, got.FreeUploadSlot)
	assert.Equal(t, uint32(3), got.QueueLength)
	assert.Equal(t, uint32(1<<20), got.UploadSpeed)
	require.Len(t, got.Files, 2)
	assert.Equal(t, "flac", got.Files[0].Extension)
	assert.Equal(t, "mp3", got.Files[1].Extension)
	for i := range files {
		assert.Equal(t, files[i], fileFromWire(got.Files[i]), "file %d survives the wire", i)
	}
}

func TestAnswer_RepliesToSearcherWithMatches(t *testing.T) {
	pc, far := tcpPeerConn(t, "searcher")
	connector := &fakePeerConnector{pc: pc}
	queue := &fakeQueue{free: true}
	s := newTestSharing(Config{
		Shares:      &fakeShares{files: []File{{Name: `Music\Frank Ocean\Blonde\Nikes.flac`, Size: 1}, {Name: `Music\Other\x.flac`, Size: 2}}},
		Uploads:     queue,
		UploadSpeed: 1000,
	}, connector)

	s.answer(context.Background(), &protocol.IncomingSearchRequest{Username: "searcher", Token: 42, Query: "*rank ocean"})

	got, err := protocol.ParseSearchResponse(readMsg(t, far))
	require.NoError(t, err)
	assert.Equal(t, uint32(42), got.Token)
	assert.True(t, got.FreeUploadSlot)
	require.Len(t, got.Files, 1)
	assert.Equal(t, `Music\Frank Ocean\Blonde\Nikes.flac`, got.Files[0].Filename)
}

func TestAnswer_NoMatchesOrOwnSearchSendsNothing(t *testing.T) {
	connector := &fakePeerConnector{}
	s := newTestSharing(Config{Shares: &fakeShares{files: []File{{Name: `a\b.flac`}}}}, connector)

	s.answer(context.Background(), &protocol.IncomingSearchRequest{Username: "x", Token: 1, Query: "nothing"})
	s.answer(context.Background(), &protocol.IncomingSearchRequest{Username: "me", Token: 2, Query: "b"})
	time.Sleep(50 * time.Millisecond)
	connector.mu.Lock()
	defer connector.mu.Unlock()
	assert.Zero(t, connector.calls, "no reply connection for an empty match or our own search")
}

func TestAnswer_CapsReplyFiles(t *testing.T) {
	files := make([]File, MaxReplyFiles+10)
	for i := range files {
		files[i] = File{Name: `bulk\f.flac`}
	}
	pc, far := tcpPeerConn(t, "searcher")
	s := newTestSharing(Config{Shares: &fakeShares{files: files}}, &fakePeerConnector{pc: pc})

	s.answer(context.Background(), &protocol.IncomingSearchRequest{Username: "searcher", Token: 1, Query: "bulk"})

	got, err := protocol.ParseSearchResponse(readMsg(t, far))
	require.NoError(t, err)
	assert.Len(t, got.Files, MaxReplyFiles)
}

func TestQueueDownload_QueuedFileGetsPosition(t *testing.T) {
	queue := &fakeQueue{position: 4}
	s := newTestSharing(Config{Uploads: queue}, nil)
	pc, far := tcpPeerConn(t, "alice")

	go s.handleQueueDownload(&peerMessage{PeerConn: pc, Raw: protocol.BuildQueueDownload(`Music\a.flac`)})

	piq, err := protocol.ParsePlaceInQueueResponse(readMsg(t, far))
	require.NoError(t, err)
	assert.Equal(t, `Music\a.flac`, piq.Filename)
	assert.Equal(t, uint32(4), piq.Place)

	queue.mu.Lock()
	defer queue.mu.Unlock()
	require.Len(t, queue.requests, 1)
	assert.Equal(t, "alice", queue.requests[0].Username)
	assert.Equal(t, `Music\a.flac`, queue.requests[0].Filename)
	assert.True(t, queue.requests[0].IP.Equal(net.IPv4(127, 0, 0, 1)), "request carries the peer's IP, got %v", queue.requests[0].IP)
}

func TestQueueDownload_Denials(t *testing.T) {
	for name, tc := range map[string]struct {
		queue  UploadQueue
		reason string
	}{
		"no upload queue": {nil, protocol.DenyFileNotShared},
		"not shared":      {&fakeQueue{err: ErrNotShared}, protocol.DenyFileNotShared},
		"wrapped":         {&fakeQueue{err: errors.Join(errors.New("x"), ErrNotShared)}, protocol.DenyFileNotShared},
		"other error":     {&fakeQueue{err: errors.New("database is locked")}, "database is locked"},
	} {
		t.Run(name, func(t *testing.T) {
			s := newTestSharing(Config{Uploads: tc.queue}, nil)
			pc, far := tcpPeerConn(t, "alice")

			go s.handleQueueDownload(&peerMessage{PeerConn: pc, Raw: protocol.BuildQueueDownload("f")})

			r, err := protocol.NewReader(readMsg(t, far))
			require.NoError(t, err)
			require.Equal(t, protocol.PeerUploadDenied, r.Code())
			file, _ := r.ReadString()
			reason, _ := r.ReadString()
			assert.Equal(t, "f", file)
			assert.Equal(t, tc.reason, reason)
		})
	}
}
