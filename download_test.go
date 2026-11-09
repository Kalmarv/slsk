package slsk

import (
	"bytes"
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/kalmarv/slsk/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockPeerConn creates a peerConn wrapping one side of a net.Pipe.
func mockPeerConn(t *testing.T, username, connType string, token uint32) (*peerConn, net.Conn) {
	t.Helper()
	serverSide, clientSide := net.Pipe()
	t.Cleanup(func() {
		serverSide.Close()
		clientSide.Close()
	})
	pc := &peerConn{
		Conn:     protocol.WrapConn(clientSide),
		Username: username,
		Type:     connType,
		Token:    token,
	}
	return pc, serverSide
}

func TestHandleIncomingFileConn_MatchingUsername(t *testing.T) {
	d := newDownloadsWith(testLogger, nil)

	username := "uploader"
	ch := make(chan *peerConn, 1)
	d.mu.Lock()
	d.pendingXfers[username] = append(d.pendingXfers[username], ch)
	d.mu.Unlock()

	pc, _ := mockPeerConn(t, username, protocol.ConnTypeTransfer, 42)

	d.handleIncomingFileConn(pc)

	select {
	case received := <-ch:
		assert.Equal(t, pc, received, "channel should receive the exact PeerConn")
		assert.Equal(t, username, received.Username)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for PeerConn on channel")
	}
}

func TestHandleIncomingFileConn_NoWaiters(t *testing.T) {
	d := newDownloadsWith(testLogger, nil)

	// Use a net.Pipe so we can detect closure.
	clientSide, serverSide := net.Pipe()
	defer serverSide.Close()

	pc := &peerConn{
		Conn:     protocol.WrapConn(clientSide),
		Username: "stranger",
		Type:     protocol.ConnTypeTransfer,
		Token:    999,
	}

	// No waiters registered -- should close the connection.
	d.handleIncomingFileConn(pc)

	// Verify the connection was closed by trying to read from the other side.
	buf := make([]byte, 1)
	serverSide.SetReadDeadline(time.Now().Add(time.Second))
	_, err := serverSide.Read(buf)
	require.Error(t, err, "connection should be closed")
}

func TestWaitForFileConn_Timeout(t *testing.T) {
	d := newDownloadsWith(testLogger, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := d.waitForFileConn(ctx, "someuser")
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	// Verify the pending entry was cleaned up.
	d.mu.Lock()
	_, exists := d.pendingXfers["someuser"]
	d.mu.Unlock()
	assert.False(t, exists, "pending entry should be cleaned up after timeout")
}

func TestWaitForFileConn_Success(t *testing.T) {
	d := newDownloadsWith(testLogger, nil)

	username := "sender"
	pc, _ := mockPeerConn(t, username, protocol.ConnTypeTransfer, 7777)

	// Deliver the connection in a goroutine.
	go func() {
		time.Sleep(10 * time.Millisecond)
		d.mu.Lock()
		waiters := d.pendingXfers[username]
		if len(waiters) > 0 {
			waiters[0] <- pc
		}
		d.mu.Unlock()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, err := d.waitForFileConn(ctx, username)
	require.NoError(t, err)
	assert.Equal(t, pc, result)

	// Verify cleanup.
	d.mu.Lock()
	_, exists := d.pendingXfers[username]
	d.mu.Unlock()
	assert.False(t, exists, "pending entry should be cleaned up after success")
}

// readFrame reads one framed message from the far side of a pipe.
func readFrame(t *testing.T, c net.Conn) []byte {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	return mustReadMessage(t, protocol.WrapConn(c))
}

func mustReadMessage(t *testing.T, c *protocol.Conn) []byte {
	t.Helper()
	msg, err := c.ReadMessage()
	require.NoError(t, err)
	return msg
}

func uploadRequest(pc *peerConn, token uint32, file string) *peerMessage {
	return &peerMessage{PeerConn: pc, Raw: protocol.BuildTransferRequest(protocol.TransferUpload, token, file, 100)}
}

func TestCopyTransfer(t *testing.T) {
	ctx := context.Background()
	data := bytes.Repeat([]byte("x"), 1000)

	t.Run("complete", func(t *testing.T) {
		var out bytes.Buffer
		n, err := copyTransfer(ctx, bytes.NewReader(data), &out, 1000, nil)
		require.NoError(t, err)
		assert.Equal(t, int64(1000), n)
	})

	t.Run("EOF before expected size is a retryable error", func(t *testing.T) {
		var out bytes.Buffer
		n, err := copyTransfer(ctx, bytes.NewReader(data[:600]), &out, 1000, nil)
		require.Error(t, err)
		assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
		assert.Equal(t, int64(600), n)
	})

	t.Run("empty file", func(t *testing.T) {
		var out bytes.Buffer
		n, err := copyTransfer(ctx, bytes.NewReader(nil), &out, 0, nil)
		require.NoError(t, err)
		assert.Zero(t, n)
	})

	t.Run("cancelled", func(t *testing.T) {
		cctx, cancel := context.WithCancel(ctx)
		cancel()
		_, err := copyTransfer(cctx, bytes.NewReader(data), io.Discard, 1000, nil)
		assert.ErrorIs(t, err, context.Canceled)
	})
}

func TestTransferRequestForUnknownFileIsDenied(t *testing.T) {
	d := newDownloadsWith(testLogger, nil)
	pc, far := mockPeerConn(t, "stranger", protocol.ConnTypePeer, 0)

	go d.handleTransferRequest(uploadRequest(pc, 77, "never\\asked.flac"))

	resp, err := protocol.ParseTransferResponse(readFrame(t, far))
	require.NoError(t, err)
	assert.Equal(t, uint32(77), resp.Token)
	assert.False(t, resp.Allowed, "unknown request must be refused, not left hanging")

	d.mu.Lock()
	defer d.mu.Unlock()
	assert.Empty(t, d.earlyTransfers, "unsolicited requests must not be buffered")
}

func TestTransferRequestFromOtherUserIsNotDelivered(t *testing.T) {
	d := newDownloadsWith(testLogger, nil)
	pd := d.register(pendingKey{"alice", "a.flac"})
	pc, far := mockPeerConn(t, "mallory", protocol.ConnTypePeer, 0)

	go d.handleTransferRequest(uploadRequest(pc, 5, "a.flac"))

	resp, err := protocol.ParseTransferResponse(readFrame(t, far))
	require.NoError(t, err)
	assert.False(t, resp.Allowed)
	assert.Empty(t, pd.ch, "a different user must not hijack alice's pending download")
}

func TestTransferRequestDeliveredAndDuplicateDoesNotBlock(t *testing.T) {
	d := newDownloadsWith(testLogger, nil)
	pd := d.register(pendingKey{"alice", "a.flac"})
	pc, far := mockPeerConn(t, "alice", protocol.ConnTypePeer, 0)

	done := make(chan struct{})
	go func() {
		d.handleTransferRequest(uploadRequest(pc, 1, "a.flac"))
		d.handleTransferRequest(uploadRequest(pc, 2, "a.flac")) // channel already full
		close(done)
	}()

	// The duplicate is refused on the wire; reading it unblocks the pipe write.
	resp, err := protocol.ParseTransferResponse(readFrame(t, far))
	require.NoError(t, err)
	assert.Equal(t, uint32(2), resp.Token)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handler blocked on a full channel")
	}
	assert.Equal(t, uint32(1), (<-pd.ch).req.Token)
}

func TestEarlyTransferHeldForRetryThenExpires(t *testing.T) {
	d := newDownloadsWith(testLogger, nil)
	d.register(pendingKey{"alice", "other.flac"}) // alice has a download in progress
	pc, far := mockPeerConn(t, "alice", protocol.ConnTypePeer, 0)

	d.handleTransferRequest(uploadRequest(pc, 9, "between-attempts.flac"))
	key := pendingKey{"alice", "between-attempts.flac"}
	d.mu.Lock()
	require.Contains(t, d.earlyTransfers, key)
	d.mu.Unlock()

	// The next attempt registers and receives the held request.
	pd := d.register(key)
	require.Len(t, pd.ch, 1)
	assert.Equal(t, uint32(9), (<-pd.ch).req.Token)

	// A held request nobody claims is pruned and refused after the TTL.
	d.mu.Lock()
	d.earlyTransfers[pendingKey{"alice", "stale.flac"}] = earlyTransfer{
		res: &transferResult{req: &protocol.TransferRequest{Token: 11}, conn: pc.Conn},
		at:  time.Now().Add(-2 * earlyTransferTTL),
	}
	d.mu.Unlock()
	go d.handleTransferRequest(uploadRequest(pc, 12, "other.flac"))
	resp, err := protocol.ParseTransferResponse(readFrame(t, far))
	require.NoError(t, err)
	assert.Equal(t, uint32(11), resp.Token)
	assert.False(t, resp.Allowed)
	d.mu.Lock()
	assert.NotContains(t, d.earlyTransfers, pendingKey{"alice", "stale.flac"})
	d.mu.Unlock()
}

func TestDeniedSignalIsKeyedByUser(t *testing.T) {
	d := newDownloadsWith(testLogger, nil)
	alice := d.register(pendingKey{"alice", "a.flac"})

	d.signalPendingDenied("mallory", "a.flac", "x")
	assert.Empty(t, alice.ch)

	d.signalPendingDenied("alice", "a.flac", uploadFailedReason)
	d.signalPendingDenied("alice", "a.flac", uploadFailedReason) // must not block
	require.Len(t, alice.ch, 1)
	assert.Equal(t, uploadFailedReason, (<-alice.ch).reason)
}

func TestQueueWaitSurvivesWhilePeerReportsPosition(t *testing.T) {
	d := newDownloadsWith(testLogger, nil)
	d.queueStall = 100 * time.Millisecond
	d.pollInterval = 10 * time.Millisecond
	key := pendingKey{"alice", "a.flac"}
	pd := d.register(key)
	pc, _ := mockPeerConn(t, "alice", protocol.ConnTypePeer, 0)

	// Position updates keep arriving for 3x the stall timeout, then the
	// peer starts the upload.
	go func() {
		for i := 0; i < 30; i++ {
			time.Sleep(10 * time.Millisecond)
			d.handlePlaceInQueue(&peerMessage{PeerConn: pc, Raw: protocol.BuildPlaceInQueueResponse("a.flac", uint32(30-i))})
		}
		pd.ch <- &transferResult{req: &protocol.TransferRequest{Token: 3}, conn: pc.Conn}
	}()

	xfer, err := d.awaitTransferRequest(context.Background(), key, pd)
	require.NoError(t, err, "a peer that reports its queue position must not time out")
	assert.Equal(t, uint32(3), xfer.req.Token)
}

func TestQueueWaitStallsOutWithoutSignal(t *testing.T) {
	d := newDownloadsWith(testLogger, nil)
	d.queueStall = 50 * time.Millisecond
	d.pollInterval = 10 * time.Millisecond
	key := pendingKey{"alice", "a.flac"}
	pd := d.register(key)

	_, err := d.awaitTransferRequest(context.Background(), key, pd)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "timeout waiting for TransferRequest")
}

type fakePeerConnector struct {
	mu    sync.Mutex
	calls int
	pc    *peerConn
}

func (f *fakePeerConnector) ConnectToPeer(ctx context.Context, username, connType string) (*peerConn, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.pc, nil
}

func TestQueueFilesSendsEveryFileOnOneConnection(t *testing.T) {
	pc, far := mockPeerConn(t, "alice", protocol.ConnTypePeer, 0)
	fc := &fakePeerConnector{pc: pc}
	d := newDownloadsWith(testLogger, fc)

	files := []string{`m\a\1.flac`, `m\a\2.flac`, `m\a\3.flac`}
	errCh := make(chan error, 1)
	go func() { errCh <- d.queue(context.Background(), "alice", files) }()

	for _, want := range files {
		got, err := protocol.ParseQueueDownload(readFrame(t, far))
		require.NoError(t, err)
		assert.Equal(t, want, got)
	}
	require.NoError(t, <-errCh)
	assert.Equal(t, 1, fc.calls)

	d.mu.Lock()
	for _, f := range files {
		assert.True(t, d.pendingDownloads[pendingKey{"alice", f}].queued, f)
	}
	d.mu.Unlock()

	// Releasing forgets the files and closes the queue connection.
	d.release("alice", files)
	d.mu.Lock()
	assert.Empty(t, d.pendingDownloads)
	assert.Empty(t, d.queueConns)
	d.mu.Unlock()
}

func TestAwaitTransferRequest_DenialIsTyped(t *testing.T) {
	d := newDownloadsWith(testLogger, nil)
	key := pendingKey{"alice", "a.flac"}
	pd := d.register(key)
	d.signalPendingDenied("alice", "a.flac", "File not shared.")

	_, err := d.awaitTransferRequest(context.Background(), key, pd)
	var denied *DeniedError
	require.ErrorAs(t, err, &denied)
	assert.Equal(t, "File not shared.", denied.Reason)
	assert.True(t, denied.Permanent())
}

func TestDeniedErrorPermanent(t *testing.T) {
	for reason, want := range map[string]bool{
		"File not shared.":   true,
		"Banned":             true,
		"Too many files":     true,
		"Too many megabytes": true,
		"File read error.":   true,
		uploadFailedReason:   false,
		"Cancelled":          false,
		"Queue full":         false,
		"":                   false,
	} {
		assert.Equal(t, want, (&DeniedError{Reason: reason}).Permanent(), reason)
	}
}
