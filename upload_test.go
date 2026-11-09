package slsk

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/kalmarv/slsk/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// uploadPeer is the far end of an upload: it hands out one P and one F
// connection and plays the downloading peer on them.
type uploadPeer struct {
	p, f       *peerConn
	pFar, fFar *protocol.Conn
	fToken     uint32
}

func newUploadPeer(t *testing.T) *uploadPeer {
	p, pFar := tcpPeerConn(t, "alice")
	f, fFar := tcpPeerConn(t, "alice")
	return &uploadPeer{p: p, f: f, pFar: pFar, fFar: fFar}
}

func (u *uploadPeer) ConnectToPeer(context.Context, string, string) (*peerConn, error) {
	return u.p, nil
}

func (u *uploadPeer) ConnectToPeerWithToken(_ context.Context, _, _ string, token uint32) (*peerConn, error) {
	u.fToken = token
	return u.f, nil
}

// accept reads the TransferRequest and answers it through ups, as the peer
// manager would deliver the peer's TransferResponse.
func (u *uploadPeer) accept(t *testing.T, ups *uploads, allowed bool) *protocol.TransferRequest {
	t.Helper()
	req, err := protocol.ParseTransferRequest(readMsg(t, u.pFar))
	require.NoError(t, err)
	var msg []byte
	if allowed {
		msg = protocol.BuildTransferResponseAllowed(req.Token)
	} else {
		msg = protocol.BuildTransferResponseDenied(req.Token, "Queued")
	}
	resp, err := protocol.ParseTransferResponse(msg)
	require.NoError(t, err)
	require.True(t, ups.deliver(resp))
	return req
}

// receive reads the token, asks for the file from offset, and reads n bytes.
func (u *uploadPeer) receive(t *testing.T, offset uint64, n int) (token uint32, data []byte) {
	t.Helper()
	raw := u.fFar.Raw()
	_ = raw.SetReadDeadline(time.Now().Add(2 * time.Second))
	var tok [4]byte
	_, err := io.ReadFull(raw, tok[:])
	require.NoError(t, err)
	require.NoError(t, binary.Write(raw, binary.LittleEndian, offset))
	data = make([]byte, n)
	_, err = io.ReadFull(raw, data)
	require.NoError(t, err)
	return binary.LittleEndian.Uint32(tok[:]), data
}

func TestUpload_SendsFileFromRequestedOffset(t *testing.T) {
	peer := newUploadPeer(t)
	ups := newUploadsWith(testLogger, peer)
	content := bytes.Repeat([]byte("0123456789"), 20_000) // 200 KB, several chunks

	var last [2]int64
	errCh := make(chan error, 1)
	go func() {
		errCh <- ups.upload(context.Background(), "alice", `Music\a.flac`, bytes.NewReader(content), int64(len(content)), func(sent, size int64) {
			last = [2]int64{sent, size}
		})
	}()

	req := peer.accept(t, ups, true)
	assert.Equal(t, protocol.TransferUpload, req.Direction)
	assert.Equal(t, `Music\a.flac`, req.Filename)
	assert.Equal(t, uint64(len(content)), req.FileSize)

	const offset = 1234
	token, data := peer.receive(t, offset, len(content)-offset)
	require.NoError(t, <-errCh)
	assert.Equal(t, req.Token, token, "F connection carries the transfer token")
	assert.Equal(t, req.Token, peer.fToken)
	assert.Equal(t, content[offset:], data)
	assert.Equal(t, [2]int64{int64(len(content)), int64(len(content))}, last)
}

func TestUpload_DeniedByPeer(t *testing.T) {
	peer := newUploadPeer(t)
	ups := newUploadsWith(testLogger, peer)

	errCh := make(chan error, 1)
	go func() {
		errCh <- ups.upload(context.Background(), "alice", "f", bytes.NewReader(nil), 0, nil)
	}()
	peer.accept(t, ups, false)

	var denied *DeniedError
	require.ErrorAs(t, <-errCh, &denied)
	assert.Equal(t, "Queued", denied.Reason)
}

// failingReader fails after the first read.
type failingReader struct{ *bytes.Reader }

func (f failingReader) Read(p []byte) (int, error) {
	if int64(f.Reader.Len()) < f.Reader.Size() {
		return 0, errors.New("disk gone")
	}
	return f.Reader.Read(p[:1])
}

func TestUpload_SourceErrorIsMarked(t *testing.T) {
	peer := newUploadPeer(t)
	ups := newUploadsWith(testLogger, peer)
	src := failingReader{bytes.NewReader([]byte("abc"))}

	errCh := make(chan error, 1)
	go func() { errCh <- ups.upload(context.Background(), "alice", "f", src, 3, nil) }()
	peer.accept(t, ups, true)
	peer.receive(t, 0, 1)

	assert.ErrorIs(t, <-errCh, ErrUploadSource)
}

func TestUpload_CancelUnblocksWait(t *testing.T) {
	peer := newUploadPeer(t)
	ups := newUploadsWith(testLogger, peer)
	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() { errCh <- ups.upload(ctx, "alice", "f", bytes.NewReader(nil), 0, nil) }()
	readMsg(t, peer.pFar) // the request went out; never answer it
	cancel()

	select {
	case err := <-errCh:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("upload did not return after cancellation")
	}
	ups.mu.Lock()
	defer ups.mu.Unlock()
	assert.Empty(t, ups.pending, "the token is unregistered")
}
