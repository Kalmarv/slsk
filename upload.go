package slsk

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/kalmarv/slsk/protocol"
)

// Upload timing.
const (
	// transferResponseTimeout bounds the wait for the peer to accept or
	// refuse the TransferRequest.
	transferResponseTimeout = 2 * time.Minute
	// uploadConnectTimeout bounds each connection Upload makes to the peer.
	uploadConnectTimeout = 30 * time.Second
	// uploadIOTimeout bounds the F-connection handshake and each chunk
	// written, so a peer that goes silent cannot hold an upload forever.
	uploadIOTimeout = 30 * time.Second
)

// ErrUploadSource wraps errors reading the file being uploaded. Upload tells
// the peer the upload failed on our side, so its client can ask again.
var ErrUploadSource = errors.New("slsk: reading the upload source failed")

// Upload sends a file to username, who asked for it with a request your
// UploadQueue accepted. filename is the name the peer asked for; r reads the
// file, which is size bytes long. Upload returns once the file is sent, or
// with the error that ended the transfer. The peer may resume a partial
// download, so r must support seeking.
//
// onProgress, if not nil, is called after each chunk sent with the bytes sent
// so far (counting from the start of the file) and the file's size.
func (c *Client) Upload(ctx context.Context, username, filename string, r io.ReadSeeker, size int64, onProgress func(sent, size int64)) error {
	err := c.uploads.upload(ctx, username, filename, r, size, onProgress)
	if errors.Is(err, ErrUploadSource) {
		c.notifyUploadFailed(username, filename)
	}
	return err
}

// NotifyUploadFailed tells username that the upload of filename failed on our
// side before Upload could start, e.g. because the file could not be opened,
// so the peer's client can ask again instead of waiting for it. Upload sends
// this notice itself when reading the file fails.
func (c *Client) NotifyUploadFailed(ctx context.Context, username, filename string) error {
	pc, err := c.peers.ConnectToPeer(ctx, username, protocol.ConnTypePeer)
	if err != nil {
		return err
	}
	defer pc.Conn.Close()
	return pc.Conn.WriteMessage(protocol.BuildUploadFailed(filename))
}

func (c *Client) notifyUploadFailed(username, filename string) {
	ctx, cancel := context.WithTimeout(context.Background(), uploadConnectTimeout)
	defer cancel()
	if err := c.NotifyUploadFailed(ctx, username, filename); err != nil {
		c.log.Debug("notify upload failed", "component", "upload", "user", username, "err", err)
	}
}

// uploadConnector opens connections to peers; *peerManager implements it.
type uploadConnector interface {
	ConnectToPeer(ctx context.Context, username, connType string) (*peerConn, error)
	ConnectToPeerWithToken(ctx context.Context, username, connType string, token uint32) (*peerConn, error)
}

// uploads runs the upload side of the transfer protocol.
type uploads struct {
	log       *slog.Logger
	connector uploadConnector

	mu      sync.Mutex
	pending map[uint32]chan *protocol.TransferResponse // token -> waiter
}

func newUploads(log *slog.Logger, peerMgr *peerManager) *uploads {
	u := newUploadsWith(log, peerMgr)
	// The TransferResponse comes back through the PeerManager, which reads
	// every P connection (the one the request went out on, or any other the
	// peer has open to us).
	peerMgr.OnPeerMessage(protocol.PeerTransferResponse, func(msg *peerMessage) {
		resp, err := protocol.ParseTransferResponse(msg.Raw)
		if err != nil {
			u.log.Warn("parse TransferResponse", "component", "upload", "err", err)
			return
		}
		if !u.deliver(resp) {
			u.log.Debug("TransferResponse for unknown token", "component", "upload", "token", resp.Token)
		}
	})
	return u
}

func newUploadsWith(log *slog.Logger, connector uploadConnector) *uploads {
	return &uploads{
		log:       log,
		connector: connector,
		pending:   make(map[uint32]chan *protocol.TransferResponse),
	}
}

func (u *uploads) register(token uint32) chan *protocol.TransferResponse {
	ch := make(chan *protocol.TransferResponse, 1)
	u.mu.Lock()
	u.pending[token] = ch
	u.mu.Unlock()
	return ch
}

func (u *uploads) unregister(token uint32) {
	u.mu.Lock()
	delete(u.pending, token)
	u.mu.Unlock()
}

func (u *uploads) deliver(resp *protocol.TransferResponse) bool {
	u.mu.Lock()
	ch, ok := u.pending[resp.Token]
	if ok {
		delete(u.pending, resp.Token)
	}
	u.mu.Unlock()
	if ok {
		select {
		case ch <- resp:
		default:
			u.log.Warn("duplicate TransferResponse dropped", "component", "upload", "token", resp.Token)
		}
	}
	return ok
}

func (u *uploads) upload(ctx context.Context, username, filename string, r io.ReadSeeker, size int64, onProgress func(sent, size int64)) error {
	token := rand.Uint32()

	// Offer the file on a P connection and wait for the peer to accept.
	pCtx, pCancel := context.WithTimeout(ctx, uploadConnectTimeout)
	defer pCancel()
	pc, err := u.connector.ConnectToPeer(pCtx, username, protocol.ConnTypePeer)
	if err != nil {
		return fmt.Errorf("connect P to %s: %w", username, err)
	}
	defer pc.Conn.Close()

	respCh := u.register(token)
	defer u.unregister(token)

	if err := pc.Conn.WriteMessage(protocol.BuildTransferRequest(protocol.TransferUpload, token, filename, uint64(size))); err != nil {
		return fmt.Errorf("send TransferRequest: %w", err)
	}
	u.log.Debug("TransferRequest sent", "component", "upload", "user", username, "token", token, "file", filename, "size", size)

	var resp *protocol.TransferResponse
	select {
	case resp = <-respCh:
	case <-time.After(transferResponseTimeout):
		return fmt.Errorf("no TransferResponse from %s within %s", username, transferResponseTimeout)
	case <-ctx.Done():
		return ctx.Err()
	}
	if !resp.Allowed {
		return &DeniedError{Reason: resp.Reason}
	}

	// Open the F connection with the transfer token.
	fCtx, fCancel := context.WithTimeout(ctx, uploadConnectTimeout)
	defer fCancel()
	fc, err := u.connector.ConnectToPeerWithToken(fCtx, username, protocol.ConnTypeTransfer, token)
	if err != nil {
		return fmt.Errorf("connect F to %s: %w", username, err)
	}
	defer fc.Conn.Close()
	// Closing the connection on cancellation unblocks a write in progress.
	stop := context.AfterFunc(ctx, func() { fc.Conn.Close() })
	defer stop()

	raw := fc.Conn.Raw()
	_ = raw.SetWriteDeadline(time.Now().Add(uploadIOTimeout))
	var tokenBuf [4]byte
	binary.LittleEndian.PutUint32(tokenBuf[:], token)
	if _, err := raw.Write(tokenBuf[:]); err != nil {
		return fmt.Errorf("send transfer token: %w", ctxOr(ctx, err))
	}

	// The peer answers with the offset to resume from.
	_ = raw.SetReadDeadline(time.Now().Add(uploadIOTimeout))
	var offsetBuf [8]byte
	if _, err := io.ReadFull(raw, offsetBuf[:]); err != nil {
		return fmt.Errorf("read offset: %w", ctxOr(ctx, err))
	}
	_ = raw.SetReadDeadline(time.Time{})
	offset := int64(binary.LittleEndian.Uint64(offsetBuf[:]))
	if offset < 0 || offset > size {
		return fmt.Errorf("peer requested offset %d of a %d-byte file", offset, size)
	}
	if offset > 0 {
		if _, err := r.Seek(offset, io.SeekStart); err != nil {
			return fmt.Errorf("%w: seek to offset %d: %w", ErrUploadSource, offset, err)
		}
	}

	u.log.Info("streaming upload", "component", "upload", "user", username, "file", filename, "size", fmtBytes(size), "offset", offset)
	start := time.Now()
	sent, err := sendFile(ctx, raw, r, offset, size, onProgress)
	if err != nil {
		return err
	}
	u.log.Info("upload complete", "component", "upload", "user", username, "file", filename, "sent", fmtBytes(sent-offset), "elapsed", time.Since(start).Round(time.Millisecond))
	return nil
}

// sendFile streams r, positioned at offset, to the peer until size bytes of
// the file have been sent, and returns the position reached.
func sendFile(ctx context.Context, w interface {
	io.Writer
	SetWriteDeadline(time.Time) error
}, r io.Reader, offset, size int64, onProgress func(sent, size int64)) (int64, error) {
	pos := offset
	buf := make([]byte, 64*1024)
	for pos < size {
		if ctx.Err() != nil {
			return pos, ctx.Err()
		}
		n, readErr := r.Read(buf[:min(int64(len(buf)), size-pos)])
		if n > 0 {
			_ = w.SetWriteDeadline(time.Now().Add(uploadIOTimeout))
			if _, err := w.Write(buf[:n]); err != nil {
				return pos, fmt.Errorf("write to peer: %w", ctxOr(ctx, err))
			}
			pos += int64(n)
			if onProgress != nil {
				onProgress(pos, size)
			}
		}
		if readErr == io.EOF && pos < size {
			return pos, fmt.Errorf("%w: file shrank: sent %d of %d bytes", ErrUploadSource, pos, size)
		}
		if readErr != nil && readErr != io.EOF {
			return pos, fmt.Errorf("%w: read: %w", ErrUploadSource, readErr)
		}
	}
	return pos, nil
}
