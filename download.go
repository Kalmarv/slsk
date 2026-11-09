package slsk

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/kalmarv/slsk/protocol"
)

// Queue waiting limits. A peer that reports queue positions is waited on for
// as long as it keeps answering; these bound the silent and the total case.
const (
	// queuePollInterval is how often we ask the peer for our queue position.
	queuePollInterval = 30 * time.Second
	// queueStallTimeout fails a wait when the peer has said nothing (no
	// position update, no request) for this long.
	queueStallTimeout = 10 * time.Minute
	// maxQueueWait bounds the total time one file may sit in a peer's queue.
	maxQueueWait = 12 * time.Hour
	// earlyTransferTTL is how long a TransferRequest that arrives between
	// retry attempts is held for the next attempt.
	earlyTransferTTL = 5 * time.Minute
	// maxEarlyTransfers caps the held requests so a peer cannot grow the map.
	maxEarlyTransfers = 256
	// denyWriteTimeout bounds writing a denial back to a peer from the
	// message loop.
	denyWriteTimeout = 5 * time.Second
)

// transferResult is delivered when a TransferRequest arrives for a pending download.
type transferResult struct {
	req    *protocol.TransferRequest
	conn   *protocol.Conn // the P-connection to respond on; nil signals an error
	reason string         // denial/failure reason from peer (empty on success)
}

// pendingKey identifies a requested file: the same path can be requested from
// different peers at once.
type pendingKey struct {
	user string
	file string
}

// pendingDownload is a file we have asked a peer for and are waiting on.
type pendingDownload struct {
	ch         chan *transferResult // capacity 1; senders never block
	queued     bool                 // QueueDownload already sent by queue
	lastSignal time.Time            // last sign of life from the peer
	position   uint32               // last reported queue position
}

// earlyTransfer is a TransferRequest held for a download that is between
// attempts.
type earlyTransfer struct {
	res *transferResult
	at  time.Time
}

// peerConnector opens connections to peers; *peerManager implements it.
type peerConnector interface {
	ConnectToPeer(ctx context.Context, username, connType string) (*peerConn, error)
}

// DeniedError is returned by Download when the peer refuses the file or
// reports that its upload failed.
type DeniedError struct {
	// Reason is the peer's explanation, e.g. "File not shared.", "Banned",
	// "Too many files", or "Upload failed".
	Reason string
}

func (e *DeniedError) Error() string {
	if e.Reason == "" {
		return "denied by peer"
	}
	return "denied by peer: " + e.Reason
}

// Permanent reports whether asking again cannot help: the file isn't shared,
// we are banned, the peer reads the file and fails, or a request exceeds the
// peer's per-user limits. Other denials (a failed upload, a cancelled queue)
// are worth retrying.
func (e *DeniedError) Permanent() bool {
	r := strings.ToLower(e.Reason)
	for _, permanent := range []string{
		"file not shared",
		"banned",
		"too many files",
		"too many megabytes",
		"file read error",
	} {
		if strings.Contains(r, permanent) {
			return true
		}
	}
	return false
}

// uploadFailedReason is the denial reason recorded when a peer sends
// UploadFailed for a file we are waiting on.
const uploadFailedReason = "Upload failed"

// QueueDownloads asks username for all files at once, so the whole set holds
// its place in the peer's queue while Download fetches the files one by one.
// Download calls for these files then only wait for the peer, instead of
// queueing each file when its turn comes. Call ReleaseDownloads for the files
// that are not downloaded after all.
func (c *Client) QueueDownloads(ctx context.Context, username string, files []string) error {
	return c.downloads.queue(ctx, username, files)
}

// ReleaseDownloads forgets files queued with QueueDownloads that will not be
// downloaded (after a failure or a cancellation).
func (c *Client) ReleaseDownloads(username string, files []string) {
	c.downloads.release(username, files)
}

// Download fetches filename (a File.Name from a search result) from username
// and writes it to w. It returns once the whole file arrived, or with the
// error that ended the transfer: ErrPeerOffline, a *DeniedError, a timeout
// while the peer keeps silent, or ctx's error.
//
// A busy peer may keep the file in its queue for hours; Download keeps
// waiting as long as the peer answers our queue position requests, failing
// after 10 minutes of silence or 12 hours overall.
//
// onProgress, if not nil, is called after each chunk written to w with the
// bytes received so far and the file's size.
func (c *Client) Download(ctx context.Context, username, filename string, w io.Writer, onProgress func(received, size int64)) error {
	return c.downloads.download(ctx, username, filename, w, onProgress)
}

// downloads runs the download side of the transfer protocol: it queues files
// with peers, matches their TransferRequests and file connections to the
// downloads waiting for them, and receives the data.
type downloads struct {
	log       *slog.Logger
	connector peerConnector

	queueStall   time.Duration
	maxQueueWait time.Duration
	pollInterval time.Duration

	mu               sync.Mutex
	pendingXfers     map[string][]chan *peerConn     // username -> FIFO queue of F connection waiters
	pendingDownloads map[pendingKey]*pendingDownload // files we are waiting on
	earlyTransfers   map[pendingKey]earlyTransfer    // TransferRequests held between attempts
	queueConns       map[string]*peerConn            // username -> P connection the queue requests went out on
}

// newDownloads returns the download side of the client and registers its
// handlers with the peer manager.
func newDownloads(log *slog.Logger, peerMgr *peerManager) *downloads {
	d := newDownloadsWith(log, peerMgr)

	peerMgr.OnFileConnection(func(pc *peerConn) {
		d.handleIncomingFileConn(pc)
	})
	peerMgr.OnPeerMessage(protocol.PeerTransferRequest, d.handleTransferRequest)
	peerMgr.OnPeerMessage(protocol.PeerPlaceInQueueResponse, d.handlePlaceInQueue)
	peerMgr.OnPeerMessage(protocol.PeerUploadDenied, d.handleUploadDenied)
	peerMgr.OnPeerMessage(protocol.PeerUploadFailed, d.handleUploadFailed)
	return d
}

func newDownloadsWith(log *slog.Logger, connector peerConnector) *downloads {
	return &downloads{
		log:              log,
		connector:        connector,
		queueStall:       queueStallTimeout,
		maxQueueWait:     maxQueueWait,
		pollInterval:     queuePollInterval,
		pendingXfers:     make(map[string][]chan *peerConn),
		pendingDownloads: make(map[pendingKey]*pendingDownload),
		earlyTransfers:   make(map[pendingKey]earlyTransfer),
		queueConns:       make(map[string]*peerConn),
	}
}

func (d *downloads) handleIncomingFileConn(pc *peerConn) {
	d.mu.Lock()
	waiters := d.pendingXfers[pc.Username]
	if len(waiters) > 0 {
		ch := waiters[0]
		d.pendingXfers[pc.Username] = waiters[1:]
		if len(d.pendingXfers[pc.Username]) == 0 {
			delete(d.pendingXfers, pc.Username)
		}
		remaining := len(d.pendingXfers[pc.Username])
		d.mu.Unlock()
		d.log.Debug("F-conn matched", "component", "peer", "user", pc.Username, "waitersRemaining", remaining)
		ch <- pc // waiter channels have capacity 1 and receive exactly one conn
	} else {
		d.mu.Unlock()
		d.log.Warn("F-conn unexpected, no waiters", "component", "peer", "user", pc.Username)
		pc.Conn.Close()
	}
}

func (d *downloads) handleTransferRequest(msg *peerMessage) {
	req, err := protocol.ParseTransferRequest(msg.Raw)
	if err != nil {
		d.log.Warn("TransferRequest parse error", "component", "peer", "user", msg.PeerConn.Username, "err", err)
		return
	}

	if req.Direction != protocol.TransferUpload {
		return
	}

	user := msg.PeerConn.Username
	d.log.Debug("TransferRequest", "component", "peer", "user", user, "file", req.Filename, "token", req.Token, "size", fmtBytes(int64(req.FileSize)))

	key := pendingKey{user: user, file: req.Filename}
	res := &transferResult{req: req, conn: msg.PeerConn.Conn}

	d.mu.Lock()
	expired := d.pruneEarlyLocked(time.Now())
	pd, pending := d.pendingDownloads[key]
	var outcome string
	switch {
	case pending:
		pd.lastSignal = time.Now()
		select {
		case pd.ch <- res:
		default:
			outcome = "duplicate" // a request for this file is already waiting
		}
	case d.userHasPendingLocked(user) && len(d.earlyTransfers) < maxEarlyTransfers:
		d.log.Debug("TransferRequest held for next attempt", "component", "peer", "user", user, "file", req.Filename)
		d.earlyTransfers[key] = earlyTransfer{res: res, at: time.Now()}
	default:
		outcome = "unknown"
	}
	d.mu.Unlock()

	for _, e := range expired {
		d.denyTransfer(e.req.Token, e.conn, "Cancelled")
	}
	if outcome != "" {
		d.log.Debug("denying TransferRequest", "component", "peer", "user", user, "file", req.Filename, "why", outcome)
		d.denyTransfer(req.Token, msg.PeerConn.Conn, "Cancelled")
	}
}

// denyTransfer answers a TransferRequest we will not serve. The peer is
// waiting on this reply, so an unanswered request would stall its queue.
func (d *downloads) denyTransfer(token uint32, conn *protocol.Conn, reason string) {
	_ = conn.Raw().SetWriteDeadline(time.Now().Add(denyWriteTimeout))
	if err := conn.WriteMessage(protocol.BuildTransferResponseDenied(token, reason)); err != nil {
		d.log.Debug("deny TransferRequest failed", "component", "peer", "err", err)
	}
}

// pruneEarlyLocked drops held requests older than earlyTransferTTL and returns
// them so the caller can deny them outside the lock.
func (d *downloads) pruneEarlyLocked(now time.Time) []*transferResult {
	var expired []*transferResult
	for k, e := range d.earlyTransfers {
		if now.Sub(e.at) > earlyTransferTTL {
			delete(d.earlyTransfers, k)
			expired = append(expired, e.res)
		}
	}
	return expired
}

// userHasPendingLocked reports whether any download from user is in progress.
func (d *downloads) userHasPendingLocked(user string) bool {
	for k := range d.pendingDownloads {
		if k.user == user {
			return true
		}
	}
	return false
}

func (d *downloads) handlePlaceInQueue(msg *peerMessage) {
	piq, err := protocol.ParsePlaceInQueueResponse(msg.Raw)
	if err != nil {
		return
	}
	user := msg.PeerConn.Username
	d.log.Debug("queue position", "component", "peer", "user", user, "file", piq.Filename, "position", piq.Place)

	d.mu.Lock()
	if pd, ok := d.pendingDownloads[pendingKey{user, piq.Filename}]; ok {
		pd.position = piq.Place
		pd.lastSignal = time.Now()
	}
	d.mu.Unlock()
}

func (d *downloads) handleUploadDenied(msg *peerMessage) {
	r, err := protocol.NewReader(msg.Raw)
	filename := "(unknown)"
	reason := ""
	if err == nil {
		if f, err := r.ReadString(); err == nil {
			filename = f
		}
		if rs, err := r.ReadString(); err == nil {
			reason = rs
		}
	}
	d.log.Warn("upload denied", "component", "peer", "user", msg.PeerConn.Username, "file", filename, "reason", reason)
	d.signalPendingDenied(msg.PeerConn.Username, filename, reason)
}

func (d *downloads) handleUploadFailed(msg *peerMessage) {
	r, err := protocol.NewReader(msg.Raw)
	filename := "(unknown)"
	if err == nil {
		if f, err := r.ReadString(); err == nil {
			filename = f
		}
	}
	d.log.Warn("upload failed", "component", "peer", "user", msg.PeerConn.Username, "file", filename)
	d.signalPendingDenied(msg.PeerConn.Username, filename, uploadFailedReason)
}

func (d *downloads) signalPendingDenied(user, filename, reason string) {
	d.mu.Lock()
	pd, ok := d.pendingDownloads[pendingKey{user, filename}]
	if ok {
		select {
		case pd.ch <- &transferResult{conn: nil, reason: reason}:
		default: // a result is already waiting to be consumed
		}
	}
	d.mu.Unlock()

	if !ok {
		d.log.Debug("deny signal for untracked file", "component", "peer", "user", user, "file", filename, "reason", reason)
	}
}

// register returns the pending entry for key, creating it if needed, and moves
// a held early TransferRequest into it.
func (d *downloads) register(key pendingKey) *pendingDownload {
	d.mu.Lock()
	defer d.mu.Unlock()
	pd, ok := d.pendingDownloads[key]
	if ok {
		return pd
	}
	pd = &pendingDownload{ch: make(chan *transferResult, 1), lastSignal: time.Now()}
	d.pendingDownloads[key] = pd
	if early, ok := d.earlyTransfers[key]; ok {
		delete(d.earlyTransfers, key)
		d.log.Debug("using held TransferRequest", "component", "peer", "user", key.user, "file", key.file)
		pd.ch <- early.res
	}
	return pd
}

// queue asks the peer for every file in one go; see Client.QueueDownloads.
func (d *downloads) queue(ctx context.Context, username string, files []string) error {
	entries := make([]*pendingDownload, len(files))
	for i, f := range files {
		entries[i] = d.register(pendingKey{username, f})
	}
	if err := d.sendQueue(ctx, username, files); err != nil {
		return err
	}
	d.mu.Lock()
	now := time.Now()
	for _, pd := range entries {
		pd.queued = true
		pd.lastSignal = now
	}
	d.mu.Unlock()
	return nil
}

// release forgets files queued by queue and closes the connection the
// requests went out on once the peer has nothing of ours pending.
func (d *downloads) release(username string, files []string) {
	d.mu.Lock()
	for _, f := range files {
		delete(d.pendingDownloads, pendingKey{username, f})
	}
	d.closeQueueConnLocked(username)
	d.mu.Unlock()
}

// closeQueueConnLocked closes username's queue connection if no download from
// them is still pending.
func (d *downloads) closeQueueConnLocked(username string) {
	if d.userHasPendingLocked(username) {
		return
	}
	if pc, ok := d.queueConns[username]; ok {
		delete(d.queueConns, username)
		pc.Conn.Close()
	}
}

// sendQueue connects to the peer and sends QueueDownload for each file. The
// connection stays open: the peer answers on it (queue position, denials) and
// it carries our position polls.
func (d *downloads) sendQueue(ctx context.Context, username string, files []string) error {
	d.log.Debug("connecting for QueueDownload", "component", "peer", "user", username, "files", len(files))
	pConn, err := d.connector.ConnectToPeer(ctx, username, protocol.ConnTypePeer)
	if err != nil {
		return fmt.Errorf("connect to peer %s: %w", username, err)
	}
	for _, f := range files {
		if err := pConn.Conn.WriteMessage(protocol.BuildQueueDownload(f)); err != nil {
			pConn.Conn.Close()
			return fmt.Errorf("send queue download: %w", err)
		}
		d.log.Debug("QueueDownload sent", "component", "peer", "user", username, "file", f)
	}

	d.mu.Lock()
	old := d.queueConns[username]
	d.queueConns[username] = pConn
	d.mu.Unlock()
	if old != nil {
		old.Conn.Close()
	}
	return nil
}

// requestPlace asks the peer for the file's queue position on the queue
// connection. Best effort: a peer that never answers is caught by the stall
// timeout.
func (d *downloads) requestPlace(key pendingKey) {
	d.mu.Lock()
	pc := d.queueConns[key.user]
	d.mu.Unlock()
	if pc == nil {
		return
	}
	if err := pc.Conn.WriteMessage(protocol.BuildPlaceInQueueRequest(key.file)); err != nil {
		d.log.Debug("place in queue request failed", "component", "peer", "user", key.user, "err", err)
	}
}

// awaitTransferRequest waits for the peer to start the upload. The peer may
// hold us in its queue for hours, so there is no fixed short timeout: the wait
// continues while the peer keeps answering position requests, and fails after
// queueStall of silence or maxQueueWait overall.
func (d *downloads) awaitTransferRequest(ctx context.Context, key pendingKey, pd *pendingDownload) (*transferResult, error) {
	overall := time.NewTimer(d.maxQueueWait)
	defer overall.Stop()
	poll := time.NewTicker(d.pollInterval)
	defer poll.Stop()

	for {
		select {
		case xfer := <-pd.ch:
			if xfer.conn == nil {
				return nil, &DeniedError{Reason: xfer.reason}
			}
			return xfer, nil
		case <-poll.C:
			d.mu.Lock()
			idle := time.Since(pd.lastSignal)
			pos := pd.position
			d.mu.Unlock()
			if idle > d.queueStall {
				return nil, fmt.Errorf("timeout waiting for TransferRequest from %s (no response for %s)", key.user, d.queueStall)
			}
			d.log.Debug("still queued", "component", "peer", "user", key.user, "file", key.file, "position", pos)
			d.requestPlace(key)
		case <-overall.C:
			return nil, fmt.Errorf("timeout waiting for TransferRequest from %s (queued %s)", key.user, d.maxQueueWait)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// download fetches one file; see Client.Download.
func (d *downloads) download(ctx context.Context, username, remotePath string, w io.Writer, onProgress func(received, size int64)) error {
	// 1. Register pending download BEFORE sending QueueDownload.
	key := pendingKey{username, remotePath}
	pd := d.register(key)
	d.mu.Lock()
	alreadyQueued := pd.queued
	d.mu.Unlock()

	defer func() {
		d.mu.Lock()
		delete(d.pendingDownloads, key)
		d.closeQueueConnLocked(username)
		d.mu.Unlock()
	}()

	// 2. Send QueueDownload unless the file was queued up front.
	if !alreadyQueued {
		if err := d.sendQueue(ctx, username, []string{remotePath}); err != nil {
			return err
		}
	}

	// 3. Wait for TransferRequest.
	d.log.Debug("waiting for TransferRequest", "component", "peer", "user", username)
	xfer, err := d.awaitTransferRequest(ctx, key, pd)
	if err != nil {
		return err
	}

	d.log.Debug("TransferRequest received", "component", "peer", "user", username, "token", xfer.req.Token, "size", fmtBytes(int64(xfer.req.FileSize)))

	// 4. Send TransferResponse (allowed).
	respMsg := protocol.BuildTransferResponseAllowed(xfer.req.Token)
	if err := xfer.conn.WriteMessage(respMsg); err != nil {
		return fmt.Errorf("send transfer response: %w", err)
	}
	d.log.Debug("TransferResponse sent, waiting for F-conn", "component", "peer", "user", username, "token", xfer.req.Token)

	// 5. Wait for F-type connection.
	fConn, err := d.waitForFileConn(ctx, username)
	if err != nil {
		return fmt.Errorf("wait for file connection: %w", err)
	}
	defer fConn.Conn.Close()
	// Reads on the socket do not observe ctx; closing it unblocks them.
	defer context.AfterFunc(ctx, func() { fConn.Conn.Close() })()

	// 6. Read transfer token, send offset.
	var tokenBuf [4]byte
	if _, err := io.ReadFull(fConn.Conn.Raw(), tokenBuf[:]); err != nil {
		return fmt.Errorf("read transfer token from peer: %w", ctxOr(ctx, err))
	}
	peerToken := binary.LittleEndian.Uint32(tokenBuf[:])
	if peerToken != xfer.req.Token {
		d.log.Warn("token mismatch", "component", "peer", "got", peerToken, "expected", xfer.req.Token, "user", username)
	}

	var offsetBuf [8]byte
	binary.LittleEndian.PutUint64(offsetBuf[:], 0)
	if _, err := fConn.Conn.Raw().Write(offsetBuf[:]); err != nil {
		return fmt.Errorf("send file offset: %w", ctxOr(ctx, err))
	}

	// 7. Read file data.
	expectedSize := int64(xfer.req.FileSize)
	d.log.Info("transfer started", "component", "peer", "user", username, "size", fmtBytes(expectedSize))
	startTime := time.Now()
	var progress func(int64)
	if onProgress != nil {
		progress = func(n int64) { onProgress(n, expectedSize) }
	}
	transferred, err := copyTransfer(ctx, fConn.Conn.Raw(), w, expectedSize, progress)
	elapsed := time.Since(startTime).Round(time.Millisecond)
	if err != nil {
		d.log.Error("transfer failed", "component", "peer", "user", username, "transferred", fmtBytes(transferred), "expected", fmtBytes(expectedSize), "elapsed", elapsed, "err", err)
		return err
	}

	var speed string
	if elapsed > 0 {
		bytesPerSec := float64(transferred) / elapsed.Seconds()
		speed = fmtBytes(int64(bytesPerSec)) + "/s"
	}
	d.log.Info("transfer complete", "component", "peer", "user", username, "transferred", fmtBytes(transferred), "expected", fmtBytes(expectedSize), "elapsed", elapsed, "speed", speed)

	return nil
}

// ctxOr prefers the context's error when it was cancelled: a socket closed to
// abort a read reports a generic "use of closed connection".
func ctxOr(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

// copyTransfer copies a file body from r to w. The peer ends the stream by
// closing the connection, so EOF alone proves nothing: it is success only when
// expected bytes (the size announced in the TransferRequest) arrived.
func copyTransfer(ctx context.Context, r io.Reader, w io.Writer, expected int64, onProgress func(int64)) (int64, error) {
	var transferred int64
	buf := make([]byte, 64*1024)

	for {
		if ctx.Err() != nil {
			return transferred, ctx.Err()
		}

		n, readErr := r.Read(buf)
		if n > 0 {
			if _, werr := w.Write(buf[:n]); werr != nil {
				return transferred, fmt.Errorf("write: %w", werr)
			}
			transferred += int64(n)
			if onProgress != nil {
				onProgress(transferred)
			}
			if expected > 0 && transferred >= expected {
				return transferred, nil
			}
		}
		if readErr == nil {
			continue
		}
		if ctx.Err() != nil {
			return transferred, ctx.Err()
		}
		if readErr == io.EOF {
			if transferred < expected {
				return transferred, fmt.Errorf("transfer truncated: received %d of %d bytes: %w", transferred, expected, io.ErrUnexpectedEOF)
			}
			return transferred, nil
		}
		return transferred, fmt.Errorf("read file data: %w", readErr)
	}
}

func (d *downloads) waitForFileConn(ctx context.Context, username string) (*peerConn, error) {
	ch := make(chan *peerConn, 1)
	d.mu.Lock()
	d.pendingXfers[username] = append(d.pendingXfers[username], ch)
	d.mu.Unlock()

	defer func() {
		d.mu.Lock()
		waiters := d.pendingXfers[username]
		for i, w := range waiters {
			if w == ch {
				d.pendingXfers[username] = append(waiters[:i], waiters[i+1:]...)
				break
			}
		}
		if len(d.pendingXfers[username]) == 0 {
			delete(d.pendingXfers, username)
		}
		d.mu.Unlock()
	}()

	select {
	case pc := <-ch:
		return pc, nil
	case <-time.After(30 * time.Second):
		return nil, fmt.Errorf("timeout waiting for F-conn from %s (30s)", username)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
