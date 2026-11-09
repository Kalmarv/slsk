package slsk

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"path"
	"strings"
	"sync/atomic"
	"time"

	"github.com/kalmarv/slsk/protocol"
)

// ShareIndex answers other users' searches for the files we share. Set it as
// Config.Shares.
type ShareIndex interface {
	// Search returns the shared files matching another user's query, at most
	// MaxReplyFiles of them (more are dropped). ParseQuery implements the
	// matching rules other Soulseek clients use. Search is called
	// concurrently and should return quickly: a few workers answer every
	// search the network sends us.
	Search(query string) []File
	// Counts returns how many folders and files are shared. The server only
	// routes searches to users who share something.
	Counts() (folders, files int)
}

// UploadQueue decides what happens when a user asks to download one of our
// files. Set it as Config.Uploads. The queue starts the transfers itself, by
// calling Client.Upload, in whatever order and number it allows.
type UploadQueue interface {
	// Enqueue is called when a user asks for a file. It returns the file's
	// place in the queue (0 if the upload starts right away), or an error
	// to refuse the request: ErrNotShared for a file we don't share, which
	// tells the peer to stop asking; any other error is sent to the peer as
	// the reason and the peer may ask again later.
	Enqueue(req UploadRequest) (position int, err error)
	// Position returns the place of a queued request in the queue, 0 if it
	// is not queued.
	Position(username, filename string) int
	// SlotState reports whether an upload could start right away and how
	// many uploads are waiting. Our search results advertise both.
	SlotState() (free bool, queued int)
}

// UploadRequest is a user's request to download one of our files.
type UploadRequest struct {
	Username string
	// Filename is the Soulseek path the user asked for, as our ShareIndex
	// returned it in a search result.
	Filename string
	// IP is the address the request came from.
	IP net.IP
}

// ErrNotShared refuses an upload request for a file we don't share.
var ErrNotShared = errors.New("file not shared")

// MaxReplyFiles is the most files one search reply carries, so broad queries
// stay cheap to send and small on the wire.
const MaxReplyFiles = 1500

// Incoming search handling: concurrent share searches, how many requests may
// wait before new ones are dropped, and how many replies may be in flight.
const (
	searchWorkerCount = 4
	searchQueueDepth  = 256
	maxSearchReplies  = 64
	// replyConnectTimeout bounds connecting to a searching peer to reply.
	replyConnectTimeout = 10 * time.Second
)

// sharing answers other users: their searches, and their requests to queue
// our files.
type sharing struct {
	log      *slog.Logger
	username string
	shares   ShareIndex
	queue    UploadQueue
	speed    uint32
	peers    peerConnector

	sendSlots      chan struct{} // bounds replies in flight
	served         atomic.Int64
	repliesDropped atomic.Int64
}

func newSharing(log *slog.Logger, cfg Config, peerMgr *peerManager) *sharing {
	s := &sharing{
		log:       log,
		username:  cfg.Username,
		shares:    cfg.Shares,
		queue:     cfg.Uploads,
		speed:     cfg.UploadSpeed,
		peers:     peerMgr,
		sendSlots: make(chan struct{}, maxSearchReplies),
	}
	peerMgr.OnPeerMessage(protocol.PeerQueueDownload, s.handleQueueDownload)
	peerMgr.OnPeerMessage(protocol.PeerPlaceInQueueRequest, s.handlePlaceInQueueRequest)
	return s
}

// slotState is the queue's SlotState, or a free slot without a queue.
func (s *sharing) slotState() (bool, int) {
	if s.queue == nil {
		return true, 0
	}
	free, queued := s.queue.SlotState()
	return free, max(queued, 0)
}

// answer replies to a search request from another user (relayed by the
// server or the distributed parent). It runs on the search worker pool, off
// the connections' read loops. Matching happens here; the reply, which needs
// a peer connection that can take seconds, goes out on its own goroutine, at
// most maxSearchReplies at once. Requests beyond that are dropped: searches
// are best effort and the searching peer has other sources.
func (s *sharing) answer(ctx context.Context, req *protocol.IncomingSearchRequest) {
	if req.Username == s.username {
		return // our own search, relayed back to us
	}
	files := s.shares.Search(req.Query)
	if len(files) == 0 {
		return // most queries match nothing
	}
	if len(files) > MaxReplyFiles {
		files = files[:MaxReplyFiles]
	}
	free, queued := s.slotState()
	reply, err := buildSearchReply(s.username, req.Token, files, free, queued, s.speed)
	if err != nil {
		s.log.Error("build search reply", "component", "share", "err", err)
		return
	}
	select {
	case s.sendSlots <- struct{}{}:
	default:
		s.repliesDropped.Add(1)
		return
	}
	s.log.Debug("search query served", "component", "share", "query", req.Query, "from", req.Username, "matches", len(files))
	s.served.Add(1)
	go func() {
		defer func() { <-s.sendSlots }()
		cctx, cancel := context.WithTimeout(ctx, replyConnectTimeout)
		defer cancel()
		pc, err := s.peers.ConnectToPeer(cctx, req.Username, protocol.ConnTypePeer)
		if err != nil {
			s.log.Debug("search reply connect failed", "component", "share", "peer", req.Username, "err", err)
			return
		}
		if err := pc.Conn.WriteMessage(reply); err != nil {
			s.log.Debug("search reply send failed", "component", "share", "peer", req.Username, "err", err)
		}
		pc.Conn.Close()
	}()
}

// logStats logs, once a minute, how many searches were answered and dropped.
func (s *sharing) logStats(ctx context.Context, workers *searchWorkers) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	var queueDropped uint64
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			total := workers.Dropped()
			dropped := s.repliesDropped.Swap(0) + int64(total-queueDropped)
			queueDropped = total
			if served := s.served.Swap(0); served > 0 || dropped > 0 {
				s.log.Info("search replies in the last minute", "component", "share", "served", served, "dropped", dropped)
			}
		}
	}
}

// buildSearchReply builds the wire search response for files.
func buildSearchReply(username string, token uint32, files []File, free bool, queued int, speed uint32) ([]byte, error) {
	wire := make([]protocol.SharedFile, len(files))
	for i, f := range files {
		var attrs []protocol.FileAttribute
		add := func(typ uint32, v int) {
			if v > 0 {
				attrs = append(attrs, protocol.FileAttribute{Type: typ, Value: uint32(v)})
			}
		}
		add(protocol.AttrBitRate, f.Bitrate)
		add(protocol.AttrDuration, f.Duration)
		if f.VBR {
			add(protocol.AttrVBR, 1)
		}
		add(protocol.AttrSampleRate, f.SampleRate)
		add(protocol.AttrBitDepth, f.BitDepth)
		wire[i] = protocol.SharedFile{
			Filename:   f.Name,
			Size:       uint64(f.Size),
			Extension:  strings.TrimPrefix(path.Ext(strings.ReplaceAll(f.Name, `\`, "/")), "."),
			Attributes: attrs,
		}
	}
	return protocol.BuildSearchResponse(protocol.SearchResponse{
		Username:       username,
		Token:          token,
		Files:          wire,
		FreeUploadSlot: free,
		UploadSpeed:    speed,
		QueueLength:    uint32(queued),
	})
}

// handleQueueDownload answers a peer's request to download one of our files.
func (s *sharing) handleQueueDownload(msg *peerMessage) {
	filename, err := protocol.ParseQueueDownload(msg.Raw)
	if err != nil {
		s.log.Warn("parse QueueDownload", "component", "upload", "user", msg.PeerConn.Username, "err", err)
		return
	}
	user := msg.PeerConn.Username
	var ip net.IP
	if addr, ok := msg.PeerConn.Conn.RemoteAddr().(*net.TCPAddr); ok {
		ip = addr.IP
	}
	s.log.Info("QueueDownload received", "component", "upload", "user", user, "ip", ip, "file", filename)

	position, err := 0, ErrNotShared
	if s.queue != nil {
		position, err = s.queue.Enqueue(UploadRequest{Username: user, Filename: filename, IP: ip})
	}
	conn := msg.PeerConn.Conn
	if err != nil {
		// Peers stop retrying only on the standard "File not shared."
		// reason; anything else (a database error, say) is worth a retry.
		reason := err.Error()
		if errors.Is(err, ErrNotShared) {
			reason = protocol.DenyFileNotShared
		}
		s.log.Info("upload denied", "component", "upload", "user", user, "file", filename, "reason", reason)
		if err := conn.WriteMessage(protocol.BuildUploadDenied(filename, reason)); err != nil {
			s.log.Debug("send UploadDenied", "component", "upload", "user", user, "err", err)
		}
		return
	}
	if position > 0 {
		if err := conn.WriteMessage(protocol.BuildPlaceInQueueResponse(filename, uint32(position))); err != nil {
			s.log.Debug("send PlaceInQueue", "component", "upload", "user", user, "err", err)
		}
	}
}

// handlePlaceInQueueRequest tells a peer where its request stands.
func (s *sharing) handlePlaceInQueueRequest(msg *peerMessage) {
	filename, err := protocol.ParsePlaceInQueueRequest(msg.Raw)
	if err != nil {
		return
	}
	position := 0
	if s.queue != nil {
		position = s.queue.Position(msg.PeerConn.Username, filename)
	}
	if err := msg.PeerConn.Conn.WriteMessage(protocol.BuildPlaceInQueueResponse(filename, uint32(position))); err != nil {
		s.log.Debug("send PlaceInQueue", "component", "upload", "user", msg.PeerConn.Username, "err", err)
	}
}
