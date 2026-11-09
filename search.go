package slsk

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/kalmarv/slsk/protocol"
)

// SearchResult is one user's answer to a search.
type SearchResult struct {
	Username string
	Files    []File
	// FreeSlot reports whether the user can start an upload right away.
	FreeSlot bool
	// UploadSpeed is the user's advertised upload speed in bytes per second.
	UploadSpeed uint32
	// QueueLength is how many uploads the user has queued.
	QueueLength uint32
}

// File is a shared file: one entry of a search result, or of our own shares.
// Attributes the sharer doesn't know are zero.
type File struct {
	// Name is the full Soulseek path, backslash-separated, e.g.
	// `Music\Artist\Album\01 Track.flac`. Download takes it as is.
	Name       string
	Size       int64
	Bitrate    int // kbps
	Duration   int // seconds
	SampleRate int // Hz
	BitDepth   int // bits per sample
	VBR        bool
}

// SearchTimeouts bounds how long a search waits for peers. Peers answer
// through the distributed network, so the first answer often takes several
// seconds; after that, answers arrive in bursts.
type SearchTimeouts struct {
	// FirstResult ends a search that no peer answered within this long.
	// Default 15s.
	FirstResult time.Duration
	// Idle ends a search once no new answer arrived for this long after the
	// first. Default 3s.
	Idle time.Duration
	// Max caps a search's total duration. Default 30s.
	Max time.Duration
}

func (t SearchTimeouts) withDefaults() SearchTimeouts {
	if t.FirstResult <= 0 {
		t.FirstResult = 15 * time.Second
	}
	if t.Idle <= 0 {
		t.Idle = 3 * time.Second
	}
	if t.Max <= 0 {
		t.Max = 30 * time.Second
	}
	return t
}

// Search searches the network for query and calls onResult for each user
// that answers, as answers arrive. It blocks until the search is over (see
// SearchTimeouts) or ctx is cancelled, and returns an error only if the search
// could not be sent; ErrNotLoggedIn while the client is offline.
//
// Query syntax is Soulseek's: every word must match a whole word of a path,
// "-word" excludes paths containing word, and "*suffix" matches words ending
// in suffix. The server silently drops searches containing any of
// ExcludedSearchPhrases; Search rewrites such phrases into a wildcard form
// (the first letter becomes "*") that peers still match.
//
// onResult is called from the connections of the answering peers; calls for
// one search are serialized.
func (c *Client) Search(ctx context.Context, query string, onResult func(SearchResult)) error {
	return c.searches.search(ctx, query, onResult)
}

// searchServer is the part of the server connection searches use.
type searchServer interface {
	Search(token uint32, query string) error
	ExcludedSearchPhrases() []string
}

// searches routes peers' search answers to the searches waiting for them.
type searches struct {
	log      *slog.Logger
	server   searchServer
	timeouts SearchTimeouts

	mu      sync.Mutex
	pending map[uint32]*pendingSearch // token -> search

	tokenCounter atomic.Uint32
}

type pendingSearch struct {
	mu       sync.Mutex // serializes onResult calls from concurrent peer connections
	onResult func(SearchResult)
	activity chan struct{} // signalled after each delivered answer; resets the idle timer
}

func newPendingSearch(onResult func(SearchResult)) *pendingSearch {
	return &pendingSearch{
		onResult: onResult,
		activity: make(chan struct{}, 1),
	}
}

// deliver hands an answer to the search's callback and signals activity
// without blocking (a pending signal already covers this answer).
func (p *pendingSearch) deliver(res SearchResult) {
	p.mu.Lock()
	p.onResult(res)
	p.mu.Unlock()

	select {
	case p.activity <- struct{}{}:
	default:
	}
}

func newSearches(log *slog.Logger, server searchServer, peers *peerManager, timeouts SearchTimeouts) *searches {
	s := &searches{
		log:      log,
		server:   server,
		timeouts: timeouts,
		pending:  make(map[uint32]*pendingSearch),
	}
	peers.OnPeerMessage(protocol.PeerSearchResponse, func(msg *peerMessage) {
		s.handleRawResponse(msg.Raw, msg.PeerConn.Username)
	})
	return s
}

// handleRawResponse parses a wire-format search response and forwards it to
// the search it answers. The token is read first so unsolicited responses
// (any peer can send them) are dropped before the file list is decompressed
// and parsed.
func (s *searches) handleRawResponse(raw []byte, peer string) {
	hdr, err := protocol.ParseSearchResponseHeader(raw)
	if err != nil {
		s.log.Warn("parse search response header", "component", "search", "user", peer, "err", err)
		return
	}
	if !s.hasPending(hdr.Token) {
		return
	}
	resp, err := protocol.ParseSearchResponse(raw)
	if err != nil {
		s.log.Warn("parse search response", "component", "search", "user", peer, "err", err)
		return
	}
	s.handleResponse(resp)
}

func (s *searches) hasPending(token uint32) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.pending[token]
	return ok
}

// handleResponse converts a parsed search response and forwards it to the
// search with its token. Answers without files are dropped.
func (s *searches) handleResponse(resp *protocol.SearchResponse) {
	s.mu.Lock()
	pending, ok := s.pending[resp.Token]
	s.mu.Unlock()
	if !ok || len(resp.Files) == 0 {
		return
	}

	res := SearchResult{
		Username:    resp.Username,
		Files:       make([]File, len(resp.Files)),
		FreeSlot:    resp.FreeUploadSlot,
		UploadSpeed: resp.UploadSpeed,
		QueueLength: resp.QueueLength,
	}
	for i, f := range resp.Files {
		res.Files[i] = fileFromWire(f)
	}
	pending.deliver(res)
}

// fileFromWire converts a file entry of a search response.
func fileFromWire(f protocol.SharedFile) File {
	file := File{Name: f.Filename, Size: int64(f.Size)}
	for _, attr := range f.Attributes {
		switch attr.Type {
		case protocol.AttrBitRate:
			file.Bitrate = int(attr.Value)
		case protocol.AttrDuration:
			file.Duration = int(attr.Value)
		case protocol.AttrVBR:
			file.VBR = attr.Value == 1
		case protocol.AttrSampleRate:
			file.SampleRate = int(attr.Value)
		case protocol.AttrBitDepth:
			file.BitDepth = int(attr.Value)
		}
	}
	return file
}

func (s *searches) search(ctx context.Context, query string, onResult func(SearchResult)) error {
	if query == "" {
		return errors.New("slsk: empty search query")
	}
	token := s.tokenCounter.Add(1)
	pending := newPendingSearch(onResult)
	s.mu.Lock()
	s.pending[token] = pending
	s.mu.Unlock()

	wire := bypassExcludedPhrases(query, s.server.ExcludedSearchPhrases())
	s.log.Debug("sending search query", "component", "search", "query", query, "sent", wire, "token", token)
	if err := s.server.Search(token, wire); err != nil {
		s.remove(token)
		return err
	}

	s.waitForCompletion(ctx, token, pending)
	s.log.Debug("search query done", "component", "search", "query", query, "token", token)
	return nil
}

// waitForCompletion blocks until the search times out. The search waits up to
// the first-result timeout for a peer to reply; each delivered answer then
// restarts the shorter idle timer. When complete, it removes the pending
// search entry.
func (s *searches) waitForCompletion(ctx context.Context, token uint32, pending *pendingSearch) {
	defer s.remove(token)

	maxTimer := time.NewTimer(s.timeouts.Max)
	defer maxTimer.Stop()
	quietTimer := time.NewTimer(s.timeouts.FirstResult)
	defer quietTimer.Stop()

	for {
		select {
		case <-pending.activity:
			quietTimer.Reset(s.timeouts.Idle)
		case <-quietTimer.C:
			return
		case <-maxTimer.C:
			return
		case <-ctx.Done():
			return
		}
	}
}

func (s *searches) remove(token uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.pending, token)
}

// bypassExcludedPhrases rewrites a query so it no longer contains any phrase
// the server drops searches for, by turning the first letter of each
// occurrence into a wildcard ("frank ocean blonde" -> "*rank ocean blonde").
// Peers still match the wildcarded word as a suffix, so results are the same.
// Matching is case-insensitive, rune by rune so positions in the lowered copy
// always line up with the original.
func bypassExcludedPhrases(query string, phrases []string) string {
	if len(phrases) == 0 {
		return query
	}
	out := []rune(query)
	q := lowerRunes(out)
	for _, phrase := range phrases {
		p := lowerRunes([]rune(phrase))
		if len(p) == 0 || p[0] == '*' {
			continue
		}
		for i := 0; i+len(p) <= len(q); i++ {
			if slices.Equal(q[i:i+len(p)], p) {
				q[i], out[i] = '*', '*'
			}
		}
	}
	return string(out)
}

func lowerRunes(rs []rune) []rune {
	out := make([]rune, len(rs))
	for i, r := range rs {
		out[i] = unicode.ToLower(r)
	}
	return out
}
