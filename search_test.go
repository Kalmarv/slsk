package slsk

import (
	"bytes"
	"compress/zlib"
	"context"
	"encoding/binary"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/kalmarv/slsk/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// resultCollector collects what a search's onResult callback receives.
type resultCollector struct {
	mu      sync.Mutex
	results []SearchResult
}

func (c *resultCollector) onResult(r SearchResult) {
	c.mu.Lock()
	c.results = append(c.results, r)
	c.mu.Unlock()
}

func (c *resultCollector) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.results)
}

func (c *resultCollector) get() []SearchResult {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]SearchResult(nil), c.results...)
}

// fakeSearchServer records the queries sent to the server.
type fakeSearchServer struct {
	mu       sync.Mutex
	sent     []string
	excluded []string
	err      error
}

func (f *fakeSearchServer) Search(_ uint32, query string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, query)
	return f.err
}

func (f *fakeSearchServer) ExcludedSearchPhrases() []string { return f.excluded }

// testTimeouts scales the real timeouts down so timing tests run in
// milliseconds while keeping first-result > idle.
var testTimeouts = SearchTimeouts{
	FirstResult: 300 * time.Millisecond,
	Idle:        100 * time.Millisecond,
	Max:         2 * time.Second,
}

func newTestSearches(timeouts SearchTimeouts) *searches {
	return &searches{
		log:      testLogger,
		server:   &fakeSearchServer{},
		timeouts: timeouts,
		pending:  make(map[uint32]*pendingSearch),
	}
}

func TestHandleResponse_ConvertsFilesAndAttributes(t *testing.T) {
	s := newTestSearches(testTimeouts)
	collector := &resultCollector{}
	s.pending[42] = newPendingSearch(collector.onResult)

	s.handleResponse(&protocol.SearchResponse{
		Username:       "bob",
		Token:          42,
		FreeUploadSlot: false,
		UploadSpeed:    1000,
		QueueLength:    10,
		Files: []protocol.SharedFile{
			{
				Filename:  `\Music\Artist\Track.flac`,
				Size:      31457280,
				Extension: "flac",
				Attributes: []protocol.FileAttribute{
					{Type: protocol.AttrBitRate, Value: 1411},
					{Type: protocol.AttrDuration, Value: 300},
					{Type: protocol.AttrVBR, Value: 1},
					{Type: protocol.AttrSampleRate, Value: 44100},
					{Type: protocol.AttrBitDepth, Value: 16},
				},
			},
			{Filename: `\Music\Artist\cover.jpg`, Size: 1000},
		},
	})

	results := collector.get()
	require.Len(t, results, 1)
	r := results[0]
	assert.Equal(t, "bob", r.Username)
	assert.False(t, r.FreeSlot)
	assert.Equal(t, uint32(1000), r.UploadSpeed)
	assert.Equal(t, uint32(10), r.QueueLength)
	assert.Equal(t, []File{
		{Name: `\Music\Artist\Track.flac`, Size: 31457280, Bitrate: 1411, Duration: 300, VBR: true, SampleRate: 44100, BitDepth: 16},
		{Name: `\Music\Artist\cover.jpg`, Size: 1000},
	}, r.Files)
}

func TestHandleResponse_IgnoresUnknownTokenAndEmptyAnswers(t *testing.T) {
	s := newTestSearches(testTimeouts)
	collector := &resultCollector{}
	s.pending[1] = newPendingSearch(collector.onResult)

	s.handleResponse(&protocol.SearchResponse{Username: "x", Token: 2, Files: []protocol.SharedFile{{Filename: "a"}}})
	s.handleResponse(&protocol.SearchResponse{Username: "y", Token: 1})
	assert.Equal(t, 0, collector.count())
}

func TestHandleResponse_ConcurrentSafe(t *testing.T) {
	s := newTestSearches(testTimeouts)
	collector := &resultCollector{}
	s.pending[100] = newPendingSearch(collector.onResult)

	const n = 50
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.handleResponse(&protocol.SearchResponse{
				Username: "user",
				Token:    100,
				Files:    []protocol.SharedFile{{Filename: `\Music\track.mp3`, Size: uint64(i * 1000)}},
			})
		}()
	}
	wg.Wait()
	assert.Equal(t, n, collector.count())
}

func TestHandleRawResponse_ParsesWireFormat(t *testing.T) {
	s := newTestSearches(testTimeouts)
	collector := &resultCollector{}
	s.pending[42] = newPendingSearch(collector.onResult)

	msg, err := protocol.BuildSearchResponse(protocol.SearchResponse{
		Username:       "peeruser",
		Token:          42,
		FreeUploadSlot: true,
		UploadSpeed:    3000,
		QueueLength:    2,
		Files: []protocol.SharedFile{{
			Filename: `\Shared\Album\01 - Track.flac`,
			Size:     25000000,
			Attributes: []protocol.FileAttribute{
				{Type: protocol.AttrBitRate, Value: 900},
				{Type: protocol.AttrDuration, Value: 210},
			},
		}},
	})
	require.NoError(t, err)

	s.handleRawResponse(msg, "peeruser")

	results := collector.get()
	require.Len(t, results, 1)
	assert.Equal(t, "peeruser", results[0].Username)
	assert.True(t, results[0].FreeSlot)
	assert.Equal(t, []File{{Name: `\Shared\Album\01 - Track.flac`, Size: 25000000, Bitrate: 900, Duration: 210}}, results[0].Files)
}

func TestSearch_SendsBypassedQueryAndCleansUp(t *testing.T) {
	s := newTestSearches(SearchTimeouts{FirstResult: 10 * time.Millisecond, Idle: 10 * time.Millisecond, Max: time.Second})
	server := &fakeSearchServer{excluded: []string{"frank ocean"}}
	s.server = server

	require.NoError(t, s.search(context.Background(), "frank ocean blonde", func(SearchResult) {}))
	assert.Equal(t, []string{"*rank ocean blonde"}, server.sent)
	assert.Empty(t, s.pending, "pending search should be removed when it ends")
}

func TestSearch_SendFailureIsReturned(t *testing.T) {
	s := newTestSearches(testTimeouts)
	s.server = &fakeSearchServer{err: ErrNotLoggedIn}

	err := s.search(context.Background(), "q", func(SearchResult) {})
	assert.ErrorIs(t, err, ErrNotLoggedIn)
	assert.Empty(t, s.pending)
}

func TestSearch_EmptyQueryIsRejected(t *testing.T) {
	s := newTestSearches(testTimeouts)
	assert.Error(t, s.search(context.Background(), "", func(SearchResult) {}))
}

func sendResult(s *searches, token uint32, user string) {
	s.handleResponse(&protocol.SearchResponse{
		Username: user,
		Token:    token,
		Files:    []protocol.SharedFile{{Filename: `\Music\` + user + `.mp3`, Size: 1024}},
	})
}

func TestWait_NoResultsEndsAtFirstResultTimeout(t *testing.T) {
	s := newTestSearches(testTimeouts)
	s.pending[42] = newPendingSearch(func(SearchResult) {})

	start := time.Now()
	s.waitForCompletion(context.Background(), 42, s.pending[42])
	elapsed := time.Since(start)

	assert.GreaterOrEqual(t, elapsed, testTimeouts.FirstResult, "must not give up at the idle timeout before any peer replied")
	assert.Less(t, elapsed, testTimeouts.Max)
	assert.False(t, s.hasPending(42), "pending search should be cleaned up after waitForCompletion")
}

func TestWait_SlowFirstResultIsKept(t *testing.T) {
	s := newTestSearches(testTimeouts)
	collector := &resultCollector{}
	pending := newPendingSearch(collector.onResult)
	s.pending[7] = pending

	// The first peer replies well after the idle timeout but before the
	// first-result timeout; a second peer follows within the idle window.
	go func() {
		time.Sleep(2 * testTimeouts.Idle)
		sendResult(s, 7, "alice")
		time.Sleep(testTimeouts.Idle / 2)
		sendResult(s, 7, "bob")
	}()

	start := time.Now()
	s.waitForCompletion(context.Background(), 7, pending)
	elapsed := time.Since(start)

	assert.Len(t, collector.get(), 2)
	// Ends one idle period after the last result, not at the first-result timeout.
	assert.GreaterOrEqual(t, elapsed, 2*testTimeouts.Idle+testTimeouts.Idle/2+testTimeouts.Idle)
	assert.Less(t, elapsed, testTimeouts.Max)
}

func TestWait_IdleTimeoutResetsOnNewResults(t *testing.T) {
	s := newTestSearches(testTimeouts)
	collector := &resultCollector{}
	pending := newPendingSearch(collector.onResult)
	s.pending[99] = pending

	// Results keep arriving inside the idle window, past the first-result
	// timeout, so the search keeps going until they stop.
	const n = 6
	go func() {
		for i := range n {
			time.Sleep(testTimeouts.Idle / 2)
			sendResult(s, 99, "user"+string(rune('a'+i)))
		}
	}()

	start := time.Now()
	s.waitForCompletion(context.Background(), 99, pending)
	elapsed := time.Since(start)

	assert.Len(t, collector.get(), n)
	assert.GreaterOrEqual(t, elapsed, n*testTimeouts.Idle/2+testTimeouts.Idle)
	assert.Less(t, elapsed, testTimeouts.Max)
}

func TestWait_MaxTimeoutCapsSteadyResults(t *testing.T) {
	timeouts := SearchTimeouts{FirstResult: 100 * time.Millisecond, Idle: 100 * time.Millisecond, Max: 300 * time.Millisecond}
	s := newTestSearches(timeouts)
	pending := newPendingSearch(func(SearchResult) {})
	s.pending[3] = pending

	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(20 * time.Millisecond):
				sendResult(s, 3, "steady")
			}
		}
	}()

	start := time.Now()
	s.waitForCompletion(context.Background(), 3, pending)
	elapsed := time.Since(start)

	assert.GreaterOrEqual(t, elapsed, timeouts.Max)
	assert.Less(t, elapsed, timeouts.Max+timeouts.Idle)
}

func TestWait_ContextCancellationEndsWait(t *testing.T) {
	s := newTestSearches(testTimeouts)
	collector := &resultCollector{}
	pending := newPendingSearch(collector.onResult)
	s.pending[55] = pending

	go func() {
		time.Sleep(20 * time.Millisecond)
		sendResult(s, 55, "charlie")
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()

	start := time.Now()
	s.waitForCompletion(ctx, 55, pending)
	elapsed := time.Since(start)

	assert.Len(t, collector.get(), 1)
	assert.Less(t, elapsed, testTimeouts.Idle+60*time.Millisecond, "context cancellation should end the wait")
}

// bombResponse builds a search response of about 20 KB on the wire that
// claims 1,000,000 empty files (21 MB decompressed); the same shape scales to
// 6,000,000 files in 122 KB.
func bombResponse(t *testing.T, token uint32) []byte {
	t.Helper()
	var payload bytes.Buffer
	w := func(v any) { require.NoError(t, binary.Write(&payload, binary.LittleEndian, v)) }
	w(uint32(1))
	payload.WriteString("u")
	w(token)
	const files = 1_000_000
	w(uint32(files))
	entry := make([]byte, 21) // code (ignored), empty name, size, empty ext, no attrs: all zero
	for range files {
		payload.Write(entry)
	}
	var z bytes.Buffer
	zw, _ := zlib.NewWriterLevel(&z, zlib.BestCompression)
	_, _ = zw.Write(payload.Bytes())
	require.NoError(t, zw.Close())
	msg := make([]byte, 8, 8+z.Len())
	binary.LittleEndian.PutUint32(msg[0:4], uint32(4+z.Len()))
	binary.LittleEndian.PutUint32(msg[4:8], protocol.PeerSearchResponse)
	return append(msg, z.Bytes()...)
}

func allocatedBytes(f func()) uint64 {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	f()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

func TestHandleRawResponse_UnsolicitedBombIsDroppedCheaply(t *testing.T) {
	s := newTestSearches(testTimeouts)
	msg := bombResponse(t, 99)
	require.Less(t, len(msg), 50<<10, "bomb must be a small wire message")

	alloc := allocatedBytes(func() { s.handleRawResponse(msg, "evil") })
	assert.Less(t, alloc, uint64(1<<20), "unsolicited response must not be decompressed")
}

func TestHandleRawResponse_BombForPendingSearchIsBounded(t *testing.T) {
	s := newTestSearches(testTimeouts)
	collector := &resultCollector{}
	s.pending[99] = newPendingSearch(collector.onResult)
	msg := bombResponse(t, 99)

	alloc := allocatedBytes(func() { s.handleRawResponse(msg, "evil") })
	assert.Less(t, alloc, uint64(40<<20), "decompression must stop at the search response cap")
	assert.Equal(t, 0, collector.count())
}

func TestBypassExcludedPhrases(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		phrases []string
		want    string
	}{
		{"no phrases", "frank ocean blonde", nil, "frank ocean blonde"},
		{"artist first", "frank ocean blonde", []string{"frank ocean"}, "*rank ocean blonde"},
		{"artist last", "blonde frank ocean", []string{"frank ocean"}, "blonde *rank ocean"},
		{"case-insensitive, keeps case", "Frank Ocean Blonde", []string{"FRANK OCEAN"}, "*rank Ocean Blonde"},
		{"every occurrence", "abc x abc", []string{"abc"}, "*bc x *bc"},
		{"several phrases", "james blake frank ocean", []string{"frank ocean", "james blake"}, "*ames blake *rank ocean"},
		{"unrelated phrase", "radiohead in rainbows", []string{"frank ocean"}, "radiohead in rainbows"},
		{"empty phrase ignored", "abc", []string{""}, "abc"},
		{"non-ascii", "björk homogenic", []string{"björk"}, "*jörk homogenic"},
		{"rune that lowercases to two", "İstanbul x", []string{"x"}, "İstanbul *"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := bypassExcludedPhrases(tt.query, tt.phrases); got != tt.want {
				t.Errorf("bypassExcludedPhrases(%q, %q) = %q, want %q", tt.query, tt.phrases, got, tt.want)
			}
		})
	}
}

func FuzzBypassExcludedPhrases(f *testing.F) {
	f.Add("frank ocean blonde", "frank ocean")
	f.Add("İİİ", "i")
	f.Add("aaaa", "aa")
	f.Fuzz(func(t *testing.T, query, phrase string) {
		if !utf8.ValidString(query) || !utf8.ValidString(phrase) {
			return
		}
		got := bypassExcludedPhrases(query, []string{phrase})
		if utf8.RuneCountInString(got) != utf8.RuneCountInString(query) {
			t.Fatalf("rune count changed: %q -> %q", query, got)
		}
		lp := string(lowerRunes([]rune(phrase)))
		if phrase != "" && !strings.HasPrefix(phrase, "*") && strings.Contains(string(lowerRunes([]rune(got))), lp) {
			t.Fatalf("%q still contains %q", got, phrase)
		}
	})
}
