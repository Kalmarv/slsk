package slsk

import (
	"encoding/binary"
	"runtime"
	"testing"
)

// ---------------------------------------------------------------------------
// SearchResponse round-trip tests
// ---------------------------------------------------------------------------

func TestSearchResponseRoundTrip(t *testing.T) {
	resp := SearchResponse{
		Username:       "testuser",
		Token:          42,
		FreeUploadSlot: true,
		UploadSpeed:    5000,
		QueueLength:    3,
		Files: []SharedFile{
			{
				Filename:  "Music/Artist/Song.mp3",
				Size:      5242880,
				Extension: "mp3",
				Attributes: []FileAttribute{
					{Type: AttrBitRate, Value: 320},
					{Type: AttrDuration, Value: 240},
					{Type: AttrVBR, Value: 0},
				},
			},
			{
				Filename:  "Music/Artist/Track2.flac",
				Size:      31457280,
				Extension: "flac",
				Attributes: []FileAttribute{
					{Type: AttrBitRate, Value: 1411},
					{Type: AttrSampleRate, Value: 44100},
					{Type: AttrBitDepth, Value: 16},
				},
			},
		},
	}

	msg, err := BuildSearchResponse(resp)
	if err != nil {
		t.Fatalf("BuildSearchResponse: %v", err)
	}

	parsed, err := ParseSearchResponse(msg)
	if err != nil {
		t.Fatalf("ParseSearchResponse: %v", err)
	}

	if parsed.Username != resp.Username {
		t.Fatalf("username = %q, want %q", parsed.Username, resp.Username)
	}
	if parsed.Token != resp.Token {
		t.Fatalf("token = %d, want %d", parsed.Token, resp.Token)
	}
	if parsed.FreeUploadSlot != resp.FreeUploadSlot {
		t.Fatalf("freeUploadSlot = %v, want %v", parsed.FreeUploadSlot, resp.FreeUploadSlot)
	}
	if parsed.UploadSpeed != resp.UploadSpeed {
		t.Fatalf("uploadSpeed = %d, want %d", parsed.UploadSpeed, resp.UploadSpeed)
	}
	if parsed.QueueLength != resp.QueueLength {
		t.Fatalf("queueLength = %d, want %d", parsed.QueueLength, resp.QueueLength)
	}
	if len(parsed.Files) != len(resp.Files) {
		t.Fatalf("file count = %d, want %d", len(parsed.Files), len(resp.Files))
	}
	for i, f := range parsed.Files {
		if f.Filename != resp.Files[i].Filename {
			t.Fatalf("file[%d] filename = %q, want %q", i, f.Filename, resp.Files[i].Filename)
		}
		if f.Size != resp.Files[i].Size {
			t.Fatalf("file[%d] size = %d, want %d", i, f.Size, resp.Files[i].Size)
		}
		if f.Extension != resp.Files[i].Extension {
			t.Fatalf("file[%d] extension = %q, want %q", i, f.Extension, resp.Files[i].Extension)
		}
		if len(f.Attributes) != len(resp.Files[i].Attributes) {
			t.Fatalf("file[%d] attr count = %d, want %d", i, len(f.Attributes), len(resp.Files[i].Attributes))
		}
		for j, a := range f.Attributes {
			if a.Type != resp.Files[i].Attributes[j].Type {
				t.Fatalf("file[%d] attr[%d] type = %d, want %d", i, j, a.Type, resp.Files[i].Attributes[j].Type)
			}
			if a.Value != resp.Files[i].Attributes[j].Value {
				t.Fatalf("file[%d] attr[%d] value = %d, want %d", i, j, a.Value, resp.Files[i].Attributes[j].Value)
			}
		}
	}
}

func TestSearchResponseWithLockedFiles(t *testing.T) {
	resp := SearchResponse{
		Username:       "sharer99",
		Token:          9999,
		FreeUploadSlot: false,
		UploadSpeed:    1000,
		QueueLength:    10,
		Files: []SharedFile{
			{
				Filename:   "Public/song.mp3",
				Size:       1024,
				Extension:  "mp3",
				Attributes: []FileAttribute{{Type: AttrBitRate, Value: 128}},
			},
		},
		LockedFiles: []SharedFile{
			{
				Filename:  "Private/premium.flac",
				Size:      52428800,
				Extension: "flac",
				Attributes: []FileAttribute{
					{Type: AttrBitRate, Value: 1411},
					{Type: AttrDuration, Value: 300},
				},
			},
			{
				Filename:   "Private/exclusive.wav",
				Size:       104857600,
				Extension:  "wav",
				Attributes: []FileAttribute{},
			},
		},
	}

	msg, err := BuildSearchResponse(resp)
	if err != nil {
		t.Fatalf("BuildSearchResponse: %v", err)
	}

	parsed, err := ParseSearchResponse(msg)
	if err != nil {
		t.Fatalf("ParseSearchResponse: %v", err)
	}

	if len(parsed.LockedFiles) != 2 {
		t.Fatalf("locked file count = %d, want 2", len(parsed.LockedFiles))
	}
	if parsed.LockedFiles[0].Filename != "Private/premium.flac" {
		t.Fatalf("locked[0] filename = %q", parsed.LockedFiles[0].Filename)
	}
	if parsed.LockedFiles[0].Size != 52428800 {
		t.Fatalf("locked[0] size = %d", parsed.LockedFiles[0].Size)
	}
	if len(parsed.LockedFiles[0].Attributes) != 2 {
		t.Fatalf("locked[0] attr count = %d", len(parsed.LockedFiles[0].Attributes))
	}
	if parsed.LockedFiles[1].Filename != "Private/exclusive.wav" {
		t.Fatalf("locked[1] filename = %q", parsed.LockedFiles[1].Filename)
	}
	if len(parsed.LockedFiles[1].Attributes) != 0 {
		t.Fatalf("locked[1] attr count = %d, want 0", len(parsed.LockedFiles[1].Attributes))
	}
}

func TestSearchResponseNoFiles(t *testing.T) {
	resp := SearchResponse{
		Username:       "emptyuser",
		Token:          1,
		FreeUploadSlot: true,
		UploadSpeed:    0,
		QueueLength:    0,
		Files:          []SharedFile{},
	}

	msg, err := BuildSearchResponse(resp)
	if err != nil {
		t.Fatalf("BuildSearchResponse: %v", err)
	}

	parsed, err := ParseSearchResponse(msg)
	if err != nil {
		t.Fatalf("ParseSearchResponse: %v", err)
	}

	if len(parsed.Files) != 0 {
		t.Fatalf("file count = %d, want 0", len(parsed.Files))
	}
	if parsed.Username != "emptyuser" {
		t.Fatalf("username = %q", parsed.Username)
	}
}

func TestSearchResponseWireCodeIs9(t *testing.T) {
	resp := SearchResponse{
		Username: "u", Token: 1, Files: []SharedFile{},
		FreeUploadSlot: false, UploadSpeed: 0, QueueLength: 0,
	}
	msg, err := BuildSearchResponse(resp)
	if err != nil {
		t.Fatal(err)
	}

	// Code is at bytes 4-7 (after length prefix).
	code := binary.LittleEndian.Uint32(msg[4:8])
	if code != PeerSearchResponse {
		t.Fatalf("wire code = %d, want %d", code, PeerSearchResponse)
	}
}

func TestSearchResponseBadCode(t *testing.T) {
	// Build a valid search response, then tamper the code.
	resp := SearchResponse{
		Username: "u", Token: 1, Files: []SharedFile{},
		FreeUploadSlot: false, UploadSpeed: 0, QueueLength: 0,
	}
	msg, err := BuildSearchResponse(resp)
	if err != nil {
		t.Fatal(err)
	}

	// Change code to something wrong.
	binary.LittleEndian.PutUint32(msg[4:8], 99)
	_, err = ParseSearchResponse(msg)
	if err == nil {
		t.Fatal("expected error for wrong code")
	}
}

func TestSearchResponseTooShort(t *testing.T) {
	_, err := ParseSearchResponse([]byte{1, 2, 3})
	if err == nil {
		t.Fatal("expected error for short message")
	}
}

// ---------------------------------------------------------------------------
// Transfer Request round-trip tests
// ---------------------------------------------------------------------------

func TestTransferRequestDownloadRoundTrip(t *testing.T) {
	msg := BuildTransferRequest(TransferDownload, 12345, "Music/song.mp3", 0)
	parsed, err := ParseTransferRequest(msg)
	if err != nil {
		t.Fatalf("ParseTransferRequest: %v", err)
	}

	if parsed.Direction != TransferDownload {
		t.Fatalf("direction = %d, want %d", parsed.Direction, TransferDownload)
	}
	if parsed.Token != 12345 {
		t.Fatalf("token = %d, want 12345", parsed.Token)
	}
	if parsed.Filename != "Music/song.mp3" {
		t.Fatalf("filename = %q", parsed.Filename)
	}
	// Download messages don't include file size.
	if parsed.FileSize != 0 {
		t.Fatalf("fileSize = %d, want 0", parsed.FileSize)
	}
}

func TestTransferRequestUploadRoundTrip(t *testing.T) {
	msg := BuildTransferRequest(TransferUpload, 67890, "Music/album/track.flac", 31457280)
	parsed, err := ParseTransferRequest(msg)
	if err != nil {
		t.Fatalf("ParseTransferRequest: %v", err)
	}

	if parsed.Direction != TransferUpload {
		t.Fatalf("direction = %d, want %d", parsed.Direction, TransferUpload)
	}
	if parsed.Token != 67890 {
		t.Fatalf("token = %d, want 67890", parsed.Token)
	}
	if parsed.Filename != "Music/album/track.flac" {
		t.Fatalf("filename = %q", parsed.Filename)
	}
	if parsed.FileSize != 31457280 {
		t.Fatalf("fileSize = %d, want 31457280", parsed.FileSize)
	}
}

func TestTransferRequestWireCode(t *testing.T) {
	msg := BuildTransferRequest(TransferDownload, 1, "f", 0)
	code := binary.LittleEndian.Uint32(msg[4:8])
	if code != PeerTransferRequest {
		t.Fatalf("wire code = %d, want %d", code, PeerTransferRequest)
	}
}

func TestTransferRequestBadCode(t *testing.T) {
	msg := BuildTransferRequest(TransferDownload, 1, "f", 0)
	binary.LittleEndian.PutUint32(msg[4:8], 99)
	_, err := ParseTransferRequest(msg)
	if err == nil {
		t.Fatal("expected error for wrong code")
	}
}

// ---------------------------------------------------------------------------
// Transfer Response tests
// ---------------------------------------------------------------------------

func TestTransferResponseAllowed(t *testing.T) {
	msg := BuildTransferResponseAllowed(55555)

	r, err := NewReader(msg)
	if err != nil {
		t.Fatal(err)
	}
	if r.Code() != PeerTransferResponse {
		t.Fatalf("code = %d, want %d", r.Code(), PeerTransferResponse)
	}

	token, err := r.ReadUint32()
	if err != nil {
		t.Fatal(err)
	}
	if token != 55555 {
		t.Fatalf("token = %d, want 55555", token)
	}

	allowed, err := r.ReadBool()
	if err != nil {
		t.Fatal(err)
	}
	if !allowed {
		t.Fatal("expected allowed=true")
	}

	if r.HasMore() {
		t.Fatal("expected no more data")
	}
}

func TestTransferResponseDenied(t *testing.T) {
	msg := BuildTransferResponseDenied(77777, "Queued")

	r, err := NewReader(msg)
	if err != nil {
		t.Fatal(err)
	}
	if r.Code() != PeerTransferResponse {
		t.Fatalf("code = %d, want %d", r.Code(), PeerTransferResponse)
	}

	token, err := r.ReadUint32()
	if err != nil {
		t.Fatal(err)
	}
	if token != 77777 {
		t.Fatalf("token = %d, want 77777", token)
	}

	allowed, err := r.ReadBool()
	if err != nil {
		t.Fatal(err)
	}
	if allowed {
		t.Fatal("expected allowed=false")
	}

	reason, err := r.ReadString()
	if err != nil {
		t.Fatal(err)
	}
	if reason != "Queued" {
		t.Fatalf("reason = %q, want %q", reason, "Queued")
	}
}

// ---------------------------------------------------------------------------
// Queue Download tests
// ---------------------------------------------------------------------------

func TestQueueDownload(t *testing.T) {
	msg := BuildQueueDownload("Music/Artist/Song.mp3")

	r, err := NewReader(msg)
	if err != nil {
		t.Fatal(err)
	}
	if r.Code() != PeerQueueDownload {
		t.Fatalf("code = %d, want %d", r.Code(), PeerQueueDownload)
	}

	filename, err := r.ReadString()
	if err != nil {
		t.Fatal(err)
	}
	if filename != "Music/Artist/Song.mp3" {
		t.Fatalf("filename = %q", filename)
	}
	if r.HasMore() {
		t.Fatal("expected no more data")
	}
}

// ---------------------------------------------------------------------------
// Place In Queue Request tests
// ---------------------------------------------------------------------------

func TestPlaceInQueueRequest(t *testing.T) {
	msg := BuildPlaceInQueueRequest("Music/file.flac")

	r, err := NewReader(msg)
	if err != nil {
		t.Fatal(err)
	}
	if r.Code() != PeerPlaceInQueueRequest {
		t.Fatalf("code = %d, want %d", r.Code(), PeerPlaceInQueueRequest)
	}

	filename, err := r.ReadString()
	if err != nil {
		t.Fatal(err)
	}
	if filename != "Music/file.flac" {
		t.Fatalf("filename = %q", filename)
	}
	if r.HasMore() {
		t.Fatal("expected no more data")
	}
}

// ---------------------------------------------------------------------------
// Place In Queue Response tests
// ---------------------------------------------------------------------------

func TestPlaceInQueueResponseRoundTrip(t *testing.T) {
	// Build a wire message manually for PlaceInQueueResponse.
	b := NewBuilder(PeerPlaceInQueueResponse)
	b.WriteString("Music/queued.mp3")
	b.WriteUint32(7)
	msg := b.Build()

	parsed, err := ParsePlaceInQueueResponse(msg)
	if err != nil {
		t.Fatalf("ParsePlaceInQueueResponse: %v", err)
	}
	if parsed.Filename != "Music/queued.mp3" {
		t.Fatalf("filename = %q", parsed.Filename)
	}
	if parsed.Place != 7 {
		t.Fatalf("place = %d, want 7", parsed.Place)
	}
}

func TestPlaceInQueueResponseBadCode(t *testing.T) {
	b := NewBuilder(99)
	b.WriteString("f")
	b.WriteUint32(1)
	msg := b.Build()

	_, err := ParsePlaceInQueueResponse(msg)
	if err == nil {
		t.Fatal("expected error for wrong code")
	}
}

// ---------------------------------------------------------------------------
// Upload Denied tests
// ---------------------------------------------------------------------------

func TestUploadDenied(t *testing.T) {
	msg := BuildUploadDenied("Music/denied.mp3", "Too many queued")

	r, err := NewReader(msg)
	if err != nil {
		t.Fatal(err)
	}
	if r.Code() != PeerUploadDenied {
		t.Fatalf("code = %d, want %d", r.Code(), PeerUploadDenied)
	}

	filename, err := r.ReadString()
	if err != nil {
		t.Fatal(err)
	}
	if filename != "Music/denied.mp3" {
		t.Fatalf("filename = %q", filename)
	}

	reason, err := r.ReadString()
	if err != nil {
		t.Fatal(err)
	}
	if reason != "Too many queued" {
		t.Fatalf("reason = %q", reason)
	}
	if r.HasMore() {
		t.Fatal("expected no more data")
	}
}

// ---------------------------------------------------------------------------
// Upload Failed tests
// ---------------------------------------------------------------------------

func TestUploadFailed(t *testing.T) {
	msg := BuildUploadFailed("Music/failed.mp3")

	r, err := NewReader(msg)
	if err != nil {
		t.Fatal(err)
	}
	if r.Code() != PeerUploadFailed {
		t.Fatalf("code = %d, want %d", r.Code(), PeerUploadFailed)
	}

	filename, err := r.ReadString()
	if err != nil {
		t.Fatal(err)
	}
	if filename != "Music/failed.mp3" {
		t.Fatalf("filename = %q", filename)
	}
	if r.HasMore() {
		t.Fatal("expected no more data")
	}
}

// ---------------------------------------------------------------------------
// Large file size test (uint64 boundary)
// ---------------------------------------------------------------------------

func TestSearchResponseLargeFileSize(t *testing.T) {
	resp := SearchResponse{
		Username:       "bigfiles",
		Token:          100,
		FreeUploadSlot: true,
		UploadSpeed:    10000,
		QueueLength:    0,
		Files: []SharedFile{
			{
				Filename:   "huge.iso",
				Size:       0xFFFFFFFFFF, // > 4GB, tests uint64 handling
				Extension:  "iso",
				Attributes: []FileAttribute{},
			},
		},
	}

	msg, err := BuildSearchResponse(resp)
	if err != nil {
		t.Fatal(err)
	}

	parsed, err := ParseSearchResponse(msg)
	if err != nil {
		t.Fatal(err)
	}

	if parsed.Files[0].Size != 0xFFFFFFFFFF {
		t.Fatalf("size = %d, want %d", parsed.Files[0].Size, uint64(0xFFFFFFFFFF))
	}
}

func TestTransferRequestLargeFileSize(t *testing.T) {
	msg := BuildTransferRequest(TransferUpload, 1, "huge.bin", 0xABCDEF0123456789)
	parsed, err := ParseTransferRequest(msg)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.FileSize != 0xABCDEF0123456789 {
		t.Fatalf("fileSize = %#x, want 0xABCDEF0123456789", parsed.FileSize)
	}
}

// ---------------------------------------------------------------------------
// ParseQueueDownload tests
// ---------------------------------------------------------------------------

func TestParseQueueDownload(t *testing.T) {
	// Wire: [len][code=43][filename: string]
	var payload []byte
	payload = appendStr(payload, `Music\Artist\Album\01 Track.flac`)
	msg := buildMsg(43, payload)

	filename, err := ParseQueueDownload(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if filename != `Music\Artist\Album\01 Track.flac` {
		t.Errorf("filename = %q, want %q", filename, `Music\Artist\Album\01 Track.flac`)
	}
}

func TestParseQueueDownloadEmpty(t *testing.T) {
	var payload []byte
	payload = appendStr(payload, "")
	msg := buildMsg(43, payload)

	_, err := ParseQueueDownload(msg)
	if err != nil {
		t.Fatalf("unexpected error for empty filename: %v", err)
	}
}

// ---------------------------------------------------------------------------
// ParseTransferResponse tests
// ---------------------------------------------------------------------------

func TestParseTransferResponseAllowed(t *testing.T) {
	// Wire: [len][code=41][token: u32][allowed: bool=1]
	var payload []byte
	payload = append(payload, u32le(12345)...)
	payload = append(payload, 1) // allowed=true
	msg := buildMsg(41, payload)

	resp, err := ParseTransferResponse(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Token != 12345 || !resp.Allowed {
		t.Errorf("got token=%d allowed=%v, want 12345/true", resp.Token, resp.Allowed)
	}
}

func TestParseTransferResponseDenied(t *testing.T) {
	// Wire: [len][code=41][token: u32][allowed: bool=0][reason: string]
	var payload []byte
	payload = append(payload, u32le(99999)...)
	payload = append(payload, 0) // allowed=false
	payload = appendStr(payload, "Queued")
	msg := buildMsg(41, payload)

	resp, err := ParseTransferResponse(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Token != 99999 || resp.Allowed || resp.Reason != "Queued" {
		t.Errorf("got token=%d allowed=%v reason=%q", resp.Token, resp.Allowed, resp.Reason)
	}
}

// ---------------------------------------------------------------------------
// BuildPlaceInQueueResponse tests
// ---------------------------------------------------------------------------

func TestBuildPlaceInQueueResponse(t *testing.T) {
	msg := BuildPlaceInQueueResponse(`Music\file.flac`, 5)
	parsed, err := ParsePlaceInQueueResponse(msg)
	if err != nil {
		t.Fatalf("round-trip failed: %v", err)
	}
	if parsed.Filename != `Music\file.flac` || parsed.Place != 5 {
		t.Errorf("got filename=%q place=%d", parsed.Filename, parsed.Place)
	}
}

// ---------------------------------------------------------------------------
// ParsePlaceInQueueRequest tests
// ---------------------------------------------------------------------------

func TestParsePlaceInQueueRequest(t *testing.T) {
	// Wire: [len][code=51][filename: string]
	var payload []byte
	payload = appendStr(payload, `Music\Artist\track.mp3`)
	msg := buildMsg(51, payload)

	filename, err := ParsePlaceInQueueRequest(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if filename != `Music\Artist\track.mp3` {
		t.Errorf("filename = %q", filename)
	}
}

// ---------------------------------------------------------------------------
// Hostile search responses
// ---------------------------------------------------------------------------

// searchResponseWire wraps an uncompressed payload in a code-9 message.
func searchResponseWire(t *testing.T, payload []byte) []byte {
	t.Helper()
	z, err := zlibCompress(payload)
	if err != nil {
		t.Fatal(err)
	}
	b := NewBuilder(PeerSearchResponse)
	b.WriteBytes(z)
	return b.Build()
}

func searchPayload(count uint32, tail ...byte) []byte {
	b := NewBuilder(0)
	b.WriteString("u")
	b.WriteUint32(7)
	b.WriteUint32(count)
	b.WriteBytes(tail)
	return b.Build()[8:]
}

func TestParseSearchResponse_RejectsCountLargerThanPayload(t *testing.T) {
	msg := searchResponseWire(t, searchPayload(0xffffffff, make([]byte, 100)...))
	if _, err := ParseSearchResponse(msg); err == nil {
		t.Fatal("expected error for file count exceeding the payload")
	}
}

func TestParseSearchResponse_RejectsAttrCountLargerThanPayload(t *testing.T) {
	b := NewBuilder(0)
	b.WriteUint8(1)
	b.WriteString("a")
	b.WriteUint64(1)
	b.WriteString("mp3")
	b.WriteUint32(0xffffffff) // attribute count
	msg := searchResponseWire(t, searchPayload(1, b.Build()[8:]...))
	if _, err := ParseSearchResponse(msg); err == nil {
		t.Fatal("expected error for attribute count exceeding the payload")
	}
}

func TestParseSearchResponse_RejectsOversizedPayload(t *testing.T) {
	msg := searchResponseWire(t, make([]byte, maxSearchResponseSize+1))
	if _, err := ParseSearchResponse(msg); err == nil {
		t.Fatal("expected error for decompressed payload over the cap")
	}
}

// A bomb of empty files compresses ~1000:1 (6,000,000 files fit in ~122 KB on
// the wire). Both the header read and the full parse must stay cheap; 1,000,000
// files (21 MB decompressed) show the same thing without a slow test.
func TestParseSearchResponse_BombIsCheap(t *testing.T) {
	const files = 1_000_000
	payload := append(searchPayload(files), make([]byte, files*minSharedFileSize)...)
	msg := searchResponseWire(t, payload)
	if len(msg) > 50<<10 {
		t.Fatalf("test bomb is %d bytes on the wire, want under 50 KB", len(msg))
	}

	alloc := func(f func()) uint64 {
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		f()
		runtime.ReadMemStats(&after)
		return after.TotalAlloc - before.TotalAlloc
	}

	var hdr *SearchResponseHeader
	if n := alloc(func() { hdr, _ = ParseSearchResponseHeader(msg) }); n > 256<<10 {
		t.Errorf("header parse allocated %d bytes", n)
	}
	if hdr == nil || hdr.Username != "u" || hdr.Token != 7 {
		t.Errorf("header = %+v, want u/7", hdr)
	}
	var err error
	if n := alloc(func() { _, err = ParseSearchResponse(msg) }); n > 8*maxSearchResponseSize {
		t.Errorf("full parse allocated %d bytes", n)
	}
	if err == nil {
		t.Error("expected the bomb to be rejected")
	}
}

func TestParseSearchResponseHeader(t *testing.T) {
	msg, err := BuildSearchResponse(SearchResponse{Username: "alice", Token: 42, Files: []SharedFile{{Filename: "a", Extension: "mp3"}}})
	if err != nil {
		t.Fatal(err)
	}
	hdr, err := ParseSearchResponseHeader(msg)
	if err != nil {
		t.Fatal(err)
	}
	if hdr.Username != "alice" || hdr.Token != 42 {
		t.Errorf("header = %+v", hdr)
	}

	long := searchResponseWire(t, append([]byte{0xff, 0xff, 0, 0}, make([]byte, 10)...))
	if _, err := ParseSearchResponseHeader(long); err == nil {
		t.Error("expected error for oversized username length")
	}
	if _, err := ParseSearchResponseHeader(BuildQueueDownload("x")); err == nil {
		t.Error("expected error for wrong code")
	}
}
