package slsk

import (
	"bytes"
	"compress/zlib"
	"io"
	"net"
	"reflect"
	"testing"
	"time"
)

// Everything a peer, a distributed parent or the server sends us goes through
// these parsers, so none of them may panic or hang on arbitrary input. Run a
// target with e.g. `go test ./internal/slsk -run '^$' -fuzz FuzzParsers`.

// messageParsers are all parsers of length-prefixed server/peer/distrib
// messages. Each is called on the same fuzzed input; only panics fail.
var messageParsers = map[string]func([]byte){
	"SearchResponse":          func(b []byte) { _, _ = ParseSearchResponse(b) },
	"SearchResponseHeader":    func(b []byte) { _, _ = ParseSearchResponseHeader(b) },
	"TransferRequest":         func(b []byte) { _, _ = ParseTransferRequest(b) },
	"TransferResponse":        func(b []byte) { _, _ = ParseTransferResponse(b) },
	"QueueDownload":           func(b []byte) { _, _ = ParseQueueDownload(b) },
	"PlaceInQueueRequest":     func(b []byte) { _, _ = ParsePlaceInQueueRequest(b) },
	"PlaceInQueueResponse":    func(b []byte) { _, _ = ParsePlaceInQueueResponse(b) },
	"LoginResponse":           func(b []byte) { _, _ = ParseLoginResponse(b) },
	"GetPeerAddressResponse":  func(b []byte) { _, _ = ParseGetPeerAddressResponse(b) },
	"GetUserStatusResponse":   func(b []byte) { _, _ = ParseGetUserStatusResponse(b) },
	"WatchUserResponse":       func(b []byte) { _, _ = ParseWatchUserResponse(b) },
	"ConnectToPeerResponse":   func(b []byte) { _, _ = ParseConnectToPeerResponse(b) },
	"ServerSearchRequest":     func(b []byte) { _, _ = ParseServerSearchRequest(b) },
	"NetInfo":                 func(b []byte) { _, _ = ParseNetInfo(b) },
	"CannotConnect":           func(b []byte) { _, _ = ParseCannotConnect(b) },
	"ExcludedSearchPhrases":   func(b []byte) { _, _ = ParseExcludedSearchPhrases(b) },
	"DistribBranchLevel":      func(b []byte) { _, _ = ParseDistribBranchLevel(b) },
	"DistribBranchRoot":       func(b []byte) { _, _ = ParseDistribBranchRoot(b) },
	"DistribSearchRequest":    func(b []byte) { _, _ = ParseDistribSearchRequest(b) },
	"PeerInit":                func(b []byte) { _, _ = ParsePeerInit(b) },
	"PierceFirewall":          func(b []byte) { _, _ = ParsePierceFirewall(b) },
	"MessageReader.ReadAllOf": readEveryField,
}

// readEveryField walks a message with every MessageReader method until it
// runs out of data, exercising the bounds checks on all field types.
func readEveryField(b []byte) {
	r, err := NewReader(b)
	if err != nil {
		return
	}
	for i := 0; r.HasMore() && i < 1000; i++ {
		var err error
		switch i % 5 {
		case 0:
			_, err = r.ReadString()
		case 1:
			_, err = r.ReadUint32()
		case 2:
			_, err = r.ReadUint64()
		case 3:
			_, err = r.ReadBool()
		case 4:
			_, err = r.ReadUint8()
		}
		if err != nil {
			return
		}
	}
}

func messageSeeds(f *testing.F) {
	sr, err := BuildSearchResponse(SearchResponse{
		Username: "peer", Token: 7, FreeUploadSlot: true, UploadSpeed: 1000, QueueLength: 2,
		Files: []SharedFile{{
			Filename: `Music\Artist\Album\01 - Track.flac`, Size: 1234, Extension: "flac",
			Attributes: []FileAttribute{{Type: AttrDuration, Value: 200}, {Type: AttrSampleRate, Value: 44100}},
		}},
		LockedFiles: []SharedFile{{Filename: `Music\x.mp3`, Size: 1}},
	})
	if err != nil {
		f.Fatal(err)
	}
	for _, seed := range [][]byte{
		sr,
		BuildTransferRequest(TransferUpload, 1, `Music\a.flac`, 99),
		BuildTransferRequest(TransferDownload, 2, `Music\b.flac`, 0),
		BuildTransferResponseAllowed(3),
		BuildTransferResponseDenied(4, "Queued"),
		BuildQueueDownload(`Music\c.flac`),
		BuildPlaceInQueueRequest(`Music\d.flac`),
		BuildPlaceInQueueResponse(`Music\e.flac`, 5),
		BuildUploadDenied(`Music\f.flac`, DenyFileNotShared),
		BuildDistribBranchLevel(3),
		BuildDistribBranchRoot("root"),
		BuildPeerInit("me", ConnTypePeer, 9),
		BuildPierceFirewall(10),
		BuildFileSearchRequest(11, "artist album"),
		BuildConnectToPeerRequest(12, "peer", ConnTypeTransfer),
		excludedPhrasesMsg("artist one", "artist two"),
		{},
		{1, 0, 0, 0},
		{8, 0, 0, 0, 9, 0, 0, 0, 0xff, 0xff, 0xff, 0xff},
	} {
		f.Add(seed)
	}
}

func excludedPhrasesMsg(phrases ...string) []byte {
	b := NewBuilder(ServerExcludedPhrases)
	b.WriteUint32(uint32(len(phrases)))
	for _, p := range phrases {
		b.WriteString(p)
	}
	return b.Build()
}

func FuzzParsers(f *testing.F) {
	messageSeeds(f)
	f.Fuzz(func(t *testing.T, msg []byte) {
		for name, parse := range messageParsers {
			func() {
				defer func() {
					if r := recover(); r != nil {
						t.Fatalf("%s panicked on %x: %v", name, msg, r)
					}
				}()
				parse(msg)
			}()
		}
	})
}

// FuzzSearchResponsePayload feeds arbitrary decompressed payloads through a
// valid zlib envelope, which byte-level fuzzing of FuzzParsers rarely reaches.
func FuzzSearchResponsePayload(f *testing.F) {
	sr, err := BuildSearchResponse(SearchResponse{Username: "u", Token: 1, Files: []SharedFile{{Filename: "a", Size: 1, Extension: "mp3"}}})
	if err != nil {
		f.Fatal(err)
	}
	payload, err := zlibDecompress(sr[8:], maxSearchResponseSize)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(payload)
	f.Add([]byte{})
	f.Add([]byte{1, 0, 0, 0, 'u', 1, 0, 0, 0, 0xff, 0xff, 0xff, 0x7f})
	f.Fuzz(func(t *testing.T, payload []byte) {
		var buf bytes.Buffer
		w := zlib.NewWriter(&buf)
		_, _ = w.Write(payload)
		_ = w.Close()
		b := NewBuilder(PeerSearchResponse)
		b.WriteBytes(buf.Bytes())
		resp, err := ParseSearchResponse(b.Build())
		if err != nil {
			return
		}
		// Work is bounded by input size: every parsed entry consumed bytes.
		if got := len(resp.Files) + len(resp.LockedFiles); got > len(payload)/minSharedFileSize {
			t.Fatalf("parsed %d files from a %d byte payload", got, len(payload))
		}
		for _, f := range append(resp.Files, resp.LockedFiles...) {
			if len(f.Attributes) > len(payload)/fileAttributeSize {
				t.Fatalf("parsed %d attributes from a %d byte payload", len(f.Attributes), len(payload))
			}
		}
	})
}

// FuzzSearchResponseRoundTrip checks that what we send in reply to searches
// parses back to the same response.
func FuzzSearchResponseRoundTrip(f *testing.F) {
	f.Add("peer", uint32(1), `Music\a.flac`, uint64(10), "flac", uint32(320), true, uint32(5), uint32(0))
	f.Add("", uint32(0), "", uint64(0), "", uint32(0), false, uint32(0), uint32(0))
	f.Fuzz(func(t *testing.T, user string, token uint32, name string, size uint64, ext string, bitrate uint32, free bool, speed, queue uint32) {
		in := SearchResponse{
			Username: user, Token: token, FreeUploadSlot: free, UploadSpeed: speed, QueueLength: queue,
			Files: []SharedFile{{Filename: name, Size: size, Extension: ext, Attributes: []FileAttribute{{Type: AttrBitRate, Value: bitrate}}}},
		}
		msg, err := BuildSearchResponse(in)
		if err != nil {
			t.Fatal(err)
		}
		out, err := ParseSearchResponse(msg)
		if err != nil {
			t.Fatalf("parse own response: %v", err)
		}
		if len(out.LockedFiles) == 0 {
			out.LockedFiles = nil
		}
		if !reflect.DeepEqual(in, *out) {
			t.Fatalf("round trip mismatch:\n in  %+v\n out %+v", in, *out)
		}
	})
}

// streamConn is a read-only net.Conn over a byte slice.
type streamConn struct {
	r io.Reader
}

func (c *streamConn) Read(p []byte) (int, error)       { return c.r.Read(p) }
func (c *streamConn) Write(p []byte) (int, error)      { return len(p), nil }
func (c *streamConn) Close() error                     { return nil }
func (c *streamConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (c *streamConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (c *streamConn) SetDeadline(time.Time) error      { return nil }
func (c *streamConn) SetReadDeadline(time.Time) error  { return nil }
func (c *streamConn) SetWriteDeadline(time.Time) error { return nil }

// FuzzReadMessage checks the framing layer on an arbitrary byte stream: it
// must return whole messages whose length prefix matches, and never panic.
func FuzzReadMessage(f *testing.F) {
	f.Add(append(BuildPing(), BuildQueueDownload("x")...))
	f.Add([]byte{0xff, 0xff, 0xff, 0xff})
	f.Add([]byte{0, 0, 0, 0})
	f.Add([]byte{4, 0, 0, 0, 1, 2})
	f.Fuzz(func(t *testing.T, stream []byte) {
		c := WrapConn(&streamConn{r: bytes.NewReader(stream)})
		for i := 0; i < 64; i++ {
			msg, err := c.ReadMessage()
			if err != nil {
				return
			}
			if got, want := len(msg)-4, int(le.Uint32(msg[:4])); got != want {
				t.Fatalf("message length %d, prefix says %d", got, want)
			}
		}
	})
}
