package slsk

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"io"
)

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

// FileAttribute represents a single file attribute (e.g., bitrate, duration).
type FileAttribute struct {
	Type  uint32
	Value uint32
}

// SharedFile represents a file in a search response or browse response.
type SharedFile struct {
	Filename   string
	Size       uint64
	Extension  string
	Attributes []FileAttribute
}

// SearchResponse is a peer search result message (code 9, zlib compressed).
type SearchResponse struct {
	Username       string
	Token          uint32
	Files          []SharedFile
	FreeUploadSlot bool
	UploadSpeed    uint32
	QueueLength    uint32
	LockedFiles    []SharedFile
}

// TransferRequest is a peer file transfer request (code 40).
type TransferRequest struct {
	Direction uint32
	Token     uint32
	Filename  string
	FileSize  uint64
}

// TransferResponse is a parsed transfer response (code 41).
type TransferResponse struct {
	Token   uint32
	Allowed bool
	Reason  string // only populated when Allowed=false
}

// PlaceInQueue is the response to a place-in-queue request (code 44).
type PlaceInQueue struct {
	Filename string
	Place    uint32
}

// ---------------------------------------------------------------------------
// Zlib helpers
// ---------------------------------------------------------------------------

func zlibCompress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := zlib.NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		return nil, fmt.Errorf("zlib compress write: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("zlib compress close: %w", err)
	}
	return buf.Bytes(), nil
}

// maxDecompressedSize bounds zlib output so a small crafted message cannot
// expand into gigabytes and kill the process (decompression bomb).
const maxDecompressedSize = 128 << 20 // 128 MiB

func zlibDecompress(data []byte) ([]byte, error) {
	r, err := zlib.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("zlib decompress init: %w", err)
	}
	defer r.Close()
	out, err := io.ReadAll(io.LimitReader(r, maxDecompressedSize+1))
	if err != nil {
		return nil, fmt.Errorf("zlib decompress read: %w", err)
	}
	if len(out) > maxDecompressedSize {
		return nil, fmt.Errorf("zlib decompress: output exceeds %d bytes", maxDecompressedSize)
	}
	return out, nil
}

// newReaderFromPayload wraps raw payload bytes in a MessageReader by prepending
// a synthetic length+code header. This lets us reuse MessageReader for parsing
// decompressed data that has no wire header.
func newReaderFromPayload(payload []byte) *MessageReader {
	// Build a synthetic wire message: [length][code=0][payload]
	synth := make([]byte, 8+len(payload))
	le.PutUint32(synth[0:4], uint32(4+len(payload))) // length = code + payload
	le.PutUint32(synth[4:8], 0)                      // dummy code
	copy(synth[8:], payload)
	r, _ := NewReader(synth) // cannot fail — synth is always >= 8 bytes
	return r
}

// ---------------------------------------------------------------------------
// Search Response (code 9, zlib compressed)
// ---------------------------------------------------------------------------

func writeSharedFiles(b *MessageBuilder, files []SharedFile) {
	b.WriteUint32(uint32(len(files)))
	for _, f := range files {
		b.WriteUint8(1) // file code
		b.WriteString(f.Filename)
		b.WriteUint64(f.Size)
		b.WriteString(f.Extension)
		b.WriteUint32(uint32(len(f.Attributes)))
		for _, a := range f.Attributes {
			b.WriteUint32(a.Type)
			b.WriteUint32(a.Value)
		}
	}
}

func readSharedFiles(r *MessageReader) ([]SharedFile, error) {
	count, err := r.ReadUint32()
	if err != nil {
		return nil, fmt.Errorf("read file count: %w", err)
	}
	files := make([]SharedFile, 0, min(int(count), 256))
	for i := uint32(0); i < count; i++ {
		var f SharedFile
		if _, err := r.ReadUint8(); err != nil { // file code
			return nil, fmt.Errorf("file[%d] code: %w", i, err)
		}
		f.Filename, err = r.ReadString()
		if err != nil {
			return nil, fmt.Errorf("file[%d] filename: %w", i, err)
		}
		f.Size, err = r.ReadUint64()
		if err != nil {
			return nil, fmt.Errorf("file[%d] size: %w", i, err)
		}
		f.Extension, err = r.ReadString()
		if err != nil {
			return nil, fmt.Errorf("file[%d] extension: %w", i, err)
		}
		attrCount, err := r.ReadUint32()
		if err != nil {
			return nil, fmt.Errorf("file[%d] attr count: %w", i, err)
		}
		f.Attributes = make([]FileAttribute, 0, min(int(attrCount), 16))
		for j := uint32(0); j < attrCount; j++ {
			var a FileAttribute
			a.Type, err = r.ReadUint32()
			if err != nil {
				return nil, fmt.Errorf("file[%d] attr[%d] type: %w", i, j, err)
			}
			a.Value, err = r.ReadUint32()
			if err != nil {
				return nil, fmt.Errorf("file[%d] attr[%d] value: %w", i, j, err)
			}
			f.Attributes = append(f.Attributes, a)
		}
		files = append(files, f)
	}
	return files, nil
}

// BuildSearchResponse builds a peer search response message (code 9).
// The payload is zlib-compressed.
func BuildSearchResponse(resp SearchResponse) ([]byte, error) {
	// Build the uncompressed inner payload.
	inner := NewBuilder(0) // dummy code — we only use the buf
	inner.WriteString(resp.Username)
	inner.WriteUint32(resp.Token)
	writeSharedFiles(inner, resp.Files)
	inner.WriteBool(resp.FreeUploadSlot)
	inner.WriteUint32(resp.UploadSpeed)
	inner.WriteUint32(resp.QueueLength)
	inner.WriteUint32(0) // unknown field

	if len(resp.LockedFiles) > 0 {
		writeSharedFiles(inner, resp.LockedFiles)
	}

	// inner.Build() gives [len][code=0][payload] — we want just the payload.
	built := inner.Build()
	rawPayload := built[8:] // skip synthetic length + code

	compressed, err := zlibCompress(rawPayload)
	if err != nil {
		return nil, fmt.Errorf("build search response: %w", err)
	}

	// Build the outer wire message: [length][code=9][compressed]
	outer := NewBuilder(PeerSearchResponse)
	outer.WriteBytes(compressed)
	return outer.Build(), nil
}

// ParseSearchResponse parses a peer search response message (code 9).
func ParseSearchResponse(msg []byte) (*SearchResponse, error) {
	if len(msg) < 8 {
		return nil, fmt.Errorf("search response too short: %d bytes", len(msg))
	}

	code := le.Uint32(msg[4:8])
	if code != PeerSearchResponse {
		return nil, fmt.Errorf("expected code %d, got %d", PeerSearchResponse, code)
	}

	decompressed, err := zlibDecompress(msg[8:])
	if err != nil {
		return nil, fmt.Errorf("search response decompress: %w", err)
	}

	r := newReaderFromPayload(decompressed)

	resp := &SearchResponse{}
	resp.Username, err = r.ReadString()
	if err != nil {
		return nil, fmt.Errorf("search response username: %w", err)
	}
	resp.Token, err = r.ReadUint32()
	if err != nil {
		return nil, fmt.Errorf("search response token: %w", err)
	}
	resp.Files, err = readSharedFiles(r)
	if err != nil {
		return nil, fmt.Errorf("search response files: %w", err)
	}
	resp.FreeUploadSlot, err = r.ReadBool()
	if err != nil {
		return nil, fmt.Errorf("search response free slot: %w", err)
	}
	resp.UploadSpeed, err = r.ReadUint32()
	if err != nil {
		return nil, fmt.Errorf("search response upload speed: %w", err)
	}
	resp.QueueLength, err = r.ReadUint32()
	if err != nil {
		return nil, fmt.Errorf("search response queue length: %w", err)
	}

	// Optional trailing fields.
	if r.HasMore() {
		// unknown uint32
		if _, err := r.ReadUint32(); err != nil {
			return nil, fmt.Errorf("search response unknown field: %w", err)
		}

		// Optional locked files.
		if r.HasMore() {
			resp.LockedFiles, err = readSharedFiles(r)
			if err != nil {
				return nil, fmt.Errorf("search response locked files: %w", err)
			}
		}
	}

	return resp, nil
}

// ---------------------------------------------------------------------------
// Transfer Request (code 40)
// ---------------------------------------------------------------------------

// BuildTransferRequest builds a peer transfer request message (code 40).
// FileSize is only included when direction is Upload (1).
func BuildTransferRequest(direction, token uint32, filename string, fileSize uint64) []byte {
	b := NewBuilder(PeerTransferRequest)
	b.WriteUint32(direction)
	b.WriteUint32(token)
	b.WriteString(filename)
	if direction == TransferUpload {
		b.WriteUint64(fileSize)
	}
	return b.Build()
}

// ParseTransferRequest parses a peer transfer request message (code 40).
func ParseTransferRequest(msg []byte) (*TransferRequest, error) {
	r, err := NewReader(msg)
	if err != nil {
		return nil, fmt.Errorf("transfer request: %w", err)
	}
	if r.Code() != PeerTransferRequest {
		return nil, fmt.Errorf("expected code %d, got %d", PeerTransferRequest, r.Code())
	}

	req := &TransferRequest{}
	req.Direction, err = r.ReadUint32()
	if err != nil {
		return nil, fmt.Errorf("transfer request direction: %w", err)
	}
	req.Token, err = r.ReadUint32()
	if err != nil {
		return nil, fmt.Errorf("transfer request token: %w", err)
	}
	req.Filename, err = r.ReadString()
	if err != nil {
		return nil, fmt.Errorf("transfer request filename: %w", err)
	}
	if r.HasMore() {
		req.FileSize, err = r.ReadUint64()
		if err != nil {
			return nil, fmt.Errorf("transfer request file size: %w", err)
		}
	}

	return req, nil
}

// ---------------------------------------------------------------------------
// Transfer Response (code 41)
// ---------------------------------------------------------------------------

// BuildTransferResponseAllowed builds an allowed transfer response (code 41).
func BuildTransferResponseAllowed(token uint32) []byte {
	b := NewBuilder(PeerTransferResponse)
	b.WriteUint32(token)
	b.WriteBool(true)
	return b.Build()
}

// BuildTransferResponseDenied builds a denied transfer response (code 41).
func BuildTransferResponseDenied(token uint32, reason string) []byte {
	b := NewBuilder(PeerTransferResponse)
	b.WriteUint32(token)
	b.WriteBool(false)
	b.WriteString(reason)
	return b.Build()
}

// ParseTransferResponse parses a transfer response (code 41).
func ParseTransferResponse(msg []byte) (*TransferResponse, error) {
	r, err := NewReader(msg)
	if err != nil {
		return nil, fmt.Errorf("transfer response: %w", err)
	}
	if r.Code() != PeerTransferResponse {
		return nil, fmt.Errorf("expected code %d, got %d", PeerTransferResponse, r.Code())
	}
	resp := &TransferResponse{}
	resp.Token, err = r.ReadUint32()
	if err != nil {
		return nil, fmt.Errorf("transfer response token: %w", err)
	}
	resp.Allowed, err = r.ReadBool()
	if err != nil {
		return nil, fmt.Errorf("transfer response allowed: %w", err)
	}
	if !resp.Allowed && r.HasMore() {
		resp.Reason, err = r.ReadString()
		if err != nil {
			return nil, fmt.Errorf("transfer response reason: %w", err)
		}
	}
	return resp, nil
}

// ---------------------------------------------------------------------------
// Queue Download (code 43)
// ---------------------------------------------------------------------------

// BuildQueueDownload builds a queue download message (code 43).
func BuildQueueDownload(filename string) []byte {
	b := NewBuilder(PeerQueueDownload)
	b.WriteString(filename)
	return b.Build()
}

// ParseQueueDownload parses a QueueDownload message (code 43).
// Returns the requested filename.
func ParseQueueDownload(msg []byte) (string, error) {
	r, err := NewReader(msg)
	if err != nil {
		return "", fmt.Errorf("queue download: %w", err)
	}
	if r.Code() != PeerQueueDownload {
		return "", fmt.Errorf("expected code %d, got %d", PeerQueueDownload, r.Code())
	}
	filename, err := r.ReadString()
	if err != nil {
		return "", fmt.Errorf("queue download filename: %w", err)
	}
	return filename, nil
}

// ---------------------------------------------------------------------------
// Place In Queue Request (code 51)
// ---------------------------------------------------------------------------

// BuildPlaceInQueueRequest builds a place-in-queue request (code 51).
func BuildPlaceInQueueRequest(filename string) []byte {
	b := NewBuilder(PeerPlaceInQueueRequest)
	b.WriteString(filename)
	return b.Build()
}

// ParsePlaceInQueueRequest parses a PlaceInQueueRequest message (code 51).
// Returns the requested filename.
func ParsePlaceInQueueRequest(msg []byte) (string, error) {
	r, err := NewReader(msg)
	if err != nil {
		return "", fmt.Errorf("place in queue request: %w", err)
	}
	if r.Code() != PeerPlaceInQueueRequest {
		return "", fmt.Errorf("expected code %d, got %d", PeerPlaceInQueueRequest, r.Code())
	}
	filename, err := r.ReadString()
	if err != nil {
		return "", fmt.Errorf("place in queue request filename: %w", err)
	}
	return filename, nil
}

// ---------------------------------------------------------------------------
// Place In Queue Response (code 44)
// ---------------------------------------------------------------------------

// BuildPlaceInQueueResponse builds a PlaceInQueueResponse message (code 44).
func BuildPlaceInQueueResponse(filename string, place uint32) []byte {
	b := NewBuilder(PeerPlaceInQueueResponse)
	b.WriteString(filename)
	b.WriteUint32(place)
	return b.Build()
}

// ParsePlaceInQueueResponse parses a place-in-queue response (code 44).
func ParsePlaceInQueueResponse(msg []byte) (*PlaceInQueue, error) {
	r, err := NewReader(msg)
	if err != nil {
		return nil, fmt.Errorf("place in queue response: %w", err)
	}
	if r.Code() != PeerPlaceInQueueResponse {
		return nil, fmt.Errorf("expected code %d, got %d", PeerPlaceInQueueResponse, r.Code())
	}

	result := &PlaceInQueue{}
	result.Filename, err = r.ReadString()
	if err != nil {
		return nil, fmt.Errorf("place in queue filename: %w", err)
	}
	result.Place, err = r.ReadUint32()
	if err != nil {
		return nil, fmt.Errorf("place in queue place: %w", err)
	}
	return result, nil
}

// ---------------------------------------------------------------------------
// Upload Denied (code 50)
// ---------------------------------------------------------------------------

// DenyFileNotShared is the standard UploadDenied reason for a file we don't
// share. Clients match it exactly and stop retrying; any other reason is
// treated as temporary and the download is requested again.
const DenyFileNotShared = "File not shared."

// BuildUploadDenied builds an upload denied message (code 50).
func BuildUploadDenied(filename, reason string) []byte {
	b := NewBuilder(PeerUploadDenied)
	b.WriteString(filename)
	b.WriteString(reason)
	return b.Build()
}

// ---------------------------------------------------------------------------
// Upload Failed (code 46)
// ---------------------------------------------------------------------------

// BuildUploadFailed builds an upload failed message (code 46).
func BuildUploadFailed(filename string) []byte {
	b := NewBuilder(PeerUploadFailed)
	b.WriteString(filename)
	return b.Build()
}
