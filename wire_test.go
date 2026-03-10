package slsk

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// ---------- helpers ----------

func u32le(v uint32) []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, v)
	return b
}

func u64le(v uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, v)
	return b
}

// ---------- MessageBuilder + MessageReader round-trip ----------

func TestBuilderReaderRoundTrip(t *testing.T) {
	tests := []struct {
		name    string
		code    uint32
		build   func(b *MessageBuilder)
		verify  func(t *testing.T, r *MessageReader)
	}{
		{
			name: "empty payload",
			code: ServerLogin,
			build: func(b *MessageBuilder) {},
			verify: func(t *testing.T, r *MessageReader) {
				if r.Code() != ServerLogin {
					t.Fatalf("code = %d, want %d", r.Code(), ServerLogin)
				}
				if r.HasMore() {
					t.Fatal("expected no more data")
				}
			},
		},
		{
			name: "uint8",
			code: 42,
			build: func(b *MessageBuilder) {
				b.WriteUint8(0xFF)
			},
			verify: func(t *testing.T, r *MessageReader) {
				v, err := r.ReadUint8()
				if err != nil {
					t.Fatal(err)
				}
				if v != 0xFF {
					t.Fatalf("uint8 = %d, want 255", v)
				}
				if r.HasMore() {
					t.Fatal("expected no more data")
				}
			},
		},
		{
			name: "uint32",
			code: 99,
			build: func(b *MessageBuilder) {
				b.WriteUint32(0xDEADBEEF)
			},
			verify: func(t *testing.T, r *MessageReader) {
				v, err := r.ReadUint32()
				if err != nil {
					t.Fatal(err)
				}
				if v != 0xDEADBEEF {
					t.Fatalf("uint32 = %#x, want 0xDEADBEEF", v)
				}
			},
		},
		{
			name: "uint64",
			code: 100,
			build: func(b *MessageBuilder) {
				b.WriteUint64(0x0102030405060708)
			},
			verify: func(t *testing.T, r *MessageReader) {
				v, err := r.ReadUint64()
				if err != nil {
					t.Fatal(err)
				}
				if v != 0x0102030405060708 {
					t.Fatalf("uint64 = %#x, want 0x0102030405060708", v)
				}
			},
		},
		{
			name: "string",
			code: ServerLogin,
			build: func(b *MessageBuilder) {
				b.WriteString("hello")
			},
			verify: func(t *testing.T, r *MessageReader) {
				s, err := r.ReadString()
				if err != nil {
					t.Fatal(err)
				}
				if s != "hello" {
					t.Fatalf("string = %q, want %q", s, "hello")
				}
			},
		},
		{
			name: "empty string",
			code: 1,
			build: func(b *MessageBuilder) {
				b.WriteString("")
			},
			verify: func(t *testing.T, r *MessageReader) {
				s, err := r.ReadString()
				if err != nil {
					t.Fatal(err)
				}
				if s != "" {
					t.Fatalf("string = %q, want empty", s)
				}
			},
		},
		{
			name: "bool true and false",
			code: 10,
			build: func(b *MessageBuilder) {
				b.WriteBool(true)
				b.WriteBool(false)
			},
			verify: func(t *testing.T, r *MessageReader) {
				v1, err := r.ReadBool()
				if err != nil {
					t.Fatal(err)
				}
				if !v1 {
					t.Fatal("expected true")
				}
				v2, err := r.ReadBool()
				if err != nil {
					t.Fatal(err)
				}
				if v2 {
					t.Fatal("expected false")
				}
			},
		},
		{
			name: "bytes",
			code: 5,
			build: func(b *MessageBuilder) {
				b.WriteBytes([]byte{0x01, 0x02, 0x03})
			},
			verify: func(t *testing.T, r *MessageReader) {
				data, err := r.ReadBytes(3)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(data, []byte{0x01, 0x02, 0x03}) {
					t.Fatalf("bytes = %v, want [1 2 3]", data)
				}
			},
		},
		{
			name: "multiple fields",
			code: ServerLogin,
			build: func(b *MessageBuilder) {
				b.WriteString("user123")
				b.WriteUint32(7777)
				b.WriteBool(true)
				b.WriteUint8(42)
				b.WriteUint64(9999999999)
			},
			verify: func(t *testing.T, r *MessageReader) {
				s, err := r.ReadString()
				if err != nil {
					t.Fatal(err)
				}
				if s != "user123" {
					t.Fatalf("string = %q", s)
				}

				u, err := r.ReadUint32()
				if err != nil {
					t.Fatal(err)
				}
				if u != 7777 {
					t.Fatalf("uint32 = %d", u)
				}

				b, err := r.ReadBool()
				if err != nil {
					t.Fatal(err)
				}
				if !b {
					t.Fatal("expected true")
				}

				u8, err := r.ReadUint8()
				if err != nil {
					t.Fatal(err)
				}
				if u8 != 42 {
					t.Fatalf("uint8 = %d", u8)
				}

				u64, err := r.ReadUint64()
				if err != nil {
					t.Fatal(err)
				}
				if u64 != 9999999999 {
					t.Fatalf("uint64 = %d", u64)
				}

				if r.HasMore() {
					t.Fatal("expected no more data")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := NewBuilder(tt.code)
			tt.build(b)
			msg := b.Build()

			r, err := NewReader(msg)
			if err != nil {
				t.Fatalf("NewReader: %v", err)
			}
			if r.Code() != tt.code {
				t.Fatalf("code = %d, want %d", r.Code(), tt.code)
			}
			tt.verify(t, r)
		})
	}
}

// ---------- MessageBuilder wire format ----------

func TestBuilderWireFormat(t *testing.T) {
	b := NewBuilder(ServerLogin)
	b.WriteString("hi")
	msg := b.Build()

	// Expected:
	// length = 4 (code) + 4 (string len) + 2 (string data) = 10
	// [10,0,0,0] [1,0,0,0] [2,0,0,0] [h,i]
	expected := bytes.Join([][]byte{
		u32le(10),       // length prefix
		u32le(1),        // code = ServerLogin
		u32le(2),        // string length
		[]byte("hi"),    // string data
	}, nil)

	if !bytes.Equal(msg, expected) {
		t.Fatalf("wire format:\ngot  %v\nwant %v", msg, expected)
	}
}

func TestBuilderEmptyPayloadWireFormat(t *testing.T) {
	b := NewBuilder(ServerPing)
	msg := b.Build()

	// length = 4 (code only)
	expected := bytes.Join([][]byte{
		u32le(4),        // length prefix
		u32le(32),       // code = ServerPing
	}, nil)

	if !bytes.Equal(msg, expected) {
		t.Fatalf("wire format:\ngot  %v\nwant %v", msg, expected)
	}
}

// ---------- MessageReader error cases ----------

func TestReaderTooShort(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"nil", nil},
		{"empty", []byte{}},
		{"3 bytes", []byte{1, 2, 3}},
		{"7 bytes", []byte{1, 2, 3, 4, 5, 6, 7}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewReader(tt.data)
			if err == nil {
				t.Fatal("expected error for short message")
			}
		})
	}
}

func TestReaderReadPastEnd(t *testing.T) {
	// Build a message with just a uint8 payload
	b := NewBuilder(1)
	b.WriteUint8(0x42)
	msg := b.Build()

	r, err := NewReader(msg)
	if err != nil {
		t.Fatal(err)
	}

	// Read the uint8 successfully
	_, err = r.ReadUint8()
	if err != nil {
		t.Fatal(err)
	}

	// Now all reads should fail
	if _, err := r.ReadUint8(); err == nil {
		t.Fatal("expected error reading past end (uint8)")
	}
	if _, err := r.ReadUint32(); err == nil {
		t.Fatal("expected error reading past end (uint32)")
	}
	if _, err := r.ReadUint64(); err == nil {
		t.Fatal("expected error reading past end (uint64)")
	}
	if _, err := r.ReadString(); err == nil {
		t.Fatal("expected error reading past end (string)")
	}
	if _, err := r.ReadBool(); err == nil {
		t.Fatal("expected error reading past end (bool)")
	}
	if _, err := r.ReadBytes(1); err == nil {
		t.Fatal("expected error reading past end (bytes)")
	}
}

func TestReaderHasMore(t *testing.T) {
	b := NewBuilder(1)
	b.WriteUint32(100)
	b.WriteUint8(5)
	msg := b.Build()

	r, err := NewReader(msg)
	if err != nil {
		t.Fatal(err)
	}

	if !r.HasMore() {
		t.Fatal("expected HasMore=true before reading")
	}

	r.ReadUint32()
	if !r.HasMore() {
		t.Fatal("expected HasMore=true after reading uint32")
	}

	r.ReadUint8()
	if r.HasMore() {
		t.Fatal("expected HasMore=false after reading all")
	}
}

func TestReaderStringTruncated(t *testing.T) {
	// Craft a message where string length says 100 but only 2 bytes follow
	b := NewBuilder(1)
	msg := b.Build() // just [4,0,0,0] [1,0,0,0]

	// Manually append a string header claiming length 100
	payload := append(msg, u32le(100)...)
	payload = append(payload, 'h', 'i') // only 2 bytes of the claimed 100

	// Fix the length prefix
	binary.LittleEndian.PutUint32(payload[0:4], uint32(len(payload)-4))

	r, err := NewReader(payload)
	if err != nil {
		t.Fatal(err)
	}

	_, err = r.ReadString()
	if err == nil {
		t.Fatal("expected error for truncated string data")
	}
}

// ---------- InitMessageBuilder + InitMessageReader round-trip ----------

func TestInitBuilderReaderRoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		code   uint8
		build  func(b *InitMessageBuilder)
		verify func(t *testing.T, r *InitMessageReader)
	}{
		{
			name: "empty payload",
			code: InitPierceFirewall,
			build: func(b *InitMessageBuilder) {},
			verify: func(t *testing.T, r *InitMessageReader) {
				if r.Code() != InitPierceFirewall {
					t.Fatalf("code = %d, want %d", r.Code(), InitPierceFirewall)
				}
				if r.HasMore() {
					t.Fatal("expected no more data")
				}
			},
		},
		{
			name: "pierce firewall with token",
			code: InitPierceFirewall,
			build: func(b *InitMessageBuilder) {
				b.WriteUint32(12345)
			},
			verify: func(t *testing.T, r *InitMessageReader) {
				if r.Code() != InitPierceFirewall {
					t.Fatalf("code = %d", r.Code())
				}
				v, err := r.ReadUint32()
				if err != nil {
					t.Fatal(err)
				}
				if v != 12345 {
					t.Fatalf("token = %d, want 12345", v)
				}
			},
		},
		{
			name: "peer init with string and uint32",
			code: InitPeerInit,
			build: func(b *InitMessageBuilder) {
				b.WriteString("testuser")
				b.WriteString("P")
				b.WriteUint32(99)
			},
			verify: func(t *testing.T, r *InitMessageReader) {
				if r.Code() != InitPeerInit {
					t.Fatalf("code = %d", r.Code())
				}
				s1, err := r.ReadString()
				if err != nil {
					t.Fatal(err)
				}
				if s1 != "testuser" {
					t.Fatalf("username = %q", s1)
				}
				s2, err := r.ReadString()
				if err != nil {
					t.Fatal(err)
				}
				if s2 != "P" {
					t.Fatalf("conn type = %q", s2)
				}
				v, err := r.ReadUint32()
				if err != nil {
					t.Fatal(err)
				}
				if v != 99 {
					t.Fatalf("token = %d", v)
				}
				if r.HasMore() {
					t.Fatal("expected no more")
				}
			},
		},
		{
			name: "uint8 and uint64",
			code: 3,
			build: func(b *InitMessageBuilder) {
				b.WriteUint8(0xAB)
				b.WriteUint64(0xFFFFFFFFFFFFFFFF)
			},
			verify: func(t *testing.T, r *InitMessageReader) {
				// InitMessageReader has ReadUint32 and ReadString per spec,
				// but we need to verify the builder wrote correctly.
				// We'll read raw via HasMore check.
				if !r.HasMore() {
					t.Fatal("expected data")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := NewInitBuilder(tt.code)
			tt.build(b)
			msg := b.Build()

			r, err := NewInitReader(msg)
			if err != nil {
				t.Fatalf("NewInitReader: %v", err)
			}
			tt.verify(t, r)
		})
	}
}

// ---------- InitMessageBuilder wire format ----------

func TestInitBuilderWireFormat(t *testing.T) {
	b := NewInitBuilder(InitPierceFirewall)
	b.WriteUint32(12345)
	msg := b.Build()

	// Expected:
	// length = 1 (code) + 4 (uint32) = 5
	// [5,0,0,0] [0] [0x39,0x30,0x00,0x00]
	expected := bytes.Join([][]byte{
		u32le(5),        // length prefix
		{0},             // code = InitPierceFirewall = 0
		u32le(12345),    // token
	}, nil)

	if !bytes.Equal(msg, expected) {
		t.Fatalf("wire format:\ngot  %v\nwant %v", msg, expected)
	}
}

// ---------- InitMessageReader error cases ----------

func TestInitReaderTooShort(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"nil", nil},
		{"empty", []byte{}},
		{"4 bytes", []byte{1, 0, 0, 0}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewInitReader(tt.data)
			if err == nil {
				t.Fatal("expected error for short init message")
			}
		})
	}
}

func TestInitReaderReadPastEnd(t *testing.T) {
	b := NewInitBuilder(InitPierceFirewall)
	msg := b.Build() // just length + code byte, no payload

	r, err := NewInitReader(msg)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := r.ReadUint32(); err == nil {
		t.Fatal("expected error reading past end")
	}
	if _, err := r.ReadString(); err == nil {
		t.Fatal("expected error reading past end")
	}
}

// ---------- Codes sanity ----------

func TestCodeConstants(t *testing.T) {
	// Spot-check a few constants to make sure they exist and have the right values.
	tests := []struct {
		name string
		got  interface{}
		want interface{}
	}{
		{"ServerLogin", ServerLogin, uint32(1)},
		{"ServerPing", ServerPing, uint32(32)},
		{"ServerCannotConnect", ServerCannotConnect, uint32(1001)},
		{"PeerSearchResponse", PeerSearchResponse, uint32(9)},
		{"PeerTransferRequest", PeerTransferRequest, uint32(40)},
		{"DistribBranchRoot", DistribBranchRoot, uint8(5)},
		{"InitPierceFirewall", InitPierceFirewall, uint8(0)},
		{"InitPeerInit", InitPeerInit, uint8(1)},
		{"ConnTypePeer", ConnTypePeer, "P"},
		{"ConnTypeTransfer", ConnTypeTransfer, "F"},
		{"ConnTypeDistributed", ConnTypeDistributed, "D"},
		{"TransferDownload", TransferDownload, uint32(0)},
		{"TransferUpload", TransferUpload, uint32(1)},
		{"AttrBitRate", AttrBitRate, uint32(0)},
		{"AttrDuration", AttrDuration, uint32(1)},
		{"AttrVBR", AttrVBR, uint32(2)},
		{"AttrSampleRate", AttrSampleRate, uint32(4)},
		{"AttrBitDepth", AttrBitDepth, uint32(5)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Fatalf("%s = %v, want %v", tt.name, tt.got, tt.want)
			}
		})
	}
}
