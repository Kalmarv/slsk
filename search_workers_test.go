package slsk

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kalmarv/slsk/protocol"
)

func TestSearchWorkers_SubmitNeverBlocksAndBoundsQueue(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	release := make(chan struct{})
	var handled atomic.Int32
	w := NewSearchWorkers(ctx, 2, 3, func(*protocol.IncomingSearchRequest) {
		<-release // a slow share search
		handled.Add(1)
	})

	done := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			w.Submit(&protocol.IncomingSearchRequest{Token: uint32(i)})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Submit blocked behind slow handlers")
	}
	// At most 2 in flight + 3 queued survive; everything else was shed (more
	// survive if a worker had not yet picked up its request when counted).
	dropped := w.Dropped()
	if dropped < 95 || dropped > 97 {
		t.Fatalf("dropped %d of 100, want 95..97", dropped)
	}
	close(release)
	want := int32(100 - dropped)
	deadline := time.After(2 * time.Second)
	for handled.Load() < want {
		select {
		case <-deadline:
			t.Fatalf("handled %d requests, want %d", handled.Load(), want)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// A slow search handler must not stall the server message loop.
func TestClient_SlowSearchHandlerDoesNotStallServerLoop(t *testing.T) {
	client, serverSide := connectedClient(t)
	sc := <-serverSide
	defer sc.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	release := make(chan struct{})
	defer close(release)
	w := NewSearchWorkers(ctx, 1, 1, func(*protocol.IncomingSearchRequest) { <-release })
	client.OnSearchRequest(w.Submit)

	search := protocol.NewBuilder(protocol.ServerEmbeddedMessage)
	search.WriteUint8(3)
	search.WriteUint32(49) // distributed search identifier
	search.WriteString("searcher")
	search.WriteUint32(1)
	search.WriteString("query")
	for i := 0; i < 3; i++ {
		if err := sc.WriteMessage(search.Build()); err != nil {
			t.Fatal(err)
		}
	}
	if err := sc.WriteMessage(protocol.BuildPing()); err != nil {
		t.Fatal(err)
	}

	sc.Raw().SetReadDeadline(time.Now().Add(2 * time.Second))
	msg, err := sc.ReadMessage()
	if err != nil {
		t.Fatalf("server loop stalled behind the search handler: %v", err)
	}
	if r, _ := protocol.NewReader(msg); r == nil || r.Code() != protocol.ServerPing {
		t.Fatalf("expected a pong, got %x", msg)
	}
}
