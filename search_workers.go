package slsk

import (
	"context"
	"sync/atomic"

	"github.com/kalmarv/slsk/protocol"
)

// searchWorkers runs search-request handling on a bounded worker pool so a slow
// share search cannot stall the goroutine that reads a connection (the server
// loop or the distributed parent). Requests beyond the queue depth are dropped:
// searches are best-effort and a flooded peer should shed load, not queue
// without bound.
type searchWorkers struct {
	queue   chan *protocol.IncomingSearchRequest
	dropped atomic.Uint64
}

// newSearchWorkers starts workers goroutines that call handle for queued
// requests until ctx is cancelled. At most depth requests wait in the queue.
func newSearchWorkers(ctx context.Context, workers, depth int, handle func(*protocol.IncomingSearchRequest)) *searchWorkers {
	w := &searchWorkers{queue: make(chan *protocol.IncomingSearchRequest, depth)}
	for range workers {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case req := <-w.queue:
					handle(req)
				}
			}
		}()
	}
	return w
}

// Submit queues a request without blocking. It is a valid callback for
// Client.OnSearchRequest and the DistribManager.
func (w *searchWorkers) Submit(req *protocol.IncomingSearchRequest) {
	select {
	case w.queue <- req:
	default:
		w.dropped.Add(1)
	}
}

// Dropped returns how many requests were shed because the queue was full.
func (w *searchWorkers) Dropped() uint64 { return w.dropped.Load() }
