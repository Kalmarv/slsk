package slsk

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
)

// SearchWorkers runs search-request handling on a bounded worker pool so a slow
// share search cannot stall the goroutine that reads a connection (the server
// loop or the distributed parent). Requests beyond the queue depth are dropped:
// searches are best-effort and a flooded peer should shed load, not queue
// without bound.
type SearchWorkers struct {
	queue   chan *IncomingSearchRequest
	dropped atomic.Uint64
}

// NewSearchWorkers starts workers goroutines that call handle for queued
// requests until ctx is cancelled. At most depth requests wait in the queue.
func NewSearchWorkers(ctx context.Context, workers, depth int, handle func(*IncomingSearchRequest)) *SearchWorkers {
	w := &SearchWorkers{queue: make(chan *IncomingSearchRequest, depth)}
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
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
func (w *SearchWorkers) Submit(req *IncomingSearchRequest) {
	select {
	case w.queue <- req:
	default:
		if n := w.dropped.Add(1); n == 1 || n%1000 == 0 {
			slog.Warn("search request queue full, dropping", "component", "slsk", "dropped", n)
		}
	}
}

// Dropped returns how many requests were shed because the queue was full.
func (w *SearchWorkers) Dropped() uint64 { return w.dropped.Load() }
