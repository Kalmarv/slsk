package slsk

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// fakeServer implements connector for testing the reconnect supervisor.
type fakeServer struct {
	mu       sync.Mutex
	failures int
	connects int
	done     chan struct{}
	reason   error
	times    []time.Time // when each Connect was called
}

func (f *fakeServer) DisconnectReason() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.reason
}

func (f *fakeServer) Connect(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.connects++
	f.times = append(f.times, time.Now())
	if f.failures > 0 {
		f.failures--
		return errors.New("dial failed")
	}
	f.done = make(chan struct{})
	return nil
}

func (f *fakeServer) Done() <-chan struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.done
}

// drop simulates the server closing the current connection.
func (f *fakeServer) drop() {
	f.mu.Lock()
	done := f.done
	f.mu.Unlock()
	if done != nil {
		close(done)
	}
}

func (f *fakeServer) stats() (connects int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.connects
}

// TestSuperviseReconnects verifies that a failed dial is retried and
// that a dropped session is re-established, each invoking onConnect.
func TestSuperviseReconnects(t *testing.T) {
	c := &fakeServer{failures: 1}
	connected := make(chan struct{}, 8)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan struct{})
	go func() {
		supervise(ctx, testLogger, c, func() { connected <- struct{}{} }, func(func(*Status)) {}, fastTiming)
		close(finished)
	}()

	// First successful login must follow the initial dial failure.
	waitFor(t, connected, "login after failed dial")

	// Server drops the session; supervisor must reconnect and call onConnect again.
	c.drop()
	waitFor(t, connected, "login after connection loss")

	if got := c.stats(); got < 3 {
		t.Fatalf("expected >=3 Connect calls (fail, success, reconnect), got %d", got)
	}

	// Cancelling the context must stop the supervisor.
	cancel()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("supervise did not return after context cancellation")
	}
}

// fastTiming is a compressed reconnect schedule for tests.
var fastTiming = supervisorTiming{
	initialBackoff: 20 * time.Millisecond,
	maxBackoff:     200 * time.Millisecond,
	dialLimit:      time.Second,
	stableSession:  100 * time.Millisecond,
	relogWait:      400 * time.Millisecond,
}

// testState records what supervise publishes for Client.Status.
type testState struct {
	mu sync.Mutex
	st Status
}

func (s *testState) set(f func(*Status)) {
	s.mu.Lock()
	f(&s.st)
	s.mu.Unlock()
}

func (s *testState) get() Status {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.st
}

func superviseInBackground(t *testing.T, c *fakeServer, state *testState) (connected chan struct{}) {
	t.Helper()
	connected = make(chan struct{}, 32)
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan struct{})
	go func() {
		if state == nil {
			state = &testState{}
		}
		supervise(ctx, testLogger, c, func() { connected <- struct{}{} }, state.set, fastTiming)
		close(finished)
	}()
	t.Cleanup(func() { cancel(); <-finished })
	return connected
}

// A session that drops right after login must not reset the backoff: the
// reconnect delay grows instead of redialing every initialBackoff.
func TestSupervise_BackoffGrowsWhileSessionsAreShort(t *testing.T) {
	c := &fakeServer{}
	connected := superviseInBackground(t, c, nil)

	for i := 0; i < 4; i++ {
		waitFor(t, connected, "login")
		c.drop()
	}
	waitFor(t, connected, "login")

	c.mu.Lock()
	defer c.mu.Unlock()
	gaps := make([]time.Duration, 0, 4)
	for i := 1; i < len(c.times); i++ {
		gaps = append(gaps, c.times[i].Sub(c.times[i-1]))
	}
	// Waits are 20, 40, 80, 160 ms; the last must clearly exceed the first.
	if len(gaps) < 4 || gaps[3] < 3*gaps[0] {
		t.Fatalf("reconnect gaps %v: backoff did not grow across short sessions", gaps)
	}
}

// A session that stayed up for stableSession resets the backoff.
func TestSupervise_StableSessionResetsBackoff(t *testing.T) {
	c := &fakeServer{}
	connected := superviseInBackground(t, c, nil)

	for i := 0; i < 3; i++ { // grow the backoff
		waitFor(t, connected, "login")
		c.drop()
	}
	waitFor(t, connected, "login")
	time.Sleep(2 * fastTiming.stableSession)
	c.drop()
	waitFor(t, connected, "login after stable session")

	c.mu.Lock()
	defer c.mu.Unlock()
	last := c.times[len(c.times)-1].Sub(c.times[len(c.times)-2])
	// Up for >=200 ms, so the wait is initialBackoff (20 ms), not 80 ms.
	if upFor := 2 * fastTiming.stableSession; last-upFor > 60*time.Millisecond {
		t.Fatalf("reconnect after stable session took %v beyond uptime, want ~%v", last-upFor, fastTiming.initialBackoff)
	}
}

// Relogged means another client holds the account: wait relogWait, report it.
func TestSupervise_ReloggedWaitsLong(t *testing.T) {
	c := &fakeServer{}
	state := &testState{}
	connected := superviseInBackground(t, c, state)

	waitFor(t, connected, "login")
	c.mu.Lock()
	c.reason = ErrRelogged
	c.mu.Unlock()
	dropped := time.Now()
	c.drop()

	time.Sleep(fastTiming.relogWait / 2)
	snap := state.get()
	if !snap.Relogged || snap.LoggedIn {
		t.Fatalf("status during relog wait = %+v, want relogged and logged out", snap)
	}
	if snap.RetryAt.IsZero() {
		t.Fatalf("status during relog wait has no RetryAt: %+v", snap)
	}

	c.mu.Lock()
	c.reason = nil
	c.mu.Unlock()
	waitFor(t, connected, "login after relog wait")
	if got := time.Since(dropped); got < fastTiming.relogWait {
		t.Fatalf("reconnected after %v, want at least relogWait %v", got, fastTiming.relogWait)
	}
	if snap := state.get(); snap.Relogged || !snap.LoggedIn {
		t.Fatalf("status after reconnect = %+v", snap)
	}
}

func waitFor(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}
