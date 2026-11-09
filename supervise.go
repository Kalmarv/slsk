package slsk

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

// connector is the part of the server connection the supervisor drives.
type connector interface {
	// Connect dials and logs in; it returns when logged in or on failure.
	Connect(ctx context.Context) error
	// Done is closed when the current connection drops.
	Done() <-chan struct{}
	// DisconnectReason says why the last connection ended.
	DisconnectReason() error
}

// supervisorTiming holds the reconnect schedule of supervise.
type supervisorTiming struct {
	initialBackoff time.Duration // first wait after a failure
	maxBackoff     time.Duration
	dialLimit      time.Duration // bound on dial plus login
	// stableSession is how long a session must stay up before it counts as
	// healthy and resets the backoff. Without it a server that accepts the
	// login and then drops us would be redialed every initialBackoff forever.
	stableSession time.Duration
	// relogWait is the pause after the server reports another client logged in
	// to the account; reconnecting sooner would start a login fight.
	relogWait time.Duration
}

var defaultSupervisorTiming = supervisorTiming{
	initialBackoff: time.Second,
	maxBackoff:     5 * time.Minute,
	dialLimit:      30 * time.Second,
	stableSession:  60 * time.Second,
	relogWait:      10 * time.Minute,
}

// supervise keeps the server session alive until ctx is cancelled. It retries
// failed logins with exponential backoff and logs in again after the server
// drops a session, calling onLogin after each successful login. The backoff
// resets only after a session stayed up for stableSession, and a Relogged
// notice (another client took the account) pauses for relogWait. setState
// publishes the session state for Client.Status.
func supervise(ctx context.Context, log *slog.Logger, c connector, onLogin func(), setState func(func(*Status)), t supervisorTiming) {
	backoff := t.initialBackoff
	// wait pauses for d, publishing when the next attempt happens.
	wait := func(d time.Duration, reason string) bool {
		setState(func(s *Status) { s.LoggedIn = false; s.LastError = reason; s.RetryAt = time.Now().Add(d) })
		ok := sleepCtx(ctx, d)
		setState(func(s *Status) { s.RetryAt = time.Time{} })
		return ok
	}
	for ctx.Err() == nil {
		dialCtx, cancel := context.WithTimeout(ctx, t.dialLimit)
		err := c.Connect(dialCtx)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Error("login failed", "component", "slsk", "err", err, "retryIn", backoff)
			if !wait(backoff, err.Error()) {
				return
			}
			backoff = min(backoff*2, t.maxBackoff)
			continue
		}

		log.Info("logged in to Soulseek", "component", "slsk")
		setState(func(s *Status) { s.LoggedIn = true; s.LastError = ""; s.Relogged = false })
		upSince := time.Now()
		onLogin()

		select {
		case <-ctx.Done():
			setState(func(s *Status) { s.LoggedIn = false })
			return
		case <-c.Done():
		}

		if time.Since(upSince) >= t.stableSession {
			backoff = t.initialBackoff
		}
		reason := c.DisconnectReason()
		delay := backoff
		if errors.Is(reason, ErrRelogged) {
			delay = t.relogWait
			setState(func(s *Status) { s.Relogged = true })
			log.Warn("another client logged in to this Soulseek account; not reconnecting until it is idle", "component", "slsk", "retryIn", delay)
		} else {
			log.Warn("server connection lost", "component", "slsk", "retryIn", delay)
			backoff = min(backoff*2, t.maxBackoff)
		}
		msg := "connection lost"
		if reason != nil {
			msg = reason.Error()
		}
		if !wait(delay, msg) {
			return
		}
	}
}

// sleepCtx waits for d or until ctx is cancelled, reporting false if the
// context was cancelled first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
