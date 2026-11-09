package slsk

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/kalmarv/slsk/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew_ValidatesAndDefaults(t *testing.T) {
	_, err := New(Config{Password: "p"})
	assert.Error(t, err, "username required")
	_, err = New(Config{Username: "u"})
	assert.Error(t, err, "password required")
	_, err = New(Config{Username: "u", Password: "p", ListenPort: 70000})
	assert.Error(t, err, "port range")

	c, err := New(Config{Username: "u", Password: "p"})
	require.NoError(t, err)
	assert.Equal(t, DefaultServerAddr, c.cfg.ServerAddr)
	assert.Equal(t, DefaultListenPort, c.cfg.ListenPort)
	assert.Equal(t, 15*time.Second, c.cfg.SearchTimeouts.FirstResult)
	assert.Equal(t, "u", c.Username())
	assert.False(t, c.LoggedIn())
}

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

// TestClient_RunLogsInAndRestoresSession runs a whole client against a mock
// server: after login it must report its shares, renew watched users, call
// OnLogin, pass status updates on, and stop cleanly when cancelled.
func TestClient_RunLogsInAndRestoresSession(t *testing.T) {
	type sent struct {
		code uint32
		msg  []byte
	}
	received := make(chan sent, 64)
	addr := startMockServer(t, func(sc *protocol.Conn) {
		defer sc.Close()
		handleLogin(t, sc, buildLoginSuccessResponse())
		for {
			msg, err := sc.ReadMessage()
			if err != nil {
				return
			}
			r, err := protocol.NewReader(msg)
			if err != nil {
				continue
			}
			received <- sent{r.Code(), msg}
			if r.Code() == protocol.ServerWatchUser {
				var payload []byte
				payload = appendStr(payload, "bob")
				payload = append(payload, u32le(uint32(StatusAway))...)
				payload = append(payload, 0)
				if err := sc.WriteMessage(buildMsg(protocol.ServerGetStatus, payload)); err != nil {
					return
				}
			}
		}
	})

	loggedIn := make(chan struct{}, 1)
	statuses := make(chan UserStatus, 1)
	c, err := New(Config{
		Username:   "me",
		Password:   "p",
		ServerAddr: addr,
		ListenPort: freePort(t),
		Logger:     testLogger,
		Shares:     &fakeShares{files: []File{{Name: `a\b.flac`}, {Name: `a\c.flac`}}},
		OnLogin:    func() { loggedIn <- struct{}{} },
		OnUserStatus: func(user string, s UserStatus) {
			if user == "bob" {
				statuses <- s
			}
		},
	})
	require.NoError(t, err)
	require.NoError(t, c.WatchUser("bob"), "watching while logged out is deferred, not an error")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	select {
	case <-loggedIn:
	case <-time.After(5 * time.Second):
		t.Fatal("OnLogin not called")
	}
	assert.True(t, c.LoggedIn())
	assert.Error(t, c.Run(ctx), "Run may only be called once")

	var sawShares, sawWatch bool
	deadline := time.After(5 * time.Second)
	for !sawShares || !sawWatch {
		select {
		case m := <-received:
			r, _ := protocol.NewReader(m.msg)
			switch m.code {
			case protocol.ServerSharedFoldersFiles:
				dirs, _ := r.ReadUint32()
				files, _ := r.ReadUint32()
				assert.Equal(t, [2]uint32{1, 2}, [2]uint32{dirs, files})
				sawShares = true
			case protocol.ServerWatchUser:
				user, _ := r.ReadString()
				assert.Equal(t, "bob", user)
				sawWatch = true
			}
		case <-deadline:
			t.Fatalf("after login: shares reported %v, bob watched %v", sawShares, sawWatch)
		}
	}

	select {
	case s := <-statuses:
		assert.Equal(t, StatusAway, s)
	case <-time.After(5 * time.Second):
		t.Fatal("OnUserStatus not called")
	}

	require.Eventually(t, func() bool { return c.Status().Listening }, 5*time.Second, 10*time.Millisecond)
	st := c.Status()
	assert.True(t, st.LoggedIn)
	assert.Empty(t, st.LastError)

	cancel()
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
	assert.False(t, c.LoggedIn())
	assert.False(t, c.Status().LoggedIn)
}
