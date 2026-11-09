# slsk

A Soulseek client library for Go. It logs in to the Soulseek server, searches the network, downloads files from other
users and shares files with them. It needs nothing outside the standard library (testify is used by the tests only).

```sh
go get github.com/kalmarv/slsk
```

It was written for [Mantis](https://github.com/kalmarv/mantis), a self-hosted music client, and runs it in production.

## Quick start

```go
client, err := slsk.New(slsk.Config{
	Username: os.Getenv("SLSK_USERNAME"),
	Password: os.Getenv("SLSK_PASSWORD"),
	OnLogin:  func() { log.Println("online") },
})
if err != nil {
	log.Fatal(err)
}
go client.Run(ctx) // logs in, and again after every dropped session

// once logged in:
client.Search(ctx, "radiohead ok computer", func(r slsk.SearchResult) {
	for _, f := range r.Files {
		fmt.Println(r.Username, f.Name, f.Bitrate)
	}
})

out, _ := os.Create("track.flac")
err = client.Download(ctx, "someuser", `Music\Radiohead\OK Computer\01 Airbag.flac`, out, nil)
```

[`examples/slsk-get`](examples/slsk-get/main.go) is a complete program: it searches, lists the answers and downloads
the best file.

One account can only have one session: a second client logging in with the same account knocks the first one off
(`Status().Relogged`), and the client waits ten minutes before logging back in so the two don't fight.

## What the client does for you

- **Session.** `Run` keeps the server session alive with exponential backoff, and after each login re-sends the share
  counts and the users you watch. `Status` reports the session, the peer listener and the next retry.
- **Peer connections.** Every connection to a peer is tried directly and through the server at the same time (for peers
  behind NAT); the first to connect wins.
- **Searches.** Answers stream in as peers send them; a search ends when no one answers within 15 s, when answers stop
  for 3 s, or after 30 s (`Config.SearchTimeouts`). The server silently drops searches that contain certain phrases (it
  sends the list at login); `Search` rewrites those phrases so peers still match them.
- **Downloads.** `QueueDownloads` asks a peer for a whole folder at once so it keeps its place in their queue. `Download`
  waits as long as the peer keeps reporting a queue position, and fails with a `*DeniedError` (see `Permanent()`) when
  the peer refuses. Truncated transfers are errors, never short files.
- **User status.** `WatchUser` subscribes to a user's online status; `Config.OnUserStatus` gets every change.

## Sharing

Implement two interfaces and set them in `Config`:

```go
type ShareIndex interface {
	Search(query string) []slsk.File // answer other users' searches
	Counts() (folders, files int)
}

type UploadQueue interface {
	Enqueue(req slsk.UploadRequest) (position int, err error) // a user asks for a file
	Position(username, filename string) int
	SlotState() (free bool, queued int)
}
```

`slsk.ParseQuery` and `slsk.Words` implement the network's matching rules (whole words, `-exclude`, `*suffix`). The
client runs the rest: it joins the distributed search network, answers searches on a bounded worker pool, replies to
queue requests, and when your queue starts a transfer, `client.Upload` sends the file (resuming at the offset the peer
asks for). Forward `Config.ListenPort` (default 2235) on your router so peers can connect to you.

## Packages

- `github.com/kalmarv/slsk`: the client.
- `github.com/kalmarv/slsk/protocol`: the wire format. Message framing, codes, and a builder and parser for every
  server, peer, init and distributed message the client uses, fuzz-tested against malformed input.

## Development

```sh
go test -race ./...
go test -fuzz=FuzzParsers ./protocol   # protocol/fuzz_test.go has the other targets
```
