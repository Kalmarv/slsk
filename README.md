# slsk

Soulseek client library for Go. Search the network, download files, share your own.

```sh
go get github.com/kalmarv/slsk
```

## Getting started

**1. Get an account.** Soulseek has no sign-up page: logging in with a username nobody has taken creates that account
with the password you logged in with. Pick a username, keep the password, and use them for every login after. Logging in
with a taken username and the wrong password fails with `INVALIDPASS`.

**2. Use one session per account.** If two clients log in with the same account, the server disconnects the first. Give
each app its own account, or stop your desktop client while yours runs.

**3. Open your port.** Forward TCP 2235 (`Config.ListenPort`) on your router to the machine running the client. Without
it, downloads from users who can't accept connections either will fail, and nobody can download from you.

**4. Connect and search.**

```go
client, err := slsk.New(slsk.Config{
	Username: os.Getenv("SLSK_USERNAME"),
	Password: os.Getenv("SLSK_PASSWORD"),
	OnLogin:  func() { log.Println("logged in") },
})
if err != nil {
	log.Fatal(err)
}
go client.Run(ctx) // stays logged in, reconnecting when the server drops the session

// after OnLogin:
client.Search(ctx, "radiohead ok computer", func(r slsk.SearchResult) {
	for _, f := range r.Files {
		fmt.Println(r.Username, f.Name, f.Bitrate)
	}
})

out, _ := os.Create("01 Airbag.flac")
err = client.Download(ctx, "someuser", `Music\Radiohead\OK Computer\01 Airbag.flac`, out, nil)
```

[`examples/slsk-get`](examples/slsk-get/main.go) is a complete program that searches and downloads the best match:

```sh
SLSK_USERNAME=me SLSK_PASSWORD=secret go run ./examples/slsk-get "radiohead airbag"
```

## Sharing

Many users only share with people who share back. To share, implement `ShareIndex` (answer searches) and `UploadQueue`
(accept or refuse download requests), set them in `Config`, and call `client.Upload` when your queue starts a transfer.
`slsk.ParseQuery` matches files the way other Soulseek clients do.

## Packages

- `slsk`: the client: session, searches, downloads, uploads, user status.
- `slsk/protocol`: the wire format, for anything the client doesn't cover.

## Development

```sh
go test -race ./...
```

## License

MIT
