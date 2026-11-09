// Package slsk is a Soulseek client library: it logs in to the Soulseek
// server, searches the network, downloads files from other users and shares
// files with them.
//
// Create a Client with New and keep it online with Run:
//
//	client, err := slsk.New(slsk.Config{Username: "me", Password: "secret"})
//	if err != nil {
//		log.Fatal(err)
//	}
//	go client.Run(ctx)
//
// Run logs in again on its own whenever the server drops the session;
// Config.OnLogin runs after every login. Search, Download and the other
// methods need a live session and return ErrNotLoggedIn without one.
//
// # Searching and downloading
//
// Search calls back with each user's answer as it arrives:
//
//	client.Search(ctx, "radiohead ok computer", func(r slsk.SearchResult) {
//		for _, f := range r.Files {
//			fmt.Println(r.Username, f.Name, f.Size, f.Bitrate)
//		}
//	})
//
// Download fetches one file of a result into any io.Writer. To download a
// whole folder, QueueDownloads its files first so the folder keeps its place
// in the peer's queue while Download fetches the files one by one.
//
// # Sharing
//
// To share files, implement ShareIndex (answer searches; ParseQuery and Words
// implement the network's matching rules) and UploadQueue (decide upload
// requests), set them in Config, and call Client.Upload when the queue starts
// a transfer. Forward Config.ListenPort on your router so other users can
// connect to you.
//
// The protocol subpackage has the wire format: message framing and a builder
// and parser for every message the client uses.
package slsk
