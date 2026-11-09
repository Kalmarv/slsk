// Command slsk-get searches Soulseek and downloads the best-matching file.
//
//	SLSK_USERNAME=me SLSK_PASSWORD=secret slsk-get "artist title"
//
// It prints every answer, then downloads the file with the highest bitrate
// from a user with a free upload slot into the current directory.
//
// Don't run it with the account of a client that is already online: the
// server lets one session per account, and the two would knock each other
// off.
package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"path"
	"strings"
	"sync"

	"github.com/kalmarv/slsk"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, `usage: slsk-get "search query"`)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	loggedIn := make(chan struct{})
	var once sync.Once
	client, err := slsk.New(slsk.Config{
		Username: os.Getenv("SLSK_USERNAME"),
		Password: os.Getenv("SLSK_PASSWORD"),
		Logger:   slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})),
		OnLogin:  func() { once.Do(func() { close(loggedIn) }) },
	})
	if err != nil {
		log.Fatal(err)
	}
	go client.Run(ctx)

	select {
	case <-loggedIn:
	case <-ctx.Done():
		return
	}

	var (
		mu   sync.Mutex
		best struct {
			user string
			file slsk.File
		}
	)
	err = client.Search(ctx, os.Args[1], func(r slsk.SearchResult) {
		mu.Lock()
		defer mu.Unlock()
		for _, f := range r.Files {
			fmt.Printf("%-20s %4d kbps %10d  %s\n", r.Username, f.Bitrate, f.Size, f.Name)
			if r.FreeSlot && f.Bitrate > best.file.Bitrate {
				best.user, best.file = r.Username, f
			}
		}
	})
	if err != nil {
		log.Fatal(err)
	}
	if best.user == "" {
		log.Fatal("no file from a user with a free slot")
	}

	name := path.Base(strings.ReplaceAll(best.file.Name, `\`, "/"))
	out, err := os.Create(name)
	if err != nil {
		log.Fatal(err)
	}
	defer out.Close()
	fmt.Printf("downloading %s from %s\n", best.file.Name, best.user)
	err = client.Download(ctx, best.user, best.file.Name, out, func(received, size int64) {
		fmt.Printf("\r%d / %d bytes", received, size)
	})
	fmt.Println()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("saved", name)
}
