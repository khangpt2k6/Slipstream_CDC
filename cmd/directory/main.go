// Command directory runs the simulated identity provider: users, teams,
// memberships and container grants, with SQLite as the source of truth and
// every change published to the identity topic through an outbox.
package main

import (
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/khangpt2k6/Slipstream_CDC/internal/config"
	"github.com/khangpt2k6/Slipstream_CDC/internal/directory"
	"github.com/khangpt2k6/Slipstream_CDC/internal/kafkax"
	"github.com/khangpt2k6/Slipstream_CDC/internal/svc"
)

func main() {
	e := config.New(os.Getenv)
	var (
		brokers    = e.List("SS_KAFKA_BROKERS", "localhost:29092")
		partitions = e.Int("SS_KAFKA_PARTITIONS", 6)
		dbPath     = e.String("SS_DIRECTORY_DB", "directory.db")
		addr       = e.String("SS_DIRECTORY_ADDR", ":8081")
		seed       = e.Int("SS_SEED", 42)
		users      = e.Int("SS_SEED_USERS", 300)
		outbox     = e.Duration("SS_OUTBOX_INTERVAL", 2*time.Second)
		dlqSuffix  = e.String("SS_DLQ_SUFFIX", ".dlq")
		logLevel   = e.String("SS_LOG_LEVEL", "info")
	)
	if err := e.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "invalid configuration:", err)
		os.Exit(1)
	}
	ctx, stop := svc.Init(logLevel)
	defer stop()

	if err := kafkax.EnsureTopics(ctx, brokers, kafkax.StandardTopics(int32(partitions), dlqSuffix)); err != nil { // #nosec G115 -- small positive config value
		svc.Fatal("ensure topics", err)
	}
	prod, err := kafkax.NewProducer(brokers, 0)
	if err != nil {
		svc.Fatal("connect kafka", err)
	}
	defer prod.Close()

	store, err := directory.Open(ctx, dbPath, prod)
	if err != nil {
		svc.Fatal("open directory", err)
	}
	defer func() { _ = store.Close() }()

	seeded, err := store.Seed(ctx, uint64(seed), users) // #nosec G115 -- positive config value
	if err != nil {
		svc.Fatal("seed directory", err)
	}
	if seeded {
		slog.Info("seeded directory", "seed", seed, "users", users)
	}

	// Outbox: deliver any committed change whose publish failed.
	go func() {
		t := time.NewTicker(outbox)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if n, err := store.RepublishPending(ctx); err != nil {
					slog.Warn("outbox republish", "err", err)
				} else if n > 0 {
					slog.Info("outbox delivered pending changes", "n", n)
				}
			}
		}
	}()

	mux := svc.OpsMux()
	mux.Handle("/v1/", directory.Handler(store))
	slog.Info("directory running", "addr", addr, "db", dbPath)
	svc.Serve(ctx, addr, mux)
}
