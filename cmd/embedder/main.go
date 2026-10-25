// Command embedder keeps the search index's vectors current: it embeds every
// document the indexer flagged as changed, through a text-embeddings-inference
// server running the same model the api embeds queries with.
package main

import (
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/khangpt2k6/Slipstream_CDC/internal/config"
	"github.com/khangpt2k6/Slipstream_CDC/internal/embed"
	"github.com/khangpt2k6/Slipstream_CDC/internal/opensearch"
	"github.com/khangpt2k6/Slipstream_CDC/internal/retry"
	"github.com/khangpt2k6/Slipstream_CDC/internal/svc"
)

func main() {
	e := config.New(os.Getenv)
	var (
		osURL       = e.String("SS_OPENSEARCH_URL", "http://localhost:9200")
		index       = e.String("SS_INDEX", "ss-docs")
		embedURL    = e.String("SS_EMBED_URL", "http://localhost:8087")
		batch       = e.Int("SS_EMBED_BATCH", 32)
		concurrency = e.Int("SS_EMBED_CONCURRENCY", 2)
		maxChars    = e.Int("SS_EMBED_MAX_CHARS", 400)
		idle        = e.Duration("SS_EMBED_IDLE", 2*time.Second)
		addr        = e.String("SS_METRICS_ADDR", ":8086")
		logLevel    = e.String("SS_LOG_LEVEL", "info")
	)
	if err := e.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "invalid configuration:", err)
		os.Exit(1)
	}
	ctx, stop := svc.Init(logLevel)
	defer stop()
	go svc.Serve(ctx, addr, svc.OpsMux())

	model := embed.NewTEI(embedURL, 2*time.Minute)
	// TEI downloads and loads the model on first start, which can take a
	// minute or two.
	if err := retry.Do(ctx, retry.Config{Base: time.Second, Max: 10 * time.Second}, func(attempt int, _ time.Duration, err error) {
		slog.Info("waiting for the embedding model", "attempt", attempt, "err", err)
	}, func() error { return model.Ready(ctx) }); err != nil {
		return
	}

	w := &embed.Worker{
		OS:          opensearch.New(osURL, 60*time.Second),
		Index:       index,
		Model:       model,
		Batch:       batch,
		Concurrency: concurrency,
		MaxChars:    maxChars,
	}
	slog.Info("embedder running", "index", index, "model", embedURL, "batch", batch, "concurrency", concurrency)
	w.Run(ctx, idle)
}
