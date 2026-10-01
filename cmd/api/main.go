// Command api serves search, permission explanations, the live event stream
// and pipeline stats, plus the built web UI.
package main

import (
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/khangpt2k6/Slipstream_CDC/internal/acl"
	"github.com/khangpt2k6/Slipstream_CDC/internal/api"
	"github.com/khangpt2k6/Slipstream_CDC/internal/config"
	"github.com/khangpt2k6/Slipstream_CDC/internal/embed"
	"github.com/khangpt2k6/Slipstream_CDC/internal/opensearch"
	"github.com/khangpt2k6/Slipstream_CDC/internal/search"
	"github.com/khangpt2k6/Slipstream_CDC/internal/svc"
)

func main() {
	e := config.New(os.Getenv)
	var (
		addr          = e.String("SS_API_ADDR", ":8080")
		brokers       = e.List("SS_KAFKA_BROKERS", "localhost:29092")
		osURL         = e.String("SS_OPENSEARCH_URL", "http://localhost:9200")
		index         = e.String("SS_INDEX", "ss-docs")
		redisAddr     = e.String("SS_REDIS_ADDR", "localhost:6379")
		directoryURL  = e.String("SS_DIRECTORY_URL", "http://localhost:8081")
		embedURL      = e.String("SS_EMBED_URL", "")
		webDir        = e.String("SS_WEB_DIR", "")
		impersonation = e.Bool("SS_DEV_IMPERSONATION", true)
		candidates    = e.Int("SS_SEARCH_CANDIDATES", 50)
		osTimeout     = e.Duration("SS_OPENSEARCH_TIMEOUT", 10*time.Second)
		logLevel      = e.String("SS_LOG_LEVEL", "info")
	)
	if err := e.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "invalid configuration:", err)
		os.Exit(1)
	}
	ctx, stop := svc.Init(logLevel)
	defer stop()

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer func() { _ = rdb.Close() }()
	osc := opensearch.New(osURL, osTimeout)

	searcher := &search.Searcher{OS: osc, Index: index, X: &acl.Expander{RDB: rdb}, Candidates: candidates}
	if embedURL != "" {
		searcher.Embed = embed.NewTEI(embedURL, 5*time.Second)
	}

	var adm *kadm.Client
	if cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...)); err == nil {
		defer cl.Close()
		adm = kadm.NewClient(cl)
	} else {
		slog.Warn("kafka admin unavailable; lag stats disabled", "err", err)
	}

	srv := &api.Server{
		Search: searcher, OS: osc, Index: index, RDB: rdb, Kadm: adm,
		DirectoryURL: directoryURL, WebDir: webDir, Impersonation: impersonation,
	}
	mux := svc.OpsMux()
	mux.Handle("/", srv.Handler())
	slog.Info("api running", "addr", addr, "impersonation", impersonation, "embeddings", embedURL != "", "web", webDir)
	svc.Serve(ctx, addr, mux)
}
