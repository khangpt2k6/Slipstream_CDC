// Command connector-github syncs GitHub repositories into Slipstream: a full
// crawl of issues, pull requests and comments on first start, then
// incremental polls (conditional requests, so an idle repo costs no rate
// limit) plus a signed webhook endpoint for repos you can install hooks on.
package main

import (
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/khangpt2k6/Slipstream_CDC/internal/config"
	"github.com/khangpt2k6/Slipstream_CDC/internal/connectors"
	"github.com/khangpt2k6/Slipstream_CDC/internal/connectors/github"
	"github.com/khangpt2k6/Slipstream_CDC/internal/dirclient"
	"github.com/khangpt2k6/Slipstream_CDC/internal/kafkax"
	"github.com/khangpt2k6/Slipstream_CDC/internal/model"
	"github.com/khangpt2k6/Slipstream_CDC/internal/retry"
	"github.com/khangpt2k6/Slipstream_CDC/internal/svc"
)

func main() {
	e := config.New(os.Getenv)
	var (
		addr          = e.String("SS_CONNECTOR_ADDR", ":8084")
		brokers       = e.List("SS_KAFKA_BROKERS", "localhost:29092")
		redisAddr     = e.String("SS_REDIS_ADDR", "localhost:6379")
		directoryURL  = e.String("SS_DIRECTORY_URL", "http://localhost:8081")
		apiBase       = e.String("SS_GITHUB_API_URL", "https://api.github.com")
		token         = e.String("SS_GITHUB_TOKEN", "")
		repos         = e.List("SS_GITHUB_REPOS", "golang/go")
		secret        = e.String("SS_GITHUB_WEBHOOK_SECRET", "local-webhook-secret")
		comments      = e.Bool("SS_GITHUB_COMMENTS", true)
		commentsSince = e.String("SS_GITHUB_COMMENTS_SINCE", "")
		poll          = e.Duration("SS_POLL_INTERVAL", time.Minute)
		dumpDir       = e.String("SS_DUMP_DIR", "")
		logLevel      = e.String("SS_LOG_LEVEL", "info")
	)
	var since time.Time
	if commentsSince != "" {
		t, err := time.Parse(time.DateOnly, commentsSince)
		if err != nil {
			fmt.Fprintln(os.Stderr, "invalid configuration: SS_GITHUB_COMMENTS_SINCE:", err)
			os.Exit(1)
		}
		since = t
	}
	if err := e.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "invalid configuration:", err)
		os.Exit(1)
	}
	ctx, stop := svc.Init(logLevel)
	defer stop()
	if token == "" {
		slog.Warn("SS_GITHUB_TOKEN is empty: unauthenticated requests are limited to 60 an hour")
	}

	prod, err := kafkax.NewProducer(brokers, 0)
	if err != nil {
		svc.Fatal("connect kafka", err)
	}
	defer prod.Close()
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer func() { _ = rdb.Close() }()
	dump, err := connectors.OpenDump(dumpDir, "github")
	if err != nil {
		svc.Fatal("open dump", err)
	}
	defer func() { _ = dump.Close() }()

	c := &github.Connector{
		API:           &github.Client{Base: apiBase, Token: token},
		Dir:           dirclient.New(directoryURL),
		Out:           &connectors.Emitter{Prod: prod, Source: "github", Dump: dump},
		RDB:           rdb,
		Repos:         repos,
		Secret:        secret,
		Comments:      comments,
		CommentsSince: since,
	}

	mux := svc.OpsMux()
	mux.Handle("POST /github/webhook", c)
	go svc.Serve(ctx, addr, mux)

	backoff := retry.Config{Base: time.Second, Max: 15 * time.Second}
	if err := retry.Do(ctx, backoff, func(attempt int, _ time.Duration, err error) {
		slog.Warn("registering repos failed", "attempt", attempt, "err", err)
	}, func() error { return c.Sync(ctx) }); err != nil {
		svc.Fatal("sync", err)
	}

	// The first crawl is the backfill (or, after a restart, a catch-up from
	// the stored cursors). A failure is retried; cursors keep the progress.
	start := time.Now()
	var total int
	if err := retry.Do(ctx, retry.Config{Base: 5 * time.Second, Max: 5 * time.Minute}, func(attempt int, _ time.Duration, err error) {
		slog.Warn("github crawl failed; resuming from cursor", "attempt", attempt, "err", err)
	}, func() error {
		n, err := c.Crawl(ctx, model.ModeBackfill)
		total += n
		return err
	}); err != nil {
		return
	}
	slog.Info("github backfill done", "repos", repos, "events", total, "took", time.Since(start).String())

	t := time.NewTicker(poll)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if n, err := c.Crawl(ctx, model.ModeLive); err != nil {
				slog.Warn("github poll", "err", err)
			} else if n > 0 {
				slog.Info("github poll caught up", "events", n)
			}
		}
	}
}
