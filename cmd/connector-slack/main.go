// Command connector-slack syncs a Slack workspace into Slipstream: a full
// backfill on first start, live changes through the Events API, and a
// periodic reconcile that repairs anything a lost event missed.
package main

import (
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/khangpt2k6/Slipstream_CDC/internal/config"
	"github.com/khangpt2k6/Slipstream_CDC/internal/connectors/slack"
	"github.com/khangpt2k6/Slipstream_CDC/internal/dirclient"
	"github.com/khangpt2k6/Slipstream_CDC/internal/kafkax"
	"github.com/khangpt2k6/Slipstream_CDC/internal/model"
	"github.com/khangpt2k6/Slipstream_CDC/internal/retry"
	"github.com/khangpt2k6/Slipstream_CDC/internal/svc"
)

func main() {
	e := config.New(os.Getenv)
	var (
		addr         = e.String("SS_CONNECTOR_ADDR", ":8083")
		brokers      = e.List("SS_KAFKA_BROKERS", "localhost:29092")
		redisAddr    = e.String("SS_REDIS_ADDR", "localhost:6379")
		directoryURL = e.String("SS_DIRECTORY_URL", "http://localhost:8081")
		apiBase      = e.String("SS_SLACK_API_URL", "http://localhost:8082")
		token        = e.String("SS_SLACK_TOKEN", "xoxb-slipstream-local")
		secret       = e.String("SS_SLACK_SIGNING_SECRET", "local-signing-secret")
		workspace    = e.String("SS_SLACK_WORKSPACE", "slipstream-sim")
		reconcile    = e.Duration("SS_RECONCILE_INTERVAL", 5*time.Minute)
		logLevel     = e.String("SS_LOG_LEVEL", "info")
	)
	if err := e.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "invalid configuration:", err)
		os.Exit(1)
	}
	ctx, stop := svc.Init(logLevel)
	defer stop()

	prod, err := kafkax.NewProducer(brokers, 0)
	if err != nil {
		svc.Fatal("connect kafka", err)
	}
	defer prod.Close()
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer func() { _ = rdb.Close() }()

	c := &slack.Connector{
		API:       &slack.Client{Base: apiBase, Token: token},
		Dir:       dirclient.New(directoryURL),
		Prod:      prod,
		RDB:       rdb,
		Secret:    secret,
		Workspace: workspace,
	}

	// Accept events right away; they only need the channel and people maps,
	// which Sync fills (and handlers re-sync on an unknown channel).
	mux := svc.OpsMux()
	mux.Handle("POST /slack/events", c)
	go svc.Serve(ctx, addr, mux)

	backoff := retry.Config{Base: time.Second, Max: 15 * time.Second}
	if err := retry.Do(ctx, backoff, func(attempt int, _ time.Duration, err error) {
		slog.Warn("initial slack sync failed", "attempt", attempt, "err", err)
	}, func() error { return c.Sync(ctx) }); err != nil {
		svc.Fatal("initial sync", err)
	}
	start := time.Now()
	n, err := c.Backfill(ctx, model.ModeBackfill)
	if err != nil {
		svc.Fatal("backfill", err)
	}
	slog.Info("slack backfill done", "messages", n, "took", time.Since(start).String())

	t := time.NewTicker(reconcile)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := c.Sync(ctx); err != nil {
				slog.Warn("reconcile sync", "err", err)
				continue
			}
			if n, err := c.Backfill(ctx, model.ModeLive); err != nil {
				slog.Warn("incremental backfill", "err", err)
			} else if n > 0 {
				slog.Info("incremental backfill caught up", "messages", n)
			}
		}
	}
}
