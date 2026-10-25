// Command connector-jira syncs Jira projects into Slipstream: a full crawl
// of issues and comments on first start, then JQL polls that re-read an
// overlap window behind the cursor, so a change Jira indexed late is still
// picked up.
package main

import (
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/khangpt2k6/Slipstream_CDC/internal/config"
	"github.com/khangpt2k6/Slipstream_CDC/internal/connectors"
	"github.com/khangpt2k6/Slipstream_CDC/internal/connectors/jira"
	"github.com/khangpt2k6/Slipstream_CDC/internal/dirclient"
	"github.com/khangpt2k6/Slipstream_CDC/internal/kafkax"
	"github.com/khangpt2k6/Slipstream_CDC/internal/model"
	"github.com/khangpt2k6/Slipstream_CDC/internal/retry"
	"github.com/khangpt2k6/Slipstream_CDC/internal/svc"
)

func main() {
	e := config.New(os.Getenv)
	var (
		addr         = e.String("SS_CONNECTOR_ADDR", ":8085")
		brokers      = e.List("SS_KAFKA_BROKERS", "localhost:29092")
		redisAddr    = e.String("SS_REDIS_ADDR", "localhost:6379")
		directoryURL = e.String("SS_DIRECTORY_URL", "http://localhost:8081")
		base         = e.String("SS_JIRA_URL", "https://issues.apache.org/jira")
		token        = e.String("SS_JIRA_TOKEN", "")
		projects     = e.List("SS_JIRA_PROJECTS", "KAFKA")
		zoneName     = e.String("SS_JIRA_TZ", "UTC")
		pageSize     = e.Int("SS_JIRA_PAGE_SIZE", 200)
		overlap      = e.Duration("SS_JIRA_OVERLAP", 2*time.Minute)
		comments     = e.Bool("SS_JIRA_COMMENTS", true)
		poll         = e.Duration("SS_POLL_INTERVAL", time.Minute)
		dumpDir      = e.String("SS_DUMP_DIR", "")
		logLevel     = e.String("SS_LOG_LEVEL", "info")
	)
	if err := e.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "invalid configuration:", err)
		os.Exit(1)
	}
	zone, err := time.LoadLocation(zoneName)
	if err != nil {
		fmt.Fprintln(os.Stderr, "invalid configuration: SS_JIRA_TZ:", err)
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
	dump, err := connectors.OpenDump(dumpDir, "jira")
	if err != nil {
		svc.Fatal("open dump", err)
	}
	defer func() { _ = dump.Close() }()

	c := &jira.Connector{
		API:      &jira.Client{Base: base, Token: token},
		Dir:      dirclient.New(directoryURL),
		Out:      &connectors.Emitter{Prod: prod, Source: "jira", Dump: dump},
		RDB:      rdb,
		Projects: projects,
		PageSize: pageSize,
		Overlap:  overlap,
		Zone:     zone,
		Comments: comments,
	}
	go svc.Serve(ctx, addr, svc.OpsMux())

	backoff := retry.Config{Base: time.Second, Max: 15 * time.Second}
	if err := retry.Do(ctx, backoff, func(attempt int, _ time.Duration, err error) {
		slog.Warn("registering projects failed", "attempt", attempt, "err", err)
	}, func() error { return c.Sync(ctx) }); err != nil {
		svc.Fatal("sync", err)
	}

	start := time.Now()
	var total int
	if err := retry.Do(ctx, retry.Config{Base: 5 * time.Second, Max: 5 * time.Minute}, func(attempt int, _ time.Duration, err error) {
		slog.Warn("jira crawl failed; resuming from cursor", "attempt", attempt, "err", err)
	}, func() error {
		n, err := c.Crawl(ctx, model.ModeBackfill)
		total += n
		return err
	}); err != nil {
		return
	}
	slog.Info("jira backfill done", "projects", projects, "events", total, "took", time.Since(start).String())

	t := time.NewTicker(poll)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if n, err := c.Crawl(ctx, model.ModeLive); err != nil {
				slog.Warn("jira poll", "err", err)
			} else if n > 0 {
				slog.Info("jira poll caught up", "events", n)
			}
		}
	}
}
