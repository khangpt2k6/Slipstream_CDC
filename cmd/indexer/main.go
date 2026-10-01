// Command indexer runs Slipstream's two write pipelines side by side:
//
//   - docs: ss.docs -> the search index (versioned, conflict-checked writes)
//   - identity: ss.identity -> the permission graph in Redis
//
// Each pipeline is its own consumer group with its own offsets, so a stalled
// search cluster never delays a permission revoke, and either view can be
// rebuilt by resetting only its group.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"

	"github.com/khangpt2k6/Slipstream_CDC/internal/acl"
	"github.com/khangpt2k6/Slipstream_CDC/internal/config"
	"github.com/khangpt2k6/Slipstream_CDC/internal/consumer"
	"github.com/khangpt2k6/Slipstream_CDC/internal/dlq"
	"github.com/khangpt2k6/Slipstream_CDC/internal/events"
	"github.com/khangpt2k6/Slipstream_CDC/internal/index"
	"github.com/khangpt2k6/Slipstream_CDC/internal/kafkax"
	"github.com/khangpt2k6/Slipstream_CDC/internal/lag"
	"github.com/khangpt2k6/Slipstream_CDC/internal/model"
	"github.com/khangpt2k6/Slipstream_CDC/internal/opensearch"
	"github.com/khangpt2k6/Slipstream_CDC/internal/pipeline"
	"github.com/khangpt2k6/Slipstream_CDC/internal/retry"
	"github.com/khangpt2k6/Slipstream_CDC/internal/svc"
)

type cfg struct {
	Brokers       []string
	Partitions    int
	Pipelines     []string
	OpenSearchURL string
	Index         string
	Refresh       string
	Dims          int
	RedisAddr     string
	BatchSize     int
	FlushInterval time.Duration
	RetryBase     time.Duration
	RetryMax      time.Duration
	OSTimeout     time.Duration
	LagInterval   time.Duration
	DLQSuffix     string
	MetricsAddr   string
	LogLevel      string
}

func load() (cfg, error) {
	e := config.New(os.Getenv)
	c := cfg{
		Brokers:       e.List("SS_KAFKA_BROKERS", "localhost:29092"),
		Partitions:    e.Int("SS_KAFKA_PARTITIONS", 6),
		Pipelines:     e.List("SS_PIPELINES", "docs,identity"),
		OpenSearchURL: e.String("SS_OPENSEARCH_URL", "http://localhost:9200"),
		Index:         e.String("SS_INDEX", "ss-docs"),
		Refresh:       e.String("SS_INDEX_REFRESH", "500ms"),
		Dims:          e.Int("SS_EMBED_DIMS", 384),
		RedisAddr:     e.String("SS_REDIS_ADDR", "localhost:6379"),
		BatchSize:     e.Int("SS_BATCH_SIZE", 1000),
		FlushInterval: e.Duration("SS_FLUSH_INTERVAL", 250*time.Millisecond),
		RetryBase:     e.Duration("SS_RETRY_BASE", 200*time.Millisecond),
		RetryMax:      e.Duration("SS_RETRY_MAX", 10*time.Second),
		OSTimeout:     e.Duration("SS_OPENSEARCH_TIMEOUT", 30*time.Second),
		LagInterval:   e.Duration("SS_LAG_INTERVAL", 5*time.Second),
		DLQSuffix:     e.String("SS_DLQ_SUFFIX", ".dlq"),
		MetricsAddr:   e.String("SS_METRICS_ADDR", ":9101"),
		LogLevel:      e.String("SS_LOG_LEVEL", "info"),
	}
	return c, e.Err()
}

func main() {
	c, err := load()
	if err != nil {
		fmt.Fprintln(os.Stderr, "invalid configuration:", err)
		os.Exit(1)
	}
	ctx, stop := svc.Init(c.LogLevel)
	defer stop()

	if err := kafkax.EnsureTopics(ctx, c.Brokers, kafkax.StandardTopics(int32(c.Partitions), c.DLQSuffix)); err != nil { // #nosec G115 -- partitions is a small positive config value
		svc.Fatal("ensure topics", err)
	}

	rdb := redis.NewClient(&redis.Options{Addr: c.RedisAddr})
	defer func() { _ = rdb.Close() }()
	pub := &events.Publisher{RDB: rdb}

	deadletter, err := dlq.New(c.Brokers, c.DLQSuffix)
	if err != nil {
		svc.Fatal("connect dlq producer", err)
	}
	defer deadletter.Close()

	go svc.Serve(ctx, c.MetricsAddr, svc.OpsMux())

	g, gctx := errgroup.WithContext(ctx)
	if slices.Contains(c.Pipelines, "docs") {
		r, cons, err := docsPipeline(gctx, c, deadletter, pub)
		if err != nil {
			svc.Fatal("start docs pipeline", err)
		}
		defer cons.Close()
		go lag.Run(gctx, "ss-index-docs", cons, c.LagInterval)
		g.Go(func() error { return r.Run(gctx) })
	}
	if slices.Contains(c.Pipelines, "identity") {
		r, cons, err := identityPipeline(c, rdb, deadletter, pub)
		if err != nil {
			svc.Fatal("start identity pipeline", err)
		}
		defer cons.Close()
		go lag.Run(gctx, "ss-index-identity", cons, c.LagInterval)
		g.Go(func() error { return r.Run(gctx) })
	}

	slog.Info("indexer running", "pipelines", c.Pipelines, "index", c.Index, "refresh", c.Refresh, "batch", c.BatchSize)
	if err := g.Wait(); err != nil && ctx.Err() == nil {
		svc.Fatal("pipeline stopped", err)
	}
	slog.Info("indexer stopped")
}

func retryCfg(c cfg) retry.Config { return retry.Config{Base: c.RetryBase, Max: c.RetryMax} }

func docsPipeline(ctx context.Context, c cfg, dl *dlq.Producer, pub *events.Publisher) (*pipeline.Runner, *consumer.Consumer, error) {
	osc := opensearch.New(c.OpenSearchURL, c.OSTimeout)
	if err := retry.Do(ctx, retryCfg(c), func(attempt int, _ time.Duration, err error) {
		slog.Warn("waiting for opensearch", "attempt", attempt, "err", err)
	}, func() error {
		return osc.EnsureIndex(ctx, c.Index, c.Index+"-v1", index.Mapping(c.Dims, c.Refresh))
	}); err != nil {
		return nil, nil, err
	}

	cons, err := consumer.New(c.Brokers, "ss-index-docs", []string{model.TopicDocs})
	if err != nil {
		return nil, nil, err
	}
	sink := &index.DocSink{
		OS:    osc,
		Index: c.Index,
		Observe: func(res index.Result) {
			if res.Applied == 0 && res.Quarantined == 0 {
				return
			}
			pub.Publish([]events.Event{{
				Type: "index", TsMs: time.Now().UnixMilli(),
				Applied: res.Applied, Discarded: res.Discarded, Conflicts: res.Conflicts,
				Quarantined: res.Quarantined, Sample: res.Sample,
			}}, events.SamplesIndex, res.FreshMs)
		},
	}
	return &pipeline.Runner{
		Config: pipeline.Config{Name: "docs", BatchSize: c.BatchSize, FlushInterval: c.FlushInterval, Retry: retryCfg(c)},
		Poller: cons,
		DLQ:    dl,
		Sink:   sink,
		Decode: func(r *kgo.Record) (pipeline.Item, error) {
			it, err := index.DecodeDocRecord(r.Key, r.Value)
			it.Rec = r
			return it, err
		},
		// An unreadable doc event hides its document: it may have been a
		// revoke, so failing open could leak.
		Quarantine: func(r *kgo.Record, _ error) (pipeline.Item, bool) {
			if len(r.Key) == 0 {
				return pipeline.Item{}, false
			}
			id := string(r.Key)
			return pipeline.Item{Key: id, Value: index.Quarantine{ID: id, AtMs: r.Timestamp.UnixMilli()}, Rec: r}, true
		},
	}, cons, nil
}

func identityPipeline(c cfg, rdb *redis.Client, dl *dlq.Producer, pub *events.Publisher) (*pipeline.Runner, *consumer.Consumer, error) {
	cons, err := consumer.New(c.Brokers, "ss-index-identity", []string{model.TopicIdentity})
	if err != nil {
		return nil, nil, err
	}
	sink := &acl.Sink{
		RDB: rdb,
		Observe: func(changes []acl.Change) {
			evs := make([]events.Event, 0, len(changes))
			samples := make([]int64, 0, len(changes))
			for _, ch := range changes {
				lat := int64(0)
				if ch.SrcTsMs > 0 {
					lat = ch.AppliedMs - ch.SrcTsMs
					samples = append(samples, lat)
				}
				evs = append(evs, events.Event{
					Type: "acl", TsMs: ch.AppliedMs, From: ch.From, To: ch.To,
					Present: ch.Present, LatencyMs: lat,
				})
			}
			// Bulk loads apply thousands of edges; the feed only needs a taste.
			if len(evs) > 20 {
				evs = evs[:20]
			}
			pub.Publish(evs, events.SamplesACL, samples)
		},
	}
	return &pipeline.Runner{
		// Identity batches stay small and flush fast: revokes are latency
		// sensitive and edges are tiny.
		Config: pipeline.Config{Name: "identity", BatchSize: 500, FlushInterval: 50 * time.Millisecond, Retry: retryCfg(c)},
		Poller: cons,
		DLQ:    dl,
		Sink:   sink,
		Decode: func(r *kgo.Record) (pipeline.Item, error) {
			it, err := acl.DecodeEdgeRecord(r.Key, r.Value)
			it.Rec = r
			return it, err
		},
	}, cons, nil
}
