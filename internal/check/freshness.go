package check

import (
	"context"
	"fmt"
	"net/http"
	"time"

	pb "github.com/khangpt2k6/Slipstream_CDC/internal/gen/slipstream/v1"
	"github.com/khangpt2k6/Slipstream_CDC/internal/model"
	"github.com/khangpt2k6/Slipstream_CDC/internal/stats"
)

// FreshnessConfig tunes the freshness canary.
type FreshnessConfig struct {
	Samples  int           // changes to time (default 100)
	Interval time.Duration // pause between samples (default 250ms)
	Poll     time.Duration // search poll interval (default 5ms)
	User     string        // user searching for the canary (default alice)
}

// FreshnessResult is what the check writes.
type FreshnessResult struct {
	Run        string            `json:"run"`
	StartedAt  time.Time         `json:"started_at"`
	Samples    int               `json:"samples"`
	Refresh    string            `json:"index_refresh_interval"`
	Acked      stats.Percentiles `json:"produce_ack_ms"` // Kafka ack
	Indexed    stats.Percentiles `json:"indexed_ms"`     // readable by realtime get
	Searchable stats.Percentiles `json:"searchable_ms"`  // returned by search
	TimedOut   int               `json:"timed_out"`
}

// Freshness times live changes end to end: a new document is produced the
// way a connector would, and the clock stops when search returns it. The
// index refresh interval (how often OpenSearch opens a new searcher) sets
// the floor; everything above it is the pipeline.
func Freshness(ctx context.Context, s *Stack, cfg FreshnessConfig) (FreshnessResult, error) {
	if cfg.Samples <= 0 {
		cfg.Samples = 100
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 250 * time.Millisecond
	}
	if cfg.Poll <= 0 {
		cfg.Poll = 5 * time.Millisecond
	}
	if cfg.User == "" {
		cfg.User = "alice"
	}
	run := RunID()
	res := FreshnessResult{Run: run, StartedAt: time.Now().UTC(), Refresh: refreshInterval(ctx, s)}
	container := "sscheck:fresh-" + run
	if err := s.Dir.RegisterContainer(ctx, container, "sscheck", container, "public"); err != nil {
		return res, err
	}
	// Wait for the public grant to reach the graph.
	if _, ok, err := waitFor(ctx, 20*time.Millisecond, 30*time.Second, func() (bool, error) {
		p, err := s.Search.Principals(userCtx(ctx, cfg.User), &pb.PrincipalsRequest{})
		if err != nil {
			return false, err
		}
		for _, x := range p.Principals {
			if x == model.Container(container) {
				return true, nil
			}
		}
		return false, nil
	}); err != nil || !ok {
		return res, fmt.Errorf("check: grant for %s never applied (err %v)", container, err)
	}

	var acked, indexed, searchable []int64
	for i := range cfg.Samples {
		token := fmt.Sprintf("freshcanary%sx%d", run, i)
		id := container + "/" + token
		t0 := time.Now()
		ev := model.DocEvent{
			Op: model.OpUpsert, Version: 1, SrcTsMs: t0.UnixMilli(), EmitTsMs: t0.UnixMilli(), Mode: model.ModeLive,
			Doc: model.Document{ID: id, Datasource: "sscheck", Container: container, Kind: "message",
				Title: "freshness canary", Body: token, Allowed: []string{model.Container(container)}},
		}
		if err := s.Prod.Docs(ctx, ev); err != nil {
			return res, err
		}
		acked = append(acked, ms(time.Since(t0)))

		gotIndexed := false
		_, ok, err := waitFor(ctx, cfg.Poll, 30*time.Second, func() (bool, error) {
			if !gotIndexed {
				docs, err := s.OS.MGet(ctx, s.Index, []string{id}, []string{"version"})
				if err != nil {
					return false, err
				}
				if len(docs) == 1 && docs[0].Found {
					gotIndexed = true
					indexed = append(indexed, ms(time.Since(t0)))
				}
			}
			resp, err := s.SearchAs(ctx, cfg.User, &pb.SearchRequest{Query: token, Mode: pb.Mode_MODE_BM25, Datasources: []string{"sscheck"}, Limit: 3})
			if err != nil {
				return false, err
			}
			for _, h := range resp.Hits {
				if h.Id == id {
					return true, nil
				}
			}
			return false, nil
		})
		if err != nil {
			return res, err
		}
		if !ok {
			res.TimedOut++
		} else {
			searchable = append(searchable, ms(time.Since(t0)))
		}
		time.Sleep(cfg.Interval)
	}
	res.Samples = cfg.Samples
	res.Acked = stats.Summarize(acked)
	res.Indexed = stats.Summarize(indexed)
	res.Searchable = stats.Summarize(searchable)

	if err := purge(context.WithoutCancel(ctx), s, container); err != nil {
		return res, err
	}
	return res, nil
}

func refreshInterval(ctx context.Context, s *Stack) string {
	var out map[string]struct {
		Settings struct {
			Index struct {
				Refresh string `json:"refresh_interval"`
			} `json:"index"`
		} `json:"settings"`
	}
	if err := s.OS.Do(ctx, http.MethodGet, "/"+s.Index+"/_settings", nil, &out); err != nil {
		return ""
	}
	for _, v := range out {
		return v.Settings.Index.Refresh
	}
	return ""
}
