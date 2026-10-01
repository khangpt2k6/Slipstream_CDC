package api

import (
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"strconv"
	"time"

	"github.com/khangpt2k6/Slipstream_CDC/internal/events"
)

// Percentiles summarizes a latency sample in milliseconds.
type Percentiles struct {
	N   int   `json:"n"`
	P50 int64 `json:"p50"`
	P95 int64 `json:"p95"`
	P99 int64 `json:"p99"`
	Max int64 `json:"max"`
}

// Summarize computes nearest-rank percentiles of samples.
func Summarize(samples []int64) Percentiles {
	if len(samples) == 0 {
		return Percentiles{}
	}
	s := slices.Clone(samples)
	slices.Sort(s)
	at := func(q float64) int64 {
		i := int(q*float64(len(s))+0.999999) - 1
		return s[max(0, min(i, len(s)-1))]
	}
	return Percentiles{N: len(s), P50: at(0.50), P95: at(0.95), P99: at(0.99), Max: s[len(s)-1]}
}

// Stats is the admin view of the pipeline.
type Stats struct {
	Docs           map[string]int   `json:"docs"`
	Total          int              `json:"total"`
	Quarantined    int              `json:"quarantined"`
	EmbedBacklog   int              `json:"embed_backlog"`
	Lag            map[string]int64 `json:"lag"`
	IndexFreshness Percentiles      `json:"index_freshness_ms"`
	ACLPropagation Percentiles      `json:"acl_propagation_ms"`
	Epoch          int64            `json:"epoch"`
	TookMs         int64            `json:"took_ms"`
}

var lagGroups = []string{"ss-index-docs", "ss-index-identity"}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	st := Stats{Docs: map[string]int{}, Lag: map[string]int64{}}

	body := map[string]any{
		"size":             0,
		"track_total_hits": true,
		"query":            map[string]any{"term": map[string]any{"deleted": false}},
		"aggs": map[string]any{
			"by_ds":       map[string]any{"terms": map[string]any{"field": "datasource", "size": 20}},
			"quarantined": map[string]any{"filter": map[string]any{"term": map[string]any{"quarantined": true}}},
			"backlog":     map[string]any{"filter": map[string]any{"term": map[string]any{"needs_embed": true}}},
		},
	}
	if resp, err := s.OS.Search(ctx, s.Index, body); err == nil {
		st.Total = resp.Hits.Total.Value
		var aggs struct {
			ByDS struct {
				Buckets []struct {
					Key   string `json:"key"`
					Count int    `json:"doc_count"`
				} `json:"buckets"`
			} `json:"by_ds"`
			Quarantined struct {
				Count int `json:"doc_count"`
			} `json:"quarantined"`
			Backlog struct {
				Count int `json:"doc_count"`
			} `json:"backlog"`
		}
		if json.Unmarshal(resp.Aggregations, &aggs) == nil {
			for _, b := range aggs.ByDS.Buckets {
				st.Docs[b.Key] = b.Count
			}
			st.Quarantined = aggs.Quarantined.Count
			st.EmbedBacklog = aggs.Backlog.Count
		}
	}

	if s.Kadm != nil {
		if lags, err := s.Kadm.Lag(ctx, lagGroups...); err == nil {
			for g, l := range lags {
				st.Lag[g] = l.Lag.Total()
			}
		}
	}

	st.IndexFreshness = Summarize(s.samples(ctx, events.SamplesIndex))
	st.ACLPropagation = Summarize(s.samples(ctx, events.SamplesACL))
	if ep, err := s.Search.X.Epoch(ctx); err == nil {
		st.Epoch = ep
	}
	st.TookMs = time.Since(start).Milliseconds()
	writeJSON(w, http.StatusOK, st)
}

func (s *Server) samples(ctx context.Context, key string) []int64 {
	vals, err := s.RDB.LRange(ctx, key, 0, -1).Result()
	if err != nil {
		return nil
	}
	out := make([]int64, 0, len(vals))
	for _, v := range vals {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			out = append(out, n)
		}
	}
	return out
}
