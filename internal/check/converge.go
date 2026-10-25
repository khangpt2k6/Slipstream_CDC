package check

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/khangpt2k6/Slipstream_CDC/internal/connectors"
	"github.com/khangpt2k6/Slipstream_CDC/internal/kafkax"
	"github.com/khangpt2k6/Slipstream_CDC/internal/model"
)

// ConvergeConfig tunes the convergence check.
type ConvergeConfig struct {
	Dumps       []string // connector dumps to draw real documents from
	Docs        int      // distinct documents (default 50000)
	Updates     int      // versions per document (default 4)
	DupProb     float64  // chance an event is also sent a second time (default 0.1)
	ReorderProb float64  // chance two consecutive versions are sent swapped (default 0.1)
	DeleteProb  float64  // chance a document's last version is a delete (default 0.05)
	Rate        int      // target events per second; 0 sends as fast as Kafka takes them
	Timeout     time.Duration
	Seed        uint64
	Keep        bool // leave the documents in the index afterwards
	Group       string
	// Slack is how many applies beyond the expected count to attribute to
	// other traffic. Run with the connectors stopped and leave it at 0 for
	// an exact check.
	Slack int
}

// ConvergeResult is what the check writes.
type ConvergeResult struct {
	Run       string    `json:"run"`
	StartedAt time.Time `json:"started_at"`

	Docs       int `json:"docs"`
	Events     int `json:"events"`      // records produced, chaos included
	Versions   int `json:"versions"`    // distinct (document, version) pairs
	Duplicates int `json:"duplicates"`  // events sent twice
	Reordered  int `json:"reordered"`   // versions sent after a newer one
	Deletes    int `json:"deletes"`     // documents whose final version is a delete
	AvgBytes   int `json:"avg_bytes"`   // mean event size
	TargetRate int `json:"target_rate"` // 0 = unthrottled

	ProduceSec    float64 `json:"produce_sec"`
	DrainSec      float64 `json:"drain_sec"`       // first produce to consumer lag zero
	ProduceRate   float64 `json:"produce_rate"`    // events/s into Kafka
	EndToEndRate  float64 `json:"end_to_end_rate"` // events/s from first produce to all applied
	PeakWriteRate float64 `json:"peak_write_rate"` // best 10s window of events through the sink, from Prometheus

	// An event changes the index only if it is newer than every earlier
	// event for its document. Kafka keeps each document's events in send
	// order, so the number of events that should apply is known exactly,
	// whatever the batching, retries or rebalances.
	ExpectedApplies int     `json:"expected_applies"`
	AppliedDelta    float64 `json:"applied_delta"`   // ss_docs_applied_total over the run
	DiscardedDelta  float64 `json:"discarded_delta"` // ss_docs_discarded_total over the run
	ConflictsDelta  float64 `json:"cas_conflicts_delta"`

	// Verification against the expected final state.
	Checked       int      `json:"checked"`
	Lost          int      `json:"lost"`      // missing, or older than the newest version sent
	Stale         int      `json:"stale"`     // newest version, wrong content or delete state
	Regressed     int      `json:"regressed"` // newer than anything sent (impossible)
	Examples      []string `json:"examples,omitempty"`
	DoubleApplies int      `json:"double_applies"` // applies beyond the expected count
	MissedApplies int      `json:"missed_applies"` // expected applies that never happened
	Pass          bool     `json:"pass"`
}

type expect struct {
	version int64
	deleted bool
	hash    string
}

// Converge replays real documents with injected duplicates and reordering
// and verifies the index converges to exactly the newest version of each.
//
// Documents are drawn from the connector dumps (real bodies and sizes) and
// re-keyed into a private container nobody holds, so the run is invisible
// to users. Each gets Updates versions; the chaos is applied in produce
// order, and since every document is one Kafka key, an older version sent
// after a newer one really does arrive late at the indexer.
func Converge(ctx context.Context, s *Stack, cfg ConvergeConfig) (ConvergeResult, error) {
	if cfg.Docs <= 0 {
		cfg.Docs = 50000
	}
	if cfg.Updates <= 0 {
		cfg.Updates = 4
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 15 * time.Minute
	}
	if cfg.Group == "" {
		cfg.Group = "ss-index-docs"
	}
	run := RunID()
	res := ConvergeResult{Run: run, StartedAt: time.Now().UTC(), TargetRate: cfg.Rate}
	rng := rand.New(rand.NewPCG(cfg.Seed, cfg.Seed^0xc0ffee)) // #nosec G404 -- simulation, not security

	templates, err := sample(cfg.Dumps, cfg.Docs, rng)
	if err != nil {
		return res, err
	}
	if len(templates) == 0 {
		return res, errors.New("check: no documents in the dumps")
	}
	res.Docs = len(templates)
	container := "sscheck:converge-" + run

	// Build each document's version history, then its chaotic send order.
	want := make(map[string]expect, len(templates))
	seqs := make([][]model.DocEvent, len(templates))
	for i, t := range templates {
		id := fmt.Sprintf("%s/%s", container, t.ID)
		var hist []model.DocEvent
		for v := 1; v <= cfg.Updates; v++ {
			d := t
			d.ID, d.Datasource, d.Container = id, "sscheck", container
			d.Allowed = []string{model.Container(container)}
			d.Duplicates = nil // keep the relevance labels to the real corpus
			d.Body = fmt.Sprintf("%s\n\nrev %d", t.Body, v)
			ev := model.DocEvent{Op: model.OpUpsert, Doc: d, Version: int64(v), Mode: model.ModeReplay}
			if v == cfg.Updates && rng.Float64() < cfg.DeleteProb {
				ev = model.DocEvent{Op: model.OpDelete, Version: int64(v), Mode: model.ModeReplay,
					Doc: model.Document{ID: id, Datasource: "sscheck", Container: container}}
				res.Deletes++
			}
			hist = append(hist, ev)
		}
		last := hist[len(hist)-1]
		want[id] = expect{version: last.Version, deleted: last.Op == model.OpDelete, hash: hashOf(last)}
		res.Versions += len(hist)

		for j := 0; j+1 < len(hist); j++ {
			if rng.Float64() < cfg.ReorderProb {
				hist[j], hist[j+1] = hist[j+1], hist[j]
				res.Reordered++
				j++ // don't swap the same version twice
			}
		}
		var sent []model.DocEvent
		for _, ev := range hist {
			sent = append(sent, ev)
			if rng.Float64() < cfg.DupProb {
				sent = append(sent, ev)
				res.Duplicates++
			}
		}
		top := int64(0)
		for _, ev := range sent {
			if ev.Version > top {
				top = ev.Version
				res.ExpectedApplies++
			}
		}
		seqs[i] = sent
	}

	// Interleave documents randomly, keeping each document's own order.
	var order []int
	for i, sq := range seqs {
		for range sq {
			order = append(order, i)
		}
	}
	rng.Shuffle(len(order), func(a, b int) { order[a], order[b] = order[b], order[a] })
	recs := make([]*kgo.Record, 0, len(order))
	next := make([]int, len(seqs))
	size := 0
	for _, i := range order {
		ev := seqs[i][next[i]]
		next[i]++
		r, err := kafkax.Record(model.TopicDocs, ev.Doc.ID, ev)
		if err != nil {
			return res, err
		}
		size += len(r.Value)
		recs = append(recs, r)
	}
	res.Events = len(recs)
	res.AvgBytes = size / max(1, len(recs))
	slog.Info("converge plan", "docs", res.Docs, "events", res.Events, "duplicates", res.Duplicates,
		"reordered", res.Reordered, "deletes", res.Deletes, "avg_bytes", res.AvgBytes)

	before := promSnapshot(ctx, s)

	// Produce, then wait for the indexer group to drain.
	start := time.Now()
	if err := produce(ctx, s.Prod, recs, cfg.Rate); err != nil {
		return res, err
	}
	res.ProduceSec = time.Since(start).Seconds()
	res.ProduceRate = float64(res.Events) / res.ProduceSec
	slog.Info("produced", "events", res.Events, "sec", res.ProduceSec, "rate", int(res.ProduceRate))

	_, drained, err := waitFor(ctx, 50*time.Millisecond, cfg.Timeout, func() (bool, error) {
		lag, err := s.Lag(ctx, cfg.Group)
		return err == nil && lag == 0, nil
	})
	if err != nil {
		return res, err
	}
	if !drained {
		return res, fmt.Errorf("check: indexer did not drain within %s", cfg.Timeout)
	}
	res.DrainSec = time.Since(start).Seconds()
	res.EndToEndRate = float64(res.Events) / res.DrainSec
	slog.Info("drained", "sec", res.DrainSec, "end_to_end_rate", int(res.EndToEndRate))

	if err := verify(ctx, s, want, &res); err != nil {
		return res, err
	}

	// Let Prometheus scrape the final counters (5s interval) twice.
	time.Sleep(11 * time.Second)
	after := promSnapshot(ctx, s)
	res.AppliedDelta = after.applied - before.applied
	res.DiscardedDelta = after.discarded - before.discarded
	res.ConflictsDelta = after.conflicts - before.conflicts
	window := fmt.Sprintf("%ds", int(time.Since(start).Seconds())+1)
	res.PeakWriteRate, _ = s.Prom(ctx, `max_over_time(sum(rate(ss_items_written_total{pipeline="docs"}[10s]))[`+window+`:1s])`)
	extra := int(res.AppliedDelta) - res.ExpectedApplies
	res.MissedApplies = max(0, -extra)
	res.DoubleApplies = max(0, extra-cfg.Slack)
	res.Pass = res.Lost == 0 && res.Stale == 0 && res.Regressed == 0 && res.DoubleApplies == 0 && res.MissedApplies == 0

	if !cfg.Keep {
		if err := purge(context.WithoutCancel(ctx), s, container); err != nil {
			slog.Warn("purge converge docs", "err", err)
		}
	}
	return res, nil
}

type snapshot struct{ applied, discarded, conflicts float64 }

func promSnapshot(ctx context.Context, s *Stack) snapshot {
	var sn snapshot
	sn.applied, _ = s.Prom(ctx, "sum(ss_docs_applied_total)")
	sn.discarded, _ = s.Prom(ctx, "sum(ss_docs_discarded_total)")
	sn.conflicts, _ = s.Prom(ctx, "sum(ss_cas_conflicts_total)")
	return sn
}

func hashOf(ev model.DocEvent) string {
	if ev.Op == model.OpDelete {
		return ""
	}
	return ev.Doc.ContentHash()
}

// sample reservoir-samples n distinct upserted documents across dumps.
func sample(paths []string, n int, rng *rand.Rand) ([]model.Document, error) {
	var out []model.Document
	seen := map[string]bool{}
	k := 0
	for _, p := range paths {
		err := connectors.ReadDump(p, func(ev model.DocEvent) error {
			if ev.Op != model.OpUpsert || seen[ev.Doc.ID] {
				return nil
			}
			seen[ev.Doc.ID] = true
			k++
			if len(out) < n {
				out = append(out, ev.Doc)
			} else if j := rng.IntN(k); j < n {
				out[j] = ev.Doc
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// produce sends recs asynchronously, pacing to rate events/s when rate > 0,
// and waits for every ack.
func produce(ctx context.Context, p *kafkax.Producer, recs []*kgo.Record, rate int) error {
	var (
		mu    sync.Mutex
		first error
		acked atomic.Int64
	)
	done := func(err error) {
		acked.Add(1)
		if err != nil {
			mu.Lock()
			if first == nil {
				first = err
			}
			mu.Unlock()
		}
	}
	start := time.Now()
	for i, r := range recs {
		if rate > 0 && i%100 == 0 {
			due := start.Add(time.Duration(float64(i) / float64(rate) * float64(time.Second)))
			if d := time.Until(due); d > 0 {
				time.Sleep(d)
			}
		}
		p.SendAsync(ctx, r, done)
	}
	if err := p.Flush(ctx); err != nil {
		return err
	}
	mu.Lock()
	defer mu.Unlock()
	return first
}

// verify reads every document back in realtime and compares it with the
// newest version sent.
func verify(ctx context.Context, s *Stack, want map[string]expect, res *ConvergeResult) error {
	ids := make([]string, 0, len(want))
	for id := range want {
		ids = append(ids, id)
	}
	for i := 0; i < len(ids); i += 1000 {
		docs, err := s.OS.MGet(ctx, s.Index, ids[i:min(i+1000, len(ids))], []string{"version", "deleted", "content_hash"})
		if err != nil {
			return err
		}
		for _, d := range docs {
			res.Checked++
			w := want[d.ID]
			var src struct {
				Version     int64  `json:"version"`
				Deleted     bool   `json:"deleted"`
				ContentHash string `json:"content_hash"`
			}
			if d.Found {
				_ = json.Unmarshal(d.Source, &src)
			}
			bad := ""
			switch {
			case !d.Found || src.Version < w.version:
				res.Lost++
				bad = fmt.Sprintf("lost %s: have v%d want v%d", d.ID, src.Version, w.version)
			case src.Version > w.version:
				res.Regressed++
				bad = fmt.Sprintf("regressed %s: have v%d want v%d", d.ID, src.Version, w.version)
			case src.Deleted != w.deleted || src.ContentHash != w.hash:
				res.Stale++
				bad = fmt.Sprintf("stale %s: deleted=%v hash=%s", d.ID, src.Deleted, src.ContentHash)
			}
			if bad != "" && len(res.Examples) < 10 {
				res.Examples = append(res.Examples, bad)
			}
		}
	}
	return nil
}

// purge physically removes a run's documents.
func purge(ctx context.Context, s *Stack, container string) error {
	body := map[string]any{"query": map[string]any{"term": map[string]any{"container": container}}}
	return s.OS.Do(ctx, http.MethodPost, "/"+s.Index+"/_delete_by_query?conflicts=proceed&refresh=true", body, nil)
}
