package check

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"net/http"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/khangpt2k6/Slipstream_CDC/internal/dirclient"
	pb "github.com/khangpt2k6/Slipstream_CDC/internal/gen/slipstream/v1"
	"github.com/khangpt2k6/Slipstream_CDC/internal/model"
	"github.com/khangpt2k6/Slipstream_CDC/internal/stats"
)

// EvalConfig tunes the relevance eval.
type EvalConfig struct {
	Max          int // labeled queries to run (default 0: all)
	Seed         uint64
	Workers      int  // concurrent queries (default 4)
	AllowBacklog bool // run even if some candidates have no embedding yet
}

// ModeScore is one retrieval mode's quality.
type ModeScore struct {
	Mode       string            `json:"mode"`
	HitAt1     float64           `json:"hit_at_1"`
	RecallAt5  float64           `json:"recall_at_5"`
	NDCGAt5    float64           `json:"ndcg_at_5"`
	RecallAt10 float64           `json:"recall_at_10"`
	MRRAt10    float64           `json:"mrr_at_10"`
	Latency    stats.Percentiles `json:"latency_ms"`
}

// EvalResult is what the eval writes.
type EvalResult struct {
	Run          string                 `json:"run"`
	StartedAt    time.Time              `json:"started_at"`
	Corpus       int                    `json:"corpus_docs"`
	Candidates   int                    `json:"candidate_docs"` // issues and pull requests searched over
	EmbedBacklog int                    `json:"embed_backlog"`
	Labeled      map[string]int         `json:"labeled_pairs"` // by datasource
	Queries      int                    `json:"queries"`
	Scores       []ModeScore            `json:"scores"`
	ByDatasource map[string][]ModeScore `json:"by_datasource"`
	// Relative change of hybrid over BM25, in percent.
	LiftRecallAt5 float64 `json:"lift_recall_at_5_pct"`
	LiftNDCGAt5   float64 `json:"lift_ndcg_at_5_pct"`
}

// Label is one evaluation query: the duplicate's title, and the issues the
// source says it duplicates.
type Label struct {
	Query      string   `json:"query"`
	From       string   `json:"from"` // the duplicate, excluded from its own results
	Relevant   []string `json:"relevant"`
	Datasource string   `json:"datasource"`
}

var issueKinds = []string{"issue", "pull_request"}

// Eval measures how well each retrieval mode finds the original of a
// duplicate issue from the duplicate's title.
//
// The labels are recorded by the sources themselves: Jira "duplicates"
// links and GitHub "Duplicate of #N" triage comments. For each pair, the
// duplicate's title is the query, the duplicate is removed from its own
// results, and the original is the one relevant answer. Search runs over
// issues and pull requests of the same source, as a user who can read them.
func Eval(ctx context.Context, s *Stack, cfg EvalConfig) (EvalResult, []LabelOutcome, error) {
	if cfg.Workers <= 0 {
		cfg.Workers = 4
	}
	res := EvalResult{Run: RunID(), StartedAt: time.Now().UTC(), Labeled: map[string]int{}, ByDatasource: map[string][]ModeScore{}}
	labels, containers, err := collectLabels(ctx, s)
	if err != nil {
		return res, nil, err
	}
	for _, l := range labels {
		res.Labeled[l.Datasource]++
	}
	rng := rand.New(rand.NewPCG(cfg.Seed, cfg.Seed+7)) // #nosec G404 -- sampling, not security
	rng.Shuffle(len(labels), func(i, j int) { labels[i], labels[j] = labels[j], labels[i] })
	if cfg.Max > 0 && len(labels) > cfg.Max {
		labels = labels[:cfg.Max]
	}
	res.Queries = len(labels)
	if res.Queries == 0 {
		return res, nil, fmt.Errorf("check: no duplicate labels in the index")
	}

	res.Corpus, _ = count(ctx, s, map[string]any{"term": map[string]any{"deleted": false}})
	res.Candidates, _ = count(ctx, s, map[string]any{"bool": map[string]any{"filter": []any{
		map[string]any{"term": map[string]any{"deleted": false}},
		map[string]any{"terms": map[string]any{"kind": issueKinds}},
	}}})
	res.EmbedBacklog, err = count(ctx, s, map[string]any{"bool": map[string]any{"filter": []any{
		map[string]any{"term": map[string]any{"deleted": false}},
		map[string]any{"term": map[string]any{"needs_embed": true}},
		map[string]any{"terms": map[string]any{"kind": issueKinds}},
	}}})
	if err != nil {
		return res, nil, err
	}
	if res.EmbedBacklog > 0 && !cfg.AllowBacklog {
		return res, nil, fmt.Errorf("check: %d candidate documents have no current embedding yet; wait for the embedder or pass -allow-backlog", res.EmbedBacklog)
	}

	user := "sscheck-eval-" + res.Run
	var grants []dirclient.Edge
	for _, c := range containers {
		grants = append(grants, dirclient.Edge{From: model.User(user), To: model.Container(c), Present: true})
	}
	if err := s.Dir.SetEdges(ctx, grants); err != nil {
		return res, nil, err
	}
	defer func() {
		for i := range grants {
			grants[i].Present = false
		}
		_ = s.Dir.SetEdges(context.WithoutCancel(ctx), grants)
	}()
	if _, ok, err := waitFor(ctx, 50*time.Millisecond, 30*time.Second, func() (bool, error) {
		p, err := s.Search.Principals(userCtx(ctx, user), &pb.PrincipalsRequest{})
		if err != nil {
			return false, err
		}
		for _, c := range containers {
			if !slices.Contains(p.Principals, model.Container(c)) {
				return false, nil
			}
		}
		return true, nil
	}); err != nil || !ok {
		return res, nil, fmt.Errorf("check: eval grants never applied (err %v)", err)
	}

	modes := []pb.Mode{pb.Mode_MODE_BM25, pb.Mode_MODE_KNN, pb.Mode_MODE_HYBRID}
	outcomes := make([]LabelOutcome, len(labels))
	jobs := make(chan int)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	for range cfg.Workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				o, err := runLabel(ctx, s, user, labels[i], modes)
				if err != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					mu.Unlock()
					continue
				}
				outcomes[i] = o
			}
		}()
	}
	for i := range labels {
		if (i+1)%200 == 0 {
			slog.Info("eval progress", "queries", i+1, "of", len(labels))
		}
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	if firstErr != nil {
		return res, nil, firstErr
	}

	res.Scores = score(outcomes, modes, "")
	for ds := range res.Labeled {
		res.ByDatasource[ds] = score(outcomes, modes, ds)
	}
	bm, hy := res.Scores[0], res.Scores[2]
	res.LiftRecallAt5 = lift(bm.RecallAt5, hy.RecallAt5)
	res.LiftNDCGAt5 = lift(bm.NDCGAt5, hy.NDCGAt5)
	return res, outcomes, nil
}

// LabelOutcome is one query's result in every mode (rank of the first
// relevant document after removing the query's own document, 0 if absent).
type LabelOutcome struct {
	Label
	Ranks  map[string][]int `json:"ranks"` // mode -> 1-based ranks of relevant docs in the top 10
	TookMs map[string]int64 `json:"took_ms"`
}

func runLabel(ctx context.Context, s *Stack, user string, l Label, modes []pb.Mode) (LabelOutcome, error) {
	o := LabelOutcome{Label: l, Ranks: map[string][]int{}, TookMs: map[string]int64{}}
	for _, m := range modes {
		t0 := time.Now()
		resp, err := s.SearchAs(ctx, user, &pb.SearchRequest{
			Query: l.Query, Mode: m, Datasources: []string{l.Datasource}, Kinds: issueKinds, Limit: 11,
		})
		if err != nil {
			return o, fmt.Errorf("check: eval query %q (%v): %w", l.Query, m, err)
		}
		name := modeName(m)
		o.TookMs[name] = ms(time.Since(t0))
		if resp.Mode != m {
			return o, fmt.Errorf("check: asked for %v, the api ran %v (is the embedding model configured?)", m, resp.Mode)
		}
		rank := 0
		for _, h := range resp.Hits {
			if h.Id == l.From {
				continue
			}
			rank++
			if rank > 10 {
				break
			}
			if slices.Contains(l.Relevant, h.Id) {
				o.Ranks[name] = append(o.Ranks[name], rank)
			}
		}
	}
	return o, nil
}

func modeName(m pb.Mode) string {
	switch m {
	case pb.Mode_MODE_BM25:
		return "bm25"
	case pb.Mode_MODE_KNN:
		return "knn"
	default:
		return "hybrid"
	}
}

func score(outs []LabelOutcome, modes []pb.Mode, ds string) []ModeScore {
	var out []ModeScore
	for _, m := range modes {
		name := modeName(m)
		sc := ModeScore{Mode: name}
		var lat []int64
		n := 0
		for _, o := range outs {
			if ds != "" && o.Datasource != ds {
				continue
			}
			n++
			lat = append(lat, o.TookMs[name])
			ranks := o.Ranks[name]
			rel := len(o.Relevant)
			in := func(k int) int {
				c := 0
				for _, r := range ranks {
					if r <= k {
						c++
					}
				}
				return c
			}
			if in(1) > 0 {
				sc.HitAt1++
			}
			sc.RecallAt5 += float64(in(5)) / float64(rel)
			sc.RecallAt10 += float64(in(10)) / float64(rel)
			if len(ranks) > 0 {
				sc.MRRAt10 += 1 / float64(slices.Min(ranks))
			}
			dcg, idcg := 0.0, 0.0
			for _, r := range ranks {
				if r <= 5 {
					dcg += 1 / math.Log2(float64(r)+1)
				}
			}
			for i := 1; i <= min(rel, 5); i++ {
				idcg += 1 / math.Log2(float64(i)+1)
			}
			sc.NDCGAt5 += dcg / idcg
		}
		if n > 0 {
			f := float64(n)
			sc.HitAt1, sc.RecallAt5, sc.RecallAt10, sc.MRRAt10, sc.NDCGAt5 = sc.HitAt1/f, sc.RecallAt5/f, sc.RecallAt10/f, sc.MRRAt10/f, sc.NDCGAt5/f
		}
		sc.Latency = stats.Summarize(lat)
		out = append(out, sc)
	}
	return out
}

func lift(base, v float64) float64 {
	if base == 0 {
		return 0
	}
	return math.Round((v-base)/base*1000) / 10
}

// collectLabels scans the index for documents that record a duplicate
// relation and turns them into queries. It returns the labels and the
// containers they live in.
func collectLabels(ctx context.Context, s *Stack) ([]Label, []string, error) {
	type marked struct {
		ID         string   `json:"id"`
		Kind       string   `json:"kind"`
		ParentID   string   `json:"parent_id"`
		Duplicates []string `json:"duplicates"`
	}
	rel := map[string][]string{} // duplicate issue -> originals
	var after []any
	for {
		body := map[string]any{
			"size":    1000,
			"_source": []string{"id", "kind", "parent_id", "duplicates"},
			"query": map[string]any{"bool": map[string]any{"filter": []any{
				map[string]any{"exists": map[string]any{"field": "duplicates"}},
				map[string]any{"term": map[string]any{"deleted": false}},
			}, "must_not": []any{map[string]any{"term": map[string]any{"datasource": "sscheck"}}}}},
			"sort": []any{map[string]any{"id": "asc"}},
		}
		if after != nil {
			body["search_after"] = after
		}
		var resp struct {
			Hits struct {
				Hits []struct {
					Source marked `json:"_source"`
					Sort   []any  `json:"sort"`
				} `json:"hits"`
			} `json:"hits"`
		}
		if err := s.OS.Do(ctx, http.MethodPost, "/"+s.Index+"/_search", body, &resp); err != nil {
			return nil, nil, err
		}
		hits := resp.Hits.Hits
		if len(hits) == 0 {
			break
		}
		for _, h := range hits {
			from := h.Source.ID
			if h.Source.Kind == "comment" {
				from = h.Source.ParentID // "Duplicate of #N" speaks for its thread
			}
			for _, d := range h.Source.Duplicates {
				if d != from && !slices.Contains(rel[from], d) {
					rel[from] = append(rel[from], d)
				}
			}
		}
		after = hits[len(hits)-1].Sort
	}

	var ids []string
	for from, tos := range rel {
		ids = append(ids, from)
		ids = append(ids, tos...)
	}
	slices.Sort(ids)
	ids = slices.Compact(ids)
	type meta struct {
		Title      string `json:"title"`
		Kind       string `json:"kind"`
		Datasource string `json:"datasource"`
		Container  string `json:"container"`
		Deleted    bool   `json:"deleted"`
	}
	docs := map[string]meta{}
	for i := 0; i < len(ids); i += 1000 {
		got, err := s.OS.MGet(ctx, s.Index, ids[i:min(i+1000, len(ids))], []string{"title", "kind", "datasource", "container", "deleted"})
		if err != nil {
			return nil, nil, err
		}
		for _, g := range got {
			var m meta
			if g.Found && json.Unmarshal(g.Source, &m) == nil && !m.Deleted {
				docs[g.ID] = m
			}
		}
	}

	usable := func(id string) (meta, bool) {
		m, ok := docs[id]
		return m, ok && slices.Contains(issueKinds, m.Kind)
	}
	var out []Label
	containers := map[string]bool{}
	for from, tos := range rel {
		a, ok := usable(from)
		if !ok || a.Title == "" {
			continue
		}
		var relevant []string
		for _, t := range tos {
			if b, ok := usable(t); ok && b.Datasource == a.Datasource {
				relevant = append(relevant, t)
			}
		}
		if len(relevant) == 0 {
			continue
		}
		slices.Sort(relevant)
		out = append(out, Label{Query: a.Title, From: from, Relevant: relevant, Datasource: a.Datasource})
		containers[a.Container] = true
	}
	sort.Slice(out, func(i, j int) bool { return out[i].From < out[j].From })
	var cs []string
	for c := range containers {
		cs = append(cs, c)
	}
	slices.Sort(cs)
	return out, cs, nil
}

func count(ctx context.Context, s *Stack, query map[string]any) (int, error) {
	var resp struct {
		Count int `json:"count"`
	}
	err := s.OS.Do(ctx, http.MethodPost, "/"+s.Index+"/_count", map[string]any{"query": query}, &resp)
	return resp.Count, err
}
