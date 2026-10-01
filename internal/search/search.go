// Package search is the read path: expand the caller into principals, run
// lexical and/or vector retrieval with the permission filter pushed into the
// engine, fuse the rankings, and re-check every hit before returning it.
//
// Permission filtering happens inside OpenSearch (a bool filter for BM25, the
// efficient-filtering path for k-NN) rather than after retrieval, so a user
// with access to a small slice of the corpus still gets a full page of
// results and no recall is lost. The final re-check in Go is defense in depth:
// it should never drop anything, and a drop is counted as a bug signal.
package search

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/khangpt2k6/Slipstream_CDC/internal/acl"
	"github.com/khangpt2k6/Slipstream_CDC/internal/metrics"
	"github.com/khangpt2k6/Slipstream_CDC/internal/opensearch"
)

// Mode selects the retrieval strategy.
type Mode string

// Retrieval modes.
const (
	ModeBM25   Mode = "bm25"
	ModeKNN    Mode = "knn"
	ModeHybrid Mode = "hybrid"
)

// ParseMode maps a request string to a mode, defaulting to hybrid.
func ParseMode(s string) (Mode, error) {
	switch Mode(s) {
	case "":
		return ModeHybrid, nil
	case ModeBM25, ModeKNN, ModeHybrid:
		return Mode(s), nil
	}
	return "", fmt.Errorf("search: unknown mode %q", s)
}

// Embedder turns text into vectors (the same model the index was embedded
// with).
type Embedder interface {
	Embed(ctx context.Context, texts []string) ([][]float32, error)
}

var postCheckDrops = promauto.NewCounter(prometheus.CounterOpts{
	Name: "ss_search_postcheck_drops_total",
	Help: "Hits the engine returned that failed the final permission re-check. Should stay 0.",
})

// Searcher answers search requests.
type Searcher struct {
	OS    *opensearch.Client
	Index string
	X     *acl.Expander
	Embed Embedder // optional; without it knn and hybrid fall back to bm25

	Candidates int // per-retriever depth before fusion (default 50)
	RRFK       int // reciprocal rank fusion constant (default 60)

	qcache sync.Map // query text -> []float32
}

// Request is one search.
type Request struct {
	Query       string
	User        string // user id, without the u: prefix
	Mode        Mode
	Datasources []string
	Limit       int
}

// Hit is one result.
type Hit struct {
	ID          string         `json:"id"`
	Title       string         `json:"title"`
	Snippet     string         `json:"snippet"` // HTML-escaped, with <mark> around matches
	URL         string         `json:"url"`
	Datasource  string         `json:"datasource"`
	Container   string         `json:"container"`
	Kind        string         `json:"kind"`
	Author      string         `json:"author"`
	ParentID    string         `json:"parent_id,omitempty"`
	UpdatedAt   string         `json:"updated_at,omitempty"`
	Score       float64        `json:"score"`
	Matched     string         `json:"matched"` // the principal that grants access
	Path        []string       `json:"path"`    // user -> ... -> matched
	FreshnessMs int64          `json:"freshness_ms,omitempty"`
	Ranks       map[string]int `json:"ranks,omitempty"` // rank per retriever
}

// Response is a page of results.
type Response struct {
	Hits       []Hit   `json:"hits"`
	Mode       Mode    `json:"mode"` // effective mode (after any fallback)
	TookMs     float64 `json:"took_ms"`
	Principals int     `json:"principals"`
	Epoch      int64   `json:"epoch"`
}

type source struct {
	ID          string   `json:"id"`
	Title       string   `json:"title"`
	Body        string   `json:"body"`
	URL         string   `json:"url"`
	Datasource  string   `json:"datasource"`
	Container   string   `json:"container"`
	Kind        string   `json:"kind"`
	Author      string   `json:"author"`
	ParentID    string   `json:"parent_id"`
	UpdatedAt   string   `json:"updated_at"`
	Allowed     []string `json:"allowed"`
	Deleted     bool     `json:"deleted"`
	Quarantined bool     `json:"quarantined"`
	SrcTsMs     int64    `json:"src_ts_ms"`
	IndexedAtMs int64    `json:"indexed_at_ms"`
	Mode        string   `json:"mode"`
}

var sourceFields = []string{"id", "title", "url", "datasource", "container", "kind", "author",
	"parent_id", "updated_at", "allowed", "deleted", "quarantined", "src_ts_ms", "indexed_at_ms", "mode"}

// Search runs req.
func (s *Searcher) Search(ctx context.Context, req Request) (Response, error) {
	start := time.Now()
	if req.User == "" {
		return Response{}, errors.New("search: a user is required")
	}
	if req.Limit <= 0 || req.Limit > 50 {
		req.Limit = 10
	}
	mode := req.Mode
	if mode == "" {
		mode = ModeHybrid
	}
	q := strings.TrimSpace(req.Query)
	if q == "" || s.Embed == nil {
		mode = ModeBM25 // vector search needs a query and a model
	}

	exp, err := s.X.Expand(ctx, req.User)
	if err != nil {
		return Response{}, err
	}
	filter := Filter(exp.Principals, req.Datasources)

	depth := s.Candidates
	if depth <= 0 {
		depth = 50
	}
	var lists map[string][]opensearch.Hit
	switch mode {
	case ModeBM25:
		hits, err := s.lexical(ctx, q, filter, req.Limit)
		if err != nil {
			return Response{}, err
		}
		lists = map[string][]opensearch.Hit{"bm25": hits}
	case ModeKNN:
		hits, err := s.vector(ctx, q, filter, req.Limit)
		if err != nil {
			return Response{}, err
		}
		lists = map[string][]opensearch.Hit{"knn": hits}
	case ModeHybrid:
		var lex, vec []opensearch.Hit
		var lerr, verr error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); lex, lerr = s.lexical(ctx, q, filter, depth) }()
		go func() { defer wg.Done(); vec, verr = s.vector(ctx, q, filter, depth) }()
		wg.Wait()
		if lerr != nil {
			return Response{}, lerr
		}
		if verr != nil {
			return Response{}, verr
		}
		lists = map[string][]opensearch.Hit{"bm25": lex, "knn": vec}
	}

	k := s.RRFK
	if k <= 0 {
		k = 60
	}
	fused := Fuse(lists, k)
	out := Response{Mode: mode, Principals: len(exp.Principals), Epoch: exp.Epoch}
	for _, f := range fused {
		if len(out.Hits) == req.Limit {
			break
		}
		h, ok := toHit(f, exp)
		if !ok {
			postCheckDrops.Inc()
			continue
		}
		out.Hits = append(out.Hits, h)
	}
	out.TookMs = float64(time.Since(start).Microseconds()) / 1000
	metrics.SearchLatency.WithLabelValues(string(mode)).Observe(time.Since(start).Seconds())
	return out, nil
}

// Filter is the permission and liveness filter every retrieval runs with.
func Filter(principals, datasources []string) []any {
	f := []any{
		map[string]any{"terms": map[string]any{"allowed": principals}},
		map[string]any{"term": map[string]any{"deleted": false}},
		map[string]any{"term": map[string]any{"quarantined": false}},
	}
	if len(datasources) > 0 {
		f = append(f, map[string]any{"terms": map[string]any{"datasource": datasources}})
	}
	return f
}

func textQuery(q string) map[string]any {
	return map[string]any{"multi_match": map[string]any{
		"query": q, "fields": []string{"title^3", "body"}, "type": "best_fields", "tie_breaker": 0.2,
	}}
}

func highlight(q string) map[string]any {
	h := map[string]any{
		"encoder":   "html",
		"pre_tags":  []string{"<mark>"},
		"post_tags": []string{"</mark>"},
		"fields": map[string]any{
			"body": map[string]any{"fragment_size": 200, "number_of_fragments": 1, "no_match_size": 200},
		},
	}
	if q != "" {
		h["highlight_query"] = textQuery(q)
	}
	return h
}

func (s *Searcher) lexical(ctx context.Context, q string, filter []any, size int) ([]opensearch.Hit, error) {
	body := map[string]any{
		"size":      size,
		"_source":   sourceFields,
		"highlight": highlight(q),
	}
	if q == "" {
		body["query"] = map[string]any{"bool": map[string]any{"filter": filter}}
		body["sort"] = []any{map[string]any{"updated_at": map[string]any{"order": "desc", "missing": "_last"}}}
	} else {
		body["query"] = map[string]any{"bool": map[string]any{"must": []any{textQuery(q)}, "filter": filter}}
	}
	resp, err := s.OS.Search(ctx, s.Index, body)
	if err != nil {
		return nil, err
	}
	return resp.Hits.Hits, nil
}

func (s *Searcher) vector(ctx context.Context, q string, filter []any, size int) ([]opensearch.Hit, error) {
	vec, err := s.queryVector(ctx, q)
	if err != nil {
		return nil, err
	}
	body := map[string]any{
		"size":      size,
		"_source":   sourceFields,
		"highlight": highlight(q),
		"query": map[string]any{"knn": map[string]any{"embedding": map[string]any{
			"vector": vec,
			"k":      size,
			"filter": map[string]any{"bool": map[string]any{"filter": filter}},
		}}},
	}
	resp, err := s.OS.Search(ctx, s.Index, body)
	if err != nil {
		return nil, err
	}
	return resp.Hits.Hits, nil
}

func (s *Searcher) queryVector(ctx context.Context, q string) ([]float32, error) {
	if v, ok := s.qcache.Load(q); ok {
		return v.([]float32), nil
	}
	vecs, err := s.Embed.Embed(ctx, []string{q})
	if err != nil {
		return nil, fmt.Errorf("search: embed query: %w", err)
	}
	if len(vecs) != 1 {
		return nil, errors.New("search: embedder returned no vector")
	}
	s.qcache.Store(q, vecs[0])
	return vecs[0], nil
}

// Fused is one document after rank fusion.
type Fused struct {
	Hit   opensearch.Hit
	Score float64
	Ranks map[string]int
}

// Fuse merges ranked lists with reciprocal rank fusion: each list adds
// 1/(k+rank) for every document it returns. It needs no score calibration
// between BM25 and cosine similarity, which live on unrelated scales. Ties
// break by best single rank, then id, so results are deterministic.
func Fuse(lists map[string][]opensearch.Hit, k int) []Fused {
	byID := map[string]*Fused{}
	names := make([]string, 0, len(lists))
	for name := range lists {
		names = append(names, name)
	}
	slices.Sort(names) // bm25 before knn, so its highlight wins
	for _, name := range names {
		for i, h := range lists[name] {
			rank := i + 1
			f, ok := byID[h.ID]
			if !ok {
				f = &Fused{Hit: h, Ranks: map[string]int{}}
				byID[h.ID] = f
			}
			f.Ranks[name] = rank
			f.Score += 1 / float64(k+rank)
			if len(f.Hit.Highlight) == 0 && len(h.Highlight) > 0 {
				f.Hit.Highlight = h.Highlight
			}
		}
	}
	out := make([]Fused, 0, len(byID))
	for _, f := range byID {
		out = append(out, *f)
	}
	best := func(f Fused) int {
		b := 1 << 30
		for _, r := range f.Ranks {
			b = min(b, r)
		}
		return b
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Score != out[j].Score {
			return out[i].Score > out[j].Score
		}
		if bi, bj := best(out[i]), best(out[j]); bi != bj {
			return bi < bj
		}
		return out[i].Hit.ID < out[j].Hit.ID
	})
	return out
}

// toHit decodes a fused hit and re-checks it against the expansion.
func toHit(f Fused, exp acl.Expansion) (Hit, bool) {
	var src source
	if err := json.Unmarshal(f.Hit.Source, &src); err != nil {
		return Hit{}, false
	}
	if src.Deleted || src.Quarantined {
		return Hit{}, false
	}
	matched := ""
	for _, p := range src.Allowed {
		if exp.Has(p) {
			matched = p
			break
		}
	}
	if matched == "" {
		return Hit{}, false
	}
	snippet := ""
	if frags := f.Hit.Highlight["body"]; len(frags) > 0 {
		snippet = frags[0]
	}
	h := Hit{
		ID: src.ID, Title: src.Title, Snippet: snippet, URL: src.URL, Datasource: src.Datasource,
		Container: src.Container, Kind: src.Kind, Author: src.Author, ParentID: src.ParentID,
		UpdatedAt: src.UpdatedAt, Score: f.Score, Matched: matched, Path: exp.PathTo(matched), Ranks: f.Ranks,
	}
	// Freshness only means something for live changes; a backfilled item's
	// source time is when it last changed, possibly years ago.
	if src.Mode == "live" && src.SrcTsMs > 0 && src.IndexedAtMs >= src.SrcTsMs {
		h.FreshnessMs = src.IndexedAtMs - src.SrcTsMs
	}
	return h, true
}
