package embed

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/khangpt2k6/Slipstream_CDC/internal/metrics"
	"github.com/khangpt2k6/Slipstream_CDC/internal/opensearch"
)

// Model turns texts into vectors.
type Model interface {
	Embed(ctx context.Context, texts []string) ([][]float32, error)
}

// Worker keeps document embeddings current. The indexer flags a document
// needs_embed whenever its title or body changes; the worker pages through
// flagged documents, embeds them, and writes each vector with a scripted
// update that only applies if the document's content hash is still the one
// it embedded. A document edited mid-flight therefore keeps its flag and is
// picked up again, and a permission or metadata change (same hash) never
// costs a re-embed.
type Worker struct {
	OS          *opensearch.Client
	Index       string
	Model       Model
	Batch       int // texts per model call (default 32)
	Concurrency int // model calls in flight (default 4)
	MaxChars    int // text sent per document (default 2000; the model truncates anyway)
}

// setVector writes a vector only if the content it was computed from is
// still current.
const setVector = `if (ctx._source.content_hash == params.h) {
  ctx._source.embedding = params.v;
  ctx._source.embed_hash = params.h;
  ctx._source.needs_embed = false;
} else {
  ctx.op = 'noop';
}`

type pending struct {
	ID    string
	Hash  string
	Title string
	Body  string
}

func (w *Worker) defaults() (batch, conc, maxChars int) {
	batch, conc, maxChars = w.Batch, w.Concurrency, w.MaxChars
	if batch <= 0 {
		batch = 32
	}
	if conc <= 0 {
		conc = 4
	}
	if maxChars <= 0 {
		maxChars = 2000
	}
	return batch, conc, maxChars
}

// Pass embeds every document flagged when the pass reaches it, paging in id
// order with search_after so a document written back during the pass is not
// visited twice. It returns the number of vectors written.
func (w *Worker) Pass(ctx context.Context) (int, error) {
	batch, conc, maxChars := w.defaults()
	total := 0
	var after []any
	for {
		page, last, err := w.fetch(ctx, batch*conc, after)
		if err != nil {
			return total, err
		}
		if len(page) == 0 {
			return total, nil
		}
		after = last

		g, gctx := errgroup.WithContext(ctx)
		counts := make([]int, (len(page)+batch-1)/batch)
		for i := 0; i < len(page); i += batch {
			part := page[i:min(i+batch, len(page))]
			slot := i / batch
			g.Go(func() error {
				n, err := w.embedBatch(gctx, part, maxChars)
				counts[slot] = n
				return err
			})
		}
		if err := g.Wait(); err != nil {
			return total, err
		}
		for _, n := range counts {
			total += n
		}
	}
}

func (w *Worker) fetch(ctx context.Context, size int, after []any) ([]pending, []any, error) {
	body := map[string]any{
		"size":    size,
		"_source": []string{"title", "body", "content_hash"},
		"query": map[string]any{"bool": map[string]any{"filter": []any{
			map[string]any{"term": map[string]any{"needs_embed": true}},
			map[string]any{"term": map[string]any{"deleted": false}},
		}}},
		"sort": []any{map[string]any{"id": "asc"}},
	}
	if after != nil {
		body["search_after"] = after
	}
	var resp struct {
		Hits struct {
			Hits []struct {
				ID     string `json:"_id"`
				Sort   []any  `json:"sort"`
				Source struct {
					Title       string `json:"title"`
					Body        string `json:"body"`
					ContentHash string `json:"content_hash"`
				} `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}
	if err := w.OS.Do(ctx, http.MethodPost, "/"+w.Index+"/_search", body, &resp); err != nil {
		return nil, nil, err
	}
	hits := resp.Hits.Hits
	out := make([]pending, 0, len(hits))
	for _, h := range hits {
		out = append(out, pending{ID: h.ID, Hash: h.Source.ContentHash, Title: h.Source.Title, Body: h.Source.Body})
	}
	if len(hits) == 0 {
		return out, nil, nil
	}
	return out, hits[len(hits)-1].Sort, nil
}

// Text is what gets embedded for a document: its title, then the start of
// its body.
func Text(title, body string, maxChars int) string {
	t := title
	if body != "" {
		t += "\n\n" + body
	}
	if r := []rune(t); len(r) > maxChars {
		t = string(r[:maxChars])
	}
	if t == "" {
		t = " " // the model rejects empty input
	}
	return t
}

func (w *Worker) embedBatch(ctx context.Context, docs []pending, maxChars int) (int, error) {
	texts := make([]string, len(docs))
	for i, d := range docs {
		texts[i] = Text(d.Title, d.Body, maxChars)
	}
	vecs, err := w.Model.Embed(ctx, texts)
	if err != nil {
		return 0, err
	}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for i, d := range docs {
		meta := map[string]any{"update": map[string]any{"_index": w.Index, "_id": d.ID, "retry_on_conflict": 3}}
		op := map[string]any{"script": map[string]any{
			"source": setVector, "lang": "painless", "params": map[string]any{"h": d.Hash, "v": vecs[i]},
		}}
		if err := enc.Encode(meta); err != nil {
			return 0, err
		}
		if err := enc.Encode(op); err != nil {
			return 0, err
		}
	}
	resp, err := w.OS.Bulk(ctx, buf.Bytes())
	if err != nil {
		return 0, err
	}
	written := 0
	for _, r := range resp.Results() {
		switch r.Status {
		case http.StatusOK, http.StatusCreated:
			if r.Result != "noop" { // noop: edited since the read, re-embedded next pass
				written++
			}
		case http.StatusNotFound:
			// deleted from the index since the page was read
		default:
			reason := ""
			if r.Error != nil {
				reason = r.Error.Type + ": " + r.Error.Reason
			}
			return written, fmt.Errorf("embed: write vector %s: status %d %s", r.ID, r.Status, reason)
		}
	}
	metrics.Embedded.Add(float64(written))
	return written, nil
}

// Backlog counts documents waiting for an up-to-date embedding.
func (w *Worker) Backlog(ctx context.Context) (int, error) {
	var resp struct {
		Count int `json:"count"`
	}
	body := map[string]any{"query": map[string]any{"bool": map[string]any{"filter": []any{
		map[string]any{"term": map[string]any{"needs_embed": true}},
		map[string]any{"term": map[string]any{"deleted": false}},
	}}}}
	if err := w.OS.Do(ctx, http.MethodPost, "/"+w.Index+"/_count", body, &resp); err != nil {
		return 0, err
	}
	return resp.Count, nil
}

// Run makes passes until ctx ends, sleeping idle between passes that found
// nothing to do. Model or cluster errors are logged and retried next pass.
func (w *Worker) Run(ctx context.Context, idle time.Duration) {
	for ctx.Err() == nil {
		start := time.Now()
		n, err := w.Pass(ctx)
		if err != nil && ctx.Err() == nil {
			slog.Warn("embed pass failed", "embedded", n, "err", err)
		}
		if b, berr := w.Backlog(ctx); berr == nil {
			metrics.EmbedBacklog.Set(float64(b))
			if n > 0 {
				slog.Info("embed pass", "embedded", n, "took", time.Since(start).String(), "backlog", b,
					"per_sec", int(float64(n)/time.Since(start).Seconds()))
			}
		}
		if n == 0 || err != nil {
			select {
			case <-ctx.Done():
			case <-time.After(idle):
			}
		}
	}
}
