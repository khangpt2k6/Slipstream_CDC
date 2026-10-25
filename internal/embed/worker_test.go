package embed

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/khangpt2k6/Slipstream_CDC/internal/opensearch"
)

type fakeModel struct{ calls int }

func (m *fakeModel) Embed(_ context.Context, texts []string) ([][]float32, error) {
	m.calls++
	out := make([][]float32, len(texts))
	for i, t := range texts {
		out[i] = []float32{float32(len(t)), 1}
	}
	return out, nil
}

// fakeIndex holds flagged documents and applies the worker's scripted
// updates the way OpenSearch would: only when the hash still matches.
type fakeIndex struct {
	mu   sync.Mutex
	docs map[string]map[string]any // id -> source
	// editDuring changes a document's hash between the read and the write.
	editDuring string
}

func (f *fakeIndex) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch {
	case strings.HasSuffix(r.URL.Path, "/_search"):
		var req struct {
			Size        int   `json:"size"`
			SearchAfter []any `json:"search_after"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		var ids []string
		for id, d := range f.docs {
			if d["needs_embed"] == true {
				ids = append(ids, id)
			}
		}
		sort.Strings(ids)
		var hits []map[string]any
		for _, id := range ids {
			if len(req.SearchAfter) == 1 && id <= req.SearchAfter[0].(string) {
				continue
			}
			if len(hits) == req.Size {
				break
			}
			hits = append(hits, map[string]any{"_id": id, "sort": []any{id}, "_source": f.docs[id]})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"hits": map[string]any{"hits": hits}})
		if f.editDuring != "" {
			f.docs[f.editDuring]["content_hash"] = "edited"
			f.editDuring = ""
		}
	case r.URL.Path == "/_bulk":
		body, _ := io.ReadAll(r.Body)
		sc := bufio.NewScanner(bytes.NewReader(body))
		sc.Buffer(make([]byte, 1<<20), 1<<20)
		var items []map[string]any
		for sc.Scan() {
			var meta struct {
				Update struct {
					ID string `json:"_id"`
				} `json:"update"`
			}
			_ = json.Unmarshal(sc.Bytes(), &meta)
			sc.Scan()
			var op struct {
				Script struct {
					Params struct {
						H string    `json:"h"`
						V []float32 `json:"v"`
					} `json:"params"`
				} `json:"script"`
			}
			_ = json.Unmarshal(sc.Bytes(), &op)
			d, ok := f.docs[meta.Update.ID]
			status, result := 200, "noop"
			switch {
			case !ok:
				status, result = 404, ""
			case d["content_hash"] == op.Script.Params.H:
				d["embedding"], d["needs_embed"] = op.Script.Params.V, false
				result = "updated"
			}
			items = append(items, map[string]any{"update": map[string]any{"_id": meta.Update.ID, "status": status, "result": result}})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"items": items})
	default:
		http.NotFound(w, r)
	}
}

func TestPassEmbedsFlaggedDocsAndSkipsEditedOnes(t *testing.T) {
	f := &fakeIndex{docs: map[string]map[string]any{}, editDuring: "d03"}
	for _, id := range []string{"d01", "d02", "d03", "d04", "d05", "d06", "d07"} {
		f.docs[id] = map[string]any{"title": "t " + id, "body": "b", "content_hash": "h-" + id, "needs_embed": true}
	}
	f.docs["d08"] = map[string]any{"title": "done", "content_hash": "x", "needs_embed": false}
	srv := httptest.NewServer(f)
	defer srv.Close()

	m := &fakeModel{}
	w := &Worker{OS: opensearch.New(srv.URL, 5*time.Second), Index: "ss-docs", Model: m, Batch: 2, Concurrency: 2}
	n, err := w.Pass(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 6 {
		t.Errorf("first pass wrote %d vectors, want 6 (the edited doc is a noop)", n)
	}
	for id, d := range f.docs {
		switch id {
		case "d03":
			if d["needs_embed"] != true || d["embedding"] != nil {
				t.Errorf("d03 was edited mid-flight but got a vector for its old text: %v", d)
			}
		case "d08":
			if d["embedding"] != nil {
				t.Error("unflagged doc re-embedded")
			}
		default:
			if d["needs_embed"] != false || d["embedding"] == nil {
				t.Errorf("%s not embedded: %v", id, d)
			}
		}
	}
	if m.calls != 4 {
		t.Errorf("model calls = %d, want 4 batches of <=2 over 7 docs", m.calls)
	}

	// The next pass picks up the edited document, now with its new text.
	if n, err := w.Pass(context.Background()); err != nil || n != 1 {
		t.Errorf("second pass embedded %d (err %v), want 1", n, err)
	}
}

func TestText(t *testing.T) {
	if got := Text("Title", "body text", 8); got != "Title\n\nb" {
		t.Errorf("Text = %q", got)
	}
	if got := Text("", "", 10); got != " " {
		t.Errorf("empty Text = %q", got)
	}
	if got := Text("héllo wörld", "", 5); got != "héllo" {
		t.Errorf("rune truncation = %q", got)
	}
}
