//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/khangpt2k6/Slipstream_CDC/internal/model"
)

var (
	apiURL = env("SS_E2E_API", "http://localhost:8080")
	dirURL = env("SS_E2E_DIRECTORY", "http://localhost:8081")
)

func postJSON(t *testing.T, u string, body any) {
	t.Helper()
	b, _ := json.Marshal(body)
	resp, err := http.Post(u, "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode >= 300 {
		t.Fatalf("POST %s: status %d", u, resp.StatusCode)
	}
}

type searchResp struct {
	Hits []struct {
		ID      string   `json:"id"`
		Matched string   `json:"matched"`
		Path    []string `json:"path"`
	} `json:"hits"`
}

func searchAs(t *testing.T, user, q string) searchResp {
	t.Helper()
	resp, err := http.Get(apiURL + "/api/search?mode=bm25&limit=50&as=" + url.QueryEscape(user) + "&q=" + url.QueryEscape(q))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var out searchResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	return out
}

func has(r searchResp, id string) bool {
	for _, h := range r.Hits {
		if h.ID == id {
			return true
		}
	}
	return false
}

// TestSearchRespectsPermissions walks the whole loop through the real
// services: a restricted container, a member who can see it, a contractor who
// cannot, then a revoke that must remove the doc from the member's results.
func TestSearchRespectsPermissions(t *testing.T) {
	s := connect(t)
	ctx := context.Background()
	run := time.Now().UnixNano()
	team := fmt.Sprintf("e2e-team-%d", run)
	container := fmt.Sprintf("slack:Csecret%d", run)
	word := fmt.Sprintf("zebrafish%d", run)
	id := container + "/1"

	postJSON(t, dirURL+"/v1/containers", map[string]string{"id": container, "datasource": "slack", "visibility": "members"})
	postJSON(t, dirURL+"/v1/edges", map[string]any{"changes": []map[string]any{
		{"from": model.User("alice"), "to": model.Group(team), "present": true},
		{"from": model.Group(team), "to": model.Container(container), "present": true},
	}})
	if err := s.prod.Docs(ctx, model.DocEvent{
		Op: model.OpUpsert, Version: 1, SrcTsMs: time.Now().UnixMilli(), Mode: model.ModeLive,
		Doc: model.Document{ID: id, Datasource: "slack", Container: container, Kind: "message",
			Body: "the " + word + " incident review", Allowed: []string{model.Container(container)}},
	}); err != nil {
		t.Fatal(err)
	}

	eventually(t, "alice sees the doc", 20*time.Second, func() (bool, error) {
		return has(searchAs(t, "alice", word), id), nil
	})
	if r := searchAs(t, "dave", word); has(r, id) {
		t.Fatal("contractor dave can see a members-only doc")
	}
	r := searchAs(t, "alice", word)
	if r.Hits[0].Matched != model.Container(container) || len(r.Hits[0].Path) != 3 {
		t.Errorf("explanation = %v via %v, want alice -> team -> container", r.Hits[0].Matched, r.Hits[0].Path)
	}

	start := time.Now()
	postJSON(t, dirURL+"/v1/edges", map[string]any{"changes": []map[string]any{
		{"from": model.User("alice"), "to": model.Group(team), "present": false},
	}})
	eventually(t, "revoke hides the doc", 20*time.Second, func() (bool, error) {
		return !has(searchAs(t, "alice", word), id), nil
	})
	t.Logf("revoke -> invisible in search: %v", time.Since(start))
}
