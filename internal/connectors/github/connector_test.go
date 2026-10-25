package github

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/khangpt2k6/Slipstream_CDC/internal/connectors"
	"github.com/khangpt2k6/Slipstream_CDC/internal/model"
)

type capture struct {
	mu  sync.Mutex
	evs []model.DocEvent
}

func (c *capture) Docs(_ context.Context, evs ...model.DocEvent) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.evs = append(c.evs, evs...)
	return nil
}

func (c *capture) all() []model.DocEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]model.DocEvent(nil), c.evs...)
}

func newConnector(t *testing.T) (*Connector, *capture) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	out := &capture{}
	return &Connector{
		Out:    &connectors.Emitter{Prod: out, Source: "github"},
		RDB:    rdb,
		Repos:  []string{"golang/go"},
		Secret: "s3cret",
		Now:    func() time.Time { return time.Unix(1_800_000_000, 0) },
	}, out
}

func deliver(t *testing.T, c *Connector, event, delivery string, payload any, secret string) int {
	t.Helper()
	body, _ := json.Marshal(payload)
	req := httptest.NewRequest(http.MethodPost, "/github/webhook", bytes.NewReader(body))
	req.Header.Set("X-GitHub-Event", event)
	req.Header.Set("X-GitHub-Delivery", delivery)
	req.Header.Set("X-Hub-Signature-256", Sign(secret, body))
	rec := httptest.NewRecorder()
	c.ServeHTTP(rec, req)
	return rec.Code
}

func issuePayload(action string, n int, updated time.Time) map[string]any {
	return map[string]any{
		"action":     action,
		"repository": map[string]any{"full_name": "golang/go"},
		"issue": map[string]any{
			"number": n, "title": "net/http: flaky test", "body": "it fails", "state": "open",
			"html_url": fmt.Sprintf("https://github.com/golang/go/issues/%d", n), "user": map[string]any{"login": "gopher"},
			"labels":     []map[string]any{{"name": "NeedsInvestigation"}},
			"created_at": updated.Add(-time.Hour).Format(time.RFC3339), "updated_at": updated.Format(time.RFC3339),
		},
	}
}

func TestWebhookRejectsBadSignature(t *testing.T) {
	c, out := newConnector(t)
	if code := deliver(t, c, "issues", "d1", issuePayload("opened", 1, time.Unix(1_700_000_000, 0)), "wrong"); code != http.StatusUnauthorized {
		t.Fatalf("status %d, want 401", code)
	}
	if len(out.all()) != 0 {
		t.Error("unsigned delivery produced events")
	}
}

func TestWebhookDedupsRedeliveries(t *testing.T) {
	c, out := newConnector(t)
	p := issuePayload("edited", 7, time.Unix(1_700_000_000, 0))
	for range 3 {
		if code := deliver(t, c, "issues", "same-delivery", p, "s3cret"); code != http.StatusOK {
			t.Fatalf("status %d", code)
		}
	}
	evs := out.all()
	if len(evs) != 1 {
		t.Fatalf("got %d events for 3 deliveries of one id, want 1", len(evs))
	}
	e := evs[0]
	if e.Doc.ID != "github:golang/go#7" || e.Doc.Kind != "issue" || e.Version != 1_700_000_000_000 || e.Mode != model.ModeLive {
		t.Errorf("event = %+v", e)
	}
	if err := e.Validate(); err != nil {
		t.Error(err)
	}
}

func TestWebhookDeleteOutranksLastUpdate(t *testing.T) {
	c, out := newConnector(t)
	// GitHub's clock is ahead of ours: the delete must still beat the update.
	updated := c.now().Add(time.Minute)
	deliver(t, c, "issues", "d1", issuePayload("edited", 9, updated), "s3cret")
	deliver(t, c, "issues", "d2", issuePayload("deleted", 9, updated), "s3cret")
	evs := out.all()
	if len(evs) != 2 || evs[1].Op != model.OpDelete {
		t.Fatalf("events = %+v", evs)
	}
	if evs[1].Version <= evs[0].Version {
		t.Errorf("delete version %d not after update %d", evs[1].Version, evs[0].Version)
	}
}

func TestWebhookIgnoresOtherRepos(t *testing.T) {
	c, out := newConnector(t)
	p := issuePayload("opened", 1, time.Unix(1_700_000_000, 0))
	p["repository"] = map[string]any{"full_name": "someone/else"}
	if code := deliver(t, c, "issues", "d1", p, "s3cret"); code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	if len(out.all()) != 0 {
		t.Error("event for an unsynced repo")
	}
}

func TestCommentMappingAndDuplicates(t *testing.T) {
	c, _ := newConnector(t)
	cm := Comment{
		ID: 99, Body: "Thanks.\n\nDuplicate of #4321\n", IssueURL: "https://api.github.com/repos/golang/go/issues/1234",
		User: User{Login: "gopherbot"}, UpdatedAt: time.Unix(1_700_000_000, 0),
	}
	e := c.commentEvent("golang/go", cm, "x/tools: crash", model.ModeBackfill)
	if e.Doc.ID != "github:golang/go#1234/c99" || e.Doc.ParentID != "github:golang/go#1234" {
		t.Errorf("ids = %s parent %s", e.Doc.ID, e.Doc.ParentID)
	}
	if e.Doc.Title != "Re: x/tools: crash" {
		t.Errorf("title = %q", e.Doc.Title)
	}
	if len(e.Doc.Duplicates) != 1 || e.Doc.Duplicates[0] != "github:golang/go#4321" {
		t.Errorf("duplicates = %v", e.Doc.Duplicates)
	}
	if got := c.commentEvent("golang/go", Comment{Body: "not a duplicate of #1", IssueURL: "x/issues/2"}, "", model.ModeLive); len(got.Doc.Duplicates) != 0 {
		t.Errorf("mid-sentence mention parsed as duplicate: %v", got.Doc.Duplicates)
	}
}

// fakeAPI serves a sorted issue list with cursor pagination, refusing any
// page past maxPage the way GitHub refuses deep offsets.
type fakeAPI struct {
	issues  []Issue
	maxPage int
	calls   int
	etag    string
}

func (f *fakeAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.calls++
	q := r.URL.Query()
	if f.etag != "" && r.Header.Get("If-None-Match") == f.etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	since := time.Time{}
	if s := q.Get("since"); s != "" {
		since, _ = time.Parse(time.RFC3339, s)
	}
	var match []Issue
	for _, is := range f.issues {
		if !is.UpdatedAt.Before(since) {
			match = append(match, is)
		}
	}
	page, _ := strconv.Atoi(q.Get("page"))
	page = max(page, 1)
	if page > f.maxPage {
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = w.Write([]byte(`{"message":"Pagination with the page parameter is not supported for large datasets"}`))
		return
	}
	per, _ := strconv.Atoi(q.Get("per_page"))
	lo, hi := min((page-1)*per, len(match)), min(page*per, len(match))
	if hi < len(match) {
		q.Set("page", strconv.Itoa(page+1))
		w.Header().Set("Link", fmt.Sprintf(`<http://%s%s?%s>; rel="next"`, r.Host, r.URL.Path, q.Encode()))
	}
	if f.etag != "" {
		w.Header().Set("ETag", f.etag)
	}
	_ = json.NewEncoder(w).Encode(match[lo:hi])
}

func TestCrawlRestartsPastDeepPagesAndResumes(t *testing.T) {
	c, out := newConnector(t)
	f := &fakeAPI{maxPage: 3}
	base := time.Unix(1_700_000_000, 0).UTC()
	for i := range 1050 {
		f.issues = append(f.issues, Issue{Number: i + 1, Title: "t", UpdatedAt: base.Add(time.Duration(i) * time.Second)})
	}
	srv := httptest.NewServer(f)
	defer srv.Close()
	c.API = &Client{Base: srv.URL}

	n, err := c.Crawl(context.Background(), model.ModeBackfill)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, e := range out.all() {
		seen[e.Doc.ID] = true
	}
	if len(seen) != 1050 {
		t.Fatalf("crawled %d distinct issues (%d events), want 1050", len(seen), n)
	}

	// A poll with nothing new only re-reads the cursor's own second.
	f.issues = append(f.issues, Issue{Number: 2000, Title: "new", UpdatedAt: base.Add(time.Hour)})
	before := len(out.all())
	if _, err := c.Crawl(context.Background(), model.ModeLive); err != nil {
		t.Fatal(err)
	}
	got := out.all()[before:]
	if len(got) != 2 || got[1].Doc.ID != "github:golang/go#2000" {
		t.Errorf("incremental poll emitted %d events: %+v", len(got), got)
	}
}

func TestPollUnchangedCostsNothing(t *testing.T) {
	c, out := newConnector(t)
	f := &fakeAPI{maxPage: 10, etag: `"v1"`, issues: []Issue{{Number: 1, UpdatedAt: time.Unix(1_700_000_000, 0)}}}
	srv := httptest.NewServer(f)
	defer srv.Close()
	c.API = &Client{Base: srv.URL}
	for range 3 {
		if _, err := c.Crawl(context.Background(), model.ModeLive); err != nil {
			t.Fatal(err)
		}
	}
	// First crawl fetches; the cursor then pins the URL and the ETag matches.
	if n := len(out.all()); n != 2 {
		t.Errorf("emitted %d events over 3 polls, want 2 (first crawl + first poll at the cursor)", n)
	}
}

func TestRateLimitWait(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	h := http.Header{}
	h.Set("Retry-After", "7")
	if d := rateLimitWait(h, now); d != 7*time.Second {
		t.Errorf("retry-after wait = %v", d)
	}
	h = http.Header{}
	h.Set("X-RateLimit-Remaining", "0")
	h.Set("X-RateLimit-Reset", strconv.FormatInt(now.Add(90*time.Second).Unix(), 10))
	if d := rateLimitWait(h, now); d != 91*time.Second {
		t.Errorf("reset wait = %v", d)
	}
}

func TestNextLink(t *testing.T) {
	h := `<https://api.github.com/repositories/1/issues?after=abc&page=2>; rel="next", <https://x/last>; rel="last"`
	if got := nextLink(h); !strings.Contains(got, "after=abc") {
		t.Errorf("next = %q", got)
	}
	if nextLink(`<https://x/prev>; rel="prev"`) != "" {
		t.Error("prev parsed as next")
	}
}
