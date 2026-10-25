package jira

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sort"
	"strconv"
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

type fakeIssue struct {
	key      string
	updated  time.Time
	comments []string // comment ids
	dupOf    string
}

// fakeJira answers JQL searches of the form the connector sends: a project,
// an optional `updated >= "yyyy/MM/dd HH:mm"` in UTC, ordered by updated.
type fakeJira struct {
	mu       sync.Mutex
	issues   []fakeIssue
	searches []string
}

var updatedClause = regexp.MustCompile(`updated >= "([^"]+)"`)

func (f *fakeJira) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	q := r.URL.Query()
	jql := q.Get("jql")
	f.searches = append(f.searches, jql)
	from := time.Time{}
	if m := updatedClause.FindStringSubmatch(jql); m != nil {
		from, _ = time.ParseInLocation(jqlTime, m[1], time.UTC)
	}
	var match []fakeIssue
	for _, is := range f.issues {
		if !is.updated.Before(from) {
			match = append(match, is)
		}
	}
	sort.Slice(match, func(i, j int) bool {
		if !match[i].updated.Equal(match[j].updated) {
			return match[i].updated.Before(match[j].updated)
		}
		return match[i].key < match[j].key
	})
	start, _ := strconv.Atoi(q.Get("startAt"))
	n, _ := strconv.Atoi(q.Get("maxResults"))
	lo, hi := min(start, len(match)), min(start+n, len(match))
	out := make([]map[string]any, 0, hi-lo)
	for _, is := range match[lo:hi] {
		ts := is.updated.Format("2006-01-02T15:04:05.000-0700")
		var cms []map[string]any
		for _, id := range is.comments {
			cms = append(cms, map[string]any{"id": id, "body": "comment " + id, "author": map[string]any{"displayName": "Ann"}, "created": ts, "updated": ts})
		}
		var links []map[string]any
		if is.dupOf != "" {
			links = append(links, map[string]any{"type": map[string]any{"name": "Duplicate"}, "outwardIssue": map[string]any{"key": is.dupOf}})
		}
		out = append(out, map[string]any{"key": is.key, "fields": map[string]any{
			"summary": "summary of " + is.key, "description": "desc", "labels": []string{"kraft"},
			"issuetype": map[string]any{"name": "Bug"}, "status": map[string]any{"name": "Open"},
			"reporter": map[string]any{"displayName": "Reporter"}, "created": ts, "updated": ts,
			"issuelinks": links, "comment": map[string]any{"comments": cms, "total": len(cms)},
		}})
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"total": len(match), "issues": out})
}

func newConnector(t *testing.T, f *fakeJira) (*Connector, *capture) {
	t.Helper()
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	out := &capture{}
	return &Connector{
		API:      &Client{Base: srv.URL},
		Out:      &connectors.Emitter{Prod: out, Source: "jira"},
		RDB:      rdb,
		Projects: []string{"KAFKA"},
		PageSize: 10,
		Overlap:  2 * time.Minute,
		Comments: true,
		Now:      func() time.Time { return time.Unix(1_800_000_000, 0) },
	}, out
}

func distinct(evs []model.DocEvent, kind string) map[string]bool {
	out := map[string]bool{}
	for _, e := range evs {
		if e.Doc.Kind == kind || kind == "" {
			out[e.Doc.ID] = true
		}
	}
	return out
}

func TestCrawlReanchorsAndSurvivesBulkMinute(t *testing.T) {
	base := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	f := &fakeJira{}
	for i := range 95 {
		// 35 issues share one minute, as after a bulk edit; the rest spread.
		at := base.Add(time.Duration(i) * time.Minute)
		if i >= 30 && i < 65 {
			at = base.Add(30*time.Minute + time.Duration(i)*time.Second%time.Minute)
		}
		f.issues = append(f.issues, fakeIssue{key: fmt.Sprintf("KAFKA-%d", i+1), updated: at})
	}
	c, out := newConnector(t, f)
	if _, err := c.Crawl(context.Background(), model.ModeBackfill); err != nil {
		t.Fatal(err)
	}
	if got := distinct(out.all(), "issue"); len(got) != 95 {
		t.Fatalf("crawled %d distinct issues, want 95", len(got))
	}
}

func TestIssueEditedMidCrawlIsNotSkipped(t *testing.T) {
	base := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	f := &fakeJira{}
	for i := range 40 {
		f.issues = append(f.issues, fakeIssue{key: fmt.Sprintf("KAFKA-%d", i+1), updated: base.Add(time.Duration(i) * time.Hour)})
	}
	c, out := newConnector(t, f)
	// After the first page, an already-read issue is edited and jumps to the
	// end of the result set. With offset paging that shifts everything left
	// and skips one issue; re-anchoring does not.
	c.Out = &connectors.Emitter{Prod: &editingProducer{capture: out, f: f}, Source: "jira"}
	if _, err := c.Crawl(context.Background(), model.ModeBackfill); err != nil {
		t.Fatal(err)
	}
	if got := distinct(out.all(), "issue"); len(got) != 40 {
		t.Fatalf("crawled %d distinct issues, want 40", len(got))
	}
}

type editingProducer struct {
	*capture
	f    *fakeJira
	done bool
}

func (p *editingProducer) Docs(ctx context.Context, evs ...model.DocEvent) error {
	if !p.done {
		p.done = true
		p.f.mu.Lock()
		p.f.issues[0].updated = time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
		p.f.mu.Unlock()
	}
	return p.capture.Docs(ctx, evs...)
}

func TestPollUsesOverlapAndDeletesVanishedComments(t *testing.T) {
	base := time.Date(2020, 1, 1, 12, 0, 0, 0, time.UTC)
	f := &fakeJira{issues: []fakeIssue{{key: "KAFKA-1", updated: base, comments: []string{"100", "101"}, dupOf: "KAFKA-9"}}}
	c, out := newConnector(t, f)
	if _, err := c.Crawl(context.Background(), model.ModeBackfill); err != nil {
		t.Fatal(err)
	}
	first := out.all()
	if len(first) != 3 {
		t.Fatalf("backfill emitted %d events, want issue + 2 comments", len(first))
	}
	is := first[0]
	if is.Doc.ID != "jira:KAFKA-1" || is.Doc.Container != "jira:KAFKA" || is.Version != base.UnixMilli() {
		t.Errorf("issue event = %+v", is)
	}
	if len(is.Doc.Duplicates) != 1 || is.Doc.Duplicates[0] != "jira:KAFKA-9" {
		t.Errorf("duplicates = %v", is.Doc.Duplicates)
	}
	if first[1].Doc.ParentID != "jira:KAFKA-1" || first[1].Doc.ID != "jira:KAFKA-1/c100" {
		t.Errorf("comment event = %+v", first[1].Doc)
	}
	for _, e := range first {
		if err := e.Validate(); err != nil {
			t.Error(err)
		}
	}

	// Comment 101 is deleted, which bumps the issue.
	f.mu.Lock()
	f.issues[0].comments = []string{"100"}
	f.issues[0].updated = base.Add(time.Minute)
	f.mu.Unlock()
	if _, err := c.Crawl(context.Background(), model.ModeLive); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	last := f.searches[len(f.searches)-1]
	f.mu.Unlock()
	if want := `updated >= "2020/01/01 11:58"`; !regexp.MustCompile(regexp.QuoteMeta(want)).MatchString(last) {
		t.Errorf("poll jql %q does not start one overlap before the cursor", last)
	}
	var del *model.DocEvent
	for _, e := range out.all()[3:] {
		if e.Op == model.OpDelete {
			del = &e
		}
	}
	if del == nil || del.Doc.ID != "jira:KAFKA-1/c101" {
		t.Fatalf("no delete for the vanished comment: %+v", out.all()[3:])
	}
	if del.Version <= base.Add(time.Minute).UnixMilli() {
		t.Errorf("delete version %d does not outrank the issue update", del.Version)
	}
}

func TestTimeParsing(t *testing.T) {
	var v Time
	if err := json.Unmarshal([]byte(`"2011-07-19T21:32:10.000+0000"`), &v); err != nil {
		t.Fatal(err)
	}
	if v.UTC() != time.Date(2011, 7, 19, 21, 32, 10, 0, time.UTC) {
		t.Errorf("parsed %v", v)
	}
	if err := json.Unmarshal([]byte(`null`), &v); err != nil {
		t.Error(err)
	}
}

func TestSyncRejectsBadProjectKey(t *testing.T) {
	c := &Connector{Projects: []string{`KAFKA" OR project = "X`}}
	if err := c.Sync(context.Background()); err == nil {
		t.Error("JQL-breaking project key accepted")
	}
}
