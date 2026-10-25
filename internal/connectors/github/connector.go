package github

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/khangpt2k6/Slipstream_CDC/internal/connectors"
	"github.com/khangpt2k6/Slipstream_CDC/internal/dirclient"
	"github.com/khangpt2k6/Slipstream_CDC/internal/metrics"
	"github.com/khangpt2k6/Slipstream_CDC/internal/model"
)

// Connector syncs a set of GitHub repositories.
type Connector struct {
	API    *Client
	Dir    *dirclient.Client
	Out    *connectors.Emitter
	RDB    *redis.Client
	Repos  []string // owner/name
	Secret string   // webhook secret
	// Comments turns on comment crawling. CommentsSince bounds how far back
	// the first comment crawl reaches (zero means all of history); busy repos
	// have several comments per issue.
	Comments      bool
	CommentsSince time.Time
	Now           func() time.Time
}

const (
	cursorKey   = "ss:cursor:github:"
	titlesKey   = "ss:github:titles:"
	dedupPrefix = "ss:dedup:github:"
	dedupTTL    = 24 * time.Hour
)

// Sync registers every repository as a container. The directory applies its
// sharing policy the first time it sees one.
func (c *Connector) Sync(ctx context.Context) error {
	for _, repo := range c.Repos {
		if err := c.Dir.RegisterContainer(ctx, containerID(repo), "github", repo, ""); err != nil {
			return err
		}
	}
	return nil
}

// Crawl emits every issue, pull request and (optionally) comment updated
// since each repo's stored cursor, advancing the cursor after every page so
// a restart resumes where it stopped. The first run is the full backfill;
// later runs are cheap incremental polls (an unchanged repo answers 304).
func (c *Connector) Crawl(ctx context.Context, mode model.Mode) (int, error) {
	total := 0
	for _, repo := range c.Repos {
		n, err := c.crawlIssues(ctx, repo, mode)
		total += n
		if err != nil {
			return total, err
		}
		if !c.Comments {
			continue
		}
		n, err = c.crawlComments(ctx, repo, mode)
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func (c *Connector) cursor(ctx context.Context, key string, floor time.Time) (time.Time, error) {
	raw, err := c.RDB.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return floor, nil
	}
	if err != nil {
		return time.Time{}, err
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return floor, nil // unreadable cursor: start over, versions dedupe
	}
	if t.Before(floor) {
		return floor, nil
	}
	return t, nil
}

func (c *Connector) crawlIssues(ctx context.Context, repo string, mode model.Mode) (int, error) {
	key := cursorKey + repo + ":issues"
	since, err := c.cursor(ctx, key, time.Time{})
	if err != nil {
		return 0, err
	}
	n := 0
	err = c.API.Issues(ctx, repo, since, func(issues []Issue) error {
		evs := make([]model.DocEvent, 0, len(issues))
		titles := make(map[string]any, len(issues))
		for _, is := range issues {
			evs = append(evs, c.issueEvent(repo, is, mode))
			titles[strconv.Itoa(is.Number)] = is.Title
		}
		if err := c.RDB.HSet(ctx, titlesKey+repo, titles).Err(); err != nil {
			return err
		}
		if err := c.Out.Emit(ctx, evs...); err != nil {
			return err
		}
		n += len(evs)
		return c.RDB.Set(ctx, key, issues[len(issues)-1].UpdatedAt.UTC().Format(time.RFC3339), 0).Err()
	})
	return n, err
}

func (c *Connector) crawlComments(ctx context.Context, repo string, mode model.Mode) (int, error) {
	key := cursorKey + repo + ":comments"
	since, err := c.cursor(ctx, key, c.CommentsSince)
	if err != nil {
		return 0, err
	}
	n := 0
	err = c.API.Comments(ctx, repo, since, func(cms []Comment) error {
		nums := make([]string, len(cms))
		for i, cm := range cms {
			nums[i] = strconv.Itoa(issueNumber(cm.IssueURL))
		}
		titles, err := c.RDB.HMGet(ctx, titlesKey+repo, nums...).Result()
		if err != nil {
			return err
		}
		evs := make([]model.DocEvent, 0, len(cms))
		for i, cm := range cms {
			title, _ := titles[i].(string)
			evs = append(evs, c.commentEvent(repo, cm, title, mode))
		}
		if err := c.Out.Emit(ctx, evs...); err != nil {
			return err
		}
		n += len(evs)
		return c.RDB.Set(ctx, key, cms[len(cms)-1].UpdatedAt.UTC().Format(time.RFC3339), 0).Err()
	})
	return n, err
}

// ServeHTTP is the webhook endpoint (content type application/json).
func (c *Connector) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 25<<20))
	if err != nil {
		http.Error(w, "body too large", http.StatusRequestEntityTooLarge)
		return
	}
	if err := Verify(c.Secret, r.Header.Get("X-Hub-Signature-256"), body); err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	event := r.Header.Get("X-GitHub-Event")
	if event == "ping" {
		w.WriteHeader(http.StatusOK)
		return
	}

	// GitHub redelivers on timeouts and on manual redelivery; the delivery
	// id is stable across both. The key is released if handling fails so a
	// redelivery is processed.
	key := dedupPrefix + r.Header.Get("X-GitHub-Delivery")
	fresh, err := c.RDB.SetNX(r.Context(), key, 1, dedupTTL).Result()
	if err != nil {
		http.Error(w, "dedup unavailable", http.StatusServiceUnavailable)
		return
	}
	if !fresh {
		metrics.DeliveriesDeduped.WithLabelValues("github").Inc()
		w.WriteHeader(http.StatusOK)
		return
	}
	evs, err := c.webhookEvents(event, body)
	if err == nil {
		err = c.Out.Emit(r.Context(), evs...)
	}
	if err != nil {
		_ = c.RDB.Del(context.WithoutCancel(r.Context()), key).Err()
		slog.Warn("github webhook failed", "event", event, "delivery", r.Header.Get("X-GitHub-Delivery"), "err", err)
		http.Error(w, "retry later", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

type hook struct {
	Action     string `json:"action"`
	Repository struct {
		FullName string `json:"full_name"`
	} `json:"repository"`
	Issue       *Issue   `json:"issue"`
	PullRequest *Issue   `json:"pull_request"`
	Comment     *Comment `json:"comment"`
}

// webhookEvents maps one webhook delivery to document events. Deliveries for
// repos this connector does not sync, and event types it does not index,
// map to nothing.
func (c *Connector) webhookEvents(event string, body []byte) ([]model.DocEvent, error) {
	var h hook
	if err := json.Unmarshal(body, &h); err != nil {
		return nil, nil // malformed payload: a redelivery would not help
	}
	repo := h.Repository.FullName
	if !slices.Contains(c.Repos, repo) {
		return nil, nil
	}
	switch {
	case event == "issues" && h.Issue != nil:
		if h.Action == "deleted" || h.Action == "transferred" {
			return []model.DocEvent{c.deleteEvent(repo, issueID(repo, h.Issue.Number), h.Issue.UpdatedAt)}, nil
		}
		return []model.DocEvent{c.issueEvent(repo, *h.Issue, model.ModeLive)}, nil
	case event == "pull_request" && h.PullRequest != nil:
		pr := *h.PullRequest
		pr.PullRequest = &struct {
			URL string `json:"url"`
		}{}
		return []model.DocEvent{c.issueEvent(repo, pr, model.ModeLive)}, nil
	case event == "issue_comment" && h.Comment != nil && h.Issue != nil:
		if h.Action == "deleted" {
			return []model.DocEvent{c.deleteEvent(repo, commentID(repo, h.Issue.Number, h.Comment.ID), h.Comment.UpdatedAt)}, nil
		}
		return []model.DocEvent{c.commentEvent(repo, *h.Comment, h.Issue.Title, model.ModeLive)}, nil
	}
	return nil, nil
}

// issueEvent maps an issue or pull request. The version is its updated time
// in milliseconds; GitHub bumps it on every edit, label or state change.
func (c *Connector) issueEvent(repo string, is Issue, mode model.Mode) model.DocEvent {
	kind := "issue"
	if is.PullRequest != nil {
		kind = "pull_request"
	}
	labels := make([]string, 0, len(is.Labels)+1)
	for _, l := range is.Labels {
		labels = append(labels, l.Name)
	}
	if is.State != "" {
		labels = append(labels, "state:"+is.State)
	}
	v := is.UpdatedAt.UnixMilli()
	return model.DocEvent{
		Op: model.OpUpsert, Version: v, SrcTsMs: v, EmitTsMs: c.now().UnixMilli(), Mode: mode,
		Doc: model.Document{
			ID:         issueID(repo, is.Number),
			Datasource: "github",
			Container:  containerID(repo),
			Kind:       kind,
			Title:      is.Title,
			Body:       is.Body,
			URL:        is.HTMLURL,
			Author:     is.User.Login,
			Labels:     labels,
			CreatedAt:  is.CreatedAt.UTC(),
			UpdatedAt:  is.UpdatedAt.UTC(),
			Allowed:    []string{model.Container(containerID(repo))},
		},
	}
}

// commentEvent maps a comment. title is the parent issue's title, if known.
func (c *Connector) commentEvent(repo string, cm Comment, title string, mode model.Mode) model.DocEvent {
	num := issueNumber(cm.IssueURL)
	if title == "" {
		title = fmt.Sprintf("%s#%d", repo, num)
	}
	var dups []string
	for _, m := range duplicateOf.FindAllStringSubmatch(cm.Body, -1) {
		if n, err := strconv.Atoi(m[1]); err == nil && n != num {
			dups = append(dups, issueID(repo, n))
		}
	}
	v := cm.UpdatedAt.UnixMilli()
	return model.DocEvent{
		Op: model.OpUpsert, Version: v, SrcTsMs: v, EmitTsMs: c.now().UnixMilli(), Mode: mode,
		Doc: model.Document{
			ID:         commentID(repo, num, cm.ID),
			Datasource: "github",
			Container:  containerID(repo),
			Kind:       "comment",
			Title:      "Re: " + title,
			Body:       cm.Body,
			URL:        cm.HTMLURL,
			Author:     cm.User.Login,
			ParentID:   issueID(repo, num),
			Duplicates: dups,
			CreatedAt:  cm.CreatedAt.UTC(),
			UpdatedAt:  cm.UpdatedAt.UTC(),
			Allowed:    []string{model.Container(containerID(repo))},
		},
	}
}

// deleteEvent removes a document. A deletion has no updated time of its own,
// so its version is the later of now and just after the last update, which
// keeps it ahead of every version the document carried.
func (c *Connector) deleteEvent(repo, id string, lastUpdate time.Time) model.DocEvent {
	now := c.now().UnixMilli()
	v := max(now, lastUpdate.UnixMilli()+1)
	return model.DocEvent{
		Op: model.OpDelete, Version: v, SrcTsMs: now, EmitTsMs: now, Mode: model.ModeLive,
		Doc: model.Document{ID: id, Datasource: "github", Container: containerID(repo)},
	}
}

func (c *Connector) now() time.Time {
	if c.Now != nil {
		return c.Now()
	}
	return time.Now()
}

// Verify checks a webhook's X-Hub-Signature-256 header.
func Verify(secret, sig string, body []byte) error {
	if secret == "" {
		return errors.New("github: webhook secret not configured")
	}
	if !hmac.Equal([]byte(Sign(secret, body)), []byte(sig)) {
		return errors.New("github: bad signature")
	}
	return nil
}

// Sign returns the X-Hub-Signature-256 value GitHub would send for body.
func Sign(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

// duplicateOf matches GitHub's duplicate convention, a line that starts with
// "Duplicate of #N" (the form gopherbot and the triage UI both use).
var duplicateOf = regexp.MustCompile(`(?im)^\s*duplicate of\s+#(\d+)\b`)

func containerID(repo string) string { return "github:" + repo }

func issueID(repo string, n int) string { return fmt.Sprintf("github:%s#%d", repo, n) }

func commentID(repo string, n int, id int64) string {
	return fmt.Sprintf("github:%s#%d/c%d", repo, n, id)
}

// issueNumber reads the number off an issue API URL (".../issues/123").
func issueNumber(u string) int {
	n, _ := strconv.Atoi(u[strings.LastIndexByte(u, '/')+1:])
	return n
}
