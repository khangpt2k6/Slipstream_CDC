// Package github is the GitHub connector: a REST client that crawls issues,
// pull requests and comments in updated order, a webhook receiver for live
// changes, and the mapping from both to Slipstream documents.
package github

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Client is a minimal GitHub REST client. It follows Link pagination, waits
// out primary and secondary rate limits, and sends conditional requests for
// the first page of a poll, so an unchanged repo costs no rate limit.
type Client struct {
	Base  string // https://api.github.com
	Token string
	HTTP  *http.Client

	mu    sync.Mutex
	etags map[string]etagged
}

type etagged struct {
	etag string
	body []byte
	next string
}

// User is the subset of a GitHub account the connector reads.
type User struct {
	Login string `json:"login"`
}

// Issue is an issue or pull request from the issues API. PullRequest is
// non-nil for pull requests.
type Issue struct {
	Number      int       `json:"number"`
	Title       string    `json:"title"`
	Body        string    `json:"body"`
	State       string    `json:"state"`
	StateReason string    `json:"state_reason"`
	HTMLURL     string    `json:"html_url"`
	User        User      `json:"user"`
	Labels      []Label   `json:"labels"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	PullRequest *struct {
		URL string `json:"url"`
	} `json:"pull_request,omitempty"`
}

// Label is an issue label.
type Label struct {
	Name string `json:"name"`
}

// Comment is an issue or pull request conversation comment.
type Comment struct {
	ID        int64     `json:"id"`
	Body      string    `json:"body"`
	HTMLURL   string    `json:"html_url"`
	IssueURL  string    `json:"issue_url"`
	User      User      `json:"user"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// errDeepPage is GitHub refusing a page offset that is too deep. The caller
// restarts the listing from its updated-at cursor instead.
var errDeepPage = errors.New("github: page too deep; restart from cursor")

// errNotModified is a conditional request that matched the cached ETag.
var errNotModified = errors.New("github: not modified")

func (c *Client) get(ctx context.Context, rawURL string, conditional bool) ([]byte, string, error) {
	hc := c.HTTP
	if hc == nil {
		hc = &http.Client{Timeout: 60 * time.Second}
	}
	for attempt := 0; ; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
		if err != nil {
			return nil, "", err
		}
		req.Header.Set("Accept", "application/vnd.github+json")
		req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
		if c.Token != "" {
			req.Header.Set("Authorization", "Bearer "+c.Token)
		}
		var cached etagged
		if conditional {
			c.mu.Lock()
			cached = c.etags[rawURL]
			c.mu.Unlock()
			if cached.etag != "" {
				req.Header.Set("If-None-Match", cached.etag)
			}
		}
		resp, err := hc.Do(req) // #nosec G704 -- GitHub base URL is operator config
		if err != nil {
			if attempt < 5 && ctx.Err() == nil {
				if werr := sleep(ctx, time.Duration(attempt+1)*2*time.Second); werr != nil {
					return nil, "", werr
				}
				continue
			}
			return nil, "", fmt.Errorf("github: GET %s: %w", rawURL, err)
		}
		body, rerr := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
		_ = resp.Body.Close()
		if rerr != nil {
			return nil, "", fmt.Errorf("github: read %s: %w", rawURL, rerr)
		}
		next := nextLink(resp.Header.Get("Link"))

		switch {
		case resp.StatusCode == http.StatusNotModified:
			return cached.body, cached.next, errNotModified
		case resp.StatusCode == http.StatusOK:
			if conditional {
				if et := resp.Header.Get("ETag"); et != "" {
					c.mu.Lock()
					if c.etags == nil {
						c.etags = map[string]etagged{}
					}
					c.etags[rawURL] = etagged{etag: et, body: body, next: next}
					c.mu.Unlock()
				}
			}
			return body, next, nil
		case resp.StatusCode == http.StatusUnprocessableEntity && strings.Contains(string(body), "Pagination"):
			return nil, "", errDeepPage
		case (resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusTooManyRequests) && attempt < 10:
			if werr := sleep(ctx, rateLimitWait(resp.Header, time.Now())); werr != nil {
				return nil, "", werr
			}
			continue
		case resp.StatusCode >= 500 && attempt < 5:
			if werr := sleep(ctx, time.Duration(attempt+1)*2*time.Second); werr != nil {
				return nil, "", werr
			}
			continue
		default:
			msg := string(body)
			if len(msg) > 300 {
				msg = msg[:300]
			}
			return nil, "", fmt.Errorf("github: GET %s: status %d: %s", rawURL, resp.StatusCode, msg)
		}
	}
}

// rateLimitWait reads how long GitHub asks us to back off: Retry-After for a
// secondary limit, else until the primary window resets. It never waits less
// than a second or more than an hour.
func rateLimitWait(h http.Header, now time.Time) time.Duration {
	if s, err := strconv.Atoi(h.Get("Retry-After")); err == nil {
		return clamp(time.Duration(s) * time.Second)
	}
	if h.Get("X-RateLimit-Remaining") == "0" {
		if reset, err := strconv.ParseInt(h.Get("X-RateLimit-Reset"), 10, 64); err == nil {
			return clamp(time.Unix(reset, 0).Sub(now) + time.Second)
		}
	}
	return time.Minute
}

func clamp(d time.Duration) time.Duration { return min(max(d, time.Second), time.Hour) }

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

var linkNext = regexp.MustCompile(`<([^>]+)>;\s*rel="next"`)

func nextLink(h string) string {
	if m := linkNext.FindStringSubmatch(h); m != nil {
		return m[1]
	}
	return ""
}

// listUpdated pages through a listing sorted by updated time ascending,
// starting at since (inclusive). It calls fn with each page and the page's
// newest updated time. When GitHub refuses a deep page, it restarts the
// listing from the newest time seen, so an arbitrarily long crawl never needs
// a deep offset. Items at the restart second are delivered twice; every
// event is versioned downstream, so that is harmless.
func listUpdated[T any](ctx context.Context, c *Client, path string, q url.Values, since time.Time, updated func(T) time.Time, fn func([]T) error) error {
	cursor := since
	for {
		roundStart := cursor
		q.Set("sort", "updated")
		q.Set("direction", "asc")
		q.Set("per_page", "100")
		if !cursor.IsZero() {
			q.Set("since", cursor.UTC().Format(time.RFC3339))
		}
		next := strings.TrimRight(c.Base, "/") + path + "?" + q.Encode()
		first, restarted := true, false
		for next != "" {
			body, link, err := c.get(ctx, next, first)
			if errors.Is(err, errDeepPage) && !first {
				restarted = true
				break
			}
			if errors.Is(err, errNotModified) {
				return nil // first page unchanged since the last poll
			}
			if err != nil {
				return err
			}
			var items []T
			if err := json.Unmarshal(body, &items); err != nil {
				return fmt.Errorf("github: decode %s: %w", path, err)
			}
			if len(items) > 0 {
				if err := fn(items); err != nil {
					return err
				}
				if u := updated(items[len(items)-1]); u.After(cursor) {
					cursor = u
				}
			}
			next, first = link, false
		}
		if !restarted {
			return nil
		}
		if !cursor.After(roundStart) {
			return fmt.Errorf("github: %s: more than a page window shares updated time %s", path, cursor.Format(time.RFC3339))
		}
	}
}

// Issues lists issues and pull requests of repo ("owner/name") updated at or
// after since, oldest first.
func (c *Client) Issues(ctx context.Context, repo string, since time.Time, fn func([]Issue) error) error {
	return listUpdated(ctx, c, "/repos/"+repo+"/issues", url.Values{"state": {"all"}}, since,
		func(i Issue) time.Time { return i.UpdatedAt }, fn)
}

// Comments lists issue and pull request comments of repo updated at or after
// since, oldest first.
func (c *Client) Comments(ctx context.Context, repo string, since time.Time, fn func([]Comment) error) error {
	return listUpdated(ctx, c, "/repos/"+repo+"/issues/comments", url.Values{}, since,
		func(cm Comment) time.Time { return cm.UpdatedAt }, fn)
}
