// Package jira is the Jira connector: a REST client that pages issues in
// updated order through JQL, a poller with an overlap window (Jira Server
// has no webhooks an outside crawler can subscribe to), and the mapping from
// issues and comments to Slipstream documents.
package jira

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Client is a minimal Jira Server / Data Center REST v2 client.
type Client struct {
	Base  string // e.g. https://issues.apache.org/jira
	Token string // optional personal access token
	HTTP  *http.Client
}

// Time is a Jira timestamp ("2011-07-19T21:32:10.000+0000").
type Time struct{ time.Time }

// UnmarshalJSON parses Jira's timestamp layout.
func (t *Time) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), `"`)
	if s == "" || s == "null" {
		return nil
	}
	v, err := time.Parse("2006-01-02T15:04:05.000-0700", s)
	if err != nil {
		return fmt.Errorf("jira: time %q: %w", s, err)
	}
	t.Time = v
	return nil
}

// Person is a reporter or comment author.
type Person struct {
	Name        string `json:"name"`
	DisplayName string `json:"displayName"`
}

// Comment is an issue comment.
type Comment struct {
	ID      string  `json:"id"`
	Body    string  `json:"body"`
	Author  *Person `json:"author"`
	Created Time    `json:"created"`
	Updated Time    `json:"updated"`
}

// Link is an issue link. Exactly one of the issue sides is set: OutwardIssue
// when this issue is the link's source ("KAFKA-1 duplicates KAFKA-2").
type Link struct {
	Type struct {
		Name string `json:"name"`
	} `json:"type"`
	OutwardIssue *struct {
		Key string `json:"key"`
	} `json:"outwardIssue"`
	InwardIssue *struct {
		Key string `json:"key"`
	} `json:"inwardIssue"`
}

// Issue is the subset of a Jira issue the connector reads.
type Issue struct {
	ID     string `json:"id"`
	Key    string `json:"key"`
	Fields struct {
		Summary     string   `json:"summary"`
		Description string   `json:"description"`
		Labels      []string `json:"labels"`
		IssueType   struct {
			Name string `json:"name"`
		} `json:"issuetype"`
		Status struct {
			Name string `json:"name"`
		} `json:"status"`
		Reporter   *Person `json:"reporter"`
		Created    Time    `json:"created"`
		Updated    Time    `json:"updated"`
		IssueLinks []Link  `json:"issuelinks"`
		Comment    struct {
			Comments []Comment `json:"comments"`
			Total    int       `json:"total"`
		} `json:"comment"`
	} `json:"fields"`
}

// Fields requested on every search.
const searchFields = "summary,description,labels,issuetype,status,reporter,created,updated,issuelinks,comment"

func (c *Client) get(ctx context.Context, path string, q url.Values, out any) error {
	hc := c.HTTP
	if hc == nil {
		hc = &http.Client{Timeout: 90 * time.Second}
	}
	target := strings.TrimRight(c.Base, "/") + path + "?" + q.Encode()
	for attempt := 0; ; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
		if err != nil {
			return err
		}
		req.Header.Set("Accept", "application/json")
		if c.Token != "" {
			req.Header.Set("Authorization", "Bearer "+c.Token)
		}
		resp, err := hc.Do(req) // #nosec G704 -- Jira base URL is operator config
		if err != nil {
			if attempt < 5 && ctx.Err() == nil {
				if werr := sleep(ctx, time.Duration(attempt+1)*3*time.Second); werr != nil {
					return werr
				}
				continue
			}
			return fmt.Errorf("jira: GET %s: %w", path, err)
		}
		retryable := resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500
		if retryable && attempt < 8 {
			wait := time.Duration(attempt+1) * 3 * time.Second
			if s, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				wait = time.Duration(s) * time.Second
			}
			_ = resp.Body.Close()
			if werr := sleep(ctx, min(max(wait, time.Second), 5*time.Minute)); werr != nil {
				return werr
			}
			continue
		}
		if resp.StatusCode != http.StatusOK {
			msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
			_ = resp.Body.Close()
			return fmt.Errorf("jira: GET %s: status %d: %s", path, resp.StatusCode, msg)
		}
		err = json.NewDecoder(resp.Body).Decode(out)
		_ = resp.Body.Close()
		if err != nil {
			return fmt.Errorf("jira: decode %s: %w", path, err)
		}
		return nil
	}
}

// Search runs one page of a JQL search.
func (c *Client) Search(ctx context.Context, jql string, startAt, limit int) ([]Issue, int, error) {
	var resp struct {
		Total  int     `json:"total"`
		Issues []Issue `json:"issues"`
	}
	q := url.Values{
		"jql": {jql}, "startAt": {strconv.Itoa(startAt)}, "maxResults": {strconv.Itoa(limit)},
		"fields": {searchFields}, "validateQuery": {"strict"},
	}
	if err := c.get(ctx, "/rest/api/2/search", q, &resp); err != nil {
		return nil, 0, err
	}
	return resp.Issues, resp.Total, nil
}

// Comments returns every comment on an issue. Search results can truncate a
// long comment list; this pages through all of it.
func (c *Client) Comments(ctx context.Context, key string) ([]Comment, error) {
	var out []Comment
	for {
		var page struct {
			Comments []Comment `json:"comments"`
			Total    int       `json:"total"`
		}
		q := url.Values{"startAt": {strconv.Itoa(len(out))}, "maxResults": {"500"}}
		if err := c.get(ctx, "/rest/api/2/issue/"+url.PathEscape(key)+"/comment", q, &page); err != nil {
			return nil, err
		}
		out = append(out, page.Comments...)
		if len(page.Comments) == 0 || len(out) >= page.Total {
			return out, nil
		}
	}
}

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
