package jira

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/khangpt2k6/Slipstream_CDC/internal/connectors"
	"github.com/khangpt2k6/Slipstream_CDC/internal/dirclient"
	"github.com/khangpt2k6/Slipstream_CDC/internal/model"
)

// Connector syncs a set of Jira projects.
type Connector struct {
	API      *Client
	Dir      *dirclient.Client
	Out      *connectors.Emitter
	RDB      *redis.Client
	Projects []string
	PageSize int // issues per search page (default 200)
	// Overlap is how far before the stored cursor every crawl starts. JQL
	// dates have minute resolution and Jira's search index trails its
	// database slightly, so a poll that started exactly at the cursor could
	// miss a change. Re-reading the window costs only discarded duplicates.
	Overlap time.Duration
	// Zone is the time zone Jira reads JQL dates in: the querying user's
	// zone, which for anonymous access is the instance default.
	Zone     *time.Location
	Comments bool
	Now      func() time.Time
}

const (
	cursorKey   = "ss:cursor:jira:"
	commentsKey = "ss:jira:comments:"
	jqlTime     = "2006/01/02 15:04"
)

var projectKey = regexp.MustCompile(`^[A-Z][A-Z0-9_]+$`)

// Sync registers every project as a container.
func (c *Connector) Sync(ctx context.Context) error {
	for _, p := range c.Projects {
		if !projectKey.MatchString(p) {
			return fmt.Errorf("jira: bad project key %q", p)
		}
		if err := c.Dir.RegisterContainer(ctx, containerID(p), "jira", p, ""); err != nil {
			return err
		}
	}
	return nil
}

// Crawl emits every issue (and its comments) updated since each project's
// cursor. The first run is the full backfill; later runs are polls.
func (c *Connector) Crawl(ctx context.Context, mode model.Mode) (int, error) {
	total := 0
	for _, p := range c.Projects {
		n, err := c.crawlProject(ctx, p, mode)
		total += n
		if err != nil {
			return total, fmt.Errorf("jira: %s: %w", p, err)
		}
	}
	return total, nil
}

// crawlProject pages a project in updated order. After every page it
// re-anchors the query at the newest updated time it has seen and starts
// again at offset zero, so it never relies on a deep offset into a result
// set that moves while it reads (an issue edited mid-crawl jumps to the end).
// Only when a whole page shares one minute, which Jira bulk edits produce,
// does it page by offset within that minute.
func (c *Connector) crawlProject(ctx context.Context, project string, mode model.Mode) (int, error) {
	key := cursorKey + project
	cursor, err := c.loadCursor(ctx, key)
	if err != nil {
		return 0, err
	}
	from := time.Time{}
	if !cursor.IsZero() {
		from = cursor.Add(-c.Overlap)
	}
	page := c.PageSize
	if page <= 0 {
		page = 200
	}

	total, startAt := 0, 0
	jql := c.jql(project, from)
	for {
		issues, n, err := c.API.Search(ctx, jql, startAt, page)
		if err != nil {
			return total, err
		}
		if len(issues) == 0 {
			return total, nil
		}
		emitted, err := c.emitPage(ctx, issues, mode)
		total += emitted
		if err != nil {
			return total, err
		}
		newest := issues[len(issues)-1].Fields.Updated.Time
		if newest.After(cursor) {
			cursor = newest
			if err := c.RDB.Set(ctx, key, cursor.UTC().Format(time.RFC3339Nano), 0).Err(); err != nil {
				return total, err
			}
		}
		if startAt+len(issues) >= n {
			return total, nil
		}
		if next := c.jql(project, cursor); next != jql {
			jql, startAt = next, 0
		} else {
			startAt += len(issues)
		}
	}
}

func (c *Connector) jql(project string, from time.Time) string {
	if from.IsZero() {
		return fmt.Sprintf(`project = "%s" ORDER BY updated ASC, key ASC`, project)
	}
	zone := c.Zone
	if zone == nil {
		zone = time.UTC
	}
	return fmt.Sprintf(`project = "%s" AND updated >= "%s" ORDER BY updated ASC, key ASC`, project, from.In(zone).Format(jqlTime))
}

func (c *Connector) loadCursor(ctx context.Context, key string) (time.Time, error) {
	raw, err := c.RDB.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, err
	}
	t, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return time.Time{}, nil // unreadable cursor: start over, versions dedupe
	}
	return t, nil
}

// emitPage maps a page of issues (and their comments) to events, produces
// them, then records each issue's comment ids. A comment that was there last
// time and is gone now was deleted, and gets a delete event.
func (c *Connector) emitPage(ctx context.Context, issues []Issue, mode model.Mode) (int, error) {
	var evs []model.DocEvent
	current := make(map[string][]string, len(issues))
	for _, is := range issues {
		evs = append(evs, c.issueEvent(is, mode))
		if !c.Comments {
			continue
		}
		comments := is.Fields.Comment.Comments
		if is.Fields.Comment.Total > len(comments) {
			all, err := c.API.Comments(ctx, is.Key)
			if err != nil {
				return 0, err
			}
			comments = all
		}
		ids := make([]string, 0, len(comments))
		for _, cm := range comments {
			evs = append(evs, c.commentEvent(is, cm, mode))
			ids = append(ids, cm.ID)
		}
		current[is.Key] = ids
	}

	var removed map[string][]string
	if c.Comments {
		var err error
		if removed, err = c.removedComments(ctx, current); err != nil {
			return 0, err
		}
		for _, is := range issues {
			for _, id := range removed[is.Key] {
				evs = append(evs, c.deleteComment(is, id))
			}
		}
	}
	if err := c.Out.Emit(ctx, evs...); err != nil {
		return 0, err
	}
	if c.Comments {
		pipe := c.RDB.Pipeline()
		for k, ids := range current {
			if gone := removed[k]; len(gone) > 0 {
				pipe.SRem(ctx, commentsKey+k, toAny(gone)...)
			}
			if len(ids) > 0 {
				pipe.SAdd(ctx, commentsKey+k, toAny(ids)...)
			}
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return len(evs), err
		}
	}
	return len(evs), nil
}

func (c *Connector) removedComments(ctx context.Context, current map[string][]string) (map[string][]string, error) {
	pipe := c.RDB.Pipeline()
	cmds := make(map[string]*redis.StringSliceCmd, len(current))
	for k := range current {
		cmds[k] = pipe.SMembers(ctx, commentsKey+k)
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	out := map[string][]string{}
	for k, cmd := range cmds {
		have := map[string]bool{}
		for _, id := range current[k] {
			have[id] = true
		}
		for _, id := range cmd.Val() {
			if !have[id] {
				out[k] = append(out[k], id)
			}
		}
	}
	return out, nil
}

func toAny(ss []string) []any {
	out := make([]any, len(ss))
	for i, s := range ss {
		out[i] = s
	}
	return out
}

// issueEvent maps an issue. The version is its updated time in
// milliseconds; Jira bumps it on every field change and comment.
func (c *Connector) issueEvent(is Issue, mode model.Mode) model.DocEvent {
	f := is.Fields
	project, _, _ := strings.Cut(is.Key, "-")
	labels := append([]string{}, f.Labels...)
	if f.IssueType.Name != "" {
		labels = append(labels, "type:"+f.IssueType.Name)
	}
	if f.Status.Name != "" {
		labels = append(labels, "status:"+f.Status.Name)
	}
	var dups []string
	for _, l := range f.IssueLinks {
		if l.Type.Name == "Duplicate" && l.OutwardIssue != nil && l.OutwardIssue.Key != "" {
			dups = append(dups, docID(l.OutwardIssue.Key))
		}
	}
	author := ""
	if f.Reporter != nil {
		author = f.Reporter.DisplayName
	}
	v := f.Updated.UnixMilli()
	return model.DocEvent{
		Op: model.OpUpsert, Version: v, SrcTsMs: v, EmitTsMs: c.now().UnixMilli(), Mode: mode,
		Doc: model.Document{
			ID:         docID(is.Key),
			Datasource: "jira",
			Container:  containerID(project),
			Kind:       "issue",
			Title:      f.Summary,
			Body:       f.Description,
			URL:        c.browse(is.Key),
			Author:     author,
			Labels:     labels,
			Duplicates: dups,
			CreatedAt:  f.Created.UTC(),
			UpdatedAt:  f.Updated.UTC(),
			Allowed:    []string{model.Container(containerID(project))},
		},
	}
}

func (c *Connector) commentEvent(is Issue, cm Comment, mode model.Mode) model.DocEvent {
	project, _, _ := strings.Cut(is.Key, "-")
	author := ""
	if cm.Author != nil {
		author = cm.Author.DisplayName
	}
	v := cm.Updated.UnixMilli()
	return model.DocEvent{
		Op: model.OpUpsert, Version: v, SrcTsMs: v, EmitTsMs: c.now().UnixMilli(), Mode: mode,
		Doc: model.Document{
			ID:         commentID(is.Key, cm.ID),
			Datasource: "jira",
			Container:  containerID(project),
			Kind:       "comment",
			Title:      "Re: " + is.Fields.Summary,
			Body:       cm.Body,
			URL:        fmt.Sprintf("%s?focusedCommentId=%s#comment-%s", c.browse(is.Key), cm.ID, cm.ID),
			Author:     author,
			ParentID:   docID(is.Key),
			CreatedAt:  cm.Created.UTC(),
			UpdatedAt:  cm.Updated.UTC(),
			Allowed:    []string{model.Container(containerID(project))},
		},
	}
}

// deleteComment removes a comment that disappeared from its issue. Deleting
// a comment bumps the issue's updated time, so a version just past it (or
// now, if later) outranks every version the comment carried.
func (c *Connector) deleteComment(is Issue, id string) model.DocEvent {
	project, _, _ := strings.Cut(is.Key, "-")
	now := c.now().UnixMilli()
	v := max(now, is.Fields.Updated.UnixMilli()+1)
	return model.DocEvent{
		Op: model.OpDelete, Version: v, SrcTsMs: now, EmitTsMs: now, Mode: model.ModeLive,
		Doc: model.Document{ID: commentID(is.Key, id), Datasource: "jira", Container: containerID(project)},
	}
}

func (c *Connector) browse(key string) string {
	return strings.TrimRight(c.API.Base, "/") + "/browse/" + key
}

func (c *Connector) now() time.Time {
	if c.Now != nil {
		return c.Now()
	}
	return time.Now()
}

func containerID(project string) string { return "jira:" + project }

func docID(key string) string { return "jira:" + key }

func commentID(key, id string) string { return "jira:" + key + "/c" + id }
