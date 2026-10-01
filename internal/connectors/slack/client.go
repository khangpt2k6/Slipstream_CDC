// Package slack is the Slack connector: a Web API client for backfill and
// reconciliation, an Events API receiver for live changes, and the mapping
// from Slack messages and channel memberships to Slipstream documents and
// permission edges.
package slack

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Client is a minimal Slack Web API client. It follows cursor pagination and
// honours 429 Retry-After, which Slack uses for its per-method rate tiers.
type Client struct {
	Base  string // https://slack.com or the simulator
	Token string
	HTTP  *http.Client
}

// User is a workspace member.
type User struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	RealName string `json:"real_name"`
	Deleted  bool   `json:"deleted"`
	IsBot    bool   `json:"is_bot"`
	Profile  struct {
		Email string `json:"email"`
	} `json:"profile"`
}

// Channel is a conversation.
type Channel struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	IsPrivate bool   `json:"is_private"`
}

// Message is a channel message.
type Message struct {
	Type     string `json:"type"`
	Subtype  string `json:"subtype,omitempty"`
	User     string `json:"user"`
	Text     string `json:"text"`
	TS       string `json:"ts"`
	ThreadTS string `json:"thread_ts,omitempty"`
	Edited   *struct {
		TS string `json:"ts"`
	} `json:"edited,omitempty"`
}

type page struct {
	OK       bool            `json:"ok"`
	Error    string          `json:"error"`
	Members  json.RawMessage `json:"members"`
	Channels []Channel       `json:"channels"`
	Messages []Message       `json:"messages"`
	HasMore  bool            `json:"has_more"`
	Meta     struct {
		NextCursor string `json:"next_cursor"`
	} `json:"response_metadata"`
}

func (c *Client) call(ctx context.Context, method string, args url.Values) (*page, error) {
	hc := c.HTTP
	if hc == nil {
		hc = &http.Client{Timeout: 30 * time.Second}
	}
	for attempt := 0; ; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.Base, "/")+"/api/"+method,
			strings.NewReader(args.Encode()))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("Authorization", "Bearer "+c.Token)
		resp, err := hc.Do(req) // #nosec G704 -- Slack base URL is operator config
		if err != nil {
			return nil, fmt.Errorf("slack: %s: %w", method, err)
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 8 {
			_ = resp.Body.Close()
			wait, _ := strconv.Atoi(resp.Header.Get("Retry-After"))
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Duration(max(wait, 1)) * time.Second):
			}
			continue
		}
		var p page
		err = json.NewDecoder(resp.Body).Decode(&p)
		_ = resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("slack: %s: decode: %w", method, err)
		}
		if !p.OK {
			return nil, fmt.Errorf("slack: %s: %s", method, p.Error)
		}
		return &p, nil
	}
}

// Users lists every workspace member.
func (c *Client) Users(ctx context.Context) ([]User, error) {
	var out []User
	cursor := ""
	for {
		p, err := c.call(ctx, "users.list", url.Values{"limit": {"200"}, "cursor": {cursor}})
		if err != nil {
			return nil, err
		}
		var us []User
		if err := json.Unmarshal(p.Members, &us); err != nil {
			return nil, fmt.Errorf("slack: users.list members: %w", err)
		}
		out = append(out, us...)
		if cursor = p.Meta.NextCursor; cursor == "" {
			return out, nil
		}
	}
}

// Channels lists public and private channels the bot can see.
func (c *Client) Channels(ctx context.Context) ([]Channel, error) {
	var out []Channel
	cursor := ""
	for {
		p, err := c.call(ctx, "conversations.list", url.Values{
			"types": {"public_channel,private_channel"}, "limit": {"200"}, "cursor": {cursor}, "exclude_archived": {"true"},
		})
		if err != nil {
			return nil, err
		}
		out = append(out, p.Channels...)
		if cursor = p.Meta.NextCursor; cursor == "" {
			return out, nil
		}
	}
}

// Members lists a channel's member ids.
func (c *Client) Members(ctx context.Context, channel string) ([]string, error) {
	var out []string
	cursor := ""
	for {
		p, err := c.call(ctx, "conversations.members", url.Values{"channel": {channel}, "limit": {"200"}, "cursor": {cursor}})
		if err != nil {
			return nil, err
		}
		var ids []string
		if err := json.Unmarshal(p.Members, &ids); err != nil {
			return nil, fmt.Errorf("slack: conversations.members: %w", err)
		}
		out = append(out, ids...)
		if cursor = p.Meta.NextCursor; cursor == "" {
			return out, nil
		}
	}
}

// History pages through messages newer than oldest (exclusive), newest first,
// calling fn for each page.
func (c *Client) History(ctx context.Context, channel, oldest string, fn func([]Message) error) error {
	cursor := ""
	for {
		p, err := c.call(ctx, "conversations.history", url.Values{
			"channel": {channel}, "oldest": {oldest}, "limit": {"500"}, "cursor": {cursor},
		})
		if err != nil {
			return err
		}
		if err := fn(p.Messages); err != nil {
			return err
		}
		if cursor = p.Meta.NextCursor; cursor == "" || !p.HasMore {
			return nil
		}
	}
}
