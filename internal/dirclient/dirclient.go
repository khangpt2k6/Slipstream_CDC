// Package dirclient is the connectors' client for the directory: register
// containers, mirror source-system permissions as edges, and resolve source
// accounts to people by email.
package dirclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Client talks to the directory service.
type Client struct {
	base string
	hc   *http.Client
}

// New returns a client for base (e.g. http://directory:8081).
func New(base string) *Client {
	return &Client{base: strings.TrimRight(base, "/"), hc: &http.Client{Timeout: 15 * time.Second}}
}

// ErrNotFound is returned when a lookup matches nothing.
var ErrNotFound = errors.New("dirclient: not found")

// Edge is an edge change or a stored edge.
type Edge struct {
	From    string `json:"from"`
	To      string `json:"to"`
	Present bool   `json:"present"`
	Version int64  `json:"version,omitempty"`
}

// User is a directory user.
type User struct {
	ID     string   `json:"id"`
	Name   string   `json:"name"`
	Email  string   `json:"email"`
	Groups []string `json:"groups"`
}

// RegisterContainer records a container (idempotent). visibility is public,
// restricted, members, or "" for the directory's default policy.
func (c *Client) RegisterContainer(ctx context.Context, id, datasource, name, visibility string) error {
	return c.do(ctx, http.MethodPost, "/v1/containers", map[string]string{
		"id": id, "datasource": datasource, "name": name, "visibility": visibility,
	}, nil)
}

// SetEdges commits edge changes. It returns the committed events' versions
// and source times through out when non-nil.
func (c *Client) SetEdges(ctx context.Context, changes []Edge) error {
	if len(changes) == 0 {
		return nil
	}
	return c.do(ctx, http.MethodPost, "/v1/edges", map[string]any{"changes": changes}, nil)
}

// EdgesTo lists stored edges into a principal.
func (c *Client) EdgesTo(ctx context.Context, to string) ([]Edge, error) {
	var out []Edge
	err := c.do(ctx, http.MethodGet, "/v1/edges?to="+url.QueryEscape(to), nil, &out)
	return out, err
}

// UserByEmail resolves an email to a directory user id.
func (c *Client) UserByEmail(ctx context.Context, email string) (string, error) {
	var out struct {
		ID string `json:"id"`
	}
	if err := c.do(ctx, http.MethodGet, "/v1/users/by-email?email="+url.QueryEscape(email), nil, &out); err != nil {
		return "", err
	}
	return out.ID, nil
}

// Users lists directory users.
func (c *Client) Users(ctx context.Context) ([]User, error) {
	var out []User
	err := c.do(ctx, http.MethodGet, "/v1/users", nil, &out)
	return out, err
}

// Principals returns the ground-truth expansion of a user.
func (c *Client) Principals(ctx context.Context, user string) ([]string, error) {
	var out []string
	err := c.do(ctx, http.MethodGet, "/v1/principals/"+url.PathEscape(user), nil, &out)
	return out, err
}

func (c *Client) do(ctx context.Context, method, path string, body, out any) error {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, rd)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.hc.Do(req) // #nosec G704 -- directory URL is operator config
	if err != nil {
		return fmt.Errorf("dirclient: %s %s: %w", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusNotFound {
		return ErrNotFound
	}
	if resp.StatusCode >= 300 && resp.StatusCode != http.StatusAccepted {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("dirclient: %s %s: status %d: %s", method, path, resp.StatusCode, msg)
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
