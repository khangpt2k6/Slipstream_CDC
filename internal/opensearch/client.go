// Package opensearch is a small JSON-over-HTTP client for the handful of
// OpenSearch APIs Slipstream uses: index bootstrap, bulk writes with
// optimistic concurrency, realtime multi-get, search, and refresh.
//
// It is hand written instead of pulling in the official client because the
// surface is tiny and the write path depends on exact bulk semantics
// (per-item status codes, if_seq_no conflicts) that are clearer stated here.
package opensearch

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

// Client talks to one OpenSearch endpoint.
type Client struct {
	base string
	hc   *http.Client
}

// New returns a client for base (e.g. http://localhost:9200). timeout bounds
// each request, so a stalled cluster surfaces as a retryable error.
func New(base string, timeout time.Duration) *Client {
	return &Client{
		base: strings.TrimRight(base, "/"),
		hc:   &http.Client{Timeout: timeout},
	}
}

// Error is a non-2xx response.
type Error struct {
	Status int
	Body   string
}

func (e *Error) Error() string {
	body := e.Body
	if len(body) > 512 {
		body = body[:512] + "..."
	}
	return fmt.Sprintf("opensearch: status %d: %s", e.Status, body)
}

// IsStatus reports whether err is an *Error with the given status.
func IsStatus(err error, status int) bool {
	var oe *Error
	return errors.As(err, &oe) && oe.Status == status
}

// Do sends one request. body may be nil, a []byte, or any JSON-encodable
// value. A 2xx response is decoded into out when out is non-nil.
func (c *Client) Do(ctx context.Context, method, path string, body, out any) error {
	var rd io.Reader
	contentType := "application/json"
	switch b := body.(type) {
	case nil:
	case []byte:
		rd = bytes.NewReader(b)
		if strings.HasPrefix(path, "/_bulk") {
			contentType = "application/x-ndjson"
		}
	default:
		buf, err := json.Marshal(b)
		if err != nil {
			return fmt.Errorf("opensearch: encode %s %s: %w", method, path, err)
		}
		rd = bytes.NewReader(buf)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.base+path, rd)
	if err != nil {
		return fmt.Errorf("opensearch: build %s %s: %w", method, path, err)
	}
	if rd != nil {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return fmt.Errorf("opensearch: %s %s: %w", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("opensearch: read %s %s: %w", method, path, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return &Error{Status: resp.StatusCode, Body: string(data)}
	}
	if out == nil || len(data) == 0 {
		return nil
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("opensearch: decode %s %s: %w", method, path, err)
	}
	return nil
}

// EnsureIndex creates index with body and points alias at it, unless alias
// already exists. Writing through an alias lets a later re-index build a new
// index and swap the alias without downtime.
func (c *Client) EnsureIndex(ctx context.Context, alias, index string, body map[string]any) error {
	err := c.Do(ctx, http.MethodHead, "/"+url.PathEscape(alias), nil, nil)
	if err == nil {
		// Already there: apply additive mapping changes (new fields). Changing
		// an existing field's type fails here and needs a re-index instead.
		if m, ok := body["mappings"].(map[string]any); ok {
			return c.Do(ctx, http.MethodPut, "/"+url.PathEscape(alias)+"/_mapping", m, nil)
		}
		return nil
	}
	if !IsStatus(err, http.StatusNotFound) {
		return err
	}
	withAlias := make(map[string]any, len(body)+1)
	for k, v := range body {
		withAlias[k] = v
	}
	withAlias["aliases"] = map[string]any{alias: map[string]any{"is_write_index": true}}
	err = c.Do(ctx, http.MethodPut, "/"+url.PathEscape(index), withAlias, nil)
	if err != nil && IsStatus(err, http.StatusBadRequest) && strings.Contains(err.Error(), "resource_already_exists") {
		return nil // lost a race with another instance; the index is there
	}
	return err
}

// GetResult is one document from a multi-get.
type GetResult struct {
	ID          string          `json:"_id"`
	Found       bool            `json:"found"`
	SeqNo       int64           `json:"_seq_no"`
	PrimaryTerm int64           `json:"_primary_term"`
	Source      json.RawMessage `json:"_source"`
}

// MGet fetches ids from index in realtime (it sees writes that have not been
// refreshed yet), returning only the source fields in includes.
func (c *Client) MGet(ctx context.Context, index string, ids []string, includes []string) ([]GetResult, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	path := "/" + url.PathEscape(index) + "/_mget?realtime=true"
	if len(includes) > 0 {
		path += "&_source_includes=" + url.QueryEscape(strings.Join(includes, ","))
	}
	var resp struct {
		Docs []GetResult `json:"docs"`
	}
	if err := c.Do(ctx, http.MethodPost, path, map[string]any{"ids": ids}, &resp); err != nil {
		return nil, err
	}
	return resp.Docs, nil
}

// BulkItem is one per-operation result in a bulk response.
type BulkItem struct {
	ID          string `json:"_id"`
	Status      int    `json:"status"`
	SeqNo       int64  `json:"_seq_no"`
	PrimaryTerm int64  `json:"_primary_term"`
	Error       *struct {
		Type   string `json:"type"`
		Reason string `json:"reason"`
	} `json:"error,omitempty"`
}

// BulkResponse is the result of a bulk request. Items are in request order.
type BulkResponse struct {
	Took   int                   `json:"took"`
	Errors bool                  `json:"errors"`
	Items  []map[string]BulkItem `json:"items"`
}

// Results flattens Items to one BulkItem per operation, in request order.
func (r *BulkResponse) Results() []BulkItem {
	out := make([]BulkItem, 0, len(r.Items))
	for _, m := range r.Items {
		for _, it := range m {
			out = append(out, it)
		}
	}
	return out
}

// Bulk sends an NDJSON bulk body. A 200 response can still carry per-item
// failures; callers inspect Results.
func (c *Client) Bulk(ctx context.Context, ndjson []byte) (*BulkResponse, error) {
	var resp BulkResponse
	if err := c.Do(ctx, http.MethodPost, "/_bulk", ndjson, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Hit is one search hit.
type Hit struct {
	ID        string              `json:"_id"`
	Score     float64             `json:"_score"`
	Source    json.RawMessage     `json:"_source"`
	Highlight map[string][]string `json:"highlight,omitempty"`
}

// SearchResponse is the subset of a search response Slipstream reads.
type SearchResponse struct {
	Took int `json:"took"`
	Hits struct {
		Total struct {
			Value int `json:"value"`
		} `json:"total"`
		Hits []Hit `json:"hits"`
	} `json:"hits"`
	Aggregations json.RawMessage `json:"aggregations,omitempty"`
}

// Search runs a query against index.
func (c *Client) Search(ctx context.Context, index string, body any) (*SearchResponse, error) {
	var resp SearchResponse
	if err := c.Do(ctx, http.MethodPost, "/"+url.PathEscape(index)+"/_search", body, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Refresh makes every acknowledged write in index visible to search.
func (c *Client) Refresh(ctx context.Context, index string) error {
	return c.Do(ctx, http.MethodPost, "/"+url.PathEscape(index)+"/_refresh", nil, nil)
}

// Ping checks the cluster answers.
func (c *Client) Ping(ctx context.Context) error {
	return c.Do(ctx, http.MethodGet, "/", nil, nil)
}
