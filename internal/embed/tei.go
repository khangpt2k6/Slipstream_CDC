// Package embed calls a text-embeddings-inference (TEI) server. The same
// model embeds documents (the embedder) and queries (the api), so their
// vectors share one space.
package embed

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// TEI is a client for Hugging Face text-embeddings-inference.
type TEI struct {
	base string
	hc   *http.Client
}

// NewTEI returns a client for base (e.g. http://tei:80).
func NewTEI(base string, timeout time.Duration) *TEI {
	return &TEI{base: strings.TrimRight(base, "/"), hc: &http.Client{Timeout: timeout}}
}

// Embed returns one normalized vector per text, in order. Inputs longer than
// the model's window are truncated server side.
func (t *TEI) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	body, err := json.Marshal(map[string]any{"inputs": texts, "normalize": true, "truncate": true})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.base+"/embed", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := t.hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("embed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("embed: status %d: %s", resp.StatusCode, msg)
	}
	var out [][]float32
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("embed: decode: %w", err)
	}
	if len(out) != len(texts) {
		return nil, fmt.Errorf("embed: got %d vectors for %d inputs", len(out), len(texts))
	}
	return out, nil
}
