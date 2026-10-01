// Package model is the source-agnostic wire format between connectors and the
// indexer. Every connector, whatever it reads (GitHub, Jira, Slack), emits the
// same two event types:
//
//   - DocEvent on the docs topic, keyed by document id. Content and the
//     document's own ACL travel together under one key, so they stay ordered
//     within a partition.
//   - EdgeEvent on the identity topic, keyed by edge. Group memberships and
//     container grants are edges in one permission graph, so a revoke is a
//     single small event rather than a re-index of every affected document.
package model

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Op is what a DocEvent does to its document.
type Op string

// Document operations.
const (
	OpUpsert Op = "upsert" // full authoritative state of the document
	OpDelete Op = "delete" // the document no longer exists at the source
)

// Mode says how an event was produced. Only live events count toward
// freshness: a backfill event's source time is when the item last changed,
// which can be years ago.
type Mode string

// Event modes.
const (
	ModeLive     Mode = "live"     // webhook, event push, or incremental poll
	ModeBackfill Mode = "backfill" // initial crawl
	ModeReplay   Mode = "replay"   // load generator or recovery replay
)

// Principal prefixes. A principal is a user, a group, or a container (a repo,
// a Jira project, a Slack channel). Documents list the principals allowed to
// read them; the identity graph says which principals a user holds.
const (
	UserPrefix      = "u:"
	GroupPrefix     = "g:"
	ContainerPrefix = "c:"
	// Everyone is held by every user implicitly. Granting it to a container
	// makes that container public.
	Everyone = "g:everyone"
)

// User returns the principal for a user id.
func User(id string) string { return UserPrefix + id }

// Group returns the principal for a group id.
func Group(id string) string { return GroupPrefix + id }

// Container returns the principal for a container id like "github:golang/go".
func Container(id string) string { return ContainerPrefix + id }

// Document is the unified record for one searchable item.
type Document struct {
	ID         string    `json:"id"`         // e.g. github:golang/go#123, jira:KAFKA-1, slack:C01/1700000000.000100
	Datasource string    `json:"datasource"` // github | jira | slack
	Container  string    `json:"container"`  // e.g. github:golang/go
	Kind       string    `json:"kind"`       // issue, pull_request, comment, ticket, message
	Title      string    `json:"title,omitempty"`
	Body       string    `json:"body,omitempty"`
	URL        string    `json:"url,omitempty"`
	Author     string    `json:"author,omitempty"`
	ParentID   string    `json:"parent_id,omitempty"`
	Labels     []string  `json:"labels,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
	// Allowed holds unexpanded principals (usually just the container). It is
	// never expanded to users at index time; expansion happens per query.
	Allowed []string `json:"allowed"`
}

// ContentHash fingerprints the text that embeddings are computed from, so
// the embedder can tell whether a stored vector is still current.
func (d Document) ContentHash() string {
	h := sha256.New()
	h.Write([]byte(d.Title))
	h.Write([]byte{0})
	h.Write([]byte(d.Body))
	return hex.EncodeToString(h.Sum(nil)[:16])
}

// DocEvent is the value on the docs topic.
type DocEvent struct {
	Op  Op       `json:"op"`
	Doc Document `json:"doc"`
	// Version is the source version, monotonic per document (the source's
	// updated-at in ms for most connectors). The index applies an event only
	// when its version is newer than what it holds.
	Version  int64 `json:"version"`
	SrcTsMs  int64 `json:"src_ts_ms"`  // when the change happened at the source
	EmitTsMs int64 `json:"emit_ts_ms"` // when the connector produced the event
	Mode     Mode  `json:"mode"`
}

// Validate reports whether e is well formed enough to index.
func (e DocEvent) Validate() error {
	if e.Doc.ID == "" {
		return errors.New("model: doc event without doc id")
	}
	if e.Version <= 0 {
		return fmt.Errorf("model: doc %s has non-positive version %d", e.Doc.ID, e.Version)
	}
	switch e.Op {
	case OpDelete:
		return nil
	case OpUpsert:
		if e.Doc.Datasource == "" || e.Doc.Container == "" {
			return fmt.Errorf("model: doc %s missing datasource or container", e.Doc.ID)
		}
		for _, p := range e.Doc.Allowed {
			if !IsPrincipal(p) {
				return fmt.Errorf("model: doc %s has malformed principal %q", e.Doc.ID, p)
			}
		}
		return nil
	default:
		return fmt.Errorf("model: doc %s has unknown op %q", e.Doc.ID, e.Op)
	}
}

// EdgeEvent is the value on the identity topic: "From holds To". User to
// group is a membership, group to group is nesting, and anything to a
// container is a grant. Present=false removes the edge.
type EdgeEvent struct {
	From    string `json:"from"`
	To      string `json:"to"`
	Present bool   `json:"present"`
	// Version orders changes to the same edge (last writer wins). The
	// directory assigns it from a monotonic counter.
	Version int64 `json:"version"`
	SrcTsMs int64 `json:"src_ts_ms"` // directory commit time
}

// Key is the Kafka key of the edge, so the compacted topic keeps exactly the
// latest state of every edge.
func (e EdgeEvent) Key() string { return EdgeKey(e.From, e.To) }

// EdgeKey joins an edge's endpoints into its key.
func EdgeKey(from, to string) string { return from + "|" + to }

// Validate reports whether e is a well-formed edge.
func (e EdgeEvent) Validate() error {
	if !IsPrincipal(e.From) || !IsPrincipal(e.To) {
		return fmt.Errorf("model: malformed edge %q -> %q", e.From, e.To)
	}
	if strings.HasPrefix(e.From, ContainerPrefix) {
		return fmt.Errorf("model: container %q cannot hold other principals", e.From)
	}
	if strings.HasPrefix(e.To, UserPrefix) {
		return fmt.Errorf("model: edge %q -> %q points at a user", e.From, e.To)
	}
	if e.Version <= 0 {
		return fmt.Errorf("model: edge %s has non-positive version", e.Key())
	}
	return nil
}

// IsPrincipal reports whether p has a known prefix and a non-empty id.
func IsPrincipal(p string) bool {
	for _, pre := range []string{UserPrefix, GroupPrefix, ContainerPrefix} {
		if strings.HasPrefix(p, pre) && len(p) > len(pre) {
			return !strings.Contains(p, "|")
		}
	}
	return false
}

// Topics used across services.
const (
	TopicDocs     = "ss.docs"
	TopicIdentity = "ss.identity"
)
