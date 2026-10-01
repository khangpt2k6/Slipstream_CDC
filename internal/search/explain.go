package search

import (
	"context"
	"encoding/json"
	"errors"
)

// ErrNotFound means the document is not in the index.
var ErrNotFound = errors.New("search: document not found")

// Explanation answers "why can (or can't) this user see this document?".
type Explanation struct {
	DocID       string   `json:"doc_id"`
	User        string   `json:"user"`
	Visible     bool     `json:"visible"`
	Reason      string   `json:"reason"`
	Allowed     []string `json:"allowed"` // principals on the document
	Matched     string   `json:"matched,omitempty"`
	Path        []string `json:"path,omitempty"` // user -> ... -> matched
	Held        int      `json:"held"`           // principals the user holds
	Epoch       int64    `json:"epoch"`
	Version     int64    `json:"version"`
	Deleted     bool     `json:"deleted"`
	Quarantined bool     `json:"quarantined"`
}

// Explain reads the document in realtime and evaluates it against the user's
// current expansion, with the same rules search applies.
func (s *Searcher) Explain(ctx context.Context, user, docID string) (Explanation, error) {
	docs, err := s.OS.MGet(ctx, s.Index, []string{docID}, []string{"allowed", "deleted", "quarantined", "version"})
	if err != nil {
		return Explanation{}, err
	}
	if len(docs) == 0 || !docs[0].Found {
		return Explanation{}, ErrNotFound
	}
	var src struct {
		Allowed     []string `json:"allowed"`
		Deleted     bool     `json:"deleted"`
		Quarantined bool     `json:"quarantined"`
		Version     int64    `json:"version"`
	}
	if err := json.Unmarshal(docs[0].Source, &src); err != nil {
		return Explanation{}, err
	}
	exp, err := s.X.Expand(ctx, user)
	if err != nil {
		return Explanation{}, err
	}

	out := Explanation{
		DocID: docID, User: exp.User, Allowed: src.Allowed, Held: len(exp.Principals), Epoch: exp.Epoch,
		Version: src.Version, Deleted: src.Deleted, Quarantined: src.Quarantined,
	}
	for _, p := range src.Allowed {
		if exp.Has(p) {
			out.Matched, out.Path = p, exp.PathTo(p)
			break
		}
	}
	switch {
	case src.Deleted:
		out.Reason = "deleted at the source"
	case src.Quarantined:
		out.Reason = "quarantined: an update for this document could not be read, so it is hidden from everyone until a newer change arrives"
	case out.Matched == "":
		out.Reason = "the user holds none of the document's principals"
	default:
		out.Visible = true
		out.Reason = "access through " + out.Matched
	}
	return out, nil
}
