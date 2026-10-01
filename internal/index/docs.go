package index

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/khangpt2k6/Slipstream_CDC/internal/metrics"
	"github.com/khangpt2k6/Slipstream_CDC/internal/model"
	"github.com/khangpt2k6/Slipstream_CDC/internal/opensearch"
	"github.com/khangpt2k6/Slipstream_CDC/internal/pipeline"
)

// Quarantine is the item value for a record on the docs topic that could not
// be decoded but whose key still names the document. The document is hidden
// from every user until a newer source change arrives, because the unreadable
// event might have been a permission revoke.
type Quarantine struct {
	ID   string
	AtMs int64 // the poison record's timestamp
}

// Stored is the part of an indexed document the planner reads.
type Stored struct {
	Found           bool
	SeqNo           int64
	PrimaryTerm     int64
	Version         int64
	ContentHash     string
	QuarantinedAtMs int64
}

// Applied is one event that changed a document.
type Applied struct {
	Op         model.Op
	Datasource string
	SrcTsMs    int64
	Mode       model.Mode
}

// Plan is the single write a batch produces for one document.
type Plan struct {
	ID          string
	Create      bool // the document does not exist yet
	SeqNo       int64
	PrimaryTerm int64
	Fields      map[string]any
	Applied     []Applied
	Discarded   map[string]int // events dropped by version check, by reason
	Quarantined bool           // a quarantine item was applied
}

// PlanDoc folds a document's batch items (model.DocEvent or Quarantine, in
// offset order) over its stored state and returns the write that leaves the
// index in the folded state. ok is false when nothing changes (every event was
// a duplicate or older than what is stored).
//
// The rules, which make replays, duplicates and out-of-order delivery safe:
//   - An event applies only if its version is newer than the stored version.
//     Equal is a duplicate, older is a late delivery; both are discarded.
//   - A delete leaves a versioned tombstone, so a late upsert cannot bring the
//     document back.
//   - A quarantine hides the document. Only an event whose source time is after
//     the poison record was produced lifts it, so a delayed older event cannot
//     un-hide a document whose newest change was unreadable.
func PlanDoc(id string, cur Stored, vals []any, nowMs int64) (Plan, bool) {
	version, qAt, hash, exists := cur.Version, cur.QuarantinedAtMs, cur.ContentHash, cur.Found
	plan := Plan{ID: id, Create: !cur.Found, SeqNo: cur.SeqNo, PrimaryTerm: cur.PrimaryTerm, Fields: map[string]any{}}
	changed, needsEmbed := false, false
	var lastOp model.Op

	for _, v := range vals {
		switch x := v.(type) {
		case model.DocEvent:
			if exists && x.Version <= version {
				reason := "stale"
				if x.Version == version {
					reason = "duplicate"
				}
				if plan.Discarded == nil {
					plan.Discarded = map[string]int{}
				}
				plan.Discarded[reason]++
				continue
			}
			version, exists, changed, lastOp = x.Version, true, true, x.Op
			if qAt > 0 && x.SrcTsMs > qAt {
				qAt = 0
			}
			if x.Op == model.OpUpsert {
				h := x.Doc.ContentHash()
				if h != hash {
					needsEmbed = true
				}
				hash = h
				for k, fv := range docFields(x, h) {
					plan.Fields[k] = fv
				}
			} else {
				hash = ""
				for k, fv := range tombstoneFields(x) {
					plan.Fields[k] = fv
				}
			}
			plan.Applied = append(plan.Applied, Applied{Op: x.Op, Datasource: x.Doc.Datasource, SrcTsMs: x.SrcTsMs, Mode: x.Mode})
		case Quarantine:
			if x.AtMs > qAt {
				qAt = x.AtMs
			}
			exists, changed, plan.Quarantined = true, true, true
		}
	}
	if !changed {
		return plan, false
	}

	plan.Fields["version"] = version
	plan.Fields["quarantined"] = qAt > 0
	plan.Fields["quarantined_at_ms"] = qAt
	plan.Fields["indexed_at_ms"] = nowMs
	switch {
	case lastOp == model.OpDelete:
		plan.Fields["needs_embed"] = false
	case needsEmbed:
		plan.Fields["needs_embed"] = true
	}
	if plan.Create {
		// A new document must carry every field filters read, even when the
		// only thing that happened to it is a quarantine.
		plan.Fields["id"] = id
		if _, ok := plan.Fields["allowed"]; !ok {
			plan.Fields["allowed"] = []string{}
		}
		if _, ok := plan.Fields["deleted"]; !ok {
			plan.Fields["deleted"] = false
		}
		if _, ok := plan.Fields["needs_embed"]; !ok {
			plan.Fields["needs_embed"] = false
		}
	}
	return plan, true
}

func docFields(e model.DocEvent, hash string) map[string]any {
	d := e.Doc
	f := map[string]any{
		"id":           d.ID,
		"datasource":   d.Datasource,
		"container":    d.Container,
		"kind":         d.Kind,
		"title":        d.Title,
		"body":         d.Body,
		"url":          d.URL,
		"author":       d.Author,
		"parent_id":    d.ParentID,
		"labels":       nonNil(d.Labels),
		"allowed":      nonNil(d.Allowed),
		"deleted":      false,
		"content_hash": hash,
		"src_ts_ms":    e.SrcTsMs,
		"emit_ts_ms":   e.EmitTsMs,
	}
	if !d.CreatedAt.IsZero() {
		f["created_at"] = d.CreatedAt.UTC().Format(time.RFC3339Nano)
	}
	if !d.UpdatedAt.IsZero() {
		f["updated_at"] = d.UpdatedAt.UTC().Format(time.RFC3339Nano)
	}
	return f
}

func tombstoneFields(e model.DocEvent) map[string]any {
	return map[string]any{
		"deleted":      true,
		"title":        "",
		"body":         "",
		"allowed":      []string{},
		"labels":       []string{},
		"content_hash": "",
		"src_ts_ms":    e.SrcTsMs,
		"emit_ts_ms":   e.EmitTsMs,
	}
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

// Result summarizes one successful Write, for the live event feed.
type Result struct {
	Applied     int
	Discarded   int
	Conflicts   int
	Quarantined int
	Sample      []string // a few applied document ids
	FreshMs     []int64  // source change to index ack, live events only
}

// DocSink writes document events to the index. It satisfies pipeline.Sink.
type DocSink struct {
	OS        *opensearch.Client
	Index     string           // alias to write through
	MaxRounds int              // conflict re-plan rounds before giving up (default 5)
	Now       func() time.Time // clock, for tests
	Observe   func(Result)     // optional, called after each successful Write
}

var storedFields = []string{"version", "content_hash", "quarantined_at_ms"}

// Write applies items, each a model.DocEvent or a Quarantine keyed by
// document id. Items for the same document are folded in order into one
// write guarded by if_seq_no, so a concurrent writer (a zombie instance after
// a rebalance) cannot be overwritten blindly: its conflict forces a re-read and
// re-plan of just that document.
func (s *DocSink) Write(ctx context.Context, items []pipeline.Item) error {
	now := time.Now
	if s.Now != nil {
		now = s.Now
	}
	maxRounds := s.MaxRounds
	if maxRounds <= 0 {
		maxRounds = 5
	}

	byID := make(map[string][]any)
	var order []string
	for _, it := range items {
		if _, seen := byID[it.Key]; !seen {
			order = append(order, it.Key)
		}
		byID[it.Key] = append(byID[it.Key], it.Value)
	}

	var res Result
	pending := order
	for round := 0; len(pending) > 0; round++ {
		if round >= maxRounds {
			return fmt.Errorf("index: %d documents still conflicting after %d rounds", len(pending), maxRounds)
		}
		cur, err := s.load(ctx, pending)
		if err != nil {
			return err
		}

		nowMs := now().UnixMilli()
		var writes []Plan
		for _, id := range pending {
			plan, ok := PlanDoc(id, cur[id], byID[id], nowMs)
			if !ok {
				recordDiscards(plan, &res) // final: nothing to write
				continue
			}
			writes = append(writes, plan)
		}
		if len(writes) == 0 {
			break
		}

		body, err := encodeBulk(s.Index, writes)
		if err != nil {
			return err
		}
		resp, err := s.OS.Bulk(ctx, body)
		if err != nil {
			return err
		}
		results := resp.Results()
		if len(results) != len(writes) {
			return fmt.Errorf("index: bulk returned %d results for %d writes", len(results), len(writes))
		}

		ackMs := now().UnixMilli()
		var next []string
		for i, r := range results {
			p := writes[i]
			switch {
			case r.Status >= 200 && r.Status < 300:
				recordApplied(p, ackMs, &res)
				recordDiscards(p, &res)
			case r.Status == http.StatusConflict:
				metrics.CASConflicts.Inc()
				res.Conflicts++
				next = append(next, p.ID)
			default:
				reason := ""
				if r.Error != nil {
					reason = r.Error.Type + ": " + r.Error.Reason
				}
				return fmt.Errorf("index: write %s failed with status %d %s", p.ID, r.Status, reason)
			}
		}
		pending = next
	}

	if s.Observe != nil {
		s.Observe(res)
	}
	return nil
}

func recordApplied(p Plan, ackMs int64, res *Result) {
	if p.Quarantined {
		metrics.Quarantined.Inc()
		res.Quarantined++
	}
	for _, a := range p.Applied {
		metrics.DocsApplied.WithLabelValues(string(a.Op)).Inc()
		res.Applied++
		if a.Mode == model.ModeLive && a.SrcTsMs > 0 && ackMs >= a.SrcTsMs {
			metrics.IndexFreshness.WithLabelValues(a.Datasource).Observe(float64(ackMs-a.SrcTsMs) / 1000)
			res.FreshMs = append(res.FreshMs, ackMs-a.SrcTsMs)
		}
	}
	if len(p.Applied) > 0 && len(res.Sample) < 5 {
		res.Sample = append(res.Sample, p.ID)
	}
}

func recordDiscards(p Plan, res *Result) {
	for reason, n := range p.Discarded {
		metrics.DocsDiscarded.WithLabelValues(reason).Add(float64(n))
		res.Discarded += n
	}
}

// load reads the stored state of ids in realtime.
func (s *DocSink) load(ctx context.Context, ids []string) (map[string]Stored, error) {
	docs, err := s.OS.MGet(ctx, s.Index, ids, storedFields)
	if err != nil {
		return nil, err
	}
	out := make(map[string]Stored, len(docs))
	for _, d := range docs {
		if !d.Found {
			continue
		}
		var src struct {
			Version         int64  `json:"version"`
			ContentHash     string `json:"content_hash"`
			QuarantinedAtMs int64  `json:"quarantined_at_ms"`
		}
		if len(d.Source) > 0 {
			if err := json.Unmarshal(d.Source, &src); err != nil {
				return nil, fmt.Errorf("index: decode stored %s: %w", d.ID, err)
			}
		}
		out[d.ID] = Stored{
			Found: true, SeqNo: d.SeqNo, PrimaryTerm: d.PrimaryTerm,
			Version: src.Version, ContentHash: src.ContentHash, QuarantinedAtMs: src.QuarantinedAtMs,
		}
	}
	return out, nil
}

// encodeBulk renders plans as bulk operations: create for a new document
// (fails with 409 if another writer created it first), and a partial update
// guarded by if_seq_no for an existing one. Partial updates leave the
// embedding untouched, so a metadata or permission change never discards a
// vector that is still valid.
func encodeBulk(index string, plans []Plan) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, p := range plans {
		var meta, doc any
		if p.Create {
			meta = map[string]any{"create": map[string]any{"_index": index, "_id": p.ID}}
			doc = p.Fields
		} else {
			meta = map[string]any{"update": map[string]any{
				"_index": index, "_id": p.ID, "if_seq_no": p.SeqNo, "if_primary_term": p.PrimaryTerm,
			}}
			doc = map[string]any{"doc": p.Fields}
		}
		if err := enc.Encode(meta); err != nil {
			return nil, fmt.Errorf("index: encode bulk meta: %w", err)
		}
		if err := enc.Encode(doc); err != nil {
			return nil, fmt.Errorf("index: encode bulk doc %s: %w", p.ID, err)
		}
	}
	return buf.Bytes(), nil
}

// DecodeDocRecord decodes a docs-topic record value into a pipeline item.
// A compaction tombstone (nil value) is skipped.
func DecodeDocRecord(key, value []byte) (pipeline.Item, error) {
	if value == nil {
		return pipeline.Item{}, pipeline.ErrSkip
	}
	var ev model.DocEvent
	if err := json.Unmarshal(value, &ev); err != nil {
		return pipeline.Item{}, fmt.Errorf("decode doc event: %w", err)
	}
	if err := ev.Validate(); err != nil {
		return pipeline.Item{}, err
	}
	if string(key) != ev.Doc.ID {
		return pipeline.Item{}, errors.New("doc event key does not match doc id")
	}
	return pipeline.Item{Key: ev.Doc.ID, Value: ev}, nil
}
