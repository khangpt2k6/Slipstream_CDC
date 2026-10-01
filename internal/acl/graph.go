// Package acl is the permission graph: a materialized view of the identity
// topic kept in Redis, plus the query-time expansion that turns a user into
// the set of principals a search filters on.
//
// Documents store only unexpanded principals (usually their container). A
// user's principals are found by walking the graph from the user through
// groups (and nested groups) to containers. Because nothing is expanded at
// index time, revoking a membership or a container grant is one edge update:
// no document is rewritten, and the very next query sees the change.
//
// Redis layout:
//
//	ss:out:<principal>  SET of principals the principal holds (its out edges)
//	ss:edgever          HASH edge key -> version of its latest applied change
//	ss:idver            counter bumped on every applied change (cache epoch)
//
// The view is rebuildable: replaying the compacted identity topic from the
// start reproduces it exactly, because every change is last-writer-wins by
// version.
package acl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/khangpt2k6/Slipstream_CDC/internal/metrics"
	"github.com/khangpt2k6/Slipstream_CDC/internal/model"
	"github.com/khangpt2k6/Slipstream_CDC/internal/pipeline"
)

// Redis keys.
const (
	KeyOutPrefix = "ss:out:"
	KeyEdgeVer   = "ss:edgever"
	KeyIDVer     = "ss:idver"
)

// applyEdge is last-writer-wins on one edge. Versions are compared per edge,
// and the version of a removed edge is kept, so a delayed older "present"
// cannot resurrect an edge that was revoked later.
var applyEdge = redis.NewScript(`
local cur = redis.call('HGET', KEYS[1], ARGV[1])
if cur and tonumber(cur) >= tonumber(ARGV[3]) then
  return 0
end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[3])
if ARGV[4] == '1' then
  redis.call('SADD', KEYS[2], ARGV[2])
else
  redis.call('SREM', KEYS[2], ARGV[2])
end
redis.call('INCR', KEYS[3])
return 1
`)

// Change is one applied edge change, reported to the observer.
type Change struct {
	model.EdgeEvent
	AppliedMs int64
}

// Sink applies identity edge events to the graph. It satisfies pipeline.Sink.
type Sink struct {
	RDB     *redis.Client
	Now     func() time.Time
	Observe func([]Change) // optional, called with the changes that applied
	loaded  bool
}

// Write applies items (model.EdgeEvent values) in one round trip. Changes to
// the same edge share a Kafka key, so they arrive in order; the version check
// makes replays and duplicates no-ops.
func (s *Sink) Write(ctx context.Context, items []pipeline.Item) error {
	if !s.loaded {
		if err := applyEdge.Load(ctx, s.RDB).Err(); err != nil {
			return fmt.Errorf("acl: load script: %w", err)
		}
		s.loaded = true
	}

	pipe := s.RDB.Pipeline()
	cmds := make([]*redis.Cmd, len(items))
	for i, it := range items {
		e := it.Value.(model.EdgeEvent)
		present := "0"
		if e.Present {
			present = "1"
		}
		cmds[i] = applyEdge.EvalSha(ctx, pipe,
			[]string{KeyEdgeVer, KeyOutPrefix + e.From, KeyIDVer},
			e.Key(), e.To, e.Version, present)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		if strings.Contains(err.Error(), "NOSCRIPT") {
			s.loaded = false // Redis restarted and lost the script; reload on retry
		}
		return fmt.Errorf("acl: apply edges: %w", err)
	}

	now := time.Now
	if s.Now != nil {
		now = s.Now
	}
	nowMs := now().UnixMilli()
	var applied []Change
	for i, c := range cmds {
		n, err := c.Int()
		if err != nil {
			return fmt.Errorf("acl: apply edge result: %w", err)
		}
		e := items[i].Value.(model.EdgeEvent)
		if n == 0 {
			metrics.EdgesApplied.WithLabelValues("stale").Inc()
			continue
		}
		metrics.EdgesApplied.WithLabelValues("applied").Inc()
		if e.SrcTsMs > 0 && nowMs >= e.SrcTsMs {
			metrics.ACLPropagation.Observe(float64(nowMs-e.SrcTsMs) / 1000)
		}
		applied = append(applied, Change{EdgeEvent: e, AppliedMs: nowMs})
	}
	if s.Observe != nil && len(applied) > 0 {
		s.Observe(applied)
	}
	return nil
}

// DecodeEdgeRecord decodes an identity-topic record into a pipeline item. A
// compaction tombstone (nil value) is skipped.
func DecodeEdgeRecord(key, value []byte) (pipeline.Item, error) {
	if value == nil {
		return pipeline.Item{}, pipeline.ErrSkip
	}
	var e model.EdgeEvent
	if err := json.Unmarshal(value, &e); err != nil {
		return pipeline.Item{}, fmt.Errorf("decode edge event: %w", err)
	}
	if err := e.Validate(); err != nil {
		return pipeline.Item{}, err
	}
	if string(key) != e.Key() {
		return pipeline.Item{}, errors.New("edge event key does not match its endpoints")
	}
	return pipeline.Item{Key: e.Key(), Value: e}, nil
}
