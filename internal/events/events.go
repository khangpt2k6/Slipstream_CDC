// Package events is the live feed between the indexer and the API: a Redis
// pub/sub channel of small JSON events (a batch was indexed, a permission
// changed) that the API fans out to browsers over server-sent events, plus
// rolling latency samples the admin view turns into percentiles.
//
// The feed is best effort. It is observability, never part of the data path:
// a dropped event changes what the UI animates, not what search returns.
package events

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

// Channel is the pub/sub channel.
const Channel = "ss:events"

// Sample list keys and their size cap.
const (
	SamplesIndex = "ss:samples:index" // live doc change -> index ack, ms
	SamplesACL   = "ss:samples:acl"   // identity commit -> graph apply, ms
	maxSamples   = 4096
)

// Event is one feed item. Type is "index" or "acl".
type Event struct {
	Type string `json:"type"`
	TsMs int64  `json:"ts_ms"`

	// index
	Applied     int      `json:"applied,omitempty"`
	Discarded   int      `json:"discarded,omitempty"`
	Conflicts   int      `json:"conflicts,omitempty"`
	Quarantined int      `json:"quarantined,omitempty"`
	Sample      []string `json:"sample,omitempty"`

	// acl
	From      string `json:"from,omitempty"`
	To        string `json:"to,omitempty"`
	Present   bool   `json:"present,omitempty"`
	LatencyMs int64  `json:"latency_ms,omitempty"`
}

// Publisher writes events and samples to Redis.
type Publisher struct {
	RDB *redis.Client
}

// Publish sends evs and appends samples (ms) under key, in one round trip.
func (p *Publisher) Publish(evs []Event, key string, samples []int64) {
	if p == nil || p.RDB == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	pipe := p.RDB.Pipeline()
	for _, e := range evs {
		b, err := json.Marshal(e)
		if err != nil {
			continue
		}
		pipe.Publish(ctx, Channel, b)
	}
	if len(samples) > 0 {
		vals := make([]any, len(samples))
		for i, s := range samples {
			vals[i] = s
		}
		pipe.LPush(ctx, key, vals...)
		pipe.LTrim(ctx, key, 0, maxSamples-1)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		slog.Debug("publish live events", "err", err)
	}
}
