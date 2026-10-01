// Package kafkax holds the Kafka pieces producers share: topic bootstrap and
// an idempotent, keyed JSON producer.
package kafkax

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/khangpt2k6/Slipstream_CDC/internal/model"
)

// Topic describes a topic to create if it is missing.
type Topic struct {
	Name       string
	Partitions int32
	Compact    bool
}

// StandardTopics returns the topics every deployment needs: documents keyed
// by id, the compacted identity graph keyed by edge, and their dead-letter
// topics.
func StandardTopics(partitions int32, dlqSuffix string) []Topic {
	return []Topic{
		{Name: model.TopicDocs, Partitions: partitions},
		{Name: model.TopicIdentity, Partitions: partitions, Compact: true},
		{Name: model.TopicDocs + dlqSuffix, Partitions: 1},
		{Name: model.TopicIdentity + dlqSuffix, Partitions: 1},
	}
}

// EnsureTopics creates any missing topics. Topics that already exist are left
// alone, so every service can call it at startup.
func EnsureTopics(ctx context.Context, brokers []string, topics []Topic) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return fmt.Errorf("kafkax: client: %w", err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)

	for _, t := range topics {
		cfg := map[string]*string{}
		if t.Compact {
			compact := "compact"
			lag := "60000"
			cfg["cleanup.policy"] = &compact
			cfg["min.compaction.lag.ms"] = &lag
		}
		resp, err := adm.CreateTopic(ctx, t.Partitions, 1, cfg, t.Name)
		if err == nil {
			err = resp.Err
		}
		// kadm surfaces a per-topic error through both err and resp.Err.
		if err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
			return fmt.Errorf("kafkax: create %s: %w", t.Name, err)
		}
	}
	return nil
}

// Producer publishes JSON values keyed for ordering. It is idempotent (no
// duplicates from producer retries) and waits for all in-sync replicas.
type Producer struct {
	cl *kgo.Client
}

// NewProducer dials brokers. linger trades latency for batching: connectors
// that push live changes use 0, the bulk loader uses a few milliseconds.
func NewProducer(brokers []string, linger time.Duration, opts ...kgo.Opt) (*Producer, error) {
	base := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ProducerLinger(linger),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchMaxBytes(8 << 20),
		kgo.MaxBufferedRecords(1 << 16),
	}
	cl, err := kgo.NewClient(append(base, opts...)...)
	if err != nil {
		return nil, fmt.Errorf("kafkax: producer: %w", err)
	}
	return &Producer{cl: cl}, nil
}

// Record encodes v as a record on topic with key.
func Record(topic, key string, v any) (*kgo.Record, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("kafkax: encode %s: %w", key, err)
	}
	return &kgo.Record{Topic: topic, Key: []byte(key), Value: b}, nil
}

// SendSync produces records and waits for every ack.
func (p *Producer) SendSync(ctx context.Context, recs ...*kgo.Record) error {
	if err := p.cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		return fmt.Errorf("kafkax: produce: %w", err)
	}
	return nil
}

// SendAsync produces rec without waiting; done (optional) gets the result.
// Use Flush to wait for everything in flight.
func (p *Producer) SendAsync(ctx context.Context, rec *kgo.Record, done func(error)) {
	p.cl.Produce(ctx, rec, func(_ *kgo.Record, err error) {
		if done != nil {
			done(err)
		}
	})
}

// Flush waits until every buffered record is acked.
func (p *Producer) Flush(ctx context.Context) error { return p.cl.Flush(ctx) }

// Docs produces document events keyed by document id.
func (p *Producer) Docs(ctx context.Context, evs ...model.DocEvent) error {
	recs := make([]*kgo.Record, 0, len(evs))
	for _, e := range evs {
		r, err := Record(model.TopicDocs, e.Doc.ID, e)
		if err != nil {
			return err
		}
		recs = append(recs, r)
	}
	return p.SendSync(ctx, recs...)
}

// Edges produces identity edge events keyed by edge.
func (p *Producer) Edges(ctx context.Context, evs ...model.EdgeEvent) error {
	recs := make([]*kgo.Record, 0, len(evs))
	for _, e := range evs {
		r, err := Record(model.TopicIdentity, e.Key(), e)
		if err != nil {
			return err
		}
		recs = append(recs, r)
	}
	return p.SendSync(ctx, recs...)
}

// Close flushes and closes the client.
func (p *Producer) Close() { p.cl.Close() }
