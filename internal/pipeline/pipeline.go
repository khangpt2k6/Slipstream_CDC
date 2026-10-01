// Package pipeline is the at-least-once consume loop every Slipstream sink runs
// on: poll records from Kafka, decode each one, buffer, flush the buffer to an
// idempotent sink with capped backoff, and commit offsets only after the flush
// lands.
//
// The guarantees, in order of importance:
//
//   - No loss. Offsets are committed only after the sink acks the batch that
//     covers them, so a crash between flush and commit replays the batch.
//   - No wedge. A record that cannot be decoded is routed to a dead-letter topic
//     (and optionally turned into a quarantine item) instead of blocking the
//     partition. The dead-letter send must succeed before the offset advances.
//   - Bounded memory. The loop is a single goroutine; while a flush is failing
//     it retries in place and stops polling, so the buffer never grows past one
//     batch. That blocking is the backpressure.
//
// Exactly-once is the sink's job: replays are expected, so every sink must make
// a repeated write a no-op (versioned writes in the index, last-writer-wins
// edges in the identity graph).
package pipeline

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/khangpt2k6/Slipstream_CDC/internal/consumer"
	"github.com/khangpt2k6/Slipstream_CDC/internal/metrics"
	"github.com/khangpt2k6/Slipstream_CDC/internal/retry"
)

// ErrSkip marks a record that is correctly ignored (a compaction tombstone, an
// event type this sink does not handle). Its offset advances with the batch and
// it is not dead-lettered.
var ErrSkip = errors.New("pipeline: record intentionally skipped")

// Item is one decoded record ready for a sink. Value holds the sink-specific
// payload; Rec is the source record, kept for its key, timestamp and headers.
type Item struct {
	Key   string
	Value any
	Rec   *kgo.Record
}

// Sink writes a batch durably. A returned error leaves the batch in the buffer
// and the loop retries the same items, so Write must be idempotent.
type Sink interface {
	Write(ctx context.Context, items []Item) error
}

// Decoder maps a record to an Item. It returns ErrSkip (wrapped or bare) for an
// intentional skip; any other error marks the record as poison.
type Decoder func(*kgo.Record) (Item, error)

// Poller is the consume side of Kafka. *consumer.Consumer satisfies it.
type Poller interface {
	Poll(context.Context) ([]*kgo.Record, error)
	Commit(context.Context, []*kgo.Record) error
}

// DeadLetterer routes a poison record aside. *dlq.Producer satisfies it.
type DeadLetterer interface {
	Send(context.Context, *kgo.Record, error) error
}

// Config tunes one loop.
type Config struct {
	Name          string        // metric label, e.g. "docs"
	BatchSize     int           // items buffered before a size-triggered flush
	FlushInterval time.Duration // poll deadline, so an idle loop still flushes
	Retry         retry.Config  // flush backoff schedule
}

// Runner wires a loop's dependencies. Quarantine is optional: when set, a
// poison record that it accepts is still written to the sink as the returned
// item after being dead-lettered. The docs pipeline uses it to hide a document
// whose update could not be read, so a lost permission change fails closed.
type Runner struct {
	Config
	Poller     Poller
	DLQ        DeadLetterer
	Sink       Sink
	Decode     Decoder
	Quarantine func(*kgo.Record, error) (Item, bool)
	// OnFlush, when set, is called after every successful non-empty flush with
	// the items that landed. It runs on the loop goroutine, so keep it cheap.
	OnFlush func(items []Item)
}

// Run consumes until ctx is cancelled or the poller closes. It returns nil on
// a clean close, ctx.Err() on shutdown, and any fetch, commit, or dead-letter
// error that stops the loop.
func (r *Runner) Run(ctx context.Context) error {
	var (
		pending []*kgo.Record // polled records not yet committed
		buf     []Item        // decoded items not yet flushed
	)
	name := r.Name

	commit := func(cctx context.Context) error {
		if len(pending) == 0 {
			return nil
		}
		if err := r.Poller.Commit(cctx, pending); err != nil {
			return err
		}
		pending = pending[:0]
		return nil
	}

	// flush is the single flush chokepoint. It retries until the sink accepts
	// the batch or fctx ends. A failed attempt leaves buf intact, so the retry
	// re-drives exactly the same items.
	flush := func(fctx context.Context) error {
		if len(buf) == 0 {
			return nil
		}
		err := retry.Do(fctx, r.Retry, func(attempt int, delay time.Duration, err error) {
			metrics.SinkRetries.WithLabelValues(name).Inc()
			slog.Warn("sink flush failed; backing off",
				"pipeline", name, "attempt", attempt, "delay", delay.String(), "err", err)
		}, func() error {
			start := time.Now()
			werr := r.Sink.Write(fctx, buf)
			metrics.FlushDuration.WithLabelValues(name).Observe(time.Since(start).Seconds())
			return werr
		})
		if err != nil {
			return err
		}
		metrics.BatchesFlushed.WithLabelValues(name).Inc()
		metrics.ItemsWritten.WithLabelValues(name).Add(float64(len(buf)))
		if r.OnFlush != nil {
			r.OnFlush(buf)
		}
		buf = buf[:0]
		return nil
	}

	for {
		if err := ctx.Err(); err != nil {
			// Best-effort final flush and commit, off the cancelled ctx.
			shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if ferr := flush(shutCtx); ferr == nil {
				_ = commit(shutCtx)
			}
			cancel()
			return err
		}

		pollCtx, cancel := context.WithTimeout(ctx, r.FlushInterval)
		recs, err := r.Poller.Poll(pollCtx)
		cancel()
		if errors.Is(err, consumer.ErrClosed) {
			return nil
		}
		if err != nil {
			return err
		}
		metrics.EventsConsumed.WithLabelValues(name).Add(float64(len(recs)))

		for _, rec := range recs {
			pending = append(pending, rec)
			it, err := r.Decode(rec)
			switch {
			case errors.Is(err, ErrSkip):
				continue
			case err != nil:
				// Poison: the dead-letter send must succeed before this
				// offset may advance, so a send failure stops the loop and
				// the record replays on restart. Nothing is dropped.
				if derr := r.DLQ.Send(ctx, rec, err); derr != nil {
					return derr
				}
				metrics.DLQTotal.WithLabelValues(name).Inc()
				slog.Warn("routed undecodable record to DLQ", "pipeline", name,
					"topic", rec.Topic, "partition", rec.Partition, "offset", rec.Offset, "err", err)
				if r.Quarantine == nil {
					continue
				}
				q, ok := r.Quarantine(rec, err)
				if !ok {
					continue
				}
				it = q
			}
			buf = append(buf, it)
			if len(buf) >= r.BatchSize {
				if err := flush(ctx); err != nil {
					return err
				}
				if err := commit(ctx); err != nil {
					return err
				}
			}
		}

		// End of a poll: flush what is buffered, then commit everything polled.
		// After a successful flush every pending record is either written or
		// intentionally skipped.
		if err := flush(ctx); err != nil {
			return err
		}
		if err := commit(ctx); err != nil {
			return err
		}
		metrics.BufferedItems.WithLabelValues(name).Set(float64(len(buf)))
	}
}
