package check

import (
	"context"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/khangpt2k6/Slipstream_CDC/internal/connectors"
	"github.com/khangpt2k6/Slipstream_CDC/internal/kafkax"
	"github.com/khangpt2k6/Slipstream_CDC/internal/model"
)

// ReplayResult is what replay writes.
type ReplayResult struct {
	Events   int     `json:"events"`
	Seconds  float64 `json:"seconds"`
	Rate     float64 `json:"rate"`
	DrainSec float64 `json:"drain_sec"`
}

// Replay re-produces connector dumps into the docs topic, unchanged, so an
// index can be rebuilt (or a fresh stack loaded) without crawling the
// sources again. Against an index that already holds the documents, every
// event is a duplicate and the index does not change: a full replay is the
// idempotency test at corpus scale.
func Replay(ctx context.Context, s *Stack, dumps []string, rate int) (ReplayResult, error) {
	var res ReplayResult
	var recs []*kgo.Record
	for _, p := range dumps {
		if err := connectors.ReadDump(p, func(ev model.DocEvent) error {
			ev.Mode = model.ModeReplay
			r, err := kafkax.Record(model.TopicDocs, ev.Doc.ID, ev)
			if err != nil {
				return err
			}
			recs = append(recs, r)
			return nil
		}); err != nil {
			return res, err
		}
	}
	res.Events = len(recs)
	start := time.Now()
	if err := produce(ctx, s.Prod, recs, rate); err != nil {
		return res, err
	}
	res.Seconds = time.Since(start).Seconds()
	res.Rate = float64(res.Events) / res.Seconds
	slog.Info("replayed", "events", res.Events, "rate", int(res.Rate))
	if _, ok, err := waitFor(ctx, 100*time.Millisecond, 30*time.Minute, func() (bool, error) {
		lag, err := s.Lag(ctx, "ss-index-docs")
		return err == nil && lag == 0, nil
	}); err != nil || !ok {
		return res, err
	}
	res.DrainSec = time.Since(start).Seconds()
	return res, nil
}
