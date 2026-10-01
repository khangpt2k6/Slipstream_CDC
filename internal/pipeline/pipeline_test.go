package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/khangpt2k6/Slipstream_CDC/internal/consumer"
	"github.com/khangpt2k6/Slipstream_CDC/internal/metrics"
	"github.com/khangpt2k6/Slipstream_CDC/internal/retry"
)

// fakePoller yields one scripted batch per Poll, then ErrClosed so Run exits.
type fakePoller struct {
	batches   [][]*kgo.Record
	next      int
	committed []*kgo.Record
}

func (f *fakePoller) Poll(context.Context) ([]*kgo.Record, error) {
	if f.next >= len(f.batches) {
		return nil, consumer.ErrClosed
	}
	b := f.batches[f.next]
	f.next++
	return b, nil
}

func (f *fakePoller) Commit(_ context.Context, recs []*kgo.Record) error {
	f.committed = append(f.committed, recs...)
	return nil
}

type fakeDLQ struct {
	sent []*kgo.Record
	err  error
}

func (f *fakeDLQ) Send(_ context.Context, rec *kgo.Record, _ error) error {
	if f.err != nil {
		return f.err
	}
	f.sent = append(f.sent, rec)
	return nil
}

// fakeSink fails failuresLeft times, then accepts. It records each accepted
// batch and the largest batch it was ever offered.
type fakeSink struct {
	failuresLeft int
	err          error
	written      [][]string
	maxOffered   int
}

func (s *fakeSink) Write(_ context.Context, items []Item) error {
	if len(items) > s.maxOffered {
		s.maxOffered = len(items)
	}
	if s.failuresLeft > 0 {
		s.failuresLeft--
		return s.err
	}
	keys := make([]string, len(items))
	for i, it := range items {
		keys[i] = it.Key
	}
	s.written = append(s.written, keys)
	return nil
}

func (s *fakeSink) keys() []string {
	var out []string
	for _, b := range s.written {
		out = append(out, b...)
	}
	return out
}

// decode treats "skip" as ErrSkip, "bad*" as poison, anything else as a key.
func decode(r *kgo.Record) (Item, error) {
	v := string(r.Value)
	switch {
	case v == "skip":
		return Item{}, ErrSkip
	case strings.HasPrefix(v, "bad"):
		return Item{}, errors.New("cannot decode " + v)
	}
	return Item{Key: v, Rec: r}, nil
}

func rec(v string) *kgo.Record { return &kgo.Record{Topic: "ss.docs", Key: []byte(v), Value: []byte(v)} }

func recs(vs ...string) []*kgo.Record {
	out := make([]*kgo.Record, len(vs))
	for i, v := range vs {
		out[i] = rec(v)
	}
	return out
}

func newRunner(name string, batches [][]*kgo.Record, dl DeadLetterer, sink Sink) (*Runner, *fakePoller) {
	fp := &fakePoller{batches: batches}
	return &Runner{
		Config: Config{
			Name:          name,
			BatchSize:     1000,
			FlushInterval: 10 * time.Millisecond,
			Retry:         retry.Config{Base: time.Millisecond, Max: 4 * time.Millisecond},
		},
		Poller: fp,
		DLQ:    dl,
		Sink:   sink,
		Decode: decode,
	}, fp
}

func TestPoisonIsDeadLetteredAndLoopContinues(t *testing.T) {
	dl := &fakeDLQ{}
	sink := &fakeSink{}
	r, fp := newRunner("t-poison", [][]*kgo.Record{recs("a", "bad1", "b")}, dl, sink)

	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run = %v, want nil", err)
	}
	if len(dl.sent) != 1 || string(dl.sent[0].Value) != "bad1" {
		t.Fatalf("DLQ got %v, want only bad1", dl.sent)
	}
	if got := sink.keys(); strings.Join(got, ",") != "a,b" {
		t.Errorf("sink got %v, want [a b] (good records survive the poison one)", got)
	}
	if len(fp.committed) != 3 {
		t.Errorf("committed %d records, want 3", len(fp.committed))
	}
	if got := testutil.ToFloat64(metrics.DLQTotal.WithLabelValues("t-poison")); got != 1 {
		t.Errorf("ss_dlq_total = %v, want 1", got)
	}
}

func TestSkipsAreCommittedButNotDeadLettered(t *testing.T) {
	dl := &fakeDLQ{}
	sink := &fakeSink{}
	r, fp := newRunner("t-skip", [][]*kgo.Record{recs("skip", "skip", "a")}, dl, sink)

	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run = %v", err)
	}
	if len(dl.sent) != 0 {
		t.Errorf("DLQ got %d records, want 0", len(dl.sent))
	}
	if len(fp.committed) != 3 {
		t.Errorf("committed %d, want 3 (skips advance the offset)", len(fp.committed))
	}
}

// TestDLQFailureBlocksOffset is the no-drop contract: a failed dead-letter
// send stops the loop without committing past the poison record.
func TestDLQFailureBlocksOffset(t *testing.T) {
	sendErr := errors.New("dlq unavailable")
	r, fp := newRunner("t-dlqfail", [][]*kgo.Record{recs("bad")}, &fakeDLQ{err: sendErr}, &fakeSink{})

	if err := r.Run(context.Background()); !errors.Is(err, sendErr) {
		t.Fatalf("Run = %v, want the DLQ send error", err)
	}
	if len(fp.committed) != 0 {
		t.Errorf("committed %d, want 0", len(fp.committed))
	}
}

// TestQuarantineItemIsWritten checks a poison record the quarantine hook
// accepts still reaches the sink as the hook's item, after the DLQ send.
func TestQuarantineItemIsWritten(t *testing.T) {
	dl := &fakeDLQ{}
	sink := &fakeSink{}
	r, _ := newRunner("t-quarantine", [][]*kgo.Record{recs("a", "bad-doc-7")}, dl, sink)
	r.Quarantine = func(rec *kgo.Record, _ error) (Item, bool) {
		return Item{Key: "quarantine:" + string(rec.Key), Rec: rec}, true
	}

	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run = %v", err)
	}
	if len(dl.sent) != 1 {
		t.Fatalf("DLQ got %d, want 1 (quarantine does not replace dead-lettering)", len(dl.sent))
	}
	if got := strings.Join(sink.keys(), ","); got != "a,quarantine:bad-doc-7" {
		t.Errorf("sink got %s, want a,quarantine:bad-doc-7", got)
	}
}

// TestStalledSinkIsBoundedAndResumes models a paused sink: flushes fail twice,
// then succeed. The buffer never exceeds the batch size, the retries are
// counted, and offsets commit only once the data lands.
func TestStalledSinkIsBoundedAndResumes(t *testing.T) {
	sink := &fakeSink{failuresLeft: 2, err: errors.New("opensearch stalled")}
	r, fp := newRunner("t-stall", [][]*kgo.Record{recs("a", "b", "c", "d", "e", "f")}, &fakeDLQ{}, sink)
	r.BatchSize = 3

	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run = %v, want nil after recovery", err)
	}
	if sink.maxOffered > 3 {
		t.Errorf("sink offered %d items at once, want <= 3 (bounded buffer)", sink.maxOffered)
	}
	if got := strings.Join(sink.keys(), ","); got != "a,b,c,d,e,f" {
		t.Errorf("sink got %s, want every item exactly once in order", got)
	}
	if got := testutil.ToFloat64(metrics.SinkRetries.WithLabelValues("t-stall")); got != 2 {
		t.Errorf("ss_sink_retries_total = %v, want 2", got)
	}
	if len(fp.committed) != 6 {
		t.Errorf("committed %d, want 6", len(fp.committed))
	}
}

// TestNoCommitWithoutFlush checks the core ordering: when the sink never
// recovers and the loop is shut down, nothing is committed.
func TestNoCommitWithoutFlush(t *testing.T) {
	sink := &fakeSink{failuresLeft: 1 << 30, err: errors.New("never recovers")}
	r, fp := newRunner("t-nocommit", [][]*kgo.Record{recs("a")}, &fakeDLQ{}, sink)
	r.Retry = retry.Config{Base: time.Hour, Max: time.Hour}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	if err := r.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Error("Run did not unblock promptly on shutdown")
	}
	if len(fp.committed) != 0 {
		t.Errorf("committed %d, want 0", len(fp.committed))
	}
}

func TestOnFlushSeesLandedItems(t *testing.T) {
	var seen []string
	r, _ := newRunner("t-onflush", [][]*kgo.Record{recs("a", "b")}, &fakeDLQ{}, &fakeSink{})
	r.OnFlush = func(items []Item) {
		for _, it := range items {
			seen = append(seen, it.Key)
		}
	}
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run = %v", err)
	}
	if strings.Join(seen, ",") != "a,b" {
		t.Errorf("OnFlush saw %v, want [a b]", seen)
	}
}
