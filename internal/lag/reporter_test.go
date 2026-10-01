package lag_test

import (
	"context"
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/khangpt2k6/Slipstream_CDC/internal/lag"
	"github.com/khangpt2k6/Slipstream_CDC/internal/metrics"
)

// fakeSource returns a scripted lag map (or an error) for one Report call.
type fakeSource struct {
	byTopic map[string]map[int32]int64
	err     error
	calls   int
}

func (f *fakeSource) Lag(context.Context) (map[string]map[int32]int64, error) {
	f.calls++
	return f.byTopic, f.err
}

func gauge(t *testing.T, topic, partition string) float64 {
	t.Helper()
	g, err := metrics.ConsumerLag.GetMetricWithLabelValues("g", topic, partition)
	if err != nil {
		t.Fatalf("get gauge %s[%s]: %v", topic, partition, err)
	}
	return testutil.ToFloat64(g)
}

func TestReportSetsGaugePerPartition(t *testing.T) {
	src := &fakeSource{byTopic: map[string]map[int32]int64{
		"ss.docs": {0: 42, 1: 7},
	}}

	lag.Report(context.Background(), "g", src)

	if got := gauge(t, "ss.docs", "0"); got != 42 {
		t.Errorf("lag orders[0] = %v, want 42", got)
	}
	if got := gauge(t, "ss.docs", "1"); got != 7 {
		t.Errorf("lag orders[1] = %v, want 7", got)
	}
}

// TestReportGoesToZeroWhenCaughtUp proves the "lag goes to zero when caught up"
// AC at the reporter level: a later sample of 0 overwrites a previous non-zero.
func TestReportGoesToZeroWhenCaughtUp(t *testing.T) {
	const topic = "ss.identity"
	lag.Report(context.Background(), "g", &fakeSource{byTopic: map[string]map[int32]int64{topic: {0: 100}}})
	if got := gauge(t, topic, "0"); got != 100 {
		t.Fatalf("setup lag = %v, want 100", got)
	}

	lag.Report(context.Background(), "g", &fakeSource{byTopic: map[string]map[int32]int64{topic: {0: 0}}})
	if got := gauge(t, topic, "0"); got != 0 {
		t.Errorf("lag after catch-up = %v, want 0", got)
	}
}

// TestReportSkipsOnError proves a sample error leaves the gauge untouched rather
// than crashing or zeroing it.
func TestReportSkipsOnError(t *testing.T) {
	const topic = "ss.errtopic"
	lag.Report(context.Background(), "g", &fakeSource{byTopic: map[string]map[int32]int64{topic: {3: 55}}})
	if got := gauge(t, topic, "3"); got != 55 {
		t.Fatalf("setup lag = %v, want 55", got)
	}

	src := &fakeSource{err: errors.New("metadata unavailable")}
	lag.Report(context.Background(), "g", src)
	if src.calls != 1 {
		t.Errorf("Lag called %d times, want 1", src.calls)
	}
	if got := gauge(t, topic, "3"); got != 55 {
		t.Errorf("lag after error = %v, want 55 (unchanged)", got)
	}
}
