// Package stats summarizes latency samples. The admin view and the
// measurement tools both report through it, so a percentile means the same
// thing everywhere it appears.
package stats

import "slices"

// Percentiles summarizes a latency sample in milliseconds.
type Percentiles struct {
	N   int   `json:"n"`
	P50 int64 `json:"p50"`
	P95 int64 `json:"p95"`
	P99 int64 `json:"p99"`
	Max int64 `json:"max"`
}

// Summarize computes nearest-rank percentiles of samples.
func Summarize(samples []int64) Percentiles {
	if len(samples) == 0 {
		return Percentiles{}
	}
	s := slices.Clone(samples)
	slices.Sort(s)
	at := func(q float64) int64 {
		i := int(q*float64(len(s))+0.999999) - 1
		return s[max(0, min(i, len(s)-1))]
	}
	return Percentiles{N: len(s), P50: at(0.50), P95: at(0.95), P99: at(0.99), Max: s[len(s)-1]}
}
