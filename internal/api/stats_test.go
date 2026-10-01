package api

import "testing"

func TestSummarizeNearestRank(t *testing.T) {
	var s []int64
	for i := int64(1); i <= 100; i++ {
		s = append(s, i)
	}
	p := Summarize(s)
	if p.N != 100 || p.P50 != 50 || p.P95 != 95 || p.P99 != 99 || p.Max != 100 {
		t.Errorf("Summarize(1..100) = %+v", p)
	}
	if got := Summarize([]int64{7}); got.P50 != 7 || got.P99 != 7 {
		t.Errorf("single sample = %+v", got)
	}
	if got := Summarize(nil); got.N != 0 {
		t.Errorf("empty = %+v", got)
	}
}
