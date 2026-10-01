package search

import (
	"encoding/json"
	"testing"

	"github.com/khangpt2k6/Slipstream_CDC/internal/acl"
	"github.com/khangpt2k6/Slipstream_CDC/internal/opensearch"
)

func hits(ids ...string) []opensearch.Hit {
	out := make([]opensearch.Hit, len(ids))
	for i, id := range ids {
		out[i] = opensearch.Hit{ID: id}
	}
	return out
}

func ids(fs []Fused) []string {
	out := make([]string, len(fs))
	for i, f := range fs {
		out[i] = f.Hit.ID
	}
	return out
}

func TestFuseRewardsAgreement(t *testing.T) {
	// b is second in both lists; a and c are each first in only one.
	got := Fuse(map[string][]opensearch.Hit{
		"bm25": hits("a", "b", "d"),
		"knn":  hits("c", "b", "e"),
	}, 60)
	if got[0].Hit.ID != "b" {
		t.Fatalf("top = %s, want b (ranked well by both retrievers); order %v", got[0].Hit.ID, ids(got))
	}
	if got[0].Ranks["bm25"] != 2 || got[0].Ranks["knn"] != 2 {
		t.Errorf("ranks = %v", got[0].Ranks)
	}
	if len(got) != 5 {
		t.Errorf("fused %d docs, want 5 distinct", len(got))
	}
}

func TestFuseIsDeterministicOnTies(t *testing.T) {
	lists := map[string][]opensearch.Hit{"bm25": hits("x"), "knn": hits("y")}
	first := ids(Fuse(lists, 60))
	for range 20 {
		if got := ids(Fuse(lists, 60)); got[0] != first[0] || got[1] != first[1] {
			t.Fatalf("tie order changed: %v vs %v", got, first)
		}
	}
}

func TestFuseSingleListKeepsOrder(t *testing.T) {
	got := ids(Fuse(map[string][]opensearch.Hit{"bm25": hits("c", "a", "b")}, 60))
	if got[0] != "c" || got[1] != "a" || got[2] != "b" {
		t.Errorf("order = %v, want engine order", got)
	}
}

func TestFilterAlwaysExcludesDeletedAndQuarantined(t *testing.T) {
	b, _ := json.Marshal(Filter([]string{"u:a"}, nil))
	for _, want := range []string{`"allowed":["u:a"]`, `"deleted":false`, `"quarantined":false`} {
		if !contains(string(b), want) {
			t.Errorf("filter %s missing %s", b, want)
		}
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func TestParseMode(t *testing.T) {
	if m, _ := ParseMode(""); m != ModeHybrid {
		t.Errorf("default mode = %s, want hybrid", m)
	}
	if _, err := ParseMode("magic"); err == nil {
		t.Error("unknown mode accepted")
	}
}

// TestPostCheckDropsUnauthorized: even if the engine returned a hit the user
// cannot see (a filter bug, a stale replica), the final re-check removes it.
func TestPostCheckDropsUnauthorized(t *testing.T) {
	exp := acl.Expansion{User: "u:dave", Principals: []string{"g:everyone", "u:dave"}}
	mk := func(src string) Fused { return Fused{Hit: opensearch.Hit{ID: "d", Source: json.RawMessage(src)}} }

	if _, ok := toHit(mk(`{"id":"d","allowed":["c:secret"]}`), exp); ok {
		t.Error("hit without a held principal survived the re-check")
	}
	if _, ok := toHit(mk(`{"id":"d","allowed":["g:everyone"],"deleted":true}`), exp); ok {
		t.Error("deleted hit survived")
	}
	if _, ok := toHit(mk(`{"id":"d","allowed":["g:everyone"],"quarantined":true}`), exp); ok {
		t.Error("quarantined hit survived")
	}
	h, ok := toHit(mk(`{"id":"d","allowed":["c:secret","g:everyone"]}`), exp)
	if !ok || h.Matched != "g:everyone" {
		t.Errorf("public hit: ok=%v matched=%q", ok, h.Matched)
	}
}
