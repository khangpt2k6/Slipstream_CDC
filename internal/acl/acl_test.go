package acl

import (
	"context"
	"slices"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/khangpt2k6/Slipstream_CDC/internal/model"
	"github.com/khangpt2k6/Slipstream_CDC/internal/pipeline"
)

func setup(t *testing.T) (*Sink, *Expander) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return &Sink{RDB: rdb}, &Expander{RDB: rdb}
}

var version int64

func edge(from, to string, present bool) model.EdgeEvent {
	version++
	return model.EdgeEvent{From: from, To: to, Present: present, Version: version}
}

func apply(t *testing.T, s *Sink, edges ...model.EdgeEvent) {
	t.Helper()
	its := make([]pipeline.Item, len(edges))
	for i, e := range edges {
		its[i] = pipeline.Item{Key: e.Key(), Value: e}
	}
	if err := s.Write(context.Background(), its); err != nil {
		t.Fatalf("Write = %v", err)
	}
}

func expand(t *testing.T, x *Expander, user string) Expansion {
	t.Helper()
	exp, err := x.Expand(context.Background(), user)
	if err != nil {
		t.Fatalf("Expand(%s) = %v", user, err)
	}
	return exp
}

var (
	alice    = model.User("alice")
	eng      = model.Group("eng")
	platform = model.Group("platform")
	repo     = model.Container("github:golang/go")
	general  = model.Container("slack:general")
)

func TestNestedGroupsReachContainers(t *testing.T) {
	s, x := setup(t)
	apply(t, s,
		edge(alice, platform, true),
		edge(platform, eng, true),
		edge(eng, repo, true),
		edge(model.Everyone, general, true),
	)
	exp := expand(t, x, "alice")
	for _, p := range []string{alice, platform, eng, repo, general, model.Everyone} {
		if !exp.Has(p) {
			t.Errorf("alice missing %s; got %v", p, exp.Principals)
		}
	}
	if got := exp.PathTo(repo); !slices.Equal(got, []string{alice, platform, eng, repo}) {
		t.Errorf("path = %v, want alice -> platform -> eng -> repo", got)
	}
	if bob := expand(t, x, "bob"); bob.Has(repo) || !bob.Has(general) {
		t.Errorf("bob = %v, want the public channel only", bob.Principals)
	}
}

// TestRevokeIsVisibleOnNextExpand is the property the search path relies on:
// once an edge removal is applied, the very next expansion excludes it, even
// though the previous expansion was cached.
func TestRevokeIsVisibleOnNextExpand(t *testing.T) {
	s, x := setup(t)
	apply(t, s, edge(alice, eng, true), edge(eng, repo, true))
	if !expand(t, x, "alice").Has(repo) {
		t.Fatal("setup: alice cannot see repo")
	}
	apply(t, s, edge(alice, eng, false))
	if expand(t, x, "alice").Has(repo) {
		t.Fatal("cached expansion served after a revoke")
	}
}

func TestOlderEventCannotResurrectEdge(t *testing.T) {
	s, x := setup(t)
	grant := edge(alice, eng, true)
	revoke := edge(alice, eng, false)
	apply(t, s, grant, revoke)
	apply(t, s, grant) // late duplicate of the original grant
	if expand(t, x, "alice").Has(eng) {
		t.Fatal("delayed grant resurrected a revoked membership")
	}
}

func TestCycleIsSafe(t *testing.T) {
	s, x := setup(t)
	a, b := model.Group("a"), model.Group("b")
	apply(t, s, edge(alice, a, true), edge(a, b, true), edge(b, a, true), edge(b, repo, true))
	if !expand(t, x, "alice").Has(repo) {
		t.Error("cycle broke expansion")
	}
}

func TestEpochAdvancesOnlyOnApply(t *testing.T) {
	s, x := setup(t)
	g := edge(alice, eng, true)
	apply(t, s, g)
	e1, _ := x.Epoch(context.Background())
	apply(t, s, g) // duplicate: no change
	e2, _ := x.Epoch(context.Background())
	if e1 != e2 {
		t.Errorf("epoch moved on a duplicate: %d -> %d", e1, e2)
	}
}

func TestDecodeEdgeRecord(t *testing.T) {
	e := model.EdgeEvent{From: alice, To: eng, Present: true, Version: 3}
	good := []byte(`{"from":"u:alice","to":"g:eng","present":true,"version":3}`)
	if _, err := DecodeEdgeRecord([]byte(e.Key()), good); err != nil {
		t.Errorf("good record rejected: %v", err)
	}
	if _, err := DecodeEdgeRecord([]byte("wrong"), good); err == nil {
		t.Error("mismatched key accepted")
	}
	if _, err := DecodeEdgeRecord([]byte(e.Key()), nil); err != pipeline.ErrSkip {
		t.Error("tombstone not skipped")
	}
}
