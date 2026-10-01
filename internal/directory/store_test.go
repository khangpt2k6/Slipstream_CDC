package directory

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/khangpt2k6/Slipstream_CDC/internal/model"
)

type fakePub struct {
	mu   sync.Mutex
	evs  []model.EdgeEvent
	fail bool
}

func (f *fakePub) Edges(_ context.Context, evs ...model.EdgeEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail {
		return errors.New("kafka down")
	}
	f.evs = append(f.evs, evs...)
	return nil
}

func open(t *testing.T, pub Publisher) *Store {
	t.Helper()
	s, err := Open(context.Background(), filepath.Join(t.TempDir(), "dir.db"), pub)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestSeedIsDeterministicAndIdempotent(t *testing.T) {
	ctx := context.Background()
	a, b := open(t, &fakePub{}), open(t, &fakePub{})
	if ok, err := a.Seed(ctx, 7, 50); !ok || err != nil {
		t.Fatalf("seed a: %v %v", ok, err)
	}
	if _, err := b.Seed(ctx, 7, 50); err != nil {
		t.Fatal(err)
	}
	ua, _ := a.Users(ctx, "", 0)
	ub, _ := b.Users(ctx, "", 0)
	if len(ua) != 55 || len(ua) != len(ub) {
		t.Fatalf("users: %d vs %d, want 55", len(ua), len(ub))
	}
	for i := range ua {
		if ua[i].ID != ub[i].ID || !slices.Equal(ua[i].Groups, ub[i].Groups) {
			t.Fatalf("seed differs at %d: %+v vs %+v", i, ua[i], ub[i])
		}
	}
	if ok, _ := a.Seed(ctx, 7, 50); ok {
		t.Error("second seed ran on a populated directory")
	}
}

func TestPrincipalsFollowNesting(t *testing.T) {
	ctx := context.Background()
	s := open(t, &fakePub{})
	if _, err := s.Seed(ctx, 1, 0); err != nil {
		t.Fatal(err)
	}
	got := s.Principals("alice")
	for _, want := range []string{"u:alice", "g:platform", "g:kafka-core", "g:eng", model.Everyone} {
		if _, ok := slices.BinarySearch(got, want); !ok {
			t.Errorf("alice missing %s in %v", want, got)
		}
	}
	if _, ok := slices.BinarySearch(s.Principals("dave"), "g:eng"); ok {
		t.Error("contractor dave is in eng")
	}
}

func TestRegisterContainerPolicy(t *testing.T) {
	ctx := context.Background()
	s := open(t, &fakePub{})
	if _, err := s.Seed(ctx, 1, 0); err != nil {
		t.Fatal(err)
	}
	mustRegister := func(c Container) {
		t.Helper()
		if _, err := s.RegisterContainer(ctx, c); err != nil {
			t.Fatal(err)
		}
	}
	mustRegister(Container{ID: "slack:Cpublic", Datasource: "slack", Visibility: "public"})
	mustRegister(Container{ID: "slack:Cprivate", Datasource: "slack", Visibility: "members"})

	if !s.CanSee("dave", []string{model.Container("slack:Cpublic")}) {
		t.Error("public channel hidden from dave")
	}
	if s.CanSee("alice", []string{model.Container("slack:Cprivate")}) {
		t.Error("members-only channel visible without membership")
	}
	if _, err := s.SetEdges(ctx, []Edge{{From: "u:alice", To: model.Container("slack:Cprivate"), Present: true}}); err != nil {
		t.Fatal(err)
	}
	if !s.CanSee("alice", []string{model.Container("slack:Cprivate")}) {
		t.Error("membership edge did not grant access")
	}

	// Auto policy is stable across calls and never re-applied.
	isNew, _ := s.RegisterContainer(ctx, Container{ID: "slack:Cpublic", Datasource: "slack"})
	if isNew {
		t.Error("re-registering reported a new container")
	}
}

// TestOutboxRedelivers: a change committed while Kafka is down is published
// by the next RepublishPending, so a committed revoke is never lost.
func TestOutboxRedelivers(t *testing.T) {
	ctx := context.Background()
	pub := &fakePub{fail: true}
	s := open(t, pub)
	evs, err := s.SetEdges(ctx, []Edge{{From: "u:x", To: "g:y", Present: false}})
	if err == nil || len(evs) != 1 {
		t.Fatalf("SetEdges with kafka down = %v, %v; want committed events and an error", evs, err)
	}
	pub.fail = false
	n, err := s.RepublishPending(ctx)
	if err != nil || n != 1 {
		t.Fatalf("RepublishPending = %d, %v; want 1", n, err)
	}
	if len(pub.evs) != 1 || pub.evs[0].Version != evs[0].Version {
		t.Errorf("published %v, want the committed change", pub.evs)
	}
	if n, _ := s.RepublishPending(ctx); n != 0 {
		t.Errorf("RepublishPending republished %d acked changes", n)
	}
}

func TestVersionsAreMonotonic(t *testing.T) {
	ctx := context.Background()
	s := open(t, &fakePub{})
	var last int64
	for i := range 100 {
		evs, err := s.SetEdges(ctx, []Edge{{From: "u:a", To: "g:b", Present: i%2 == 0}})
		if err != nil {
			t.Fatal(err)
		}
		if evs[0].Version <= last {
			t.Fatalf("version %d not after %d", evs[0].Version, last)
		}
		last = evs[0].Version
	}
}

func TestRejectsMalformedEdges(t *testing.T) {
	s := open(t, &fakePub{})
	if _, err := s.SetEdges(context.Background(), []Edge{{From: "c:x", To: "g:y"}}); err == nil {
		t.Error("container as edge source accepted")
	}
}
