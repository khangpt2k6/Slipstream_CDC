package index

import (
	"context"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/khangpt2k6/Slipstream_CDC/internal/model"
	"github.com/khangpt2k6/Slipstream_CDC/internal/opensearch"
	"github.com/khangpt2k6/Slipstream_CDC/internal/opensearch/ostest"
	"github.com/khangpt2k6/Slipstream_CDC/internal/pipeline"
)

func upsert(id string, version int64, body string) model.DocEvent {
	return model.DocEvent{
		Op: model.OpUpsert, Version: version, SrcTsMs: version, Mode: model.ModeLive,
		Doc: model.Document{
			ID: id, Datasource: "slack", Container: "slack:C1", Kind: "message",
			Title: "t", Body: body, Allowed: []string{model.Container("slack:C1")},
		},
	}
}

func del(id string, version int64) model.DocEvent {
	return model.DocEvent{Op: model.OpDelete, Version: version, SrcTsMs: version, Doc: model.Document{ID: id}}
}

func items(vals ...any) []pipeline.Item {
	out := make([]pipeline.Item, len(vals))
	for i, v := range vals {
		switch x := v.(type) {
		case model.DocEvent:
			out[i] = pipeline.Item{Key: x.Doc.ID, Value: x}
		case Quarantine:
			out[i] = pipeline.Item{Key: x.ID, Value: x}
		}
	}
	return out
}

func newSink(t *testing.T) (*DocSink, *ostest.Server) {
	t.Helper()
	srv := ostest.New()
	t.Cleanup(srv.Close)
	return &DocSink{OS: opensearch.New(srv.URL, 5*time.Second), Index: "ss-docs"}, srv
}

func version(t *testing.T, srv *ostest.Server, id string) int64 {
	t.Helper()
	d := srv.Get(id)
	if d == nil {
		t.Fatalf("doc %s not stored", id)
	}
	return int64(d["version"].(float64))
}

func TestPlanDocDiscardsDuplicatesAndStale(t *testing.T) {
	cur := Stored{Found: true, Version: 5}
	_, ok := PlanDoc("d", cur, []any{upsert("d", 5, "x"), upsert("d", 3, "y")}, 0)
	if ok {
		t.Fatal("equal and older versions produced a write")
	}
	plan, _ := PlanDoc("d", cur, []any{upsert("d", 5, "x"), upsert("d", 3, "y")}, 0)
	if plan.Discarded["duplicate"] != 1 || plan.Discarded["stale"] != 1 {
		t.Errorf("discards = %v, want one duplicate and one stale", plan.Discarded)
	}
}

func TestPlanDocFoldsToNewest(t *testing.T) {
	plan, ok := PlanDoc("d", Stored{}, []any{upsert("d", 2, "two"), upsert("d", 9, "nine"), upsert("d", 4, "four")}, 0)
	if !ok || !plan.Create {
		t.Fatal("want a create")
	}
	if plan.Fields["body"] != "nine" || plan.Fields["version"] != int64(9) {
		t.Errorf("folded to body=%v version=%v, want nine/9", plan.Fields["body"], plan.Fields["version"])
	}
	if plan.Discarded["stale"] != 1 {
		t.Errorf("want the late version 4 discarded as stale, got %v", plan.Discarded)
	}
}

// TestConvergesUnderChaos is the core write-path property: whatever order,
// duplication, and batching events arrive in, the index ends at the newest
// version of every document.
func TestConvergesUnderChaos(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	for trial := range 20 {
		sink, srv := newSink(t)
		want := map[string]int64{}
		var stream []any
		for d := range 30 {
			id := fmt.Sprintf("slack:C1/%d", d)
			n := 1 + rng.IntN(6)
			for v := 1; v <= n; v++ {
				ev := upsert(id, int64(v*10), fmt.Sprintf("v%d", v))
				if v == n && rng.IntN(4) == 0 {
					ev = del(id, int64(v*10))
				}
				stream = append(stream, ev)
				if rng.IntN(3) == 0 {
					stream = append(stream, ev) // duplicate delivery
				}
				want[id] = int64(v * 10)
			}
		}
		rng.Shuffle(len(stream), func(i, j int) { stream[i], stream[j] = stream[j], stream[i] })

		for len(stream) > 0 {
			n := min(1+rng.IntN(25), len(stream))
			if err := sink.Write(context.Background(), items(stream[:n]...)); err != nil {
				t.Fatalf("trial %d: Write = %v", trial, err)
			}
			stream = stream[n:]
		}
		for id, v := range want {
			if got := version(t, srv, id); got != v {
				t.Fatalf("trial %d: %s at version %d, want %d", trial, id, got, v)
			}
		}
	}
}

func TestTombstoneBlocksResurrection(t *testing.T) {
	sink, srv := newSink(t)
	ctx := context.Background()
	if err := sink.Write(ctx, items(upsert("d", 1, "hello"), del("d", 2))); err != nil {
		t.Fatal(err)
	}
	if err := sink.Write(ctx, items(upsert("d", 1, "hello"))); err != nil { // late replay
		t.Fatal(err)
	}
	doc := srv.Get("d")
	if doc["deleted"] != true || doc["body"] != "" {
		t.Errorf("late upsert resurrected a deleted doc: %v", doc)
	}
}

func TestQuarantineFailsClosed(t *testing.T) {
	sink, srv := newSink(t)
	ctx := context.Background()
	if err := sink.Write(ctx, items(upsert("d", 100, "x"))); err != nil {
		t.Fatal(err)
	}
	// An unreadable event for d was produced at t=500.
	if err := sink.Write(ctx, items(Quarantine{ID: "d", AtMs: 500})); err != nil {
		t.Fatal(err)
	}
	if srv.Get("d")["quarantined"] != true {
		t.Fatal("doc not quarantined after poison event")
	}
	// A delayed event from before the poison one must not lift it.
	if err := sink.Write(ctx, items(upsert("d", 200, "older"))); err != nil {
		t.Fatal(err)
	}
	if srv.Get("d")["quarantined"] != true {
		t.Fatal("an event older than the poison record un-hid the doc")
	}
	// A change made at the source after the poison record lifts it.
	if err := sink.Write(ctx, items(upsert("d", 600, "newer"))); err != nil {
		t.Fatal(err)
	}
	if doc := srv.Get("d"); doc["quarantined"] != false || doc["body"] != "newer" {
		t.Errorf("newer source change did not lift quarantine: %v", doc)
	}
}

func TestQuarantineOfUnknownDocCreatesHiddenStub(t *testing.T) {
	sink, srv := newSink(t)
	if err := sink.Write(context.Background(), items(Quarantine{ID: "ghost", AtMs: 5})); err != nil {
		t.Fatal(err)
	}
	doc := srv.Get("ghost")
	if doc["quarantined"] != true || len(doc["allowed"].([]any)) != 0 {
		t.Errorf("stub = %v, want quarantined with no principals", doc)
	}
}

// TestMetadataChangeKeepsEmbedding checks a permission-only change does not
// flag the doc for re-embedding or drop its vector.
func TestMetadataChangeKeepsEmbedding(t *testing.T) {
	sink, srv := newSink(t)
	ctx := context.Background()
	if err := sink.Write(ctx, items(upsert("d", 1, "same text"))); err != nil {
		t.Fatal(err)
	}
	doc := srv.Get("d")
	doc["embedding"] = []any{0.1, 0.2}
	doc["needs_embed"] = false
	srv.Put("d", doc)

	ev := upsert("d", 2, "same text")
	ev.Doc.Allowed = []string{model.User("alice")}
	if err := sink.Write(ctx, items(ev)); err != nil {
		t.Fatal(err)
	}
	got := srv.Get("d")
	if got["needs_embed"] != false || got["embedding"] == nil {
		t.Errorf("acl-only change touched embedding state: %v", got)
	}

	if err := sink.Write(ctx, items(upsert("d", 3, "new text"))); err != nil {
		t.Fatal(err)
	}
	if srv.Get("d")["needs_embed"] != true {
		t.Error("content change did not flag the doc for re-embedding")
	}
}

// TestConflictForcesReplan simulates a zombie writer landing a newer version
// between our read and our write. Our stale write must be rejected and the
// re-plan must keep the newer state.
func TestConflictForcesReplan(t *testing.T) {
	sink, srv := newSink(t)
	ctx := context.Background()
	if err := sink.Write(ctx, items(upsert("d", 1, "one"))); err != nil {
		t.Fatal(err)
	}

	fired := false
	srv.BeforeBulk = func() {
		if fired {
			return
		}
		fired = true
		doc := srv.Get("d")
		doc["version"] = float64(50)
		doc["body"] = "zombie wrote fifty"
		srv.Put("d", doc)
	}
	if err := sink.Write(ctx, items(upsert("d", 20, "twenty"))); err != nil {
		t.Fatal(err)
	}
	if got := srv.Get("d"); got["body"] != "zombie wrote fifty" {
		t.Errorf("stale write overwrote a newer concurrent write: %v", got)
	}
}

func TestBulkFailureIsReturnedForRetry(t *testing.T) {
	sink, srv := newSink(t)
	srv.FailNextBulk = 1
	if err := sink.Write(context.Background(), items(upsert("d", 1, "x"))); err == nil {
		t.Fatal("Write = nil on a 503, want an error so the pipeline retries")
	}
	if err := sink.Write(context.Background(), items(upsert("d", 1, "x"))); err != nil {
		t.Fatalf("retry Write = %v", err)
	}
	if version(t, srv, "d") != 1 {
		t.Error("retry did not land the doc")
	}
}

func TestDecodeDocRecord(t *testing.T) {
	if _, err := DecodeDocRecord([]byte("k"), nil); err != pipeline.ErrSkip {
		t.Errorf("nil value: err = %v, want ErrSkip", err)
	}
	if _, err := DecodeDocRecord([]byte("k"), []byte("{")); err == nil {
		t.Error("malformed JSON decoded")
	}
	good := []byte(`{"op":"upsert","version":1,"doc":{"id":"jira:K-1","datasource":"jira","container":"jira:K","allowed":["c:jira:K"]}}`)
	if _, err := DecodeDocRecord([]byte("jira:K-2"), good); err == nil {
		t.Error("key/id mismatch accepted")
	}
	it, err := DecodeDocRecord([]byte("jira:K-1"), good)
	if err != nil || it.Key != "jira:K-1" {
		t.Errorf("good record: %v %v", it, err)
	}
}
