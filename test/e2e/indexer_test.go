//go:build e2e

// Package e2e drives the running compose stack end to end. Run with
// `go test -tags e2e ./test/e2e/` after `docker compose up -d`.
package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/khangpt2k6/Slipstream_CDC/internal/acl"
	"github.com/khangpt2k6/Slipstream_CDC/internal/kafkax"
	"github.com/khangpt2k6/Slipstream_CDC/internal/model"
	"github.com/khangpt2k6/Slipstream_CDC/internal/opensearch"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

type stack struct {
	prod *kafkax.Producer
	os   *opensearch.Client
	rdb  *redis.Client
	x    *acl.Expander
}

func connect(t *testing.T) *stack {
	t.Helper()
	prod, err := kafkax.NewProducer([]string{env("SS_E2E_KAFKA", "localhost:29092")}, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(prod.Close)
	rdb := redis.NewClient(&redis.Options{Addr: env("SS_E2E_REDIS", "localhost:6379")})
	t.Cleanup(func() { _ = rdb.Close() })
	return &stack{
		prod: prod,
		os:   opensearch.New(env("SS_E2E_OPENSEARCH", "http://localhost:9200"), 10*time.Second),
		rdb:  rdb,
		x:    &acl.Expander{RDB: rdb},
	}
}

func eventually(t *testing.T, what string, timeout time.Duration, cond func() (bool, error)) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		ok, err := cond()
		if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s (last err: %v)", what, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func stored(ctx context.Context, s *stack, id string) (map[string]any, error) {
	docs, err := s.os.MGet(ctx, "ss-docs", []string{id}, nil)
	if err != nil || len(docs) == 0 || !docs[0].Found {
		return nil, err
	}
	var src map[string]any
	return src, json.Unmarshal(docs[0].Source, &src)
}

func TestDocsConvergeThroughKafka(t *testing.T) {
	s := connect(t)
	ctx := context.Background()
	run := time.Now().UnixNano()
	id := fmt.Sprintf("slack:Ce2e/%d", run)
	gone := id + "-gone"
	ev := func(id string, v int64, op model.Op, body string) model.DocEvent {
		return model.DocEvent{Op: op, Version: v, SrcTsMs: time.Now().UnixMilli(), Mode: model.ModeLive,
			Doc: model.Document{ID: id, Datasource: "slack", Container: "slack:Ce2e", Kind: "message",
				Body: body, Allowed: []string{model.Container("slack:Ce2e")}}}
	}

	// Out of order and duplicated on purpose: v2 lands, then the late v1 twice.
	if err := s.prod.Docs(ctx,
		ev(id, 2, model.OpUpsert, "second"),
		ev(id, 1, model.OpUpsert, "first"),
		ev(id, 1, model.OpUpsert, "first"),
		ev(gone, 1, model.OpUpsert, "doomed"),
		ev(gone, 2, model.OpDelete, ""),
	); err != nil {
		t.Fatal(err)
	}

	eventually(t, "doc at version 2", 20*time.Second, func() (bool, error) {
		src, err := stored(ctx, s, id)
		return src != nil && src["version"] == float64(2) && src["body"] == "second", err
	})
	eventually(t, "tombstone", 20*time.Second, func() (bool, error) {
		src, err := stored(ctx, s, gone)
		return src != nil && src["deleted"] == true, err
	})
}

func TestRevokePropagatesToGraph(t *testing.T) {
	s := connect(t)
	ctx := context.Background()
	run := time.Now().UnixNano()
	user := fmt.Sprintf("e2e-%d", run)
	group := model.Group(fmt.Sprintf("e2e-team-%d", run))
	repo := model.Container(fmt.Sprintf("github:e2e/%d", run))

	if err := s.prod.Edges(ctx,
		model.EdgeEvent{From: model.User(user), To: group, Present: true, Version: 1, SrcTsMs: time.Now().UnixMilli()},
		model.EdgeEvent{From: group, To: repo, Present: true, Version: 1, SrcTsMs: time.Now().UnixMilli()},
	); err != nil {
		t.Fatal(err)
	}
	eventually(t, "grant visible", 20*time.Second, func() (bool, error) {
		x, err := s.x.Expand(ctx, user)
		return err == nil && x.Has(repo), err
	})

	start := time.Now()
	if err := s.prod.Edges(ctx, model.EdgeEvent{From: model.User(user), To: group, Present: false, Version: 2, SrcTsMs: time.Now().UnixMilli()}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "revoke visible", 20*time.Second, func() (bool, error) {
		x, err := s.x.Expand(ctx, user)
		return err == nil && !x.Has(repo), err
	})
	t.Logf("revoke reached the graph in %v", time.Since(start))
}
