//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/khangpt2k6/Slipstream_CDC/internal/model"
)

func TestFreshnessProbe(t *testing.T) {
	s := connect(t)
	ctx := context.Background()
	postJSON(t, dirURL+"/v1/containers", map[string]string{"id": "slack:Cprobe", "datasource": "slack", "visibility": "public"})
	time.Sleep(500 * time.Millisecond)
	for i := range 8 {
		word := fmt.Sprintf("probe%dx%d", time.Now().UnixNano(), i)
		id := "slack:Cprobe/" + word
		t0 := time.Now()
		if err := s.prod.Docs(ctx, model.DocEvent{Op: model.OpUpsert, Version: 1, SrcTsMs: t0.UnixMilli(), Mode: model.ModeLive,
			Doc: model.Document{ID: id, Datasource: "slack", Container: "slack:Cprobe", Kind: "message", Body: word, Allowed: []string{model.Container("slack:Cprobe")}}}); err != nil {
			t.Fatal(err)
		}
		tp := time.Since(t0)
		var tm time.Duration
		eventually(t, "mget", 30*time.Second, func() (bool, error) {
			src, err := stored(ctx, s, id)
			if src != nil && tm == 0 {
				tm = time.Since(t0)
			}
			return src != nil, err
		})
		eventually(t, "search", 30*time.Second, func() (bool, error) { return has(searchAs(t, "dave", word), id), nil })
		t.Logf("produce=%v indexed=%v searchable=%v", tp, tm, time.Since(t0))
	}
}
