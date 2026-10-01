package slacksim

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func people() []Member {
	return []Member{
		{Name: "alice", RealName: "Alice", Email: "alice@x", Teams: []string{"platform"}},
		{Name: "bob", RealName: "Bob", Email: "bob@x", Teams: []string{"security"}},
	}
}

func TestHistoryPagesNewestFirstWithoutGaps(t *testing.T) {
	w := New(1, people(), time.Now)
	w.GenerateHistory(500, 10)
	ch := w.Channels()[0].ID

	var all []Message
	latest := ""
	for {
		page, more, ok := w.History(ch, "", latest, 37)
		if !ok {
			t.Fatal("channel missing")
		}
		all = append(all, page...)
		if !more {
			break
		}
		latest = page[len(page)-1].TS
	}
	full, _, _ := w.History(ch, "", "", 100000)
	if len(all) != len(full) {
		t.Fatalf("paged %d messages, want %d", len(all), len(full))
	}
	for i := 1; i < len(all); i++ {
		if parseTS(all[i].TS) >= parseTS(all[i-1].TS) {
			t.Fatalf("not newest first at %d", i)
		}
	}
}

func TestPrivateChannelsOnlyHaveTeamMembers(t *testing.T) {
	w := New(1, people(), time.Now)
	for _, c := range w.Channels() {
		if c.Name == "sec-incident-response" {
			if len(c.Members) != 1 {
				t.Fatalf("security channel members = %v, want only bob", c.Members)
			}
		}
	}
}

// TestDelivererRetriesAndSigns: the first attempt fails, the retry carries
// X-Slack-Retry-Num and a valid signature.
func TestDelivererRetriesAndSigns(t *testing.T) {
	var mu sync.Mutex
	var attempts []string
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if Sign("sec", r.Header.Get("X-Slack-Request-Timestamp"), body) != r.Header.Get("X-Slack-Signature") {
			t.Error("bad signature on delivery")
		}
		mu.Lock()
		attempts = append(attempts, r.Header.Get("X-Slack-Retry-Num"))
		mu.Unlock()
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		var env Envelope
		_ = json.Unmarshal(body, &env)
		if env.Type != "event_callback" || env.EventID == "" {
			t.Errorf("envelope = %+v", env)
		}
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d := &Deliverer{URL: srv.URL, Secret: "sec", RetryDelays: []time.Duration{time.Millisecond, time.Millisecond}}
	d.Start(ctx)
	d.Send(map[string]any{"type": "message", "text": "hi"})

	deadline := time.Now().Add(3 * time.Second)
	for d.Delivered.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(attempts) != 2 || attempts[0] != "" || attempts[1] != "1" {
		t.Errorf("attempts = %q, want first try then retry 1", attempts)
	}
}
