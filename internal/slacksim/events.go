package slacksim

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"
)

// Sign returns Slack's v0 signature for a request body sent at ts (unix
// seconds): "v0=" + hex(HMAC-SHA256(secret, "v0:" + ts + ":" + body)).
func Sign(secret, ts string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte("v0:" + ts + ":"))
	_, _ = mac.Write(body)
	return "v0=" + hex.EncodeToString(mac.Sum(nil))
}

// Envelope is an Events API callback.
type Envelope struct {
	Token     string         `json:"token"`
	TeamID    string         `json:"team_id"`
	APIAppID  string         `json:"api_app_id"`
	Event     map[string]any `json:"event"`
	Type      string         `json:"type"`
	EventID   string         `json:"event_id"`
	EventTime int64          `json:"event_time"`
}

// Deliverer pushes events to the subscriber URL the way Slack does: signed,
// at least once, retried on a non-2xx or timeout (up to three retries, with
// X-Slack-Retry-Num). Chaos knobs add the failure modes a connector must
// survive: duplicate deliveries and reordering.
type Deliverer struct {
	URL         string
	Secret      string
	TeamID      string
	HTTP        *http.Client
	RetryDelays []time.Duration // default 200ms, 1s, 3s
	DupProb     float64         // chance an event is delivered twice
	ReorderProb float64         // chance an event is held back and sent after the next one

	queue chan Envelope
	seq   atomic.Int64
	rng   *rand.Rand

	Delivered atomic.Int64
	Retried   atomic.Int64
	Failed    atomic.Int64
}

// Start runs the delivery loop until ctx ends.
func (d *Deliverer) Start(ctx context.Context) {
	if d.HTTP == nil {
		d.HTTP = &http.Client{Timeout: 3 * time.Second}
	}
	if d.RetryDelays == nil {
		d.RetryDelays = []time.Duration{200 * time.Millisecond, time.Second, 3 * time.Second}
	}
	d.queue = make(chan Envelope, 10000)
	d.rng = rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 7)) // #nosec G404 G115 -- chaos injection, not security
	go d.loop(ctx)
}

// Send queues an inner event for delivery.
func (d *Deliverer) Send(event map[string]any) {
	if d.queue == nil || d.URL == "" {
		return
	}
	n := d.seq.Add(1)
	env := Envelope{
		Token: "simulated", TeamID: d.TeamID, APIAppID: "ASLIPSTREAM", Event: event,
		Type: "event_callback", EventID: "Ev" + strconv.FormatInt(n, 36) + strconv.FormatInt(time.Now().UnixNano()%1e6, 36),
		EventTime: time.Now().Unix(),
	}
	select {
	case d.queue <- env:
	default:
		d.Failed.Add(1)
		slog.Warn("slacksim delivery queue full; dropping event")
	}
}

func (d *Deliverer) loop(ctx context.Context) {
	var held *Envelope
	for {
		select {
		case <-ctx.Done():
			return
		case env := <-d.queue:
			if held == nil && d.ReorderProb > 0 && d.rng.Float64() < d.ReorderProb {
				held = &env
				continue
			}
			d.deliver(ctx, env)
			if d.DupProb > 0 && d.rng.Float64() < d.DupProb {
				d.deliver(ctx, env) // same event_id: the receiver must dedup
			}
			if held != nil {
				d.deliver(ctx, *held) // arrives after a newer event
				held = nil
			}
		case <-time.After(500 * time.Millisecond):
			if held != nil {
				d.deliver(ctx, *held)
				held = nil
			}
		}
	}
}

func (d *Deliverer) deliver(ctx context.Context, env Envelope) {
	body, err := json.Marshal(env)
	if err != nil {
		return
	}
	for attempt := 0; ; attempt++ {
		ok := d.post(ctx, body, attempt)
		if ok {
			d.Delivered.Add(1)
			return
		}
		if attempt >= len(d.RetryDelays) {
			d.Failed.Add(1)
			slog.Warn("slacksim gave up delivering event", "event_id", env.EventID)
			return
		}
		d.Retried.Add(1)
		select {
		case <-ctx.Done():
			return
		case <-time.After(d.RetryDelays[attempt]):
		}
	}
}

func (d *Deliverer) post(ctx context.Context, body []byte, attempt int) bool {
	ts := strconv.FormatInt(time.Now().Unix(), 10)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.URL, bytes.NewReader(body))
	if err != nil {
		return false
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Slack-Request-Timestamp", ts)
	req.Header.Set("X-Slack-Signature", Sign(d.Secret, ts, body))
	if attempt > 0 {
		req.Header.Set("X-Slack-Retry-Num", strconv.Itoa(attempt))
		req.Header.Set("X-Slack-Retry-Reason", "http_error")
	}
	resp, err := d.HTTP.Do(req) // #nosec G704 -- subscriber URL is operator config
	if err != nil {
		return false
	}
	_ = resp.Body.Close()
	return resp.StatusCode >= 200 && resp.StatusCode < 300
}

// Event builders, matching Slack's payload shapes.

func messageEvent(m *Message) map[string]any {
	ev := map[string]any{"type": "message", "channel": m.Channel, "user": m.User, "text": m.Text, "ts": m.TS, "event_ts": m.TS}
	if m.ThreadTS != "" {
		ev["thread_ts"] = m.ThreadTS
	}
	return ev
}

func changedEvent(m *Message) map[string]any {
	inner := map[string]any{"type": "message", "user": m.User, "text": m.Text, "ts": m.TS,
		"edited": map[string]any{"user": m.User, "ts": m.EditedTS}}
	if m.ThreadTS != "" {
		inner["thread_ts"] = m.ThreadTS
	}
	return map[string]any{"type": "message", "subtype": "message_changed", "channel": m.Channel,
		"message": inner, "ts": m.EditedTS, "event_ts": m.EditedTS}
}

func deletedEvent(m *Message, at string) map[string]any {
	return map[string]any{"type": "message", "subtype": "message_deleted", "channel": m.Channel,
		"deleted_ts": m.TS, "ts": at, "event_ts": at}
}

func memberEvent(channel, user string, joined bool) map[string]any {
	t := "member_left_channel"
	if joined {
		t = "member_joined_channel"
	}
	return map[string]any{"type": t, "channel": channel, "user": user, "event_ts": fmt.Sprintf("%d.000000", time.Now().Unix())}
}
