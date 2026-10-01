package slack

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/khangpt2k6/Slipstream_CDC/internal/dirclient"
	"github.com/khangpt2k6/Slipstream_CDC/internal/kafkax"
	"github.com/khangpt2k6/Slipstream_CDC/internal/metrics"
	"github.com/khangpt2k6/Slipstream_CDC/internal/model"
)

// Connector syncs one Slack workspace.
type Connector struct {
	API       *Client
	Dir       *dirclient.Client
	Prod      *kafkax.Producer
	RDB       *redis.Client
	Secret    string // signing secret for the Events API
	Workspace string // URL host prefix, e.g. "slipstream-sim"
	Now       func() time.Time

	mu        sync.RWMutex
	people    map[string]person // slack user id -> directory identity
	channels  map[string]Channel
	refreshed time.Time
}

type person struct {
	DirID string
	Name  string
}

const (
	maxSkew     = 5 * time.Minute // Slack's replay window for signed requests
	dedupTTL    = time.Hour
	cursorKey   = "ss:cursor:slack:"
	dedupPrefix = "ss:dedup:slack:"
)

// Sync resolves identities, registers channels as containers, and reconciles
// private-channel membership with the directory: members are granted, and
// anyone the directory still lists who has left is revoked. It runs at
// startup and periodically, which repairs anything a lost event missed.
func (c *Connector) Sync(ctx context.Context) error {
	if err := c.refreshPeople(ctx); err != nil {
		return err
	}
	chans, err := c.API.Channels(ctx)
	if err != nil {
		return err
	}
	byID := make(map[string]Channel, len(chans))
	for _, ch := range chans {
		byID[ch.ID] = ch
		vis := "public"
		if ch.IsPrivate {
			vis = "members"
		}
		if err := c.Dir.RegisterContainer(ctx, containerID(ch.ID), "slack", "#"+ch.Name, vis); err != nil {
			return err
		}
		if ch.IsPrivate {
			if err := c.reconcileMembers(ctx, ch); err != nil {
				return err
			}
		}
	}
	c.mu.Lock()
	c.channels = byID
	c.mu.Unlock()
	return nil
}

func (c *Connector) refreshPeople(ctx context.Context) error {
	users, err := c.API.Users(ctx)
	if err != nil {
		return err
	}
	dirUsers, err := c.Dir.Users(ctx)
	if err != nil {
		return err
	}
	byEmail := make(map[string]string, len(dirUsers))
	for _, u := range dirUsers {
		byEmail[strings.ToLower(u.Email)] = u.ID
	}
	people := make(map[string]person, len(users))
	unresolved := 0
	for _, u := range users {
		if u.IsBot || u.Deleted {
			continue
		}
		id := byEmail[strings.ToLower(u.Profile.Email)]
		if id == "" {
			unresolved++
		}
		people[u.ID] = person{DirID: id, Name: u.RealName}
	}
	c.mu.Lock()
	c.people = people
	c.refreshed = time.Now()
	c.mu.Unlock()
	if unresolved > 0 {
		slog.Warn("slack users without a directory identity", "n", unresolved)
	}
	return nil
}

func (c *Connector) reconcileMembers(ctx context.Context, ch Channel) error {
	members, err := c.API.Members(ctx, ch.ID)
	if err != nil {
		return err
	}
	target := model.Container(containerID(ch.ID))
	want := map[string]bool{}
	c.mu.RLock()
	for _, m := range members {
		if p := c.people[m]; p.DirID != "" {
			want[model.User(p.DirID)] = true
		}
	}
	c.mu.RUnlock()

	have, err := c.Dir.EdgesTo(ctx, target)
	if err != nil {
		return err
	}
	var changes []dirclient.Edge
	present := map[string]bool{}
	for _, e := range have {
		if !strings.HasPrefix(e.From, model.UserPrefix) {
			continue
		}
		if e.Present {
			present[e.From] = true
			if !want[e.From] {
				changes = append(changes, dirclient.Edge{From: e.From, To: target, Present: false})
			}
		}
	}
	for u := range want {
		if !present[u] {
			changes = append(changes, dirclient.Edge{From: u, To: target, Present: true})
		}
	}
	return c.Dir.SetEdges(ctx, changes)
}

// Backfill emits every message newer than each channel's stored cursor and
// advances the cursor. The first run is the full crawl; later runs are cheap
// incremental catch-ups.
func (c *Connector) Backfill(ctx context.Context, mode model.Mode) (int, error) {
	c.mu.RLock()
	chans := make([]Channel, 0, len(c.channels))
	for _, ch := range c.channels {
		chans = append(chans, ch)
	}
	c.mu.RUnlock()

	total := 0
	for _, ch := range chans {
		oldest, err := c.RDB.Get(ctx, cursorKey+ch.ID).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			return total, err
		}
		newest := oldest
		err = c.API.History(ctx, ch.ID, oldest, func(msgs []Message) error {
			evs := make([]model.DocEvent, 0, len(msgs))
			for _, m := range msgs {
				if m.Subtype != "" && m.Subtype != "thread_broadcast" {
					continue
				}
				ev := c.upsert(ch.ID, m, mode)
				evs = append(evs, ev)
				if tsMicros(m.TS) > tsMicros(newest) {
					newest = m.TS
				}
			}
			if err := c.Prod.Docs(ctx, evs...); err != nil {
				return err
			}
			total += len(evs)
			metrics.ConnectorEvents.WithLabelValues("slack", string(mode)).Add(float64(len(evs)))
			return nil
		})
		if err != nil {
			return total, err
		}
		if newest != oldest {
			if err := c.RDB.Set(ctx, cursorKey+ch.ID, newest, 0).Err(); err != nil {
				return total, err
			}
		}
	}
	return total, nil
}

// ServeHTTP is the Events API endpoint.
func (c *Connector) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		http.Error(w, "body too large", http.StatusRequestEntityTooLarge)
		return
	}
	if err := Verify(c.Secret, r.Header.Get("X-Slack-Request-Timestamp"), r.Header.Get("X-Slack-Signature"), body, c.now()); err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	var env struct {
		Type      string          `json:"type"`
		Challenge string          `json:"challenge"`
		EventID   string          `json:"event_id"`
		Event     json.RawMessage `json:"event"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	if env.Type == "url_verification" {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte(env.Challenge))
		return
	}
	if env.Type != "event_callback" {
		w.WriteHeader(http.StatusOK)
		return
	}

	// Slack delivers at least once and retries on errors, so drop a delivery
	// we already handled. The key is released if handling fails, so the
	// retry is processed. Duplicates that slip through are harmless: every
	// event is versioned downstream.
	key := dedupPrefix + env.EventID
	fresh, err := c.RDB.SetNX(r.Context(), key, 1, dedupTTL).Result()
	if err != nil {
		http.Error(w, "dedup unavailable", http.StatusServiceUnavailable)
		return
	}
	if !fresh {
		metrics.DeliveriesDeduped.WithLabelValues("slack").Inc()
		w.WriteHeader(http.StatusOK)
		return
	}
	if err := c.handle(r.Context(), env.Event); err != nil {
		_ = c.RDB.Del(context.WithoutCancel(r.Context()), key).Err()
		slog.Warn("slack event failed; slack will retry", "event_id", env.EventID, "err", err)
		http.Error(w, "retry later", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

type event struct {
	Type      string   `json:"type"`
	Subtype   string   `json:"subtype"`
	Channel   string   `json:"channel"`
	User      string   `json:"user"`
	Text      string   `json:"text"`
	TS        string   `json:"ts"`
	ThreadTS  string   `json:"thread_ts"`
	DeletedTS string   `json:"deleted_ts"`
	Message   *Message `json:"message"`
}

func (c *Connector) handle(ctx context.Context, raw json.RawMessage) error {
	var ev event
	if err := json.Unmarshal(raw, &ev); err != nil {
		return nil // malformed inner event: nothing to retry
	}
	switch ev.Type {
	case "message":
		switch ev.Subtype {
		case "", "thread_broadcast":
			return c.emit(ctx, c.upsert(ev.Channel, Message{User: ev.User, Text: ev.Text, TS: ev.TS, ThreadTS: ev.ThreadTS}, model.ModeLive))
		case "message_changed":
			if ev.Message == nil {
				return nil
			}
			return c.emit(ctx, c.upsert(ev.Channel, *ev.Message, model.ModeLive))
		case "message_deleted":
			v := tsMicros(ev.TS)
			return c.emit(ctx, model.DocEvent{
				Op: model.OpDelete, Version: v, SrcTsMs: v / 1000, EmitTsMs: c.now().UnixMilli(), Mode: model.ModeLive,
				Doc: model.Document{ID: docID(ev.Channel, ev.DeletedTS), Datasource: "slack", Container: containerID(ev.Channel)},
			})
		}
	case "member_joined_channel", "member_left_channel":
		return c.membership(ctx, ev.Channel, ev.User, ev.Type == "member_joined_channel")
	}
	return nil
}

func (c *Connector) membership(ctx context.Context, channel, user string, joined bool) error {
	c.mu.RLock()
	ch, known := c.channels[channel]
	p := c.people[user]
	c.mu.RUnlock()
	if !known {
		if err := c.Sync(ctx); err != nil {
			return err
		}
		c.mu.RLock()
		ch, known = c.channels[channel]
		p = c.people[user]
		c.mu.RUnlock()
	}
	if !known || !ch.IsPrivate || p.DirID == "" {
		return nil // public channels are open to everyone; membership is not an ACL
	}
	return c.Dir.SetEdges(ctx, []dirclient.Edge{{From: model.User(p.DirID), To: model.Container(containerID(channel)), Present: joined}})
}

func (c *Connector) emit(ctx context.Context, ev model.DocEvent) error {
	if err := c.Prod.Docs(ctx, ev); err != nil {
		return err
	}
	metrics.ConnectorEvents.WithLabelValues("slack", string(ev.Mode)).Inc()
	return nil
}

// upsert maps a message to a document event. The version is the edit time
// when the message was edited, else its post time, so every change to a
// message carries a larger version than the last.
func (c *Connector) upsert(channel string, m Message, mode model.Mode) model.DocEvent {
	versionTS := m.TS
	if m.Edited != nil && m.Edited.TS != "" {
		versionTS = m.Edited.TS
	}
	v := tsMicros(versionTS)

	c.mu.RLock()
	author := c.people[m.User].Name
	chName := c.channels[channel].Name
	c.mu.RUnlock()
	if author == "" {
		author = m.User
	}

	doc := model.Document{
		ID:         docID(channel, m.TS),
		Datasource: "slack",
		Container:  containerID(channel),
		Kind:       "message",
		Title:      title(m.Text),
		Body:       m.Text,
		URL:        fmt.Sprintf("https://%s.slack.com/archives/%s/p%s", c.Workspace, channel, strings.ReplaceAll(m.TS, ".", "")),
		Author:     author,
		CreatedAt:  time.UnixMicro(tsMicros(m.TS)).UTC(),
		UpdatedAt:  time.UnixMicro(v).UTC(),
		Allowed:    []string{model.Container(containerID(channel))},
	}
	if chName != "" {
		doc.Labels = []string{"#" + chName}
	}
	if m.ThreadTS != "" && m.ThreadTS != m.TS {
		doc.ParentID = docID(channel, m.ThreadTS)
	}
	return model.DocEvent{Op: model.OpUpsert, Doc: doc, Version: v, SrcTsMs: v / 1000, EmitTsMs: c.now().UnixMilli(), Mode: mode}
}

func (c *Connector) now() time.Time {
	if c.Now != nil {
		return c.Now()
	}
	return time.Now()
}

// Verify checks a Slack v0 request signature and the timestamp window.
func Verify(secret, ts, sig string, body []byte, now time.Time) error {
	sec, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return errors.New("slack: missing or bad request timestamp")
	}
	if d := now.Sub(time.Unix(sec, 0)); d > maxSkew || d < -maxSkew {
		return errors.New("slack: request timestamp outside the replay window")
	}
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte("v0:" + ts + ":"))
	_, _ = mac.Write(body)
	want := "v0=" + hex.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(want), []byte(sig)) {
		return errors.New("slack: bad signature")
	}
	return nil
}

func containerID(channel string) string { return "slack:" + channel }

func docID(channel, ts string) string { return "slack:" + channel + "/" + ts }

// title is the message's first line, trimmed to a readable length.
func title(text string) string {
	line, _, _ := strings.Cut(text, "\n")
	if r := []rune(line); len(r) > 90 {
		return string(r[:87]) + "..."
	}
	return line
}

// tsMicros parses a Slack timestamp ("seconds.micros") to microseconds.
func tsMicros(ts string) int64 {
	if ts == "" {
		return 0
	}
	sec, frac, _ := strings.Cut(ts, ".")
	s, err := strconv.ParseInt(sec, 10, 64)
	if err != nil {
		return 0
	}
	frac = (frac + "000000")[:6]
	f, _ := strconv.ParseInt(frac, 10, 64)
	return s*1_000_000 + f
}
