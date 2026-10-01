package slacksim

import (
	"context"
	"encoding/json"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// Server exposes the Web API subset, admin controls, and the activity loop.
type Server struct {
	W        *Workspace
	D        *Deliverer
	Token    string // expected bot token
	TeamID   string
	Throttle int // every Nth Web API call returns 429 (0 disables)

	calls atomic.Int64
}

// Handler returns the routes. Web API methods live under /api/<method>, the
// same paths slack.com uses.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/users.list", s.web(s.usersList))
	mux.HandleFunc("/api/conversations.list", s.web(s.conversationsList))
	mux.HandleFunc("/api/conversations.members", s.web(s.conversationsMembers))
	mux.HandleFunc("/api/conversations.history", s.web(s.conversationsHistory))
	mux.HandleFunc("/api/auth.test", s.web(func(_ *http.Request) any {
		return map[string]any{"ok": true, "team_id": s.TeamID, "team": "Slipstream (simulated)", "user_id": "UBOT"}
	}))
	mux.HandleFunc("GET /admin/channels", s.adminChannels)
	mux.HandleFunc("POST /admin/post", s.adminPost)
	mux.HandleFunc("POST /admin/membership", s.adminMembership)
	mux.HandleFunc("GET /admin/stats", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, map[string]any{
			"messages": s.W.Count(), "delivered": s.D.Delivered.Load(),
			"retried": s.D.Retried.Load(), "failed": s.D.Failed.Load(),
		})
	})
	return mux
}

// web wraps a Web API method with bearer auth and optional throttling.
func (s *Server) web(fn func(*http.Request) any) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if tok := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "); tok != s.Token {
			writeJSON(w, map[string]any{"ok": false, "error": "invalid_auth"})
			return
		}
		if n := s.calls.Add(1); s.Throttle > 0 && n%int64(s.Throttle) == 0 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			writeJSON(w, map[string]any{"ok": false, "error": "ratelimited"})
			return
		}
		writeJSON(w, fn(r))
	}
}

func pageArgs(r *http.Request, def int) (offset, limit int) {
	limit, _ = strconv.Atoi(r.FormValue("limit"))
	if limit <= 0 || limit > 1000 {
		limit = def
	}
	offset, _ = strconv.Atoi(r.FormValue("cursor"))
	return max(0, offset), limit
}

func next(offset, limit, total int) string {
	if offset+limit >= total {
		return ""
	}
	return strconv.Itoa(offset + limit)
}

func (s *Server) usersList(r *http.Request) any {
	all := s.W.Members()
	offset, limit := pageArgs(r, 200)
	end := min(len(all), offset+limit)
	var members []map[string]any
	for _, m := range all[min(offset, len(all)):end] {
		members = append(members, map[string]any{
			"id": m.ID, "name": m.Name, "real_name": m.RealName, "deleted": false, "is_bot": false,
			"profile": map[string]any{"email": m.Email, "real_name": m.RealName},
		})
	}
	return map[string]any{"ok": true, "members": members, "response_metadata": map[string]any{"next_cursor": next(offset, limit, len(all))}}
}

func (s *Server) conversationsList(r *http.Request) any {
	all := s.W.Channels()
	types := r.FormValue("types")
	var chans []Channel
	for _, c := range all {
		if (c.IsPrivate && strings.Contains(types, "private_channel")) || (!c.IsPrivate && (types == "" || strings.Contains(types, "public_channel"))) {
			chans = append(chans, c)
		}
	}
	offset, limit := pageArgs(r, 100)
	end := min(len(chans), offset+limit)
	var out []map[string]any
	for _, c := range chans[min(offset, len(chans)):end] {
		out = append(out, map[string]any{
			"id": c.ID, "name": c.Name, "is_channel": true, "is_private": c.IsPrivate, "is_archived": false,
			"num_members": len(c.Members), "topic": map[string]any{"value": c.Topic},
		})
	}
	return map[string]any{"ok": true, "channels": out, "response_metadata": map[string]any{"next_cursor": next(offset, limit, len(chans))}}
}

func (s *Server) conversationsMembers(r *http.Request) any {
	members, ok := s.W.ChannelMembers(r.FormValue("channel"))
	if !ok {
		return map[string]any{"ok": false, "error": "channel_not_found"}
	}
	offset, limit := pageArgs(r, 200)
	end := min(len(members), offset+limit)
	return map[string]any{"ok": true, "members": members[min(offset, len(members)):end],
		"response_metadata": map[string]any{"next_cursor": next(offset, limit, len(members))}}
}

// conversationsHistory pages newest first. The cursor is the ts of the last
// message returned, used as the exclusive upper bound of the next page.
func (s *Server) conversationsHistory(r *http.Request) any {
	limit, _ := strconv.Atoi(r.FormValue("limit"))
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	latest := r.FormValue("latest")
	if c := r.FormValue("cursor"); c != "" {
		latest = c
	}
	msgs, more, ok := s.W.History(r.FormValue("channel"), r.FormValue("oldest"), latest, limit)
	if !ok {
		return map[string]any{"ok": false, "error": "channel_not_found"}
	}
	out := make([]map[string]any, 0, len(msgs))
	for i := range msgs {
		m := &msgs[i]
		item := map[string]any{"type": "message", "user": m.User, "text": m.Text, "ts": m.TS}
		if m.ThreadTS != "" {
			item["thread_ts"] = m.ThreadTS
		}
		if m.EditedTS != "" {
			item["edited"] = map[string]any{"user": m.User, "ts": m.EditedTS}
		}
		out = append(out, item)
	}
	cursor := ""
	if more && len(msgs) > 0 {
		cursor = msgs[len(msgs)-1].TS
	}
	return map[string]any{"ok": true, "messages": out, "has_more": more, "response_metadata": map[string]any{"next_cursor": cursor}}
}

func (s *Server) resolveChannel(nameOrID string) string {
	nameOrID = strings.TrimPrefix(nameOrID, "#")
	for _, c := range s.W.Channels() {
		if c.ID == nameOrID || c.Name == nameOrID {
			return c.ID
		}
	}
	return ""
}

func (s *Server) resolveUser(emailOrID string) string {
	for _, m := range s.W.Members() {
		if m.ID == emailOrID || strings.EqualFold(m.Email, emailOrID) {
			return m.ID
		}
	}
	return ""
}

func (s *Server) adminChannels(w http.ResponseWriter, _ *http.Request) {
	var out []map[string]any
	for _, c := range s.W.Channels() {
		out = append(out, map[string]any{"id": c.ID, "name": c.Name, "is_private": c.IsPrivate, "members": len(c.Members)})
	}
	writeJSON(w, out)
}

// adminPost posts a message now and pushes its event. The freshness canary
// uses it: posted_ms is the source commit time.
func (s *Server) adminPost(w http.ResponseWriter, r *http.Request) {
	var req struct{ Channel, User, Text string }
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ch := s.resolveChannel(req.Channel)
	user := ""
	if req.User != "" {
		if user = s.resolveUser(req.User); user == "" {
			http.Error(w, "unknown user", http.StatusBadRequest)
			return
		}
	}
	m, err := s.W.Post(ch, user, req.Text)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.D.Send(messageEvent(m))
	writeJSON(w, map[string]any{"ok": true, "channel": m.Channel, "ts": m.TS, "user": m.User, "posted_ms": parseTS(m.TS) / 1000})
}

func (s *Server) adminMembership(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Channel, User string
		Member        bool
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ch, user := s.resolveChannel(req.Channel), s.resolveUser(req.User)
	changed, err := s.W.SetMember(ch, user, req.Member)
	if err != nil || user == "" {
		http.Error(w, "unknown channel or user", http.StatusBadRequest)
		return
	}
	if changed {
		s.D.Send(memberEvent(ch, user, req.Member))
	}
	writeJSON(w, map[string]any{"ok": true, "changed": changed, "at_ms": time.Now().UnixMilli()})
}

// RunActivity generates live workspace activity at rate events per second
// until ctx ends: mostly new messages, some edits and deletes, and private
// channel joins and leaves.
func (s *Server) RunActivity(ctx context.Context, rate float64) {
	if rate <= 0 {
		return
	}
	rng := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 3)) // #nosec G404 G115 -- simulation
	t := time.NewTicker(time.Duration(float64(time.Second) / rate))
	defer t.Stop()
	channels := s.W.Channels()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		switch p := rng.IntN(100); {
		case p < 70:
			c := channels[rng.IntN(len(channels))]
			if m, err := s.W.Post(c.ID, "", ""); err == nil {
				s.D.Send(messageEvent(m))
			}
		case p < 82:
			if m, ok := s.W.Edit(); ok {
				s.D.Send(changedEvent(m))
			}
		case p < 88:
			if m, at, ok := s.W.Delete(); ok {
				s.D.Send(deletedEvent(m, at))
			}
		default:
			if ch, u, joined, ok := s.W.Membership(); ok {
				s.D.Send(memberEvent(ch, u, joined))
			}
		}
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
