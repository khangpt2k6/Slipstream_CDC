// Package slacksim is a simulated Slack workspace. It serves a subset of the
// Slack Web API (users.list, conversations.list/members/history) and pushes
// Events API callbacks (messages, edits, deletes, channel joins and leaves)
// with Slack's v0 request signatures and retry semantics, so the Slack
// connector talks to it exactly as it would to Slack. Pointing the connector
// at slack.com with a real bot token is a configuration change.
//
// There is no public Slack dataset, so the workspace is generated: channels
// map to the simulated company's teams, members are the directory's users,
// and messages come from engineering-chatter templates. It is labelled
// simulated everywhere it appears.
package slacksim

import (
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Member is a workspace user.
type Member struct {
	ID       string // U...
	Name     string // handle
	RealName string
	Email    string
	Teams    []string // directory teams, used to pick channel members
}

// Message is a channel message.
type Message struct {
	Channel  string
	User     string
	Text     string
	TS       string // Slack timestamp "seconds.micros", unique per channel
	ThreadTS string // parent ts for replies
	EditedTS string
	Deleted  bool
}

// Channel is a public or private channel.
type Channel struct {
	ID        string
	Name      string
	IsPrivate bool
	Topic     string
	Members   map[string]bool
	messages  []*Message // ascending ts
}

// Workspace is the simulated workspace state. Safe for concurrent use.
type Workspace struct {
	mu       sync.RWMutex
	members  []*Member
	byID     map[string]*Member
	channels []*Channel
	chByID   map[string]*Channel
	rng      *rand.Rand
	lastTS   int64 // micros, so generated timestamps are unique and increasing
	now      func() time.Time
}

type channelSpec struct {
	name    string
	private bool
	teams   []string
	topic   string
}

var channelSpecs = []channelSpec{
	{"general", false, nil, "general"},
	{"eng-announcements", false, nil, "eng"},
	{"random", false, nil, "random"},
	{"incidents", false, nil, "incident"},
	{"deploys", false, nil, "deploy"},
	{"help-kafka", false, []string{"kafka-core", "data"}, "kafka"},
	{"help-k8s", false, []string{"k8s-node", "k8s-network", "infra"}, "k8s"},
	{"go-users", false, []string{"go-runtime", "go-tools"}, "go"},
	{"frontend-guild", false, []string{"frontend", "design"}, "frontend"},
	{"search-relevance", false, []string{"search", "ml"}, "search"},
	{"data-eng", false, []string{"data", "spark-sql", "flink-runtime"}, "data"},
	{"oncall", false, []string{"sre", "infra"}, "incident"},
	{"sec-incident-response", true, []string{"security"}, "security"},
	{"payments-war-room", true, []string{"payments", "security"}, "payments"},
	{"perf-calibration", true, []string{"product"}, "perf"},
	{"k8s-node-oncall", true, []string{"k8s-node", "sre"}, "k8s"},
	{"kafka-core-private", true, []string{"kafka-core"}, "kafka"},
	{"project-falcon", true, []string{"platform"}, "falcon"},
	{"vuln-triage", true, []string{"security", "infra"}, "security"},
	{"customer-escalations", true, []string{"support-eng", "connectors"}, "escalation"},
}

// New builds a workspace for the given people. Each person is a
// (id-hint, real name, email, teams) tuple from the directory.
func New(seed uint64, people []Member, now func() time.Time) *Workspace {
	w := &Workspace{
		byID:   map[string]*Member{},
		chByID: map[string]*Channel{},
		rng:    rand.New(rand.NewPCG(seed, seed^0xdeadbeef)), // #nosec G404 -- simulation data
		now:    now,
	}
	for _, p := range people {
		m := p
		m.ID = "U" + strings.ToUpper(strconv.FormatUint(hash(p.Email), 36))[:9]
		w.members = append(w.members, &m)
		w.byID[m.ID] = &m
	}
	for _, spec := range channelSpecs {
		c := &Channel{
			ID:        "C" + strings.ToUpper(strconv.FormatUint(hash("chan:"+spec.name), 36))[:9],
			Name:      spec.name,
			IsPrivate: spec.private,
			Topic:     spec.topic,
			Members:   map[string]bool{},
		}
		for _, m := range w.members {
			if spec.teams == nil || overlaps(m.Teams, spec.teams) {
				c.Members[m.ID] = true
			}
		}
		w.channels = append(w.channels, c)
		w.chByID[c.ID] = c
	}
	return w
}

// GenerateHistory fills channels with n messages spread over the last days.
func (w *Workspace) GenerateHistory(n int, days int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	start := w.now().Add(-time.Duration(days) * 24 * time.Hour).UnixMicro()
	span := w.now().UnixMicro() - start
	stamps := make([]int64, n)
	for i := range stamps {
		stamps[i] = start + w.rng.Int64N(span)
	}
	slices.Sort(stamps)
	for _, at := range stamps {
		w.lastTS = max(w.lastTS+1, at)
		c := w.channels[w.rng.IntN(len(w.channels))]
		w.appendLocked(c, w.lastTS)
	}
}

func (w *Workspace) appendLocked(c *Channel, at int64) *Message {
	author := w.memberOf(c)
	m := &Message{Channel: c.ID, User: author, TS: formatTS(at), Text: w.say(c)}
	if len(c.messages) > 0 && w.rng.IntN(10) < 3 {
		parent := c.messages[max(0, len(c.messages)-1-w.rng.IntN(min(20, len(c.messages))))]
		if parent.ThreadTS == "" {
			m.ThreadTS = parent.TS
		} else {
			m.ThreadTS = parent.ThreadTS
		}
	}
	c.messages = append(c.messages, m)
	return m
}

func (w *Workspace) memberOf(c *Channel) string {
	ids := make([]string, 0, len(c.Members))
	for id := range c.Members {
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return w.members[w.rng.IntN(len(w.members))].ID
	}
	slices.Sort(ids)
	return ids[w.rng.IntN(len(ids))]
}

// nextTS returns a fresh, increasing timestamp at the current time.
func (w *Workspace) nextTS() int64 {
	w.lastTS = max(w.lastTS+1, w.now().UnixMicro())
	return w.lastTS
}

// Post appends a message now and returns it. user may be empty (random member).
func (w *Workspace) Post(channelID, user, text string) (*Message, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	c, ok := w.chByID[channelID]
	if !ok {
		return nil, fmt.Errorf("slacksim: no channel %s", channelID)
	}
	m := w.appendLocked(c, w.nextTS())
	if user != "" {
		m.User = user
	}
	if text != "" {
		m.Text = text
	}
	return m, nil
}

// Edit rewrites a random recent message and returns a copy of it.
func (w *Workspace) Edit() (*Message, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	m := w.recentLocked()
	if m == nil {
		return nil, false
	}
	m.Text = m.Text + " (edit: " + w.pick(fixups) + ")"
	m.EditedTS = formatTS(w.nextTS())
	cp := *m
	return &cp, true
}

// Delete removes a random recent message; it returns the message and the
// deletion's own timestamp.
func (w *Workspace) Delete() (*Message, string, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	m := w.recentLocked()
	if m == nil {
		return nil, "", false
	}
	m.Deleted = true
	cp := *m
	return &cp, formatTS(w.nextTS()), true
}

func (w *Workspace) recentLocked() *Message {
	c := w.channels[w.rng.IntN(len(w.channels))]
	for range 8 {
		if len(c.messages) == 0 {
			return nil
		}
		m := c.messages[len(c.messages)-1-w.rng.IntN(min(50, len(c.messages)))]
		if !m.Deleted {
			return m
		}
	}
	return nil
}

// Membership flips a random member of a random private channel: a leave if
// they are in it, a join otherwise. It returns the channel, user and whether
// they joined.
func (w *Workspace) Membership() (string, string, bool, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	var private []*Channel
	for _, c := range w.channels {
		if c.IsPrivate {
			private = append(private, c)
		}
	}
	if len(private) == 0 {
		return "", "", false, false
	}
	c := private[w.rng.IntN(len(private))]
	u := w.members[w.rng.IntN(len(w.members))].ID
	joined := !c.Members[u]
	if joined {
		c.Members[u] = true
	} else {
		delete(c.Members, u)
	}
	return c.ID, u, joined, true
}

// SetMember adds or removes a user from a channel and reports whether that
// changed anything.
func (w *Workspace) SetMember(channelID, user string, in bool) (bool, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	c, ok := w.chByID[channelID]
	if !ok {
		return false, fmt.Errorf("slacksim: no channel %s", channelID)
	}
	if c.Members[user] == in {
		return false, nil
	}
	if in {
		c.Members[user] = true
	} else {
		delete(c.Members, user)
	}
	return true, nil
}

// Snapshot views used by the Web API.

// Members returns all members.
func (w *Workspace) Members() []Member {
	w.mu.RLock()
	defer w.mu.RUnlock()
	out := make([]Member, len(w.members))
	for i, m := range w.members {
		out[i] = *m
	}
	return out
}

// Channels returns channel metadata.
func (w *Workspace) Channels() []Channel {
	w.mu.RLock()
	defer w.mu.RUnlock()
	out := make([]Channel, len(w.channels))
	for i, c := range w.channels {
		out[i] = Channel{ID: c.ID, Name: c.Name, IsPrivate: c.IsPrivate, Topic: c.Topic, Members: maps(c.Members)}
	}
	return out
}

// ChannelMembers returns a channel's member ids, sorted.
func (w *Workspace) ChannelMembers(id string) ([]string, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	c, ok := w.chByID[id]
	if !ok {
		return nil, false
	}
	out := make([]string, 0, len(c.Members))
	for m := range c.Members {
		out = append(out, m)
	}
	slices.Sort(out)
	return out, true
}

// History returns live messages with ts in (oldest, latest), newest first,
// like conversations.history. latest "" means now.
func (w *Workspace) History(id, oldest, latest string, limit int) ([]Message, bool, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	c, ok := w.chByID[id]
	if !ok {
		return nil, false, false
	}
	lo, hi := parseTS(oldest), parseTS(latest)
	var out []Message
	for i := len(c.messages) - 1; i >= 0; i-- {
		m := c.messages[i]
		at := parseTS(m.TS)
		if hi > 0 && at >= hi {
			continue
		}
		if at <= lo {
			break
		}
		if m.Deleted {
			continue
		}
		if len(out) == limit {
			return out, true, true
		}
		out = append(out, *m)
	}
	return out, false, true
}

// Count returns the number of live messages.
func (w *Workspace) Count() int {
	w.mu.RLock()
	defer w.mu.RUnlock()
	n := 0
	for _, c := range w.channels {
		for _, m := range c.messages {
			if !m.Deleted {
				n++
			}
		}
	}
	return n
}

func maps(m map[string]bool) map[string]bool {
	out := make(map[string]bool, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func overlaps(a, b []string) bool {
	for _, x := range a {
		if slices.Contains(b, x) {
			return true
		}
	}
	return false
}

func hash(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}

func formatTS(micros int64) string {
	return fmt.Sprintf("%d.%06d", micros/1_000_000, micros%1_000_000)
}

// parseTS turns "seconds.micros" into micros; "" or malformed is 0.
func parseTS(ts string) int64 {
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
