package check

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"slices"
	"time"

	"github.com/khangpt2k6/Slipstream_CDC/internal/dirclient"
	pb "github.com/khangpt2k6/Slipstream_CDC/internal/gen/slipstream/v1"
	"github.com/khangpt2k6/Slipstream_CDC/internal/model"
	"github.com/khangpt2k6/Slipstream_CDC/internal/stats"
)

// ACLConfig tunes the permission check.
type ACLConfig struct {
	Trials        int           // revokes to measure (default 200)
	Canaries      int           // canary containers the trials rotate over (default 8)
	Poll          time.Duration // search poll interval while waiting (default 5ms)
	Timeout       time.Duration // give up on one revoke after this (default 15s)
	OracleUsers   int           // users the leak oracle searches as (default 100)
	OracleQueries []string
	Seed          uint64
}

// DefaultOracleQueries span all three datasources.
var DefaultOracleQueries = []string{
	"consumer", "broker", "timeout", "flaky test", "panic", "deploy", "rebalance",
	"incident", "security", "release", "error", "kafka streams", "garbage collector", "",
}

// ACLResult is what the check writes.
type ACLResult struct {
	Run       string    `json:"run"`
	StartedAt time.Time `json:"started_at"`
	TookSec   float64   `json:"took_sec"`

	Trials            int               `json:"trials"`
	MembershipRevokes int               `json:"membership_revokes"`
	GrantRevokes      int               `json:"grant_revokes"`
	TimedOut          int               `json:"timed_out"`
	RevokeToInvisible stats.Percentiles `json:"revoke_to_invisible_ms"`
	GrantToVisible    stats.Percentiles `json:"grant_to_visible_ms"`
	Under700ms        float64           `json:"under_700ms_ratio"`

	Oracle OracleResult `json:"oracle"`
}

// OracleResult reports the leak oracle.
type OracleResult struct {
	Users       int `json:"users"`
	Searches    int `json:"searches"`
	HitsChecked int `json:"hits_checked"`
	// Leaks are hits the directory says the user cannot see, still there
	// on a re-check a few seconds later. Transient ones disappeared on the
	// re-check: a permission change raced the search.
	Leaks     int      `json:"leaks"`
	Transient int      `json:"transient"`
	LeakIDs   []string `json:"leak_ids,omitempty"`
	// Expansion mismatches: users whose principals in the search path's
	// Redis graph differ from the directory's.
	ExpansionChecked    int `json:"expansion_checked"`
	ExpansionMismatches int `json:"expansion_mismatches"`
}

type canary struct {
	container string // container id, without the c: prefix
	group     string // group principal granted the container
	doc       string
	token     string
}

// ACL measures revoke-to-invisible latency and runs the leak oracle.
//
// Each trial uses a fresh user, a group, and a canary container holding one
// document: user -> group -> container, the same two hops a real team grant
// has. The user is added to the group and the check waits until search shows
// the canary. Then it revokes either the membership (user leaves the team)
// or the grant (the container is unshared) and polls search until the
// canary is gone. The clock starts before the revoke request is sent, so the
// latency includes the directory commit, Kafka, the identity pipeline, the
// Redis graph and the search itself.
func ACL(ctx context.Context, s *Stack, cfg ACLConfig) (ACLResult, error) {
	if cfg.Trials <= 0 {
		cfg.Trials = 200
	}
	if cfg.Canaries <= 0 {
		cfg.Canaries = 8
	}
	if cfg.Poll <= 0 {
		cfg.Poll = 5 * time.Millisecond
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 15 * time.Second
	}
	if cfg.OracleUsers <= 0 {
		cfg.OracleUsers = 100
	}
	if cfg.OracleQueries == nil {
		cfg.OracleQueries = DefaultOracleQueries
	}
	run := RunID()
	res := ACLResult{Run: run, StartedAt: time.Now().UTC()}

	canaries, err := setupCanaries(ctx, s, run, cfg.Canaries)
	if err != nil {
		return res, err
	}
	defer cleanupCanaries(context.WithoutCancel(ctx), s, canaries)

	var revoke, grant []int64
	for i := range cfg.Trials {
		c := canaries[i%len(canaries)]
		user := fmt.Sprintf("sscheck-%s-%d", run, i)
		membership := i%2 == 0

		t0 := time.Now()
		if err := s.Dir.SetEdges(ctx, []dirclient.Edge{{From: model.User(user), To: c.group, Present: true}}); err != nil {
			return res, err
		}
		_, ok, err := waitFor(ctx, cfg.Poll, cfg.Timeout, func() (bool, error) { return visible(ctx, s, user, c) })
		if err != nil {
			return res, err
		}
		if !ok {
			return res, fmt.Errorf("check: trial %d: canary never became visible to %s", i, user)
		}
		grant = append(grant, ms(time.Since(t0)))

		edge := dirclient.Edge{From: model.User(user), To: c.group, Present: false}
		if !membership {
			edge = dirclient.Edge{From: c.group, To: model.Container(c.container), Present: false}
		}
		t0 = time.Now()
		if err := s.Dir.SetEdges(ctx, []dirclient.Edge{edge}); err != nil {
			return res, err
		}
		_, gone, err := waitFor(ctx, cfg.Poll, cfg.Timeout, func() (bool, error) {
			v, err := visible(ctx, s, user, c)
			return !v, err
		})
		if err != nil {
			return res, err
		}
		if !gone {
			res.TimedOut++
			slog.Warn("revoke did not hide the canary in time", "trial", i, "user", user)
		} else {
			revoke = append(revoke, ms(time.Since(t0)))
		}
		if membership {
			res.MembershipRevokes++
		} else {
			res.GrantRevokes++
			// Re-share the container for the trials that rotate back to it.
			if err := s.Dir.SetEdges(ctx, []dirclient.Edge{{From: c.group, To: model.Container(c.container), Present: true}}); err != nil {
				return res, err
			}
		}
		if (i+1)%50 == 0 {
			p := stats.Summarize(revoke)
			slog.Info("acl trials", "done", i+1, "p50_ms", p.P50, "p99_ms", p.P99)
		}
	}
	res.Trials = cfg.Trials
	res.RevokeToInvisible = stats.Summarize(revoke)
	res.GrantToVisible = stats.Summarize(grant)
	under := 0
	for _, v := range revoke {
		if v < 700 {
			under++
		}
	}
	if len(revoke) > 0 {
		res.Under700ms = float64(under) / float64(len(revoke))
	}

	res.Oracle, err = oracle(ctx, s, cfg)
	res.TookSec = time.Since(res.StartedAt).Seconds()
	return res, err
}

func setupCanaries(ctx context.Context, s *Stack, run string, n int) ([]canary, error) {
	out := make([]canary, n)
	var edges []dirclient.Edge
	var evs []model.DocEvent
	now := time.Now().UnixMilli()
	for k := range n {
		c := canary{
			container: fmt.Sprintf("sscheck:acl-%s-%d", run, k),
			group:     model.Group(fmt.Sprintf("sscheck-acl-%s-%d", run, k)),
			token:     fmt.Sprintf("aclcanary%sx%d", run, k),
		}
		c.doc = c.container + "/canary"
		out[k] = c
		// "members": nobody gets access except through the edges added here.
		if err := s.Dir.RegisterContainer(ctx, c.container, "sscheck", c.container, "members"); err != nil {
			return nil, err
		}
		edges = append(edges, dirclient.Edge{From: c.group, To: model.Container(c.container), Present: true})
		evs = append(evs, model.DocEvent{
			Op: model.OpUpsert, Version: 1, SrcTsMs: now, EmitTsMs: now, Mode: model.ModeReplay,
			Doc: model.Document{
				ID: c.doc, Datasource: "sscheck", Container: c.container, Kind: "message",
				Title: "acl canary " + c.token, Body: c.token, Allowed: []string{model.Container(c.container)},
			},
		})
	}
	if err := s.Dir.SetEdges(ctx, edges); err != nil {
		return nil, err
	}
	if err := s.Prod.Docs(ctx, evs...); err != nil {
		return nil, err
	}
	return out, nil
}

func cleanupCanaries(ctx context.Context, s *Stack, cs []canary) {
	var edges []dirclient.Edge
	var evs []model.DocEvent
	now := time.Now().UnixMilli()
	for _, c := range cs {
		edges = append(edges, dirclient.Edge{From: c.group, To: model.Container(c.container), Present: false})
		evs = append(evs, model.DocEvent{Op: model.OpDelete, Version: 2, SrcTsMs: now, EmitTsMs: now, Mode: model.ModeReplay,
			Doc: model.Document{ID: c.doc, Datasource: "sscheck", Container: c.container}})
	}
	if err := s.Dir.SetEdges(ctx, edges); err != nil {
		slog.Warn("cleanup edges", "err", err)
	}
	if err := s.Prod.Docs(ctx, evs...); err != nil {
		slog.Warn("cleanup canaries", "err", err)
	}
}

func visible(ctx context.Context, s *Stack, user string, c canary) (bool, error) {
	resp, err := s.SearchAs(ctx, user, &pb.SearchRequest{
		Query: c.token, Mode: pb.Mode_MODE_BM25, Datasources: []string{"sscheck"}, Limit: 5,
	})
	if err != nil {
		return false, err
	}
	for _, h := range resp.Hits {
		if h.Id == c.doc {
			return true, nil
		}
	}
	return false, nil
}

// oracle searches as a sample of real users and checks every hit against
// the directory's ground truth, using the document's stored principals (not
// the matched principal search reports, which would be circular).
func oracle(ctx context.Context, s *Stack, cfg ACLConfig) (OracleResult, error) {
	var out OracleResult
	users, err := s.Dir.Users(ctx)
	if err != nil {
		return out, err
	}
	rng := rand.New(rand.NewPCG(cfg.Seed, cfg.Seed+1)) // #nosec G404 -- sampling, not security
	rng.Shuffle(len(users), func(i, j int) { users[i], users[j] = users[j], users[i] })
	personas := []string{"alice", "bob", "carol", "dave", "erin"}
	ids := slices.Clone(personas)
	for _, u := range users {
		if len(ids) >= cfg.OracleUsers {
			break
		}
		if !slices.Contains(personas, u.ID) {
			ids = append(ids, u.ID)
		}
	}
	out.Users = len(ids)

	allowed := map[string][]string{}
	type suspect struct{ user, query, doc string }
	var suspects []suspect
	var mismatched []string
	for _, u := range ids {
		for _, q := range cfg.OracleQueries {
			resp, err := s.SearchAs(ctx, u, &pb.SearchRequest{Query: q, Mode: pb.Mode_MODE_BM25, Limit: 50})
			if err != nil {
				return out, err
			}
			out.Searches++
			if err := fillAllowed(ctx, s, resp.Hits, allowed); err != nil {
				return out, err
			}
			for _, h := range resp.Hits {
				out.HitsChecked++
				ok, err := s.Dir.CanSee(ctx, u, allowed[h.Id])
				if err != nil {
					return out, err
				}
				if !ok {
					suspects = append(suspects, suspect{u, q, h.Id})
				}
			}
		}
		// The search path's view of the user must equal the ground truth.
		same, err := sameExpansion(ctx, s, u)
		if err != nil {
			return out, err
		}
		out.ExpansionChecked++
		if !same {
			mismatched = append(mismatched, u)
		}
	}

	if len(suspects) > 0 || len(mismatched) > 0 {
		time.Sleep(3 * time.Second) // let any racing permission change land
	}
	for _, u := range mismatched {
		same, err := sameExpansion(ctx, s, u)
		if err != nil {
			return out, err
		}
		if !same {
			out.ExpansionMismatches++
			slog.Warn("expansion differs from ground truth", "user", u)
		}
	}
	for _, sp := range suspects {
		resp, err := s.SearchAs(ctx, sp.user, &pb.SearchRequest{Query: sp.query, Mode: pb.Mode_MODE_BM25, Limit: 50})
		if err != nil {
			return out, err
		}
		still := false
		for _, h := range resp.Hits {
			if h.Id == sp.doc {
				delete(allowed, h.Id)
				if err := fillAllowed(ctx, s, resp.Hits, allowed); err != nil {
					return out, err
				}
				ok, err := s.Dir.CanSee(ctx, sp.user, allowed[h.Id])
				if err != nil {
					return out, err
				}
				still = !ok
			}
		}
		if still {
			out.Leaks++
			out.LeakIDs = append(out.LeakIDs, sp.user+" -> "+sp.doc)
		} else {
			out.Transient++
		}
	}
	return out, nil
}

func sameExpansion(ctx context.Context, s *Stack, user string) (bool, error) {
	p, err := s.Search.Principals(userCtx(ctx, user), &pb.PrincipalsRequest{})
	if err != nil {
		return false, err
	}
	truth, err := s.Dir.Principals(ctx, user)
	if err != nil {
		return false, err
	}
	slices.Sort(truth)
	return slices.Equal(truth, p.Principals), nil
}

// fillAllowed reads the stored principals of hits not yet in m.
func fillAllowed(ctx context.Context, s *Stack, hits []*pb.Hit, m map[string][]string) error {
	var need []string
	for _, h := range hits {
		if _, ok := m[h.Id]; !ok {
			need = append(need, h.Id)
		}
	}
	if len(need) == 0 {
		return nil
	}
	docs, err := s.OS.MGet(ctx, s.Index, need, []string{"allowed"})
	if err != nil {
		return err
	}
	for _, d := range docs {
		var src struct {
			Allowed []string `json:"allowed"`
		}
		if d.Found {
			_ = json.Unmarshal(d.Source, &src)
		}
		m[d.ID] = src.Allowed
	}
	return nil
}
