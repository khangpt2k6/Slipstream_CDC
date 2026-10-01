package directory

import (
	"context"
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"slices"
	"strings"

	"github.com/khangpt2k6/Slipstream_CDC/internal/model"
)

// Teams and orgs of the simulated company. Teams nest into orgs.
var (
	teamOrg = map[string]string{
		"platform": "eng", "search": "eng", "connectors": "eng", "infra": "eng", "security": "eng",
		"frontend": "eng", "ml": "eng", "data": "eng", "sre": "eng", "devex": "eng", "mobile": "eng",
		"payments": "eng", "k8s-node": "eng", "k8s-network": "eng", "kafka-core": "eng",
		"flink-runtime": "eng", "spark-sql": "eng", "go-runtime": "eng", "go-tools": "eng",
		"support-eng": "gtm", "docs": "product-org", "design": "product-org", "product": "product-org",
		"growth": "gtm",
	}
	orgs = []string{"eng", "product-org", "gtm"}

	// Containers whose id contains a key are granted to these teams when
	// restricted, so the demo company's access looks plausible.
	affinity = map[string][]string{
		"golang":     {"go-runtime", "go-tools", "devex"},
		"kubernetes": {"k8s-node", "k8s-network", "infra", "sre"},
		"kafka":      {"kafka-core", "data", "platform"},
		"flink":      {"flink-runtime", "data"},
		"spark":      {"spark-sql", "data", "ml"},
		"security":   {"security"},
		"incident":   {"sre", "infra", "security"},
		"frontend":   {"frontend", "design"},
		"search":     {"search", "ml"},
		"payments":   {"payments", "security"},
	}

	firstNames = []string{"ava", "ben", "chloe", "dev", "emma", "felix", "grace", "hiro", "isla", "jon",
		"kai", "lena", "mateo", "nora", "omar", "priya", "quinn", "rosa", "sam", "tara", "uma", "victor",
		"wren", "xin", "yara", "zane", "linh", "minh", "an", "khoa", "mai", "tuan", "hana", "leo", "maya", "noah"}
	lastNames = []string{"nguyen", "carter", "martin", "patel", "schmidt", "tran", "kim", "garcia", "okafor",
		"rossi", "silva", "cohen", "yamada", "novak", "haddad", "larsen", "dubois", "ivanova", "pham", "le"}
)

// Personas are fixed demo users with known access, so a reviewer can switch
// between them and see permissions differ.
var personas = []struct {
	User
	teams []string
}{
	{User{ID: "alice", Name: "Alice Nguyen", Email: "alice@slipstream.dev", Title: "Staff Engineer, Platform"}, []string{"platform", "kafka-core"}},
	{User{ID: "bob", Name: "Bob Carter", Email: "bob@slipstream.dev", Title: "Security Engineer"}, []string{"security", "sre"}},
	{User{ID: "carol", Name: "Carol Martin", Email: "carol@slipstream.dev", Title: "Frontend Engineer"}, []string{"frontend"}},
	{User{ID: "dave", Name: "Dave Patel", Email: "dave@slipstream.dev", Title: "Contractor"}, nil},
	{User{ID: "erin", Name: "Erin Schmidt", Email: "erin@slipstream.dev", Title: "Engineering Manager"}, []string{"platform", "search", "connectors", "k8s-node"}},
}

// Teams returns the seeded team ids, sorted.
func Teams() []string {
	out := make([]string, 0, len(teamOrg))
	for t := range teamOrg {
		out = append(out, t)
	}
	slices.Sort(out)
	return out
}

// Seed creates the simulated company if the directory is empty: personas plus
// n generated users, teams nested in orgs, and team memberships. It is
// deterministic for a given seed. It returns false if users already existed.
func (s *Store) Seed(ctx context.Context, seed uint64, n int) (bool, error) {
	count, err := s.CountUsers(ctx)
	if err != nil {
		return false, err
	}
	if count > 0 {
		return false, nil
	}
	rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)) // #nosec G404 -- simulation data, not security

	var groups []Group
	for _, o := range orgs {
		groups = append(groups, Group{ID: o, Name: o, Kind: "org"})
	}
	teams := Teams()
	for _, t := range teams {
		groups = append(groups, Group{ID: t, Name: t, Kind: "team"})
	}
	if err := s.UpsertGroups(ctx, groups); err != nil {
		return false, err
	}

	var users []User
	var edges []Edge
	add := func(from, to string) { edges = append(edges, Edge{From: from, To: to, Present: true}) }
	for _, t := range teams {
		add(model.Group(t), model.Group(teamOrg[t]))
	}
	for _, p := range personas {
		users = append(users, p.User)
		for _, t := range p.teams {
			add(model.User(p.ID), model.Group(t))
		}
	}
	taken := map[string]bool{}
	for _, p := range personas {
		taken[p.ID] = true
	}
	for len(users) < n+len(personas) {
		first, last := firstNames[rng.IntN(len(firstNames))], lastNames[rng.IntN(len(lastNames))]
		id := first + "." + last
		for i := 2; taken[id]; i++ {
			id = fmt.Sprintf("%s.%s%d", first, last, i)
		}
		taken[id] = true
		users = append(users, User{
			ID:    id,
			Name:  titleCase(first) + " " + titleCase(last),
			Email: id + "@slipstream.dev",
			Title: "Engineer",
		})
		k := 1
		if rng.IntN(10) < 3 {
			k = 2
		}
		for _, t := range pick(rng, teams, k) {
			add(model.User(id), model.Group(t))
		}
	}
	if err := s.UpsertUsers(ctx, users); err != nil {
		return false, err
	}
	if _, err := s.SetEdges(ctx, edges); err != nil {
		return true, err
	}
	return true, nil
}

// RegisterContainer records a container the first time a connector sees it
// and applies the sharing policy:
//   - "public": shared with everyone.
//   - "members": access comes only from per-user edges the connector adds
//     (Slack private channels).
//   - "" (auto): deterministic by id. About 55% are public; the rest are
//     restricted to one to three teams, preferring teams whose topic matches.
//
// It reports whether the container was new.
func (s *Store) RegisterContainer(ctx context.Context, c Container) (bool, error) {
	if c.Visibility == "" {
		if h := hash(c.ID); h%100 < 55 {
			c.Visibility = "public"
		} else {
			c.Visibility = "restricted"
		}
	}
	isNew, err := s.insertContainer(ctx, c)
	if err != nil || !isNew {
		return isNew, err
	}

	target := model.Container(c.ID)
	var edges []Edge
	switch c.Visibility {
	case "public":
		edges = append(edges, Edge{From: model.Everyone, To: target, Present: true})
	case "restricted":
		for _, t := range restrictedTeams(c.ID) {
			edges = append(edges, Edge{From: model.Group(t), To: target, Present: true})
		}
	}
	if len(edges) > 0 {
		if _, err := s.SetEdges(ctx, edges); err != nil {
			return true, err
		}
	}
	return true, nil
}

// restrictedTeams picks the teams a restricted container is shared with.
func restrictedTeams(id string) []string {
	h := hash(id)
	k := 1 + int(h%3)
	lower := strings.ToLower(id)
	var pool []string
	for key, teams := range affinity {
		if strings.Contains(lower, key) {
			pool = append(pool, teams...)
		}
	}
	slices.Sort(pool)
	pool = slices.Compact(pool)
	if len(pool) == 0 {
		pool = Teams()
	}
	rng := rand.New(rand.NewPCG(h, h>>7)) // #nosec G404 -- deterministic policy, not security
	return pick(rng, pool, k)
}

func pick(rng *rand.Rand, pool []string, k int) []string {
	k = min(k, len(pool))
	idx := rng.Perm(len(pool))[:k]
	out := make([]string, k)
	for i, j := range idx {
		out[i] = pool[j]
	}
	slices.Sort(out)
	return out
}

func hash(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}

func titleCase(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}
