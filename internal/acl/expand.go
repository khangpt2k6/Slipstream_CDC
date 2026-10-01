package acl

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/redis/go-redis/v9"

	"github.com/khangpt2k6/Slipstream_CDC/internal/model"
)

// ErrTooDeep means the group graph is nested deeper than the walk allows,
// which almost always means a cycle or a modelling error upstream.
var ErrTooDeep = errors.New("acl: group nesting exceeds max depth")

// Expander turns a user into their effective principals by walking the graph
// breadth first. Results are cached per user and tagged with the graph epoch
// (ss:idver) read before the walk, so any applied change invalidates every
// cached expansion on the next query. A query therefore costs one GET when
// nothing changed, and never serves an expansion older than the last change
// the indexer applied.
type Expander struct {
	RDB      *redis.Client
	MaxDepth int // default 8
	MaxCache int // cached users before the cache is reset (default 10000)

	mu    sync.Mutex
	cache map[string]cached
}

type cached struct {
	epoch      int64
	principals []string
	parent     map[string]string
}

// Expansion is a user's effective principals at one graph epoch.
type Expansion struct {
	User       string   // principal, e.g. u:alice
	Principals []string // sorted, includes the user and g:everyone
	Epoch      int64
	parent     map[string]string
}

// Has reports whether the expansion includes p.
func (x Expansion) Has(p string) bool {
	_, ok := slices.BinarySearch(x.Principals, p)
	return ok
}

// PathTo returns the chain of principals from the user to target, e.g.
// [u:alice g:platform c:github:golang/go], or nil when target is not held.
func (x Expansion) PathTo(target string) []string {
	if !x.Has(target) {
		return nil
	}
	var path []string
	for p := target; p != ""; p = x.parent[p] {
		path = append(path, p)
		if p == x.User || len(path) > 64 {
			break
		}
	}
	slices.Reverse(path)
	return path
}

// Epoch returns the current graph epoch.
func (e *Expander) Epoch(ctx context.Context) (int64, error) {
	v, err := e.RDB.Get(ctx, KeyIDVer).Int64()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("acl: read epoch: %w", err)
	}
	return v, nil
}

// Expand returns userID's effective principals.
func (e *Expander) Expand(ctx context.Context, userID string) (Expansion, error) {
	user := model.User(userID)
	epoch, err := e.Epoch(ctx)
	if err != nil {
		return Expansion{}, err
	}

	e.mu.Lock()
	if c, ok := e.cache[user]; ok && c.epoch == epoch {
		e.mu.Unlock()
		return Expansion{User: user, Principals: c.principals, Epoch: epoch, parent: c.parent}, nil
	}
	e.mu.Unlock()

	principals, parent, err := e.walk(ctx, user)
	if err != nil {
		return Expansion{}, err
	}

	e.mu.Lock()
	maxCache := e.MaxCache
	if maxCache <= 0 {
		maxCache = 10000
	}
	if e.cache == nil || len(e.cache) >= maxCache {
		e.cache = make(map[string]cached)
	}
	e.cache[user] = cached{epoch: epoch, principals: principals, parent: parent}
	e.mu.Unlock()
	return Expansion{User: user, Principals: principals, Epoch: epoch, parent: parent}, nil
}

// walk does the breadth-first expansion, one pipelined round trip per level.
func (e *Expander) walk(ctx context.Context, user string) ([]string, map[string]string, error) {
	maxDepth := e.MaxDepth
	if maxDepth <= 0 {
		maxDepth = 8
	}
	parent := map[string]string{user: "", model.Everyone: ""}
	frontier := []string{user, model.Everyone}

	for depth := 0; len(frontier) > 0; depth++ {
		if depth > maxDepth {
			return nil, nil, ErrTooDeep
		}
		pipe := e.RDB.Pipeline()
		cmds := make([]*redis.StringSliceCmd, len(frontier))
		for i, p := range frontier {
			cmds[i] = pipe.SMembers(ctx, KeyOutPrefix+p)
		}
		if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
			return nil, nil, fmt.Errorf("acl: expand: %w", err)
		}
		var next []string
		for i, c := range cmds {
			for _, to := range c.Val() {
				if _, seen := parent[to]; seen {
					continue
				}
				parent[to] = frontier[i]
				// Containers are leaves: nothing hangs off them.
				if !strings.HasPrefix(to, model.ContainerPrefix) {
					next = append(next, to)
				}
			}
		}
		frontier = next
	}

	out := make([]string, 0, len(parent))
	for p := range parent {
		out = append(out, p)
	}
	slices.Sort(out)
	return out, parent, nil
}
