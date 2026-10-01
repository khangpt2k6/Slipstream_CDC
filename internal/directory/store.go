// Package directory is the simulated identity provider and permission
// service: the source of truth for users, groups, memberships and container
// grants.
//
// Every edge change is committed to SQLite first (with a fresh monotonic
// version and published=0), then produced to the identity topic, then marked
// published. A change whose produce failed stays unpublished and a background
// loop retries it, so Kafka never misses a committed change (a transactional
// outbox). The in-memory graph mirrors the table and answers ground-truth
// questions for the leak oracle.
package directory

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite" // pure-Go SQLite driver

	"github.com/khangpt2k6/Slipstream_CDC/internal/model"
)

const schema = `
CREATE TABLE IF NOT EXISTS users (
  id    TEXT PRIMARY KEY,
  name  TEXT NOT NULL,
  email TEXT NOT NULL UNIQUE,
  title TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS groups (
  id   TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  kind TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS containers (
  id         TEXT PRIMARY KEY,
  datasource TEXT NOT NULL,
  name       TEXT NOT NULL,
  visibility TEXT NOT NULL,
  created_ms INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS edges (
  src        TEXT NOT NULL,
  dst        TEXT NOT NULL,
  present    INTEGER NOT NULL,
  version    INTEGER NOT NULL,
  updated_ms INTEGER NOT NULL,
  published  INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (src, dst)
);
CREATE INDEX IF NOT EXISTS edges_unpublished ON edges (published) WHERE published = 0;
`

// User is a person in the directory.
type User struct {
	ID     string   `json:"id"`
	Name   string   `json:"name"`
	Email  string   `json:"email"`
	Title  string   `json:"title"`
	Groups []string `json:"groups,omitempty"` // direct group principals
}

// Group is a team or org.
type Group struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Kind string `json:"kind"` // team | org
}

// Container is a repo, project or channel and how it is shared.
type Container struct {
	ID         string   `json:"id"`
	Datasource string   `json:"datasource"`
	Name       string   `json:"name"`
	Visibility string   `json:"visibility"` // public | restricted | members
	Grants     []string `json:"grants,omitempty"`
}

// Edge is one stored edge.
type Edge struct {
	From      string `json:"from"`
	To        string `json:"to"`
	Present   bool   `json:"present"`
	Version   int64  `json:"version"`
	UpdatedMs int64  `json:"updated_ms"`
}

// Publisher sends edge events to the identity topic.
type Publisher interface {
	Edges(ctx context.Context, evs ...model.EdgeEvent) error
}

// Store is the directory's state.
type Store struct {
	db  *sql.DB
	pub Publisher

	mu      sync.RWMutex
	out     map[string]map[string]bool // present edges, from -> to
	lastVer int64
	now     func() time.Time
}

// Open opens (or creates) the SQLite database at path.
func Open(ctx context.Context, path string, pub Publisher) (*Store, error) {
	db, err := sql.Open("sqlite", path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)")
	if err != nil {
		return nil, fmt.Errorf("directory: open: %w", err)
	}
	db.SetMaxOpenConns(1) // SQLite has one writer; serialize to avoid SQLITE_BUSY
	if _, err := db.ExecContext(ctx, schema); err != nil {
		return nil, fmt.Errorf("directory: schema: %w", err)
	}
	s := &Store{db: db, pub: pub, out: map[string]map[string]bool{}, now: time.Now}
	if err := s.loadGraph(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

// Close closes the database.
func (s *Store) Close() error { return s.db.Close() }

func (s *Store) loadGraph(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, `SELECT src, dst, present, version FROM edges`)
	if err != nil {
		return fmt.Errorf("directory: load edges: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var from, to string
		var present bool
		var v int64
		if err := rows.Scan(&from, &to, &present, &v); err != nil {
			return err
		}
		if present {
			s.addLocked(from, to)
		}
		s.lastVer = max(s.lastVer, v)
	}
	return rows.Err()
}

func (s *Store) addLocked(from, to string) {
	m := s.out[from]
	if m == nil {
		m = map[string]bool{}
		s.out[from] = m
	}
	m[to] = true
}

// nextVersion returns a version greater than any issued before, close to
// wall-clock nanoseconds so versions also read as commit times.
func (s *Store) nextVersion() int64 {
	v := max(s.now().UnixNano(), s.lastVer+1)
	s.lastVer = v
	return v
}

// SetEdges commits edge changes atomically, then publishes them. It returns
// the events as committed. A publish failure is returned to the caller but the
// changes stay committed and are retried by RepublishPending.
func (s *Store) SetEdges(ctx context.Context, changes []Edge) ([]model.EdgeEvent, error) {
	for _, c := range changes {
		e := model.EdgeEvent{From: c.From, To: c.To, Present: c.Present, Version: 1}
		if err := e.Validate(); err != nil {
			return nil, err
		}
	}

	s.mu.Lock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	evs := make([]model.EdgeEvent, 0, len(changes))
	nowMs := s.now().UnixMilli()
	for _, c := range changes {
		v := s.nextVersion()
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO edges (src, dst, present, version, updated_ms, published) VALUES (?, ?, ?, ?, ?, 0)
			ON CONFLICT (src, dst) DO UPDATE SET present = excluded.present, version = excluded.version,
				updated_ms = excluded.updated_ms, published = 0`,
			c.From, c.To, c.Present, v, nowMs); err != nil {
			_ = tx.Rollback()
			s.mu.Unlock()
			return nil, fmt.Errorf("directory: write edge: %w", err)
		}
		evs = append(evs, model.EdgeEvent{From: c.From, To: c.To, Present: c.Present, Version: v, SrcTsMs: nowMs})
	}
	if err := tx.Commit(); err != nil {
		s.mu.Unlock()
		return nil, fmt.Errorf("directory: commit: %w", err)
	}
	for _, e := range evs {
		if e.Present {
			s.addLocked(e.From, e.To)
		} else if m := s.out[e.From]; m != nil {
			delete(m, e.To)
		}
	}
	s.mu.Unlock()

	if err := s.publish(ctx, evs); err != nil {
		return evs, err
	}
	return evs, nil
}

func (s *Store) publish(ctx context.Context, evs []model.EdgeEvent) error {
	if s.pub == nil || len(evs) == 0 {
		return nil
	}
	const chunk = 2000
	for i := 0; i < len(evs); i += chunk {
		part := evs[i:min(i+chunk, len(evs))]
		if err := s.pub.Edges(ctx, part...); err != nil {
			return fmt.Errorf("directory: publish: %w", err)
		}
		if err := s.markPublished(ctx, part); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) markPublished(ctx context.Context, evs []model.EdgeEvent) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	for _, e := range evs {
		// Only the exact version we sent: a newer change keeps published=0.
		if _, err := tx.ExecContext(ctx, `UPDATE edges SET published = 1 WHERE src = ? AND dst = ? AND version = ?`,
			e.From, e.To, e.Version); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

// RepublishPending publishes committed changes that have not been acked yet.
func (s *Store) RepublishPending(ctx context.Context) (int, error) {
	return s.republish(ctx, `SELECT src, dst, present, version, updated_ms FROM edges WHERE published = 0 ORDER BY version LIMIT 5000`)
}

// RepublishAll publishes every edge's current state. The identity topic is
// compacted and the graph is last-writer-wins, so this is always safe; it
// rebuilds a lost or reset graph.
func (s *Store) RepublishAll(ctx context.Context) (int, error) {
	return s.republish(ctx, `SELECT src, dst, present, version, updated_ms FROM edges ORDER BY version`)
}

func (s *Store) republish(ctx context.Context, q string) (int, error) {
	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return 0, err
	}
	var evs []model.EdgeEvent
	for rows.Next() {
		var e model.EdgeEvent
		if err := rows.Scan(&e.From, &e.To, &e.Present, &e.Version, &e.SrcTsMs); err != nil {
			_ = rows.Close()
			return 0, err
		}
		evs = append(evs, e)
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	return len(evs), s.publish(ctx, evs)
}

// Principals is the ground-truth expansion of a user: every principal they
// hold, walking present edges from the user and g:everyone.
func (s *Store) Principals(userID string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	start := []string{model.User(userID), model.Everyone}
	seen := map[string]bool{}
	for _, p := range start {
		seen[p] = true
	}
	frontier := start
	for len(frontier) > 0 {
		var next []string
		for _, p := range frontier {
			for to := range s.out[p] {
				if !seen[to] {
					seen[to] = true
					next = append(next, to)
				}
			}
		}
		frontier = next
	}
	out := make([]string, 0, len(seen))
	for p := range seen {
		out = append(out, p)
	}
	slices.Sort(out)
	return out
}

// CanSee reports whether userID holds any of allowed, per the ground truth.
func (s *Store) CanSee(userID string, allowed []string) bool {
	held := s.Principals(userID)
	for _, a := range allowed {
		if _, ok := slices.BinarySearch(held, a); ok {
			return true
		}
	}
	return false
}

// UpsertUsers inserts or updates users.
func (s *Store) UpsertUsers(ctx context.Context, users []User) error {
	return s.tx(ctx, func(tx *sql.Tx) error {
		for _, u := range users {
			if _, err := tx.ExecContext(ctx, `INSERT INTO users (id, name, email, title) VALUES (?, ?, ?, ?)
				ON CONFLICT (id) DO UPDATE SET name = excluded.name, email = excluded.email, title = excluded.title`,
				u.ID, u.Name, u.Email, u.Title); err != nil {
				return err
			}
		}
		return nil
	})
}

// UpsertGroups inserts or updates groups.
func (s *Store) UpsertGroups(ctx context.Context, groups []Group) error {
	return s.tx(ctx, func(tx *sql.Tx) error {
		for _, g := range groups {
			if _, err := tx.ExecContext(ctx, `INSERT INTO groups (id, name, kind) VALUES (?, ?, ?)
				ON CONFLICT (id) DO UPDATE SET name = excluded.name, kind = excluded.kind`, g.ID, g.Name, g.Kind); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) tx(ctx context.Context, fn func(*sql.Tx) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// Users lists users, optionally filtered by a substring of id, name or email.
func (s *Store) Users(ctx context.Context, q string, limit int) ([]User, error) {
	if limit <= 0 || limit > 5000 {
		limit = 5000
	}
	like := "%" + strings.ToLower(q) + "%"
	rows, err := s.db.QueryContext(ctx, `SELECT id, name, email, title FROM users
		WHERE lower(id) LIKE ? OR lower(name) LIKE ? OR lower(email) LIKE ? ORDER BY id LIMIT ?`, like, like, like, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []User
	for rows.Next() {
		var u User
		if err := rows.Scan(&u.ID, &u.Name, &u.Email, &u.Title); err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	for i := range out {
		for to := range s.out[model.User(out[i].ID)] {
			if strings.HasPrefix(to, model.GroupPrefix) {
				out[i].Groups = append(out[i].Groups, to)
			}
		}
		slices.Sort(out[i].Groups)
	}
	s.mu.RUnlock()
	return out, nil
}

// UserByEmail resolves an email (case-insensitive) to a user id. This is the
// identity resolution step connectors use to map source accounts to people.
func (s *Store) UserByEmail(ctx context.Context, email string) (string, error) {
	var id string
	err := s.db.QueryRowContext(ctx, `SELECT id FROM users WHERE lower(email) = lower(?)`, email).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return id, err
}

// Groups lists groups.
func (s *Store) Groups(ctx context.Context) ([]Group, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, name, kind FROM groups ORDER BY kind, id`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []Group
	for rows.Next() {
		var g Group
		if err := rows.Scan(&g.ID, &g.Name, &g.Kind); err != nil {
			return nil, err
		}
		out = append(out, g)
	}
	return out, rows.Err()
}

// CountUsers returns the number of users.
func (s *Store) CountUsers(ctx context.Context) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM users`).Scan(&n)
	return n, err
}

// Edges lists stored edges matching from and/or to (empty matches all).
func (s *Store) Edges(ctx context.Context, from, to string, limit int) ([]Edge, error) {
	if limit <= 0 || limit > 10000 {
		limit = 10000
	}
	rows, err := s.db.QueryContext(ctx, `SELECT src, dst, present, version, updated_ms FROM edges
		WHERE (? = '' OR src = ?) AND (? = '' OR dst = ?) ORDER BY src, dst LIMIT ?`, from, from, to, to, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []Edge
	for rows.Next() {
		var e Edge
		if err := rows.Scan(&e.From, &e.To, &e.Present, &e.Version, &e.UpdatedMs); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// Container returns a registered container, or false.
func (s *Store) Container(ctx context.Context, id string) (Container, bool, error) {
	var c Container
	err := s.db.QueryRowContext(ctx, `SELECT id, datasource, name, visibility FROM containers WHERE id = ?`, id).
		Scan(&c.ID, &c.Datasource, &c.Name, &c.Visibility)
	if errors.Is(err, sql.ErrNoRows) {
		return Container{}, false, nil
	}
	if err != nil {
		return Container{}, false, err
	}
	c.Grants = s.grantsTo(model.Container(id))
	return c, true, nil
}

func (s *Store) grantsTo(target string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []string
	for from, tos := range s.out {
		if tos[target] && !strings.HasPrefix(from, model.UserPrefix) {
			out = append(out, from)
		}
	}
	slices.Sort(out)
	return out
}

// Containers lists registered containers.
func (s *Store) Containers(ctx context.Context) ([]Container, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, datasource, name, visibility FROM containers ORDER BY id`)
	if err != nil {
		return nil, err
	}
	var out []Container
	for rows.Next() {
		var c Container
		if err := rows.Scan(&c.ID, &c.Datasource, &c.Name, &c.Visibility); err != nil {
			_ = rows.Close()
			return nil, err
		}
		out = append(out, c)
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for i := range out {
		out[i].Grants = s.grantsTo(model.Container(out[i].ID))
	}
	return out, nil
}

// insertContainer records a container; it reports false if it already existed.
func (s *Store) insertContainer(ctx context.Context, c Container) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	res, err := s.db.ExecContext(ctx, `INSERT INTO containers (id, datasource, name, visibility, created_ms)
		VALUES (?, ?, ?, ?, ?) ON CONFLICT (id) DO NOTHING`, c.ID, c.Datasource, c.Name, c.Visibility, s.now().UnixMilli())
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}
