// Package api is the HTTP surface the web UI and the measurement tools use:
// search, "why can I see this", the live event stream, pipeline stats, and a
// thin proxy to the directory for the permission demo.
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/khangpt2k6/Slipstream_CDC/internal/events"
	"github.com/khangpt2k6/Slipstream_CDC/internal/opensearch"
	"github.com/khangpt2k6/Slipstream_CDC/internal/search"
)

// Server holds the API's dependencies.
type Server struct {
	Search       *search.Searcher
	OS           *opensearch.Client
	Index        string
	RDB          *redis.Client
	Kadm         *kadm.Client // consumer lag; optional
	DirectoryURL string
	WebDir       string // built UI to serve at /; optional
	// Impersonation lets a caller pick the user with ?as= (local demo and the
	// measurement tools). With it off, the user comes only from the
	// X-Slipstream-User header, which a trusted auth proxy would set.
	Impersonation bool

	HTTP *http.Client
}

// UserHeader carries the authenticated user when impersonation is off.
const UserHeader = "X-Slipstream-User"

// Handler returns the API routes plus the static UI.
func (s *Server) Handler() http.Handler {
	if s.HTTP == nil {
		s.HTTP = &http.Client{Timeout: 10 * time.Second}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/search", s.handleSearch)
	mux.HandleFunc("GET /api/why", s.handleWhy)
	mux.HandleFunc("GET /api/principals", s.handlePrincipals)
	mux.HandleFunc("GET /api/stats", s.handleStats)
	mux.HandleFunc("GET /api/events", s.handleEvents)
	mux.HandleFunc("GET /api/users", s.proxy(http.MethodGet, "/v1/users"))
	mux.HandleFunc("GET /api/groups", s.proxy(http.MethodGet, "/v1/groups"))
	mux.HandleFunc("GET /api/containers", s.proxy(http.MethodGet, "/v1/containers"))
	mux.HandleFunc("POST /api/acl", s.handleACL)
	if s.WebDir != "" {
		mux.Handle("/", spa(s.WebDir))
	}
	return mux
}

func (s *Server) user(r *http.Request) (string, error) {
	u := r.Header.Get(UserHeader)
	if s.Impersonation {
		if as := r.URL.Query().Get("as"); as != "" {
			u = as
		}
	}
	u = strings.TrimPrefix(strings.TrimSpace(u), "u:")
	if u == "" {
		return "", errors.New("no user: pass ?as=<user> (dev) or the " + UserHeader + " header")
	}
	return u, nil
}

func (s *Server) handleSearch(w http.ResponseWriter, r *http.Request) {
	user, err := s.user(r)
	if err != nil {
		httpError(w, http.StatusUnauthorized, err)
		return
	}
	q := r.URL.Query()
	mode, err := search.ParseMode(q.Get("mode"))
	if err != nil {
		httpError(w, http.StatusBadRequest, err)
		return
	}
	limit, _ := strconv.Atoi(q.Get("limit"))
	var ds []string
	if v := q.Get("ds"); v != "" {
		ds = strings.Split(v, ",")
	}
	resp, err := s.Search.Search(r.Context(), search.Request{Query: q.Get("q"), User: user, Mode: mode, Datasources: ds, Limit: limit})
	if err != nil {
		httpError(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleWhy(w http.ResponseWriter, r *http.Request) {
	user, err := s.user(r)
	if err != nil {
		httpError(w, http.StatusUnauthorized, err)
		return
	}
	id := r.URL.Query().Get("id")
	exp, err := s.Search.Explain(r.Context(), user, id)
	switch {
	case errors.Is(err, search.ErrNotFound):
		httpError(w, http.StatusNotFound, err)
	case err != nil:
		httpError(w, http.StatusBadGateway, err)
	default:
		writeJSON(w, http.StatusOK, exp)
	}
}

// handlePrincipals shows the user's expansion as search sees it next to the
// directory's ground truth, so the UI can show they agree.
func (s *Server) handlePrincipals(w http.ResponseWriter, r *http.Request) {
	user, err := s.user(r)
	if err != nil {
		httpError(w, http.StatusUnauthorized, err)
		return
	}
	exp, err := s.Search.X.Expand(r.Context(), user)
	if err != nil {
		httpError(w, http.StatusBadGateway, err)
		return
	}
	var truth []string
	if err := s.getJSON(r.Context(), "/v1/principals/"+url.PathEscape(user), &truth); err != nil {
		slog.Debug("directory principals", "err", err)
	}
	slices.Sort(truth)
	writeJSON(w, http.StatusOK, map[string]any{
		"user": user, "epoch": exp.Epoch, "principals": exp.Principals,
		"truth": truth, "consistent": truth != nil && slices.Equal(truth, exp.Principals),
	})
}

// handleACL forwards permission changes to the directory (the source of
// truth). Only available with impersonation on: it is a demo control.
func (s *Server) handleACL(w http.ResponseWriter, r *http.Request) {
	if !s.Impersonation {
		httpError(w, http.StatusForbidden, errors.New("permission changes go through the directory"))
		return
	}
	s.proxy(http.MethodPost, "/v1/edges")(w, r)
}

// proxyParams are the only query parameters forwarded to the directory.
var proxyParams = []string{"q", "limit", "from", "to", "email"}

func (s *Server) proxy(method, path string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		target, err := s.directoryURL(path, r.URL.Query())
		if err != nil {
			httpError(w, http.StatusInternalServerError, err)
			return
		}
		var body io.Reader
		if method == http.MethodPost {
			b, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
			if err != nil {
				httpError(w, http.StatusBadRequest, err)
				return
			}
			body = bytes.NewReader(b)
		}
		req, err := http.NewRequestWithContext(r.Context(), method, target, body)
		if err != nil {
			httpError(w, http.StatusInternalServerError, err)
			return
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := s.HTTP.Do(req) // #nosec G704 -- scheme and host come from config; only whitelisted query params are forwarded
		if err != nil {
			httpError(w, http.StatusBadGateway, err)
			return
		}
		defer func() { _ = resp.Body.Close() }()
		w.Header().Set("Content-Type", resp.Header.Get("Content-Type"))
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
	}
}

// directoryURL builds a directory URL from the configured base, a fixed
// path, and only the whitelisted query parameters.
func (s *Server) directoryURL(path string, in url.Values) (string, error) {
	u, err := url.Parse(strings.TrimRight(s.DirectoryURL, "/") + path)
	if err != nil {
		return "", err
	}
	q := url.Values{}
	for _, k := range proxyParams {
		if v := in.Get(k); v != "" {
			q.Set(k, v)
		}
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func (s *Server) getJSON(ctx context.Context, path string, out any) error {
	target, err := s.directoryURL(path, nil)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return err
	}
	resp, err := s.HTTP.Do(req) // #nosec G704 -- scheme and host come from config
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("directory %s: status %d", path, resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// handleEvents streams the live feed as server-sent events. Each connection
// has its own Redis subscription; a comment line every 15s keeps proxies from
// closing an idle stream.
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		httpError(w, http.StatusInternalServerError, errors.New("streaming unsupported"))
		return
	}
	sub := s.RDB.Subscribe(r.Context(), events.Channel)
	defer func() { _ = sub.Close() }()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprint(w, ": connected\n\n")
	flusher.Flush()

	ch := sub.Channel()
	ping := time.NewTicker(15 * time.Second)
	defer ping.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-ping.C:
			_, _ = fmt.Fprint(w, ": ping\n\n")
			flusher.Flush()
		case m, ok := <-ch:
			if !ok {
				return
			}
			_, _ = fmt.Fprintf(w, "data: %s\n\n", m.Payload)
			flusher.Flush()
		}
	}
}

// spa serves the built UI, falling back to index.html for client routes.
// os.DirFS rejects paths that escape dir.
func spa(dir string) http.Handler {
	fsys := os.DirFS(dir)
	files := http.FileServerFS(fsys)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(path.Clean("/"+r.URL.Path), "/")
		if name == "" {
			name = "index.html"
		}
		if st, err := fs.Stat(fsys, name); err != nil || st.IsDir() {
			http.ServeFileFS(w, r, fsys, "index.html")
			return
		}
		files.ServeHTTP(w, r)
	})
}

func httpError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
