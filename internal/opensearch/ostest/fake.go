// Package ostest is an in-memory stand-in for the slice of OpenSearch the
// write path uses (_mget and _bulk create/update with if_seq_no), so the
// index sink's concurrency and ordering rules can be tested without a cluster.
package ostest

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
)

// Doc is a stored document.
type Doc struct {
	Source map[string]any
	SeqNo  int64
}

// Server is a fake single-index cluster.
type Server struct {
	*httptest.Server

	mu     sync.Mutex
	docs   map[string]*Doc
	seq    int64
	BeforeBulk func() // optional hook run before each bulk is applied
	FailNextBulk int  // respond 503 to this many bulk requests
}

// New starts a fake server. Close it when done.
func New() *Server {
	s := &Server{docs: map[string]*Doc{}}
	s.Server = httptest.NewServer(http.HandlerFunc(s.handle))
	return s
}

// Get returns a copy of a stored document's source, or nil.
func (s *Server) Get(id string) map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.docs[id]
	if !ok {
		return nil
	}
	out := make(map[string]any, len(d.Source))
	for k, v := range d.Source {
		out[k] = v
	}
	return out
}

// Put stores a document directly, as another writer would.
func (s *Server) Put(id string, src map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	s.docs[id] = &Doc{Source: src, SeqNo: s.seq}
}

// Len is the number of stored documents.
func (s *Server) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.docs)
}

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	switch {
	case strings.HasSuffix(r.URL.Path, "/_mget"):
		s.mget(w, r)
	case r.URL.Path == "/_bulk":
		s.bulk(w, r)
	default:
		http.Error(w, "not implemented in fake", http.StatusNotImplemented)
	}
}

func (s *Server) mget(w http.ResponseWriter, r *http.Request) {
	var req struct {
		IDs []string `json:"ids"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	includes := map[string]bool{}
	if inc := r.URL.Query().Get("_source_includes"); inc != "" {
		for _, f := range strings.Split(inc, ",") {
			includes[f] = true
		}
	}
	s.mu.Lock()
	docs := make([]map[string]any, 0, len(req.IDs))
	for _, id := range req.IDs {
		d, ok := s.docs[id]
		if !ok {
			docs = append(docs, map[string]any{"_id": id, "found": false})
			continue
		}
		src := map[string]any{}
		for k, v := range d.Source {
			if len(includes) == 0 || includes[k] {
				src[k] = v
			}
		}
		docs = append(docs, map[string]any{"_id": id, "found": true, "_seq_no": d.SeqNo, "_primary_term": 1, "_source": src})
	}
	s.mu.Unlock()
	writeJSON(w, map[string]any{"docs": docs})
}

func (s *Server) bulk(w http.ResponseWriter, r *http.Request) {
	if s.BeforeBulk != nil {
		s.BeforeBulk()
	}
	s.mu.Lock()
	if s.FailNextBulk > 0 {
		s.FailNextBulk--
		s.mu.Unlock()
		http.Error(w, `{"error":"unavailable"}`, http.StatusServiceUnavailable)
		return
	}
	s.mu.Unlock()

	body, _ := io.ReadAll(r.Body)
	sc := bufio.NewScanner(bytes.NewReader(body))
	sc.Buffer(make([]byte, 1<<20), 64<<20)
	var items []map[string]any
	errorsSeen := false
	for sc.Scan() {
		var meta map[string]map[string]any
		if err := json.Unmarshal(sc.Bytes(), &meta); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if !sc.Scan() {
			http.Error(w, "missing bulk source line", http.StatusBadRequest)
			return
		}
		var src map[string]any
		if err := json.Unmarshal(sc.Bytes(), &src); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		for action, m := range meta {
			id, _ := m["_id"].(string)
			status := s.apply(action, id, m, src)
			if status >= 300 {
				errorsSeen = true
			}
			items = append(items, map[string]any{action: map[string]any{"_id": id, "status": status}})
		}
	}
	writeJSON(w, map[string]any{"took": 1, "errors": errorsSeen, "items": items})
}

func (s *Server) apply(action, id string, meta, src map[string]any) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, exists := s.docs[id]
	switch action {
	case "create":
		if exists {
			return http.StatusConflict
		}
		s.seq++
		s.docs[id] = &Doc{Source: src, SeqNo: s.seq}
		return http.StatusCreated
	case "update":
		if !exists {
			return http.StatusNotFound
		}
		if want, ok := meta["if_seq_no"].(float64); ok && int64(want) != d.SeqNo {
			return http.StatusConflict
		}
		partial, _ := src["doc"].(map[string]any)
		for k, v := range partial {
			d.Source[k] = v
		}
		s.seq++
		d.SeqNo = s.seq
		return http.StatusOK
	default:
		return http.StatusBadRequest
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
