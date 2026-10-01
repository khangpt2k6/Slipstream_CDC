package directory

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/khangpt2k6/Slipstream_CDC/internal/model"
)

// Handler serves the directory API:
//
//	GET  /v1/users?q=&limit=        users with their direct groups
//	GET  /v1/users/by-email?email=  identity resolution: email -> user id
//	GET  /v1/groups                 teams and orgs
//	GET  /v1/containers             containers with visibility and grants
//	POST /v1/containers             register {id, datasource, name, visibility}
//	GET  /v1/edges?from=&to=        stored edges
//	POST /v1/edges                  {"changes":[{from,to,present}]} commit + publish
//	GET  /v1/principals/{user}      ground-truth expansion
//	POST /v1/can-see                {"user":..., "allowed":[...]} ground truth check
//	POST /v1/republish              re-publish every edge (rebuilds the graph)
func Handler(s *Store) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /v1/users", func(w http.ResponseWriter, r *http.Request) {
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		users, err := s.Users(r.Context(), r.URL.Query().Get("q"), limit)
		reply(w, users, err)
	})
	mux.HandleFunc("GET /v1/users/by-email", func(w http.ResponseWriter, r *http.Request) {
		id, err := s.UserByEmail(r.Context(), r.URL.Query().Get("email"))
		if err == nil && id == "" {
			http.Error(w, "no such user", http.StatusNotFound)
			return
		}
		reply(w, map[string]string{"id": id}, err)
	})
	mux.HandleFunc("GET /v1/groups", func(w http.ResponseWriter, r *http.Request) {
		groups, err := s.Groups(r.Context())
		reply(w, groups, err)
	})
	mux.HandleFunc("GET /v1/containers", func(w http.ResponseWriter, r *http.Request) {
		cs, err := s.Containers(r.Context())
		reply(w, cs, err)
	})
	mux.HandleFunc("GET /v1/containers/{id...}", func(w http.ResponseWriter, r *http.Request) {
		c, ok, err := s.Container(r.Context(), r.PathValue("id"))
		if err == nil && !ok {
			http.Error(w, "no such container", http.StatusNotFound)
			return
		}
		reply(w, c, err)
	})
	mux.HandleFunc("POST /v1/containers", func(w http.ResponseWriter, r *http.Request) {
		var c Container
		if !decode(w, r, &c) {
			return
		}
		if c.ID == "" || c.Datasource == "" {
			http.Error(w, "id and datasource are required", http.StatusBadRequest)
			return
		}
		switch c.Visibility {
		case "", "public", "restricted", "members":
		default:
			http.Error(w, "visibility must be public, restricted, members or empty", http.StatusBadRequest)
			return
		}
		if c.Name == "" {
			c.Name = c.ID
		}
		isNew, err := s.RegisterContainer(r.Context(), c)
		reply(w, map[string]bool{"created": isNew}, err)
	})
	mux.HandleFunc("GET /v1/edges", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		limit, _ := strconv.Atoi(q.Get("limit"))
		edges, err := s.Edges(r.Context(), q.Get("from"), q.Get("to"), limit)
		reply(w, edges, err)
	})
	mux.HandleFunc("POST /v1/edges", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Changes []Edge `json:"changes"`
		}
		if !decode(w, r, &req) {
			return
		}
		if len(req.Changes) == 0 {
			http.Error(w, "no changes", http.StatusBadRequest)
			return
		}
		evs, err := s.SetEdges(r.Context(), req.Changes)
		if err != nil && evs == nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		// Committed but not yet published: the outbox loop will deliver it.
		status := http.StatusOK
		if err != nil {
			status = http.StatusAccepted
		}
		writeJSON(w, status, map[string]any{"committed": evs, "published": err == nil})
	})
	mux.HandleFunc("GET /v1/principals/{user}", func(w http.ResponseWriter, r *http.Request) {
		reply(w, s.Principals(r.PathValue("user")), nil)
	})
	mux.HandleFunc("POST /v1/can-see", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			User    string   `json:"user"`
			Allowed []string `json:"allowed"`
		}
		if !decode(w, r, &req) {
			return
		}
		reply(w, map[string]bool{"visible": s.CanSee(strings.TrimPrefix(req.User, model.UserPrefix), req.Allowed)}, nil)
	})
	mux.HandleFunc("POST /v1/republish", func(w http.ResponseWriter, r *http.Request) {
		n, err := s.RepublishAll(r.Context())
		reply(w, map[string]int{"published": n}, err)
	})
	return mux
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 8<<20)
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
		return false
	}
	return true
}

func reply(w http.ResponseWriter, v any, err error) {
	if err != nil {
		status := http.StatusInternalServerError
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			status = http.StatusRequestEntityTooLarge
		}
		http.Error(w, err.Error(), status)
		return
	}
	writeJSON(w, http.StatusOK, v)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
