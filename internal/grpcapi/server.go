// Package grpcapi serves the SearchService gRPC API (proto/slipstream/v1).
// It is a second front door onto the same Searcher the HTTP API uses, for
// service-to-service callers that want typed messages, deadlines that
// propagate, and a streaming event feed without SSE.
package grpcapi

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	"github.com/khangpt2k6/Slipstream_CDC/internal/events"
	pb "github.com/khangpt2k6/Slipstream_CDC/internal/gen/slipstream/v1"
	"github.com/khangpt2k6/Slipstream_CDC/internal/search"
)

// UserKey is the metadata key carrying the authenticated user.
const UserKey = "x-slipstream-user"

var rpcLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "ss_grpc_latency_seconds",
	Help:    "Unary gRPC handler latency by method and status code.",
	Buckets: []float64{.001, .0025, .005, .01, .02, .03, .05, .075, .1, .15, .2, .3, .5, 1},
}, []string{"method", "code"})

// Server implements pb.SearchServiceServer.
type Server struct {
	pb.UnimplementedSearchServiceServer
	Searcher *search.Searcher
	RDB      *redis.Client
	// Impersonation lets a request's as_user pick the user (local demo and
	// the measurement tools), mirroring the HTTP API's ?as=.
	Impersonation bool
}

// NewGRPC returns a gRPC server with the search service, the standard health
// service, and reflection (so grpcurl and ghz work without the proto file).
func NewGRPC(s *Server) *grpc.Server {
	g := grpc.NewServer(grpc.ChainUnaryInterceptor(observe))
	pb.RegisterSearchServiceServer(g, s)
	hs := health.NewServer()
	hs.SetServingStatus(pb.SearchService_ServiceDesc.ServiceName, healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(g, hs)
	reflection.Register(g)
	return g
}

func observe(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
	start := time.Now()
	resp, err := h(ctx, req)
	method := info.FullMethod[strings.LastIndexByte(info.FullMethod, '/')+1:]
	rpcLatency.WithLabelValues(method, status.Code(err).String()).Observe(time.Since(start).Seconds())
	return resp, err
}

func (s *Server) user(ctx context.Context, asUser string) (string, error) {
	u := ""
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if v := md.Get(UserKey); len(v) > 0 {
			u = v[0]
		}
	}
	if s.Impersonation && asUser != "" {
		u = asUser
	}
	u = strings.TrimPrefix(strings.TrimSpace(u), "u:")
	if u == "" {
		return "", status.Error(codes.Unauthenticated, "no user: set the "+UserKey+" metadata key (or as_user in dev)")
	}
	return u, nil
}

var toMode = map[pb.Mode]search.Mode{
	pb.Mode_MODE_UNSPECIFIED: search.ModeHybrid,
	pb.Mode_MODE_BM25:        search.ModeBM25,
	pb.Mode_MODE_KNN:         search.ModeKNN,
	pb.Mode_MODE_HYBRID:      search.ModeHybrid,
}

var fromMode = map[search.Mode]pb.Mode{
	search.ModeBM25:   pb.Mode_MODE_BM25,
	search.ModeKNN:    pb.Mode_MODE_KNN,
	search.ModeHybrid: pb.Mode_MODE_HYBRID,
}

// Search implements SearchService.Search.
func (s *Server) Search(ctx context.Context, req *pb.SearchRequest) (*pb.SearchResponse, error) {
	user, err := s.user(ctx, req.GetAsUser())
	if err != nil {
		return nil, err
	}
	mode, ok := toMode[req.GetMode()]
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "unknown mode %v", req.GetMode())
	}
	if l := req.GetLimit(); l < 0 || l > 50 {
		return nil, status.Error(codes.InvalidArgument, "limit must be within [0, 50]")
	}
	res, err := s.Searcher.Search(ctx, search.Request{
		Query: req.GetQuery(), User: user, Mode: mode, Datasources: req.GetDatasources(),
		Kinds: req.GetKinds(), Limit: int(req.GetLimit()),
	})
	if err != nil {
		return nil, upstream(ctx, err)
	}
	out := &pb.SearchResponse{
		Mode: fromMode[res.Mode], TookMs: res.TookMs, Principals: int32(res.Principals), // #nosec G115 -- bounded by the graph size
		Epoch: res.Epoch, Hits: make([]*pb.Hit, 0, len(res.Hits)),
	}
	for _, h := range res.Hits {
		ranks := make(map[string]int32, len(h.Ranks))
		for k, v := range h.Ranks {
			ranks[k] = int32(v) // #nosec G115 -- ranks are at most the candidate depth
		}
		out.Hits = append(out.Hits, &pb.Hit{
			Id: h.ID, Title: h.Title, Snippet: h.Snippet, Url: h.URL, Datasource: h.Datasource,
			Container: h.Container, Kind: h.Kind, Author: h.Author, ParentId: h.ParentID, UpdatedAt: h.UpdatedAt,
			Score: h.Score, Matched: h.Matched, Path: h.Path, FreshnessMs: h.FreshnessMs, Ranks: ranks,
		})
	}
	return out, nil
}

// Explain implements SearchService.Explain.
func (s *Server) Explain(ctx context.Context, req *pb.ExplainRequest) (*pb.ExplainResponse, error) {
	user, err := s.user(ctx, req.GetAsUser())
	if err != nil {
		return nil, err
	}
	if req.GetDocId() == "" {
		return nil, status.Error(codes.InvalidArgument, "doc_id is required")
	}
	e, err := s.Searcher.Explain(ctx, user, req.GetDocId())
	if errors.Is(err, search.ErrNotFound) {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	if err != nil {
		return nil, upstream(ctx, err)
	}
	return &pb.ExplainResponse{
		DocId: e.DocID, User: e.User, Visible: e.Visible, Reason: e.Reason, Allowed: e.Allowed,
		Matched: e.Matched, Path: e.Path, Held: int32(e.Held), Epoch: e.Epoch, Version: e.Version, // #nosec G115 -- bounded by the graph size
		Deleted: e.Deleted, Quarantined: e.Quarantined,
	}, nil
}

// Principals implements SearchService.Principals.
func (s *Server) Principals(ctx context.Context, req *pb.PrincipalsRequest) (*pb.PrincipalsResponse, error) {
	user, err := s.user(ctx, req.GetAsUser())
	if err != nil {
		return nil, err
	}
	x, err := s.Searcher.X.Expand(ctx, user)
	if err != nil {
		return nil, upstream(ctx, err)
	}
	return &pb.PrincipalsResponse{User: x.User, Epoch: x.Epoch, Principals: x.Principals}, nil
}

// WatchEvents implements SearchService.WatchEvents. The feed is best effort:
// a slow client misses events rather than holding up the pipeline.
func (s *Server) WatchEvents(req *pb.WatchEventsRequest, stream grpc.ServerStreamingServer[pb.WatchEventsResponse]) error {
	ctx := stream.Context()
	sub := s.RDB.Subscribe(ctx, events.Channel)
	defer func() { _ = sub.Close() }()
	if _, err := sub.Receive(ctx); err != nil {
		return upstream(ctx, err)
	}
	types := req.GetTypes()
	ch := sub.Channel()
	for {
		select {
		case <-ctx.Done():
			return nil
		case m, ok := <-ch:
			if !ok {
				return status.Error(codes.Unavailable, "event feed closed")
			}
			var e events.Event
			if json.Unmarshal([]byte(m.Payload), &e) != nil {
				continue
			}
			if len(types) > 0 && !slices.Contains(types, e.Type) {
				continue
			}
			if err := stream.Send(&pb.WatchEventsResponse{
				Type: e.Type, TsMs: e.TsMs, Applied: int32(e.Applied), Discarded: int32(e.Discarded), // #nosec G115 -- per-batch counts
				Conflicts: int32(e.Conflicts), Quarantined: int32(e.Quarantined), Sample: e.Sample, // #nosec G115 -- per-batch counts
				From: e.From, To: e.To, Present: e.Present, LatencyMs: e.LatencyMs,
			}); err != nil {
				return err
			}
		}
	}
}

// upstream maps a dependency failure to a status: the caller's own deadline
// or cancellation keeps its code, anything else is Unavailable (retryable).
func upstream(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return status.FromContextError(ctx.Err()).Err()
	}
	return status.Error(codes.Unavailable, err.Error())
}
