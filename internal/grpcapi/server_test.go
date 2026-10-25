package grpcapi

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/khangpt2k6/Slipstream_CDC/internal/acl"
	pb "github.com/khangpt2k6/Slipstream_CDC/internal/gen/slipstream/v1"
	"github.com/khangpt2k6/Slipstream_CDC/internal/opensearch"
	"github.com/khangpt2k6/Slipstream_CDC/internal/search"
)

// fakeEngine answers every search with the same two hits, one of which the
// caller cannot see. A correct engine would never return it; the server's
// final re-check must drop it anyway.
func fakeEngine(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hit := func(id, allowed string) map[string]any {
			return map[string]any{"_id": id, "_score": 1, "_source": map[string]any{
				"id": id, "title": "title " + id, "datasource": "jira", "kind": "issue", "allowed": []string{allowed},
			}}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"hits": map[string]any{"hits": []any{
			hit("jira:KAFKA-1", "c:jira:KAFKA"), hit("jira:SECRET-1", "c:jira:SECRET"),
		}}})
	}))
	t.Cleanup(srv.Close)
	return srv
}

func dial(t *testing.T, impersonation bool) pb.SearchServiceClient {
	t.Helper()
	mr := miniredis.RunT(t)
	_, _ = mr.SAdd(acl.KeyOutPrefix+"u:alice", "g:platform")
	_, _ = mr.SAdd(acl.KeyOutPrefix+"g:platform", "c:jira:KAFKA")
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	s := &Server{
		Searcher:      &search.Searcher{OS: opensearch.New(fakeEngine(t).URL, 5*time.Second), Index: "ss-docs", X: &acl.Expander{RDB: rdb}},
		RDB:           rdb,
		Impersonation: impersonation,
	}
	lis := bufconn.Listen(1 << 20)
	g := NewGRPC(s)
	go func() { _ = g.Serve(lis) }()
	t.Cleanup(g.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return pb.NewSearchServiceClient(conn)
}

func TestSearchRequiresAUser(t *testing.T) {
	c := dial(t, false)
	_, err := c.Search(context.Background(), &pb.SearchRequest{Query: "kraft", AsUser: "alice"})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("as_user without impersonation: code %v, want Unauthenticated", status.Code(err))
	}
}

func TestSearchFiltersAndExplainsThroughMetadata(t *testing.T) {
	c := dial(t, false)
	ctx := metadata.AppendToOutgoingContext(context.Background(), UserKey, "alice")
	resp, err := c.Search(ctx, &pb.SearchRequest{Query: "kraft", Mode: pb.Mode_MODE_BM25})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Hits) != 1 || resp.Hits[0].Id != "jira:KAFKA-1" {
		t.Fatalf("hits = %v, want only the doc alice can see", resp.Hits)
	}
	h := resp.Hits[0]
	if h.Matched != "c:jira:KAFKA" || len(h.Path) != 3 || h.Path[1] != "g:platform" {
		t.Errorf("access path = %v via %v", h.Matched, h.Path)
	}
	if resp.Mode != pb.Mode_MODE_BM25 || h.Ranks["bm25"] != 1 {
		t.Errorf("mode %v ranks %v", resp.Mode, h.Ranks)
	}
}

func TestImpersonationAndValidation(t *testing.T) {
	c := dial(t, true)
	ctx := context.Background()
	p, err := c.Principals(ctx, &pb.PrincipalsRequest{AsUser: "alice"})
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Principals) != 4 { // u:alice g:everyone g:platform c:jira:KAFKA
		t.Errorf("principals = %v", p.Principals)
	}
	if _, err := c.Search(ctx, &pb.SearchRequest{AsUser: "alice", Limit: 500}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("limit 500: code %v, want InvalidArgument", status.Code(err))
	}
	if _, err := c.Explain(ctx, &pb.ExplainRequest{AsUser: "alice"}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("explain without doc id: code %v", status.Code(err))
	}
}
