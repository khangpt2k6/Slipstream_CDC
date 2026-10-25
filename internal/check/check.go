// Package check holds Slipstream's measurement tools. Each one drives the
// running stack from the outside, through the same interfaces a client uses
// (Kafka, the directory, the gRPC search API), and writes a JSON result:
//
//   - acl: how long a permission revoke takes to hide a document from
//     search, plus a leak oracle that checks search hits against the
//     directory's ground truth.
//   - converge: replays real documents through Kafka with duplicates and
//     out-of-order versions at a target rate, then checks the index holds
//     exactly the newest version of every document.
//   - freshness: how long a live change takes to become searchable.
//   - eval: retrieval quality of BM25, k-NN and hybrid on duplicate-issue
//     labels the sources themselves recorded.
//   - replay: re-produce connector dumps into Kafka.
//
// Every number quoted in the README comes from one of these.
package check

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/khangpt2k6/Slipstream_CDC/internal/dirclient"
	pb "github.com/khangpt2k6/Slipstream_CDC/internal/gen/slipstream/v1"
	"github.com/khangpt2k6/Slipstream_CDC/internal/grpcapi"
	"github.com/khangpt2k6/Slipstream_CDC/internal/kafkax"
	"github.com/khangpt2k6/Slipstream_CDC/internal/opensearch"
)

// Env is where the stack lives.
type Env struct {
	Kafka      []string
	OpenSearch string
	Index      string
	GRPC       string
	Directory  string
	Prometheus string
	Out        string // results directory
}

// Stack holds clients for every endpoint the checks use.
type Stack struct {
	Env
	OS     *opensearch.Client
	Dir    *dirclient.Client
	Search pb.SearchServiceClient
	Prod   *kafkax.Producer
	Adm    *kadm.Client

	conn *grpc.ClientConn
	kcl  *kgo.Client
	hc   *http.Client
}

// Connect dials the stack.
func Connect(env Env) (*Stack, error) {
	conn, err := grpc.NewClient(env.GRPC, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("check: grpc: %w", err)
	}
	prod, err := kafkax.NewProducer(env.Kafka, 5*time.Millisecond)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	kcl, err := kgo.NewClient(kgo.SeedBrokers(env.Kafka...))
	if err != nil {
		_ = conn.Close()
		prod.Close()
		return nil, err
	}
	return &Stack{
		Env:    env,
		OS:     opensearch.New(env.OpenSearch, 60*time.Second),
		Dir:    dirclient.New(env.Directory),
		Search: pb.NewSearchServiceClient(conn),
		Prod:   prod,
		Adm:    kadm.NewClient(kcl),
		conn:   conn,
		kcl:    kcl,
		hc:     &http.Client{Timeout: 10 * time.Second},
	}, nil
}

// Close releases every client.
func (s *Stack) Close() {
	_ = s.conn.Close()
	s.Prod.Close()
	s.kcl.Close()
}

// SearchAs runs a search as user, identified the way an auth proxy would.
func (s *Stack) SearchAs(ctx context.Context, user string, req *pb.SearchRequest) (*pb.SearchResponse, error) {
	return s.Search.Search(userCtx(ctx, user), req)
}

func userCtx(ctx context.Context, user string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, grpcapi.UserKey, user)
}

// Lag returns a consumer group's total uncommitted records.
func (s *Stack) Lag(ctx context.Context, group string) (int64, error) {
	lags, err := s.Adm.Lag(ctx, group)
	if err != nil {
		return 0, err
	}
	l, ok := lags[group]
	if !ok {
		return 0, fmt.Errorf("check: no such group %s", group)
	}
	if l.Error() != nil {
		return 0, l.Error()
	}
	return l.Lag.Total(), nil
}

// Prom evaluates a PromQL expression and returns the sum of the resulting
// vector (0 for an empty result).
func (s *Stack) Prom(ctx context.Context, query string) (float64, error) {
	u := strings.TrimRight(s.Prometheus, "/") + "/api/v1/query?query=" + url.QueryEscape(query)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return 0, err
	}
	resp, err := s.hc.Do(req) // #nosec G704 -- Prometheus URL is a CLI flag
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	var out struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Value [2]any `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return 0, err
	}
	if out.Status != "success" {
		return 0, fmt.Errorf("check: prometheus query %q failed", query)
	}
	sum := 0.0
	for _, r := range out.Data.Result {
		if s, ok := r.Value[1].(string); ok {
			v, err := strconv.ParseFloat(s, 64)
			if err == nil {
				sum += v
			}
		}
	}
	return sum, nil
}

// RunID is a short id that namespaces everything one check creates.
func RunID() string { return strconv.FormatInt(time.Now().Unix()%1_000_000, 36) }

// Write stores v as <out>/<name>.json and returns the path.
func Write(dir, name string, v any) (string, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return "", err
	}
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return "", err
	}
	path := filepath.Join(dir, name+".json")
	return path, os.WriteFile(path, append(b, '\n'), 0o600)
}

// waitFor polls cond every interval until it reports true, ctx ends, or
// timeout passes. It returns how long it waited and whether cond held.
func waitFor(ctx context.Context, interval, timeout time.Duration, cond func() (bool, error)) (time.Duration, bool, error) {
	start := time.Now()
	for {
		ok, err := cond()
		if err != nil {
			return time.Since(start), false, err
		}
		if ok {
			return time.Since(start), true, nil
		}
		if time.Since(start) > timeout {
			return time.Since(start), false, nil
		}
		select {
		case <-ctx.Done():
			return time.Since(start), false, ctx.Err()
		case <-time.After(interval):
		}
	}
}

func ms(d time.Duration) int64 { return d.Milliseconds() }
