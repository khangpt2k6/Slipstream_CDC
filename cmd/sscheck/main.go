// Command sscheck measures a running Slipstream stack from the outside. Every
// number the README quotes comes from one of its subcommands:
//
//	sscheck acl        revoke-to-invisible latency + leak oracle
//	sscheck converge   chaos replay at full speed, then an exact state check
//	sscheck freshness  live change to searchable latency
//	sscheck eval       BM25 vs k-NN vs hybrid on duplicate-issue labels
//	sscheck replay     re-produce connector dumps into Kafka
//
// Results are written as JSON under -out (default results/).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"time"

	"github.com/khangpt2k6/Slipstream_CDC/internal/check"
)

func usage() {
	fmt.Fprint(os.Stderr, `usage: sscheck <acl|converge|freshness|eval|replay> [flags]

Run "sscheck <command> -h" for a command's flags.
`)
	os.Exit(2)
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	cmd := os.Args[1]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	var env check.Env
	kafka := fs.String("kafka", "localhost:29092", "Kafka bootstrap brokers, comma separated")
	fs.StringVar(&env.OpenSearch, "opensearch", "http://localhost:9200", "OpenSearch URL")
	fs.StringVar(&env.Index, "index", "ss-docs", "index alias")
	fs.StringVar(&env.GRPC, "grpc", "localhost:9095", "SearchService gRPC address")
	fs.StringVar(&env.Directory, "directory", "http://localhost:8081", "directory URL")
	fs.StringVar(&env.Prometheus, "prometheus", "http://localhost:9090", "Prometheus URL")
	fs.StringVar(&env.Out, "out", "results", "results directory")
	seed := fs.Uint64("seed", 42, "random seed")

	var run func(context.Context, *check.Stack) (any, error)
	switch cmd {
	case "acl":
		var c check.ACLConfig
		fs.IntVar(&c.Trials, "trials", 200, "revokes to measure")
		fs.IntVar(&c.OracleUsers, "oracle-users", 100, "users the leak oracle searches as")
		fs.DurationVar(&c.Poll, "poll", 5*time.Millisecond, "search poll interval")
		run = func(ctx context.Context, s *check.Stack) (any, error) {
			c.Seed = *seed
			return check.ACL(ctx, s, c)
		}
	case "converge":
		var c check.ConvergeConfig
		dumps := fs.String("dumps", "data/*.jsonl", "connector dumps to draw documents from (glob)")
		fs.IntVar(&c.Docs, "docs", 50000, "distinct documents")
		fs.IntVar(&c.Updates, "updates", 4, "versions per document")
		fs.Float64Var(&c.DupProb, "dup", 0.1, "probability an event is sent twice")
		fs.Float64Var(&c.ReorderProb, "reorder", 0.1, "probability two consecutive versions are swapped")
		fs.Float64Var(&c.DeleteProb, "delete", 0.05, "probability a document ends deleted")
		fs.IntVar(&c.Rate, "rate", 0, "target events/s (0: as fast as Kafka accepts)")
		fs.IntVar(&c.Slack, "slack", 0, "applies to attribute to other traffic")
		fs.BoolVar(&c.Keep, "keep", false, "keep the documents in the index afterwards")
		run = func(ctx context.Context, s *check.Stack) (any, error) {
			c.Seed = *seed
			paths, err := filepath.Glob(*dumps)
			if err != nil || len(paths) == 0 {
				return nil, fmt.Errorf("no dumps match %q", *dumps)
			}
			c.Dumps = paths
			return check.Converge(ctx, s, c)
		}
	case "freshness":
		var c check.FreshnessConfig
		fs.IntVar(&c.Samples, "samples", 100, "changes to time")
		fs.DurationVar(&c.Interval, "interval", 250*time.Millisecond, "pause between samples")
		run = func(ctx context.Context, s *check.Stack) (any, error) { return check.Freshness(ctx, s, c) }
	case "eval":
		var c check.EvalConfig
		fs.IntVar(&c.Max, "max", 0, "labeled queries to run (0: all)")
		fs.IntVar(&c.Workers, "workers", 4, "concurrent queries")
		fs.BoolVar(&c.AllowBacklog, "allow-backlog", false, "run even if some candidates lack an embedding")
		run = func(ctx context.Context, s *check.Stack) (any, error) {
			c.Seed = *seed
			res, outcomes, err := check.Eval(ctx, s, c)
			if err == nil {
				err = writeJSONL(filepath.Join(env.Out, "eval-queries.jsonl"), outcomes)
			}
			return res, err
		}
	case "replay":
		dumps := fs.String("dumps", "data/*.jsonl", "connector dumps to replay (glob)")
		rate := fs.Int("rate", 0, "target events/s (0: unthrottled)")
		run = func(ctx context.Context, s *check.Stack) (any, error) {
			paths, err := filepath.Glob(*dumps)
			if err != nil || len(paths) == 0 {
				return nil, fmt.Errorf("no dumps match %q", *dumps)
			}
			return check.Replay(ctx, s, paths, *rate)
		}
	default:
		usage()
	}
	_ = fs.Parse(os.Args[2:])
	env.Kafka = strings.Split(*kafka, ",")

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	s, err := check.Connect(env)
	if err != nil {
		fail(err)
	}
	defer s.Close()
	res, err := run(ctx, s)
	if err != nil {
		if res != nil {
			b, _ := json.MarshalIndent(res, "", "  ")
			fmt.Fprintln(os.Stderr, string(b))
		}
		fail(err)
	}
	path, err := check.Write(env.Out, cmd, res)
	if err != nil {
		fail(err)
	}
	b, _ := json.MarshalIndent(res, "", "  ")
	fmt.Println(string(b))
	fmt.Fprintln(os.Stderr, "wrote", path)
}

func writeJSONL[T any](path string, rows []T) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return err
	}
	f, err := os.Create(path) // #nosec G304 -- path is under the results dir flag
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	for _, r := range rows {
		if err := enc.Encode(r); err != nil {
			_ = f.Close()
			return err
		}
	}
	return f.Close()
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "sscheck:", err)
	os.Exit(1)
}
