// Command slacksim runs the simulated Slack workspace (see package slacksim).
// Its members are the directory's users, so identity resolution by email
// works the way it does against a real workspace.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/khangpt2k6/Slipstream_CDC/internal/config"
	"github.com/khangpt2k6/Slipstream_CDC/internal/retry"
	"github.com/khangpt2k6/Slipstream_CDC/internal/slacksim"
	"github.com/khangpt2k6/Slipstream_CDC/internal/svc"
)

func main() {
	e := config.New(os.Getenv)
	var (
		addr         = e.String("SS_SLACKSIM_ADDR", ":8082")
		directoryURL = e.String("SS_DIRECTORY_URL", "http://localhost:8081")
		eventsURL    = e.String("SS_SLACK_EVENTS_URL", "http://localhost:8083/slack/events")
		token        = e.String("SS_SLACK_TOKEN", "xoxb-slipstream-local")
		secret       = e.String("SS_SLACK_SIGNING_SECRET", "local-signing-secret")
		seed         = e.Int("SS_SEED", 42)
		history      = e.Int("SS_SLACKSIM_HISTORY", 30000)
		days         = e.Int("SS_SLACKSIM_DAYS", 120)
		rate         = e.Rate("SS_SLACKSIM_RATE", 2) // live events per second
		dupProb      = e.Float("SS_SLACKSIM_DUP_PROB", 0.05)
		reorderProb  = e.Float("SS_SLACKSIM_REORDER_PROB", 0.05)
		throttle     = e.Int("SS_SLACKSIM_THROTTLE_EVERY", 50)
		logLevel     = e.String("SS_LOG_LEVEL", "info")
	)
	if err := e.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "invalid configuration:", err)
		os.Exit(1)
	}
	ctx, stop := svc.Init(logLevel)
	defer stop()

	people, err := loadPeople(ctx, directoryURL)
	if err != nil {
		svc.Fatal("load directory users", err)
	}
	w := slacksim.New(uint64(seed), people, time.Now) // #nosec G115 -- positive config value
	w.GenerateHistory(history, days)

	d := &slacksim.Deliverer{URL: eventsURL, Secret: secret, TeamID: "TSLIPSTREAM", DupProb: dupProb, ReorderProb: reorderProb}
	d.Start(ctx)
	s := &slacksim.Server{W: w, D: d, Token: token, TeamID: "TSLIPSTREAM", Throttle: throttle}
	go s.RunActivity(ctx, rate)

	mux := svc.OpsMux()
	mux.Handle("/", s.Handler())
	slog.Info("slacksim running", "addr", addr, "members", len(people), "messages", w.Count(),
		"events_url", eventsURL, "rate_per_sec", rate, "dup_prob", dupProb, "reorder_prob", reorderProb)
	svc.Serve(ctx, addr, mux)
}

// loadPeople reads users from the directory, waiting for it to come up.
func loadPeople(ctx context.Context, base string) ([]slacksim.Member, error) {
	var users []struct {
		ID, Name, Email string
		Groups          []string
	}
	err := retry.Do(ctx, retry.Config{Base: 500 * time.Millisecond, Max: 5 * time.Second}, func(attempt int, _ time.Duration, err error) {
		slog.Info("waiting for directory", "attempt", attempt, "err", err)
	}, func() error {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(base, "/")+"/v1/users", nil)
		if err != nil {
			return err
		}
		resp, err := http.DefaultClient.Do(req) // #nosec G704 -- directory URL is operator config
		if err != nil {
			return err
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("status %d", resp.StatusCode)
		}
		if err := json.NewDecoder(resp.Body).Decode(&users); err != nil {
			return err
		}
		if len(users) == 0 {
			return fmt.Errorf("directory has no users yet")
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	out := make([]slacksim.Member, 0, len(users))
	for _, u := range users {
		var teams []string
		for _, g := range u.Groups {
			teams = append(teams, strings.TrimPrefix(g, "g:"))
		}
		handle := strings.ReplaceAll(u.ID, ".", "")
		out = append(out, slacksim.Member{Name: handle, RealName: u.Name, Email: u.Email, Teams: teams})
	}
	return out, nil
}
