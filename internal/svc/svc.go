// Package svc holds the process plumbing every Slipstream binary shares:
// structured logging, a signal-aware context, and the ops HTTP endpoints.
package svc

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/khangpt2k6/Slipstream_CDC/internal/metrics"
)

// Init installs a JSON slog logger at level and returns a context cancelled
// on SIGINT or SIGTERM.
func Init(level string) (context.Context, context.CancelFunc) {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: parseLevel(level)})))
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

// Fatal logs msg with err and exits non-zero.
func Fatal(msg string, err error) {
	slog.Error(msg, "err", err)
	os.Exit(1)
}

// OpsMux returns a mux serving /metrics and /healthz.
func OpsMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	return mux
}

// Serve runs an HTTP server on addr until ctx ends. A bind failure is logged,
// not fatal: losing observability must not stop the data path.
func Serve(ctx context.Context, addr string, h http.Handler) {
	srv := &http.Server{Addr: addr, Handler: h, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shut, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shut)
	}()
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Warn("http server stopped", "addr", addr, "err", err)
	}
}

func parseLevel(s string) slog.Level {
	switch s {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
