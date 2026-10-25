// Package connectors holds what every connector shares: the emitter that
// produces document events, counts them, and appends them to a replayable
// dump, and the dump reader the measurement tools replay from.
//
// Dumps are plain JSONL, one model.DocEvent per line, one file per source
// (data/github.jsonl, data/jira.jsonl, data/slack.jsonl). They let a crawl
// that took an hour against rate-limited APIs be replayed into Kafka in
// seconds, which is how throughput and convergence are measured on real
// documents rather than synthetic ones.
package connectors

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/khangpt2k6/Slipstream_CDC/internal/metrics"
	"github.com/khangpt2k6/Slipstream_CDC/internal/model"
)

// Producer sends document events to the docs topic. *kafkax.Producer
// satisfies it.
type Producer interface {
	Docs(ctx context.Context, evs ...model.DocEvent) error
}

// Emitter produces a connector's document events.
type Emitter struct {
	Prod   Producer
	Source string // metric label: github, jira, slack
	Dump   *Dump  // optional
}

// Emit produces evs and waits for the acks, then dumps them. A dump failure
// is logged by the caller's metrics only: the dump is a convenience for
// replay, never part of the data path.
func (e *Emitter) Emit(ctx context.Context, evs ...model.DocEvent) error {
	if len(evs) == 0 {
		return nil
	}
	if err := e.Prod.Docs(ctx, evs...); err != nil {
		return err
	}
	for _, ev := range evs {
		metrics.ConnectorEvents.WithLabelValues(e.Source, string(ev.Mode)).Inc()
	}
	if e.Dump != nil {
		if err := e.Dump.Write(evs...); err != nil {
			metrics.DumpErrors.WithLabelValues(e.Source).Inc()
		}
	}
	return nil
}

// Dump appends events to a JSONL file. Safe for concurrent use.
type Dump struct {
	mu sync.Mutex
	f  *os.File
	w  *bufio.Writer
}

// OpenDump opens (or creates) <dir>/<source>.jsonl for appending. An empty
// dir disables dumping and returns nil, which Emitter accepts.
func OpenDump(dir, source string) (*Dump, error) {
	if dir == "" {
		return nil, nil
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("connectors: dump dir: %w", err)
	}
	path := filepath.Join(dir, source+".jsonl")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o640) // #nosec G304 -- dump dir is operator config
	if err != nil {
		return nil, fmt.Errorf("connectors: open dump: %w", err)
	}
	return &Dump{f: f, w: bufio.NewWriterSize(f, 1<<20)}, nil
}

// Write appends evs and flushes them to the file.
func (d *Dump) Write(evs ...model.DocEvent) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	enc := json.NewEncoder(d.w)
	for _, ev := range evs {
		if err := enc.Encode(ev); err != nil {
			return err
		}
	}
	return d.w.Flush()
}

// Close flushes and closes the file.
func (d *Dump) Close() error {
	if d == nil {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return errors.Join(d.w.Flush(), d.f.Close())
}

// ReadDump calls fn for every event in a dump file, in file order. A line
// that does not decode stops the read with an error naming the line.
func ReadDump(path string, fn func(model.DocEvent) error) error {
	f, err := os.Open(path) // #nosec G304 -- path is a CLI argument
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	r := bufio.NewReaderSize(f, 1<<20)
	for line := 1; ; line++ {
		b, err := r.ReadBytes('\n')
		if len(b) > 0 {
			var ev model.DocEvent
			if jerr := json.Unmarshal(b, &ev); jerr != nil {
				return fmt.Errorf("connectors: %s line %d: %w", path, line, jerr)
			}
			if ferr := fn(ev); ferr != nil {
				return ferr
			}
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}
