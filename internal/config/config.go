// Package config reads service configuration from SS_* environment variables.
//
// Every Slipstream binary declares its own config struct and fills it through
// an Env, so each service documents exactly the variables it reads, every value
// has a built-in default, and a malformed value is reported once at startup
// instead of surfacing as a confusing runtime failure.
package config

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Env reads typed values from an os.Getenv-style lookup, applying defaults and
// collecting parse errors. Read every field first, then check Err once.
type Env struct {
	get  func(string) string
	errs []error
}

// New returns an Env over getenv (os.Getenv semantics: "" when unset).
// Injecting it keeps config loading testable without touching the process
// environment.
func New(getenv func(string) string) *Env { return &Env{get: getenv} }

// Err reports every malformed or out-of-range value seen so far, joined.
func (e *Env) Err() error { return errors.Join(e.errs...) }

// String returns key's value, or def when it is unset or empty.
func (e *Env) String(key, def string) string {
	if v := strings.TrimSpace(e.get(key)); v != "" {
		return v
	}
	return def
}

// List splits key's comma-separated value, trimming spaces and dropping
// empties. def uses the same format.
func (e *Env) List(key, def string) []string {
	parts := strings.Split(e.String(key, def), ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// Int returns key as a positive integer, or def when unset.
func (e *Env) Int(key string, def int) int {
	raw := e.get(key)
	if strings.TrimSpace(raw) == "" {
		return def
	}
	v, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("%s: %w", key, err))
		return def
	}
	if v <= 0 {
		e.errs = append(e.errs, fmt.Errorf("%s must be positive, got %d", key, v))
		return def
	}
	return v
}

// Float returns key as a float in [0, 1], or def when unset. It is used for
// probabilities and ratios.
func (e *Env) Float(key string, def float64) float64 {
	raw := strings.TrimSpace(e.get(key))
	if raw == "" {
		return def
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("%s: %w", key, err))
		return def
	}
	if v < 0 || v > 1 {
		e.errs = append(e.errs, fmt.Errorf("%s must be within [0,1], got %v", key, v))
		return def
	}
	return v
}

// Duration returns key as a positive time.Duration, or def when unset.
func (e *Env) Duration(key string, def time.Duration) time.Duration {
	raw := strings.TrimSpace(e.get(key))
	if raw == "" {
		return def
	}
	v, err := time.ParseDuration(raw)
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("%s: %w", key, err))
		return def
	}
	if v <= 0 {
		e.errs = append(e.errs, fmt.Errorf("%s must be positive, got %s", key, v))
		return def
	}
	return v
}

// Bool returns key parsed by strconv.ParseBool, or def when unset.
func (e *Env) Bool(key string, def bool) bool {
	raw := strings.TrimSpace(e.get(key))
	if raw == "" {
		return def
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("%s: %w", key, err))
		return def
	}
	return v
}
