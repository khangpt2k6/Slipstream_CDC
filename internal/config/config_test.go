package config_test

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/khangpt2k6/Slipstream_CDC/internal/config"
)

func envOf(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestDefaultsWhenUnset(t *testing.T) {
	e := config.New(envOf(nil))

	if got := e.String("SS_A", "x"); got != "x" {
		t.Errorf("String = %q, want default x", got)
	}
	if got := e.List("SS_B", "a, b,,c"); !reflect.DeepEqual(got, []string{"a", "b", "c"}) {
		t.Errorf("List = %v, want [a b c]", got)
	}
	if got := e.Int("SS_C", 7); got != 7 {
		t.Errorf("Int = %d, want 7", got)
	}
	if got := e.Duration("SS_D", time.Second); got != time.Second {
		t.Errorf("Duration = %v, want 1s", got)
	}
	if got := e.Bool("SS_E", true); !got {
		t.Error("Bool = false, want default true")
	}
	if got := e.Float("SS_F", 0.25); got != 0.25 {
		t.Errorf("Float = %v, want 0.25", got)
	}
	if err := e.Err(); err != nil {
		t.Fatalf("Err = %v, want nil", err)
	}
}

func TestOverrides(t *testing.T) {
	e := config.New(envOf(map[string]string{
		"SS_A": " kafka:9092 ",
		"SS_C": "42",
		"SS_D": "250ms",
		"SS_E": "false",
		"SS_F": "0.5",
	}))
	if got := e.String("SS_A", ""); got != "kafka:9092" {
		t.Errorf("String = %q, want trimmed kafka:9092", got)
	}
	if got := e.Int("SS_C", 1); got != 42 {
		t.Errorf("Int = %d, want 42", got)
	}
	if got := e.Duration("SS_D", time.Hour); got != 250*time.Millisecond {
		t.Errorf("Duration = %v, want 250ms", got)
	}
	if got := e.Bool("SS_E", true); got {
		t.Error("Bool = true, want false")
	}
	if got := e.Float("SS_F", 0); got != 0.5 {
		t.Errorf("Float = %v, want 0.5", got)
	}
	if err := e.Err(); err != nil {
		t.Fatalf("Err = %v, want nil", err)
	}
}

// TestErrorsAreCollected checks every bad value is reported, not just the
// first, so one restart fixes the whole config.
func TestErrorsAreCollected(t *testing.T) {
	e := config.New(envOf(map[string]string{
		"SS_INT":  "abc",
		"SS_NEG":  "-3",
		"SS_DUR":  "soon",
		"SS_ZERO": "0s",
		"SS_BOOL": "maybe",
		"SS_PROB": "1.5",
	}))
	e.Int("SS_INT", 1)
	e.Int("SS_NEG", 1)
	e.Duration("SS_DUR", time.Second)
	e.Duration("SS_ZERO", time.Second)
	e.Bool("SS_BOOL", false)
	e.Float("SS_PROB", 0)

	err := e.Err()
	if err == nil {
		t.Fatal("Err = nil, want every malformed value reported")
	}
	for _, key := range []string{"SS_INT", "SS_NEG", "SS_DUR", "SS_ZERO", "SS_BOOL", "SS_PROB"} {
		if !strings.Contains(err.Error(), key) {
			t.Errorf("error %q does not mention %s", err, key)
		}
	}
}
