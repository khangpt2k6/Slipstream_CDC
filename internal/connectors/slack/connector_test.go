package slack

import (
	"strconv"
	"testing"
	"time"

	"github.com/khangpt2k6/Slipstream_CDC/internal/model"
	"github.com/khangpt2k6/Slipstream_CDC/internal/slacksim"
)

// TestVerifyMatchesSimulatorSigning ties the receiver to the sender: what the
// simulator signs, the connector accepts, and any tampering is rejected.
func TestVerifyMatchesSimulatorSigning(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	ts := strconv.FormatInt(now.Unix(), 10)
	body := []byte(`{"type":"event_callback"}`)
	sig := slacksim.Sign("s3cret", ts, body)

	if err := Verify("s3cret", ts, sig, body, now); err != nil {
		t.Fatalf("valid signature rejected: %v", err)
	}
	if err := Verify("s3cret", ts, sig, []byte(`{"type":"tampered"}`), now); err == nil {
		t.Error("tampered body accepted")
	}
	if err := Verify("other", ts, sig, body, now); err == nil {
		t.Error("wrong secret accepted")
	}
	if err := Verify("s3cret", ts, sig, body, now.Add(6*time.Minute)); err == nil {
		t.Error("replayed request outside the window accepted")
	}
}

func TestVersionsIncreaseAcrossEdits(t *testing.T) {
	c := &Connector{Workspace: "sim", Now: func() time.Time { return time.Unix(0, 0) }}
	posted := c.upsert("C1", Message{User: "U1", Text: "hello", TS: "1700000000.000100"}, model.ModeLive)
	m := Message{User: "U1", Text: "hello (edit)", TS: "1700000000.000100"}
	m.Edited = &struct {
		TS string `json:"ts"`
	}{TS: "1700000005.000000"}
	edited := c.upsert("C1", m, model.ModeLive)

	if posted.Doc.ID != edited.Doc.ID {
		t.Fatalf("edit changed the doc id: %s vs %s", posted.Doc.ID, edited.Doc.ID)
	}
	if edited.Version <= posted.Version {
		t.Errorf("edit version %d not after post %d", edited.Version, posted.Version)
	}
	if got := posted.Doc.Allowed; len(got) != 1 || got[0] != "c:slack:C1" {
		t.Errorf("allowed = %v, want the channel container", got)
	}
	if err := posted.Validate(); err != nil {
		t.Errorf("mapped event invalid: %v", err)
	}
}

func TestThreadRepliesPointAtParent(t *testing.T) {
	c := &Connector{Workspace: "sim"}
	r := c.upsert("C1", Message{TS: "1700000001.000000", ThreadTS: "1700000000.000100", Text: "reply"}, model.ModeLive)
	if r.Doc.ParentID != "slack:C1/1700000000.000100" {
		t.Errorf("parent = %q", r.Doc.ParentID)
	}
	root := c.upsert("C1", Message{TS: "1700000000.000100", ThreadTS: "1700000000.000100", Text: "root"}, model.ModeLive)
	if root.Doc.ParentID != "" {
		t.Errorf("thread root has parent %q", root.Doc.ParentID)
	}
}

func TestTSMicros(t *testing.T) {
	cases := map[string]int64{"1700000000.000100": 1700000000000100, "1700000000.5": 1700000000500000, "": 0, "x": 0}
	for in, want := range cases {
		if got := tsMicros(in); got != want {
			t.Errorf("tsMicros(%q) = %d, want %d", in, got, want)
		}
	}
}
