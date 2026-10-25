package connectors

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/khangpt2k6/Slipstream_CDC/internal/model"
)

type nopProducer struct{ n int }

func (p *nopProducer) Docs(_ context.Context, evs ...model.DocEvent) error {
	p.n += len(evs)
	return nil
}

func TestDumpRoundTrip(t *testing.T) {
	dir := t.TempDir()
	d, err := OpenDump(dir, "jira")
	if err != nil {
		t.Fatal(err)
	}
	p := &nopProducer{}
	e := &Emitter{Prod: p, Source: "jira", Dump: d}
	want := []model.DocEvent{
		{Op: model.OpUpsert, Version: 1, Doc: model.Document{ID: "jira:KAFKA-1", Title: "a\nb", Duplicates: []string{"jira:KAFKA-2"}}},
		{Op: model.OpDelete, Version: 2, Doc: model.Document{ID: "jira:KAFKA-1"}},
	}
	if err := e.Emit(context.Background(), want...); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if p.n != 2 {
		t.Errorf("produced %d", p.n)
	}

	var got []model.DocEvent
	if err := ReadDump(filepath.Join(dir, "jira.jsonl"), func(ev model.DocEvent) error {
		got = append(got, ev)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Doc.Title != "a\nb" || got[0].Doc.Duplicates[0] != "jira:KAFKA-2" || got[1].Op != model.OpDelete {
		t.Errorf("read back %+v", got)
	}
}

func TestOpenDumpDisabled(t *testing.T) {
	d, err := OpenDump("", "x")
	if err != nil || d != nil {
		t.Fatalf("empty dir: %v %v", d, err)
	}
	if err := d.Close(); err != nil {
		t.Error(err)
	}
}
