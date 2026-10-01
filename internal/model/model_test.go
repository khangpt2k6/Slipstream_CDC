package model

import "testing"

func TestDocEventValidate(t *testing.T) {
	good := DocEvent{
		Op:      OpUpsert,
		Version: 10,
		Doc: Document{
			ID: "github:golang/go#1", Datasource: "github", Container: "github:golang/go",
			Allowed: []string{Container("github:golang/go")},
		},
	}
	if err := good.Validate(); err != nil {
		t.Fatalf("valid event rejected: %v", err)
	}

	cases := map[string]func(e *DocEvent){
		"no id":         func(e *DocEvent) { e.Doc.ID = "" },
		"zero version":  func(e *DocEvent) { e.Version = 0 },
		"unknown op":    func(e *DocEvent) { e.Op = "merge" },
		"no container":  func(e *DocEvent) { e.Doc.Container = "" },
		"bad principal": func(e *DocEvent) { e.Doc.Allowed = []string{"alice"} },
	}
	for name, mutate := range cases {
		e := good
		e.Doc.Allowed = append([]string(nil), good.Doc.Allowed...)
		mutate(&e)
		if err := e.Validate(); err == nil {
			t.Errorf("%s: Validate = nil, want error", name)
		}
	}

	del := DocEvent{Op: OpDelete, Version: 3, Doc: Document{ID: "jira:KAFKA-1"}}
	if err := del.Validate(); err != nil {
		t.Errorf("delete with only an id rejected: %v", err)
	}
}

func TestEdgeEventValidate(t *testing.T) {
	ok := []EdgeEvent{
		{From: User("alice"), To: Group("eng"), Version: 1},
		{From: Group("eng"), To: Group("all-eng"), Version: 1},
		{From: Everyone, To: Container("slack:C1"), Version: 1},
		{From: User("bob"), To: Container("slack:C2"), Version: 1},
	}
	for _, e := range ok {
		if err := e.Validate(); err != nil {
			t.Errorf("%s rejected: %v", e.Key(), err)
		}
	}
	bad := []EdgeEvent{
		{From: Container("slack:C1"), To: Group("eng"), Version: 1}, // containers are leaves
		{From: Group("eng"), To: User("alice"), Version: 1},         // nothing points at a user
		{From: "alice", To: Group("eng"), Version: 1},               // no prefix
		{From: User("a|b"), To: Group("eng"), Version: 1},           // separator in id
		{From: User("alice"), To: Group("eng"), Version: 0},         // no version
	}
	for _, e := range bad {
		if err := e.Validate(); err == nil {
			t.Errorf("%q -> %q accepted, want error", e.From, e.To)
		}
	}
}

func TestContentHashIgnoresMetadata(t *testing.T) {
	a := Document{Title: "t", Body: "b", Labels: []string{"x"}}
	b := Document{Title: "t", Body: "b", Labels: []string{"y"}, Author: "z"}
	if a.ContentHash() != b.ContentHash() {
		t.Error("hash changed with metadata only; embeddings would be recomputed needlessly")
	}
	c := Document{Title: "tb", Body: ""}
	if a.ContentHash() == c.ContentHash() {
		t.Error("title/body boundary not separated in hash")
	}
}
