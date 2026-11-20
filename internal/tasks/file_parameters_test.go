package tasks

import (
	"context"
	"errors"
	"testing"
)

func TestCanonicalFileParametersAndStoreIdempotency(t *testing.T) {
	first, _ := CanonicalFileParameters(map[string]string{"b": "two\n", "a": "one"})
	second, _ := CanonicalFileParameters(map[string]string{"a": "one", "b": "two\n"})
	if first != second {
		t.Fatal("encoding is order dependent")
	}
	ctx := context.Background()
	_, store := openSQLiteStore(t)
	files := map[string]string{"payload": "line 1\nline 2"}
	if _, inserted, err := store.Create(ctx, Task{ID: "t1", Recipe: "r", IdempotencyKey: "k", FileParameters: files}); err != nil || !inserted {
		t.Fatalf("create = %t, %v", inserted, err)
	}
	got, err := store.Get(ctx, "t1")
	if err != nil || got.FileParameters["payload"] != files["payload"] {
		t.Fatalf("get = %#v, %v", got.FileParameters, err)
	}
	listed, err := store.List(ctx, ListFilter{Limit: 10})
	if err != nil || listed[0].FileParameters["payload"] != files["payload"] {
		t.Fatalf("list = %#v, %v", listed, err)
	}
	claimed, err := store.Claim(ctx)
	if err != nil || claimed.FileParameters["payload"] != files["payload"] {
		t.Fatalf("claim = %#v, %v", claimed, err)
	}
	if existing, inserted, err := store.Create(ctx, Task{ID: "ignored", Recipe: "r", IdempotencyKey: "k", FileParameters: map[string]string{"payload": "line 1\nline 2"}}); err != nil || inserted || existing.ID != "t1" {
		t.Fatalf("replay = %#v, %t, %v", existing, inserted, err)
	}
	for _, changed := range []map[string]string{{"payload": "changed"}, nil, {"payload": "line 1\nline 2", "extra": "x"}} {
		if _, _, err := store.Create(ctx, Task{ID: "bad", Recipe: "r", IdempotencyKey: "k", FileParameters: changed}); !errors.Is(err, ErrIdempotencyConflict) {
			t.Fatalf("changed %#v = %v", changed, err)
		}
	}
}

func TestDecodeFileParametersFailsClosed(t *testing.T) {
	for _, stored := range []string{"{", "[]", "null", "{\"k\":1}"} {
		if _, err := decodeFileParameters(stored); err == nil {
			t.Errorf("accepted %q", stored)
		}
	}
	ctx := context.Background()
	db, store := openSQLiteStore(t)
	if _, _, err := store.Create(ctx, Task{ID: "t", Recipe: "r", IdempotencyKey: "k"}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("UPDATE tasks SET file_parameters='[1]' WHERE id='t'"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(ctx, "t"); err == nil {
		t.Fatal("corrupt file parameters accepted")
	}
}
