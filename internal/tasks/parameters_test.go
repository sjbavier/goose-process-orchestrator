package tasks

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestCanonicalParametersIsOrderIndependentAndEmptyIsBlank(t *testing.T) {
	if encoded, err := CanonicalParameters(nil); err != nil || encoded != "" {
		t.Fatalf("CanonicalParameters(nil) = (%q, %v), want (\"\", nil)", encoded, err)
	}
	if encoded, err := CanonicalParameters(map[string]string{}); err != nil || encoded != "" {
		t.Fatalf("CanonicalParameters(empty) = (%q, %v), want (\"\", nil)", encoded, err)
	}
	// Go map iteration order is randomized, so building the same logical map
	// by different insertion orders must still encode identically.
	first, err := CanonicalParameters(map[string]string{"alpha": "1", "beta": "2", "gamma": "3"})
	if err != nil {
		t.Fatal(err)
	}
	second := map[string]string{}
	for _, k := range []string{"gamma", "beta", "alpha"} {
		second[k] = map[string]string{"alpha": "1", "beta": "2", "gamma": "3"}[k]
	}
	encodedSecond, err := CanonicalParameters(second)
	if err != nil {
		t.Fatal(err)
	}
	if first != encodedSecond {
		t.Fatalf("encoding is order dependent: %q vs %q", first, encodedSecond)
	}
	if !strings.HasPrefix(first, `{"alpha"`) {
		t.Fatalf("encoding = %q, want deterministic key-sorted JSON", first)
	}
}

func TestCanonicalParametersPreservesAwkwardButValidValues(t *testing.T) {
	// PRM-01 evidence showed Goose renders `=` in a value unchanged; the
	// encoding boundary must not mangle it either. Shape limits are the
	// validator's job, not this function's.
	params := map[string]string{"pair": "a=b", "blank": ""}
	encoded, err := CanonicalParameters(params)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeParameters(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if decoded["pair"] != "a=b" || decoded["blank"] != "" {
		t.Fatalf("round trip lost data: %v", decoded)
	}
}

func TestDecodeParametersFailsClosedOnCorruptValue(t *testing.T) {
	for _, stored := range []string{"{", "[]", "null", `{"k":1}`, "not json"} {
		if _, err := decodeParameters(stored); err == nil {
			t.Errorf("decodeParameters(%q) = nil error, want fail closed", stored)
		}
	}
	if params, err := decodeParameters(""); err != nil || params != nil {
		t.Fatalf("decodeParameters(\"\") = (%v, %v), want (nil, nil)", params, err)
	}
}

func TestStoreRoundTripsParametersAndIncludesThemInIdempotency(t *testing.T) {
	ctx := context.Background()
	_, store := openSQLiteStore(t)
	params := map[string]string{"feature_slug": "spawn-recipe-parameters", "mode": "local-autonomous"}
	created, inserted, err := store.Create(ctx, Task{ID: "task-1", Recipe: "allowed.yaml", IdempotencyKey: "key-1", Parameters: params})
	if err != nil || !inserted {
		t.Fatalf("Create = (%+v, %t, %v)", created, inserted, err)
	}
	loaded, err := store.Get(ctx, "task-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(loaded.Parameters) != 2 || loaded.Parameters["mode"] != "local-autonomous" {
		t.Fatalf("persisted parameters = %v", loaded.Parameters)
	}

	// An unordered-equivalent map is the same request.
	reordered := map[string]string{"mode": "local-autonomous", "feature_slug": "spawn-recipe-parameters"}
	existing, inserted, err := store.Create(ctx, Task{ID: "ignored", Recipe: "allowed.yaml", IdempotencyKey: "key-1", Parameters: reordered})
	if err != nil || inserted || existing.ID != "task-1" {
		t.Fatalf("equivalent replay = (%+v, %t, %v)", existing, inserted, err)
	}

	// A different map under the same key is a conflict, not a silent reuse of
	// a task that would run different inputs.
	changed := map[string]string{"feature_slug": "spawn-recipe-parameters", "mode": "bounded-repair"}
	if _, _, err := store.Create(ctx, Task{ID: "conflict", Recipe: "allowed.yaml", IdempotencyKey: "key-1", Parameters: changed}); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("changed parameters = %v, want ErrIdempotencyConflict", err)
	}
	if _, _, err := store.Create(ctx, Task{ID: "conflict", Recipe: "allowed.yaml", IdempotencyKey: "key-1"}); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("dropped parameters = %v, want ErrIdempotencyConflict", err)
	}
}

func TestStoreListAndClaimCarryParameters(t *testing.T) {
	ctx := context.Background()
	_, store := openSQLiteStore(t)
	if _, _, err := store.Create(ctx, Task{ID: "task-1", Recipe: "allowed.yaml", IdempotencyKey: "key-1", Parameters: map[string]string{"k": "v"}}); err != nil {
		t.Fatal(err)
	}
	listed, err := store.List(ctx, ListFilter{Limit: 10})
	if err != nil || len(listed) != 1 {
		t.Fatalf("List = (%v, %v)", listed, err)
	}
	if listed[0].Parameters["k"] != "v" {
		t.Fatalf("listed parameters = %v", listed[0].Parameters)
	}
	claimed, err := store.Claim(ctx)
	if err != nil || claimed == nil {
		t.Fatalf("Claim = (%v, %v)", claimed, err)
	}
	if claimed.Parameters["k"] != "v" {
		t.Fatalf("claimed parameters = %v, want the persisted map so the runner can launch with them", claimed.Parameters)
	}
}

func TestStoreRejectsCorruptStoredParameters(t *testing.T) {
	ctx := context.Background()
	db, store := openSQLiteStore(t)
	if _, _, err := store.Create(ctx, Task{ID: "task-1", Recipe: "allowed.yaml", IdempotencyKey: "key-1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE tasks SET parameters = '{oops' WHERE id = 'task-1'`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(ctx, "task-1"); err == nil {
		t.Fatal("Get must fail closed on a corrupt parameter encoding")
	}
	if _, err := store.List(ctx, ListFilter{Limit: 10}); err == nil {
		t.Fatal("List must fail closed on a corrupt parameter encoding")
	}
}
