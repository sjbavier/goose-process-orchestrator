package tasks

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

// TestMigration007UpgradesFrom006PreservingLegacyRows applies migrations 1-6
// only, inserts a row, then runs the full Migrate and checks the row reads back
// with every new field absent.
func TestMigration007UpgradesFrom006PreservingLegacyRows(t *testing.T) {
	ctx := context.Background()
	db := openRaw(t, "from006.db")
	files, err := loadMigrations()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at TIMESTAMP NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		if f.version > 6 {
			continue
		}
		if _, err := db.ExecContext(ctx, f.sql); err != nil {
			t.Fatalf("apply %s: %v", f.name, err)
		}
		if _, err := db.ExecContext(ctx, `INSERT INTO schema_migrations (version, applied_at) VALUES (?, CURRENT_TIMESTAMP)`, f.version); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO tasks (id, recipe, idempotency_key, status, created_at, updated_at, max_turns) VALUES ('old', 'r.yaml', 'k', 'succeeded', '2026-01-01 00:00:00+00:00', '2026-01-01 00:00:00+00:00', 3)`); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("Migrate from 006: %v", err)
	}
	got, err := NewStore(db).Get(ctx, "old")
	if err != nil {
		t.Fatal(err)
	}
	if got.Recipe != "r.yaml" || got.MaxTurns != 3 || got.Status != Succeeded {
		t.Fatalf("legacy row changed: %+v", got)
	}
	if got.RecipeSHA256 != "" || got.Provider != "" || got.Model != "" || got.GooseVersion != "" ||
		got.ExpectRecipeSHA256 != "" || got.ExpectProvider != "" || got.ExpectModel != "" ||
		got.TimeoutSeconds != 0 || got.LeftoverPIDs != nil || got.LeftoverDetection != "" {
		t.Fatalf("legacy row has non-absent dispatch fields: %+v", got)
	}
}

func TestDispatchGuaranteeFieldsRoundTrip(t *testing.T) {
	ctx := context.Background()
	_, store := openSQLiteStore(t)
	in := Task{ID: "t1", Recipe: "r", IdempotencyKey: "k", RecipeSHA256: "abc", ExpectRecipeSHA256: "abc", ExpectProvider: "p", ExpectModel: "m", TimeoutSeconds: 90}
	if _, inserted, err := store.Create(ctx, in); err != nil || !inserted {
		t.Fatalf("create = %t, %v", inserted, err)
	}
	got, err := store.Get(ctx, "t1")
	if err != nil || got.RecipeSHA256 != "abc" || got.ExpectRecipeSHA256 != "abc" || got.ExpectProvider != "p" || got.ExpectModel != "m" || got.TimeoutSeconds != 90 {
		t.Fatalf("get = %+v, %v", got, err)
	}
	// Identical replay returns the original task (ADJ-1).
	replay, inserted, err := store.Create(ctx, Task{ID: "x", Recipe: "r", IdempotencyKey: "k", ExpectRecipeSHA256: "abc", ExpectProvider: "p", ExpectModel: "m", TimeoutSeconds: 90})
	if err != nil || inserted || replay.ID != "t1" {
		t.Fatalf("replay = %+v, %t, %v", replay, inserted, err)
	}
	if _, err := store.RecordEffectiveProviderModel(ctx, "t1", "claude-acp", "sonnet"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordGooseVersion(ctx, "t1", "1.60.2"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordRecipeSHA256(ctx, "t1", "def"); err != nil {
		t.Fatal(err)
	}
	listed, err := store.List(ctx, ListFilter{Limit: 5})
	if err != nil || len(listed) != 1 || listed[0].Provider != "claude-acp" || listed[0].Model != "sonnet" || listed[0].GooseVersion != "1.60.2" || listed[0].RecipeSHA256 != "def" {
		t.Fatalf("list = %+v, %v", listed, err)
	}
	for _, id := range []string{"missing"} {
		if _, err := store.RecordGooseVersion(ctx, id, "v"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("missing task err = %v", err)
		}
	}
}

func TestRecordLeftoversDistinguishesNoneUnavailableAndAbsent(t *testing.T) {
	ctx := context.Background()
	_, store := openSQLiteStore(t)
	if _, _, err := store.Create(ctx, Task{ID: "t", Recipe: "r", IdempotencyKey: "k"}); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		pids      []int
		detection string
		want      []int
	}{
		{[]int{11, 22}, LeftoverDetectionOK, []int{11, 22}},
		{nil, LeftoverDetectionOK, []int{}},
		{nil, LeftoverDetectionUnavailable, nil},
	}
	for _, c := range cases {
		got, err := store.RecordLeftovers(ctx, "t", c.pids, c.detection)
		if err != nil || got.LeftoverDetection != c.detection || !reflect.DeepEqual(got.LeftoverPIDs, c.want) {
			t.Fatalf("RecordLeftovers(%v,%s) = %+v, %v; want pids %#v", c.pids, c.detection, got, err, c.want)
		}
	}
	if _, err := store.RecordLeftovers(ctx, "t", []int{1}, LeftoverDetectionUnavailable); err == nil {
		t.Fatal("unavailable with pids accepted")
	}
	if _, err := store.RecordLeftovers(ctx, "t", nil, "bogus"); err == nil {
		t.Fatal("bogus detection accepted")
	}
}

func TestDecodeLeftoverPIDsFailsClosed(t *testing.T) {
	ctx := context.Background()
	db, store := openSQLiteStore(t)
	if _, _, err := store.Create(ctx, Task{ID: "t", Recipe: "r", IdempotencyKey: "k"}); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{"{", "null", `{"a":1}`} {
		if _, err := db.Exec(`UPDATE tasks SET leftover_pids=? WHERE id='t'`, bad); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Get(ctx, "t"); err == nil {
			t.Fatalf("corrupt leftover_pids %q accepted", bad)
		}
	}
}

func TestIdempotencyEquivalenceCoversGuardFields(t *testing.T) {
	ctx := context.Background()
	_, store := openSQLiteStore(t)
	base := Task{Recipe: "r", IdempotencyKey: "k", MaxTurns: 5, ExpectRecipeSHA256: "abc", ExpectProvider: "p", ExpectModel: "m", TimeoutSeconds: 60}
	base.ID = "t1"
	if _, inserted, err := store.Create(ctx, base); err != nil || !inserted {
		t.Fatalf("create = %t, %v", inserted, err)
	}
	mutations := map[string]func(*Task){
		"max_turns":            func(x *Task) { x.MaxTurns = 6 },
		"timeout_seconds":      func(x *Task) { x.TimeoutSeconds = 61 },
		"expect_recipe_sha256": func(x *Task) { x.ExpectRecipeSHA256 = "def" },
		"expect_provider":      func(x *Task) { x.ExpectProvider = "other" },
		"expect_model":         func(x *Task) { x.ExpectModel = "other" },
		"omitted_max_turns":    func(x *Task) { x.MaxTurns = 0 },
		"omitted_expect_model": func(x *Task) { x.ExpectModel = "" },
	}
	for name, mutate := range mutations {
		in := base
		in.ID = "x-" + name
		mutate(&in)
		if _, _, err := store.Create(ctx, in); !errors.Is(err, ErrIdempotencyConflict) {
			t.Fatalf("%s: err = %v, want ErrIdempotencyConflict", name, err)
		}
	}
	same := base
	same.ID = "x-same"
	if got, inserted, err := store.Create(ctx, same); err != nil || inserted || got.ID != "t1" {
		t.Fatalf("identical replay = %+v, %t, %v", got, inserted, err)
	}
}

func TestIdempotencyOmittedGuardsAndLegacyRowReplay(t *testing.T) {
	ctx := context.Background()
	db, store := openSQLiteStore(t)
	if _, inserted, err := store.Create(ctx, Task{ID: "a", Recipe: "r", IdempotencyKey: "k1"}); err != nil || !inserted {
		t.Fatalf("create = %t, %v", inserted, err)
	}
	if got, inserted, err := store.Create(ctx, Task{ID: "b", Recipe: "r", IdempotencyKey: "k1"}); err != nil || inserted || got.ID != "a" {
		t.Fatalf("omitted replay = %+v, %t, %v", got, inserted, err)
	}
	// Legacy-shaped row: guard columns left at their defaults.
	if _, err := db.ExecContext(ctx, `INSERT INTO tasks (id, recipe, idempotency_key, status, created_at, updated_at, max_turns) VALUES ('old', 'r', 'k2', 'succeeded', '2026-01-01 00:00:00+00:00', '2026-01-01 00:00:00+00:00', 0)`); err != nil {
		t.Fatal(err)
	}
	if got, inserted, err := store.Create(ctx, Task{ID: "c", Recipe: "r", IdempotencyKey: "k2"}); err != nil || inserted || got.ID != "old" {
		t.Fatalf("legacy replay = %+v, %t, %v", got, inserted, err)
	}
}
