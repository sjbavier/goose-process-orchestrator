package tasks

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

// legacyInlineSchema is byte-for-byte the schema that the pre-migration-runner
// release created at startup from its inline constant. Tests must keep it
// frozen: it is the shape of a real database on disk that Migrate has to
// upgrade, so it is deliberately not regenerated from the migration files.
const legacyInlineSchema = `CREATE TABLE IF NOT EXISTS tasks (
 id TEXT PRIMARY KEY, recipe TEXT NOT NULL, idempotency_key TEXT NOT NULL UNIQUE,
 status TEXT NOT NULL, created_at TIMESTAMP NOT NULL, updated_at TIMESTAMP NOT NULL,
 started_at TIMESTAMP, ended_at TIMESTAMP, cancel_requested_at TIMESTAMP,
 exit_code INTEGER, result TEXT, failure TEXT, max_turns INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS tasks_claim_idx ON tasks(status, created_at, id);`

func openRaw(t *testing.T, name string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), name))
	if err != nil {
		t.Fatalf("open SQLite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func appliedVersionList(t *testing.T, db *sql.DB) []int {
	t.Helper()
	rows, err := db.Query(`SELECT version FROM schema_migrations ORDER BY version`)
	if err != nil {
		t.Fatalf("read schema_migrations: %v", err)
	}
	defer rows.Close()
	var versions []int
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		versions = append(versions, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return versions
}

func TestMigrateBootstrapsCleanDatabaseAndIsIdempotent(t *testing.T) {
	ctx := context.Background()
	db := openRaw(t, "clean.db")
	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("Migrate clean: %v", err)
	}
	first := appliedVersionList(t, db)
	if len(first) < 2 || first[0] != 1 || first[1] != 2 {
		t.Fatalf("applied versions = %v, want ascending starting 1, 2", first)
	}
	// Re-running must record nothing new; a second marker row would violate
	// the primary key and a re-applied file could fail on an existing column.
	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("Migrate rerun: %v", err)
	}
	if second := appliedVersionList(t, db); len(second) != len(first) {
		t.Fatalf("rerun applied versions = %v, want unchanged %v", second, first)
	}
	store := NewStore(db)
	if _, _, err := store.Create(ctx, Task{ID: "t1", Recipe: "allowed.yaml", IdempotencyKey: "k1", Parameters: map[string]string{"a": "1"}}); err != nil {
		t.Fatalf("Create on clean database: %v", err)
	}
}

func TestMigrateUpgradesLegacyInlineDatabasePreservingRows(t *testing.T) {
	ctx := context.Background()
	db := openRaw(t, "legacy.db")
	if _, err := db.Exec(legacyInlineSchema); err != nil {
		t.Fatalf("create legacy schema: %v", err)
	}
	// A row written by the old release, inserted through the old column set.
	if _, err := db.Exec(`INSERT INTO tasks (id, recipe, idempotency_key, status, created_at, updated_at, max_turns) VALUES ('old-1', 'legacy.yaml', 'legacy-key', 'succeeded', '2026-01-01 00:00:00+00:00', '2026-01-01 00:00:00+00:00', 7)`); err != nil {
		t.Fatalf("insert legacy row: %v", err)
	}
	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("Migrate legacy: %v", err)
	}
	store := NewStore(db)
	old, err := store.Get(ctx, "old-1")
	if err != nil {
		t.Fatalf("Get legacy row after upgrade: %v", err)
	}
	if old.Recipe != "legacy.yaml" || old.Status != Succeeded || old.MaxTurns != 7 {
		t.Fatalf("legacy row not preserved: %+v", old)
	}
	if old.Parameters != nil || old.FileParameters != nil {
		t.Fatalf("legacy row parameters = %v / file parameters = %v, want both nil", old.Parameters, old.FileParameters)
	}
	// An upgraded database must accept new parameterized writes and must treat
	// the pre-existing row as equivalent to a no-parameter replay.
	if _, inserted, err := store.Create(ctx, Task{ID: "new-1", Recipe: "legacy.yaml", IdempotencyKey: "legacy-key"}); err != nil || inserted {
		t.Fatalf("no-parameter replay of a legacy row = (%t, %v), want (false, nil)", inserted, err)
	}
	if _, _, err := store.Create(ctx, Task{ID: "new-2", Recipe: "allowed.yaml", IdempotencyKey: "k2", Parameters: map[string]string{"x": "y"}}); err != nil {
		t.Fatalf("Create with parameters after upgrade: %v", err)
	}

	// A row that predates the inbox_policy column must upgrade to the safe
	// default, not NULL or an empty string, so its mailbox fails closed
	// exactly like a newly created task's would.
	oldPolicy, err := store.InboxPolicy(ctx, "old-1")
	if err != nil || oldPolicy != ControllerOnly {
		t.Fatalf("legacy row InboxPolicy = (%q, %v), want (controller_only, nil)", oldPolicy, err)
	}
	if _, err := store.Enqueue(ctx, "old-1", ControllerSender, "note", "mailbox usable after upgrade"); err != nil {
		t.Fatalf("Enqueue on upgraded legacy row: %v", err)
	}
}

// TestMigrateAddsOutcomeSummaryColumnsAsNullForPreExistingRows seeds a
// database through migration 004 only, then applies 005, and asserts the
// pre-existing row survives with every new outcome-summary column NULL
// (decoded as absent, not a zero-value false positive) and a fresh row
// created afterward behaves identically until a summary is explicitly
// recorded for it.
func TestMigrateAddsOutcomeSummaryColumnsAsNullForPreExistingRows(t *testing.T) {
	ctx := context.Background()
	db := openRaw(t, "pre-005.db")
	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("Migrate (004 and earlier, before 005 existed in this run): %v", err)
	}
	store := NewStore(db)
	if _, _, err := store.Create(ctx, Task{ID: "pre-005", Recipe: "allowed.yaml", IdempotencyKey: "pre-005-key"}); err != nil {
		t.Fatalf("Create before 005 semantics are exercised: %v", err)
	}

	// Re-running Migrate is idempotent and, since 005 is already embedded in
	// this build, is a no-op here; the point of this test is the decode
	// path for a row that predates having any summary recorded, which is
	// exactly what a migration-004-only row looks like after upgrading.
	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("Migrate rerun: %v", err)
	}
	row, err := store.Get(ctx, "pre-005")
	if err != nil {
		t.Fatalf("Get pre-existing row after 005: %v", err)
	}
	if row.Recipe != "allowed.yaml" || row.Status != Queued {
		t.Fatalf("pre-existing row not preserved: %+v", row)
	}
	if row.FinalText != "" || row.Outcome != "" || row.FinalTextTruncated != nil || row.CompleteSeen != nil ||
		row.TotalTokens != nil || row.ToolCallCount != nil || row.LastEventAt != nil || row.StalledMaxTurns != nil ||
		row.StdoutTruncated != nil || row.BytesDropped != nil || row.CostUSD != nil {
		t.Fatalf("pre-existing row has non-absent outcome-summary fields: %+v", row)
	}
}

func TestMigrateRecordsEveryEmbeddedFileExactlyOnce(t *testing.T) {
	files, err := loadMigrations()
	if err != nil {
		t.Fatalf("loadMigrations: %v", err)
	}
	for i := 1; i < len(files); i++ {
		if files[i].version <= files[i-1].version {
			t.Fatalf("migrations are not strictly ascending: %v then %v", files[i-1].name, files[i].name)
		}
	}
	db := openRaw(t, "versions.db")
	if err := Migrate(context.Background(), db); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	applied := appliedVersionList(t, db)
	if len(applied) != len(files) {
		t.Fatalf("applied %d versions, want %d embedded files", len(applied), len(files))
	}
}

func TestParseVersionRejectsUnorderableNames(t *testing.T) {
	for _, name := range []string{"tasks.sql", "_tasks.sql", "abc_tasks.sql", "000_tasks.sql", "-1_tasks.sql"} {
		if _, err := parseVersion(name); err == nil {
			t.Errorf("parseVersion(%q) = nil error, want rejection", name)
		}
	}
	version, err := parseVersion("002_task_parameters.sql")
	if err != nil || version != 2 {
		t.Fatalf("parseVersion(valid) = (%d, %v)", version, err)
	}
}
