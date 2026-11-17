package tasks

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/local/goose-process-orchestrator/migrations"
)

// migrationFile is one parsed, embedded forward-only schema file.
type migrationFile struct {
	version int
	name    string
	sql     string
}

// Migrate applies every unapplied embedded migration in ascending version
// order and records each applied version in schema_migrations. It is the single
// schema authority: production startup and test bootstraps both call it, so a
// schema change cannot reach tests without also reaching real databases.
//
// It is idempotent and forward-only. A database created by an earlier release's
// inline CREATE TABLE statements has no schema_migrations rows, so version 1 is
// re-applied; that file is written with IF NOT EXISTS throughout and is a no-op
// on such a database apart from recording the version. Migrate never drops,
// rewrites, or downgrades anything.
func Migrate(ctx context.Context, db *sql.DB) error {
	files, err := loadMigrations()
	if err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, createSchemaMigrationsSQL); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}
	applied, err := appliedVersions(ctx, db)
	if err != nil {
		return err
	}
	for _, file := range files {
		if applied[file.version] {
			continue
		}
		if err := applyMigration(ctx, db, file); err != nil {
			return err
		}
	}
	return nil
}

const createSchemaMigrationsSQL = `CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at TIMESTAMP NOT NULL
);`

// applyMigration runs one file and records its version in the same transaction,
// so a failure leaves neither a partial schema nor a false applied marker.
func applyMigration(ctx context.Context, db *sql.DB, file migrationFile) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, file.sql); err != nil {
		return fmt.Errorf("apply migration %s: %w", file.name, err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)`, file.version, time.Now().UTC()); err != nil {
		return fmt.Errorf("record migration %s: %w", file.name, err)
	}
	return tx.Commit()
}

func appliedVersions(ctx context.Context, db *sql.DB) (map[int]bool, error) {
	rows, err := db.QueryContext(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return nil, fmt.Errorf("read schema_migrations: %w", err)
	}
	defer rows.Close()
	applied := make(map[int]bool)
	for rows.Next() {
		var version int
		if err := rows.Scan(&version); err != nil {
			return nil, err
		}
		applied[version] = true
	}
	return applied, rows.Err()
}

// loadMigrations parses and orders the embedded files, rejecting any name that
// does not carry a unique positive version prefix rather than guessing an order.
func loadMigrations() ([]migrationFile, error) {
	entries, err := fs.Glob(migrations.FS, "*.sql")
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("no embedded migrations found")
	}
	files := make([]migrationFile, 0, len(entries))
	seen := make(map[int]string, len(entries))
	for _, entry := range entries {
		version, err := parseVersion(entry)
		if err != nil {
			return nil, err
		}
		if other, dup := seen[version]; dup {
			return nil, fmt.Errorf("duplicate migration version %d in %s and %s", version, other, entry)
		}
		seen[version] = entry
		contents, err := fs.ReadFile(migrations.FS, entry)
		if err != nil {
			return nil, err
		}
		files = append(files, migrationFile{version: version, name: entry, sql: string(contents)})
	}
	sort.Slice(files, func(i, j int) bool { return files[i].version < files[j].version })
	return files, nil
}

func parseVersion(name string) (int, error) {
	base := path.Base(name)
	prefix, _, found := strings.Cut(strings.TrimSuffix(base, ".sql"), "_")
	if !found || prefix == "" {
		return 0, fmt.Errorf("migration %q must be named NNN_name.sql", base)
	}
	version, err := strconv.Atoi(prefix)
	if err != nil || version < 1 {
		return 0, fmt.Errorf("migration %q must start with a positive version number", base)
	}
	return version, nil
}
