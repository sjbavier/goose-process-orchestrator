// Package migrations exposes the forward-only SQL schema files as an embedded
// filesystem so production and tests apply the same schema source. The files
// stay in this directory rather than moving under a consumer package, because
// go:embed cannot reach a parent directory and an applied migration's path is
// part of its identity.
package migrations

import "embed"

// FS holds every numbered migration file. Names must match NNN_name.sql; the
// numeric prefix is the migration's version and must be unique.
//
//go:embed *.sql
var FS embed.FS
