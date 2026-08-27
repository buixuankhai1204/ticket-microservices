// Package migrations embeds the SQL migration files applied at startup by
// internal/platform/db.Migrate. The embed directive can only reach files at or
// below this directory, which is why the migrations live in their own package.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
