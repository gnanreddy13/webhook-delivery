// Package migrations embeds the plain SQL migration files.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
