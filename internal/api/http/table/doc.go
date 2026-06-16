// Package table owns the HTTP handlers for the table-resource
// endpoints (/v1/tables, /v1/tables/<name>).
//
// Handlers are exposed as methods on *Handlers so the composition
// root (internal/api.Server) can wrap each handler with its standard
// auth + metrics middleware via the same register helper it uses
// for every other route. The package depends only on:
//
//   - catalog.Catalog              — table metadata read/write
//   - internal/auth                — scope checks
//   - internal/api/http/httpx      — JSON write helpers
//   - pkg/types                    — wire types
//
// It deliberately has no back-channel into internal/api so the
// import graph stays one-way (internal/api → internal/api/http/table,
// never the reverse).
package table
