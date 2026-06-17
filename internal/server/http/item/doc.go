// Package item owns the HTTP handlers for the item-resource
// endpoints (/v1/PutItem, /v1/GetItem, /v1/DeleteItem,
// /v1/BatchWriteItem, /v1/BatchGetItem).
//
// Handlers are exposed as methods on *Handlers so the composition
// root (internal/api.Server) can wrap each handler with its standard
// auth + metrics middleware via the same register helper it uses
// for every other route. The package depends only on:
//
//   - catalog.Catalog              — table metadata reads
//   - storage.DB                   — local single-shard storage fallback
//   - internal/auth                — scope checks
//   - internal/api/http/httpx      — JSON write helpers
//   - pkg/types                    — wire types
//   - pkg/ddbjson                  — DynamoDB-style JSON attribute encoding
//
// All multi-shard routing concerns (write-target selection, batch
// fan-out, strong-read leader checks, range-metric observation)
// arrive as function-typed fields on Deps so the package never has
// to know about cluster.Manager and stays one-way in the import
// graph (internal/api → internal/api/http/item, never the reverse).
package item
