// Package query owns the HTTP handlers for the query-style endpoints
// (/v1/Query, /v1/SpatialQuery, /v1/Sql and /v1/PartiQL).
//
// Handlers are exposed as methods on *Handlers so the composition
// root (internal/api.Server) can wrap each handler with its standard
// auth + metrics middleware via the same register helper it uses
// for every other route. The package depends only on:
//
//   - catalog.Catalog              — table metadata read
//   - storage.DB                   — SQL executor backing store
//   - internal/auth                — scope checks
//   - internal/spatial             — spatial query primitives
//   - internal/api/http/httpx      — JSON write helpers
//   - pkg/types                    — wire types
//   - pkg/sql                      — SQL parser/planner/executor
//   - pkg/ddbjson                  — DynamoDB-tagged JSON codec
//
// It deliberately has no back-channel into internal/api so the
// import graph stays one-way (internal/api → internal/api/http/query,
// never the reverse).
package query
