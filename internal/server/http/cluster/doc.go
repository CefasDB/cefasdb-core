// Package cluster owns the HTTP handlers for the cluster-admin
// endpoints under /v1/cluster/* (status, AddVoter, RemoveServer,
// placement plan/apply/audit, split + range-move finalize/rollback).
//
// Handlers are exposed as methods on *Handlers so the composition
// root (internal/api.Server) can wrap each handler with its standard
// auth + metrics middleware via the same register helper it uses for
// every other route. Public-bypass for /v1/cluster/status is enforced
// by the server's publicPaths map; this package treats every route
// uniformly and lets the middleware decide.
//
// The package depends on:
//
//   - internal/cluster             — Manager + placement types
//   - internal/auth                — scope checks
//   - internal/api/http/httpx      — JSON write helpers
//   - pkg/core/model               — wire NodeID
//
// It deliberately has no back-channel into internal/api so the import
// graph stays one-way (internal/api → internal/api/http/cluster,
// never the reverse).
package cluster
