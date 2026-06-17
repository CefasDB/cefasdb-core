// Package backup owns the HTTP handlers for the backup / admin
// endpoints:
//
//   - /v1/RestoreTableFromBackup
//   - /v1/DeleteBackup
//   - /v1/ApplyBackupRetention
//   - /v1/admin/snapshots
//   - /v1/admin/compact
//
// Handlers are exposed as methods on *Handlers so the composition
// root (internal/api.Server) can wrap each handler with its standard
// auth + metrics middleware via the same register helper it uses for
// every other route. The package depends only on:
//
//   - catalog.Catalog              — descriptor write during restore
//   - storage.DB                   — backup/restore primitives
//   - internal/auth                — scope checks
//   - internal/api/http/httpx      — JSON write helpers
//   - pkg/types                    — wire types
//
// It deliberately has no back-channel into internal/api so the import
// graph stays one-way (internal/api → internal/api/http/backup, never
// the reverse). Cluster fan-out during restore and compaction are
// passed in as callbacks so the package never imports
// internal/cluster.
package backup
