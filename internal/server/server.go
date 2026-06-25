// Package server exposes cefas operations over HTTP/JSON.
//
// Phase 1 ships HTTP/JSON only. A gRPC server (generated from
// proto/cefas.proto) will plug in at Phase 2 — it will share the same
// underlying Server type and just translate from generated stubs.
//
// JSON shape: AttributeValue mirrors DynamoDB's single-letter tagged
// union, e.g. `{"S": "alice"}` or `{"N": "42"}`. Item is a flat object
// of attribute name → AttributeValue.
package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/CefasDb/cefasdb/internal/auth"
	"github.com/CefasDb/cefasdb/internal/catalog"
	"github.com/CefasDb/cefasdb/internal/cluster"
	"github.com/CefasDb/cefasdb/internal/metrics"
	"github.com/CefasDb/cefasdb/internal/placement"
	craft "github.com/CefasDb/cefasdb/internal/replication"
	backuphttp "github.com/CefasDb/cefasdb/internal/server/http/backup"
	clusterhttp "github.com/CefasDb/cefasdb/internal/server/http/cluster"
	itemhttp "github.com/CefasDb/cefasdb/internal/server/http/item"
	queryhttp "github.com/CefasDb/cefasdb/internal/server/http/query"
	streamhttp "github.com/CefasDb/cefasdb/internal/server/http/stream"
	tablehttp "github.com/CefasDb/cefasdb/internal/server/http/table"
	"github.com/CefasDb/cefasdb/internal/server/streamcore"
	"github.com/CefasDb/cefasdb/internal/storage"
	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	"github.com/CefasDb/cefasdb/pkg/types"
)

// Cluster is the optional cluster-management surface the API exposes
// when the server runs in Raft mode. nil means single-node.
type Cluster interface {
	IsLeader() bool
	LeaderInfo() (id, addr string)
	LeaderHTTPAddr() string
	AddVoter(id, addr string, timeout time.Duration) error
	RemoveServer(id string, timeout time.Duration) error
	Barrier(timeout time.Duration) error
	SelfID() string
	BindAddr() string
}

// ChangeStream is the CDC subset of the Cluster surface. Implemented
// by *raft.DB when CDC is wired; nil when the server runs without
// raft or without an attached publisher.
type ChangeStream interface {
	SubscribeChanges(ctx context.Context) (<-chan ChangeEvent, func())
	ListSnapshots() ([]SnapshotMetadata, error)
}

// ChangeEvent is the wire-agnostic shape of a CDC entry. It is a
// type alias to streamcore.ChangeEvent so internal/api and
// internal/api/http/stream share a single source of truth.
type ChangeEvent = streamcore.ChangeEvent

// SnapshotMetadata mirrors raft.SnapshotMetadata for API consumers.
type SnapshotMetadata struct {
	ID          string
	Index       uint64
	Term        uint64
	UnixSeconds int64
	SizeBytes   int64
}

type Server struct {
	db        *pebble.DB
	cat       *catalog.Catalog
	cluster   Cluster          // nil when not running in raft mode
	stream    ChangeStream     // nil when no CDC source attached
	manager   *cluster.Manager // nil when single-shard
	validator *auth.Validator  // nil when auth disabled (dev mode)
	metrics   *metrics.Metrics // nil when metrics disabled
	backups   BackupSchedulerStatusProvider
	lifecycle *Lifecycle
}

type BackupSchedulerStatusProvider interface {
	Status() pebble.ScheduledBackupStatus
}

// AttachChangeStream wires the CDC source. Passing nil keeps the
// stream endpoints off.
func (s *Server) AttachChangeStream(c ChangeStream) { s.stream = c }

// backupChangeStream adapts s.stream into the minimal interface the
// backup handler package expects. Returns nil when no stream is
// attached so the handlers can short-circuit with an empty list.
func (s *Server) backupChangeStream() backuphttp.ChangeStream {
	if s.stream == nil {
		return nil
	}
	return backupChangeStreamAdapter{cs: s.stream}
}

type backupChangeStreamAdapter struct {
	cs ChangeStream
}

func (a backupChangeStreamAdapter) ListSnapshots() ([]backuphttp.SnapshotMetadata, error) {
	metas, err := a.cs.ListSnapshots()
	if err != nil {
		return nil, err
	}
	out := make([]backuphttp.SnapshotMetadata, len(metas))
	for i, m := range metas {
		out[i] = backuphttp.SnapshotMetadata{
			ID:          m.ID,
			Index:       m.Index,
			Term:        m.Term,
			UnixSeconds: m.UnixSeconds,
			SizeBytes:   m.SizeBytes,
		}
	}
	return out, nil
}

func (s *Server) AttachBackupScheduler(p BackupSchedulerStatusProvider) { s.backups = p }

// AttachMetrics wires the Prometheus surface. When attached, every
// handler records latency + outcome; /metrics serves the registry.
func (s *Server) AttachMetrics(m *metrics.Metrics) { s.metrics = m }

func New(db *pebble.DB, cat *catalog.Catalog) *Server {
	return &Server{db: db, cat: cat, lifecycle: NewLifecycle()}
}

// AttachManager wires the multi-shard manager. Pass nil to keep the
// server single-shard. When attached, every PK-bearing handler
// resolves the table descriptor on shard 0 (the metadata shard) and
// routes the actual write/read to the shard that owns the PK.
func (s *Server) AttachManager(m *cluster.Manager) { s.manager = m }

// readStorageFor returns the local replica that can authoritatively
// serve an eventual read for the supplied partition key.
func (s *Server) readStorageFor(pkBytes []byte) (*pebble.DB, error) {
	if s.manager == nil {
		return s.db, nil
	}
	shard, err := s.manager.ReadShardForPK(pkBytes, 0)
	if err != nil {
		return nil, err
	}
	if shard.Storage == nil {
		return nil, fmt.Errorf("cluster: read shard %d has no storage", shard.ID)
	}
	return shard.Storage, nil
}

// allShards iterates every pebble.DB this server manages. Catalog
// fan-out (CreateTable, DropTable) uses this so descriptors land on
// every shard.
func (s *Server) allShards() []*pebble.DB {
	if s.manager == nil {
		return []*pebble.DB{s.db}
	}
	out := make([]*pebble.DB, 0, len(s.manager.Shards()))
	for _, sh := range s.manager.Shards() {
		out = append(out, sh.Storage)
	}
	return out
}

func (s *Server) readShardStores() ([]*pebble.DB, error) {
	if s.manager == nil {
		return []*pebble.DB{s.db}, nil
	}
	shards, err := s.manager.ReadShards(0)
	if err != nil {
		return nil, err
	}
	out := make([]*pebble.DB, 0, len(shards))
	for _, sh := range shards {
		if sh.Storage == nil {
			return nil, fmt.Errorf("cluster: read shard %d has no storage", sh.ID)
		}
		out = append(out, sh.Storage)
	}
	return out, nil
}

func (s *Server) compact(table, lowerB64, upperB64 string, parallelize bool) ([]pebble.CompactionResult, error) {
	dbs := s.allShards()
	results := make([]pebble.CompactionResult, 0, len(dbs))
	if table != "" {
		if _, err := s.cat.Describe(table); err != nil {
			return nil, err
		}
		for _, db := range dbs {
			res, err := db.CompactTable(table, parallelize)
			if err != nil {
				return nil, err
			}
			results = append(results, res)
		}
		return results, nil
	}
	if lowerB64 == "" || upperB64 == "" {
		return nil, fmt.Errorf("table or lower/upper range is required")
	}
	lower, err := base64.StdEncoding.DecodeString(lowerB64)
	if err != nil {
		return nil, fmt.Errorf("lower: %w", err)
	}
	upper, err := base64.StdEncoding.DecodeString(upperB64)
	if err != nil {
		return nil, fmt.Errorf("upper: %w", err)
	}
	for _, db := range dbs {
		res, err := db.CompactRange(lower, upper, parallelize)
		if err != nil {
			return nil, err
		}
		results = append(results, res)
	}
	return results, nil
}

// AttachCluster wires the optional cluster-management surface. Pass a
// *raft.DB; nil keeps the server in single-node mode.
func (s *Server) AttachCluster(c Cluster) { s.cluster = c }

// AttachAuth wires bearer-token validation onto every non-public
// route. Pass nil to keep the server open (dev mode).
func (s *Server) AttachAuth(v *auth.Validator) { s.validator = v }

// publicPaths bypass the auth middleware so probes and cluster status
// stay reachable on an unjoined or pre-credentialed node.
var publicPaths = map[string]bool{
	"/v1/Health":         true,
	"/v1/cluster/status": true,
	"/livez":             true,
	"/readyz":            true,
	"/startupz":          true,
	"/raftz":             true,
}

// Routes attaches cefas HTTP endpoints onto mux. Path layout follows
// DynamoDB-ish verbs as resources under /v1/. If AttachAuth has been
// called, every non-public route is wrapped in the bearer-token
// middleware; handlers then enforce per-operation, per-table scopes
// internally.
func (s *Server) Routes(mux *http.ServeMux) {
	register := func(path string, handler http.HandlerFunc) {
		var h http.Handler = handler
		if s.metrics != nil {
			h = s.instrument(path, h)
		}
		if s.validator != nil && !publicPaths[path] {
			h = s.validator.Middleware(publicPaths)(h)
		}
		mux.Handle(path, h)
	}
	tableHandlers := tablehttp.New(s.cat, s.fanOutCatalog)
	register("/v1/tables", tableHandlers.HandleTables) // POST=create, GET=list
	register("/v1/tables/", tableHandlers.HandleTable) // GET=describe
	streamHandlers := streamhttp.New(
		s.cat,
		s.db,
		s.stream,
		s.observeStreamIteratorFailure,
		s.observeStreamGetRecords,
	)
	backupHandlers := backuphttp.New(s.db, s.cat, s.backupChangeStream(), s.allShards, s.compact)
	itemHandlers := itemhttp.New(itemhttp.Deps{
		Cat:               s.cat,
		StorageFor:        s.readStorageFor,
		WriteTargetsForPK: s.itemWriteTargetsForPK,
		BatchWriteByShard: s.batchWriteByShard,
		BatchGetByShard:   s.batchGetByShard,
		EnsureStrongRead:  s.ensureStrongRead,
		WriteWriteErr:     writeWriteErr,
		ObserveWrite:      s.observeItemWrite,
		ObserveRead:       s.observeItemRead,
	})
	queryHandlers := queryhttp.New(
		s.cat,
		s.db,
		s.readStorageFor,
		s.readShardStores,
		s.spatialAllShards,
		s.ensureStrongRead,
		s.observeRangeMetric,
	)
	register("/v1/ListStreams", streamHandlers.HandleListStreams)
	register("/v1/DescribeStream", streamHandlers.HandleDescribeStream)
	register("/v1/GetShardIterator", streamHandlers.HandleGetShardIterator)
	register("/v1/GetRecords", streamHandlers.HandleGetRecords)
	register("/v1/PutItem", itemHandlers.HandlePutItem)
	register("/v1/GetItem", itemHandlers.HandleGetItem)
	register("/v1/DeleteItem", itemHandlers.HandleDeleteItem)
	register("/v1/Query", queryHandlers.HandleQuery)
	register("/v1/BatchWriteItem", itemHandlers.HandleBatchWriteItem)
	register("/v1/BatchGetItem", itemHandlers.HandleBatchGetItem)
	register("/v1/SpatialQuery", queryHandlers.HandleSpatialQuery)
	register("/v1/Sql", queryHandlers.HandleSql)
	register("/v1/PartiQL", queryHandlers.HandlePartiQL)
	register("/v1/RestoreTableFromBackup", backupHandlers.HandleRestoreTableFromBackup)
	register("/v1/DeleteBackup", backupHandlers.HandleDeleteBackup)
	register("/v1/ApplyBackupRetention", backupHandlers.HandleApplyBackupRetention)
	register("/v1/Health", s.handleHealth)
	register("/livez", s.handleLivez)
	register("/readyz", s.handleReadyz)
	register("/startupz", s.handleStartupz)
	register("/raftz", s.handleRaftz)
	clusterHandlers := clusterhttp.New(s.cluster, s.manager, writeWriteErr, s.clusterStatusExtras)
	register("/v1/cluster/status", clusterHandlers.HandleStatus)
	register("/v1/cluster/AddVoter", clusterHandlers.HandleAddVoter)
	register("/v1/cluster/RemoveServer", clusterHandlers.HandleRemoveServer)
	register("/v1/cluster/placement/plan", clusterHandlers.HandlePlacementPlan)
	register("/v1/cluster/placement/apply", clusterHandlers.HandlePlacementApply)
	register("/v1/cluster/placement/audit", clusterHandlers.HandlePlacementAudit)
	register("/v1/cluster/placement/split/finalize", clusterHandlers.HandleSplitFinalize)
	register("/v1/cluster/placement/split/rollback", clusterHandlers.HandleSplitRollback)
	register("/v1/cluster/placement/range-move/finalize", clusterHandlers.HandleRangeMoveFinalize)
	if s.metrics != nil {
		mux.Handle("/metrics", s.metrics.Handler())
	}
	mux.HandleFunc("/v1/Stream", streamHandlers.HandleStream)
	mux.HandleFunc("/v1/admin/snapshots", backupHandlers.HandleListSnapshots)
	mux.HandleFunc("/v1/admin/compact", backupHandlers.HandleCompact)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if err := s.db.Health(r.Context()); err != nil {
		writeErr(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// clusterStatusExtras supplies the optional sections of the
// /v1/cluster/status payload (hot ranges, backup scheduler) so the
// clusterhttp package never has to import internal/metrics or
// internal/storage directly.
func (s *Server) clusterStatusExtras() map[string]any {
	out := map[string]any{}
	if s.metrics != nil {
		out["hotRanges"] = s.metrics.RangeHotspotSummaries(0)
	}
	if s.backups != nil {
		status := s.backups.Status()
		out["backupScheduler"] = &status
	}
	return out
}

func sortedPlacementNodes(cat placement.PlacementCatalog) []placement.NodeDescriptor {
	out := make([]placement.NodeDescriptor, 0, len(cat.Nodes))
	for _, node := range cat.Nodes {
		node.Capacity.Tags = append([]string(nil), node.Capacity.Tags...)
		out = append(out, node)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// barrierTimeout is the per-request wait the strong-consistency read
// path applies when forcing this node to catch up on the log before
// serving a read.
const barrierTimeout = 5 * time.Second

// ensureStrongRead implements the strong-consistency contract for
// reads: redirect non-leader reads with 307, then Barrier the leader
// so the local Pebble has applied every entry committed before this
// call returned.
//
// Returns true when the caller can proceed with a local read; false
// when the response has already been written and the handler should
// return.
func (s *Server) ensureStrongRead(w http.ResponseWriter, r *http.Request) bool {
	if s.cluster == nil {
		return true
	}
	if !s.cluster.IsLeader() {
		leader := s.cluster.LeaderHTTPAddr()
		if leader != "" {
			w.Header().Set("Location", leader+r.URL.RequestURI())
			http.Error(w, "strong read must hit the leader; redirected", http.StatusTemporaryRedirect)
		} else {
			writeErr(w, http.StatusServiceUnavailable, fmt.Errorf("no leader currently elected"))
		}
		return false
	}
	if err := s.cluster.Barrier(barrierTimeout); err != nil {
		writeErr(w, http.StatusInternalServerError, fmt.Errorf("barrier: %w", err))
		return false
	}
	return true
}

// batchWriteByShard groups ops by the shard that owns each op's PK
// and issues a per-shard BatchWriteItem. Multi-shard mode loses the
// global atomicity guarantee — the trade-off for horizontal scale.
// Single-shard mode collapses to one batch.
func (s *Server) batchWriteByShard(td types.TableDescriptor, ops []pebble.BatchOp) error {
	if s.manager == nil {
		started := time.Now()
		if err := s.db.BatchWriteItem(td, ops); err != nil {
			return err
		}
		for _, op := range ops {
			probe := op.Item
			approxBytes := estimatedItemBytes(op.Item)
			if op.Op == pebble.BatchOpDelete {
				probe = op.Key
				approxBytes = estimatedItemBytes(op.Key)
			}
			pkBytes, err := pkBytesFromItem(probe, td.KeySchema)
			if err != nil {
				return err
			}
			s.observeRangeMetric(rangeMetricWrite, pkBytes, approxBytes, started)
		}
		return nil
	}
	primaryBuckets := make(map[*pebble.DB][]pebble.BatchOp)
	mirrorBuckets := make(map[*pebble.DB][]pebble.BatchOp)
	type observation struct {
		pkBytes     []byte
		approxBytes uint64
	}
	observations := make([]observation, 0, len(ops))
	var releases []func()
	defer func() {
		for i := len(releases) - 1; i >= 0; i-- {
			releases[i]()
		}
	}()
	for _, op := range ops {
		probe := op.Item
		approxBytes := estimatedItemBytes(op.Item)
		if op.Op == pebble.BatchOpDelete {
			probe = op.Key
			approxBytes = estimatedItemBytes(op.Key)
		}
		pkBytes, err := pkBytesFromItem(probe, td.KeySchema)
		if err != nil {
			return err
		}
		observations = append(observations, observation{pkBytes: append([]byte(nil), pkBytes...), approxBytes: approxBytes})
		targets, err := s.writeTargetsForPK(pkBytes)
		if err != nil {
			return err
		}
		releases = append(releases, targets.Release)
		primaryBuckets[targets.primary] = append(primaryBuckets[targets.primary], op)
		for _, mirror := range targets.mirrors {
			mirrorBuckets[mirror] = append(mirrorBuckets[mirror], op)
		}
	}
	started := time.Now()
	if err := batchWriteBuckets(td, primaryBuckets); err != nil {
		return err
	}
	if err := batchWriteBuckets(td, mirrorBuckets); err != nil {
		return err
	}
	for _, obs := range observations {
		s.observeRangeMetric(rangeMetricWrite, obs.pkBytes, obs.approxBytes, started)
	}
	return nil
}

// batchGetByShard routes each key to the shard that owns it and
// preserves input ordering in the response. Misses stay nil.
func (s *Server) batchGetByShard(table string, ks types.KeySchema, keys []types.Item) ([]types.Item, error) {
	if s.manager == nil {
		started := time.Now()
		out, err := s.db.BatchGetItem(table, ks, keys)
		if err != nil {
			return nil, err
		}
		for i, k := range keys {
			pkBytes, err := pkBytesFromItem(k, ks)
			if err != nil {
				return nil, err
			}
			approxBytes := uint64(len(pkBytes))
			if i < len(out) && out[i] != nil {
				approxBytes = estimatedItemBytes(out[i])
			}
			s.observeRangeMetric(rangeMetricRead, pkBytes, approxBytes, started)
		}
		return out, nil
	}
	out := make([]types.Item, len(keys))
	for i, k := range keys {
		started := time.Now()
		pkBytes, err := pkBytesFromItem(k, ks)
		if err != nil {
			return nil, err
		}
		db, err := s.readStorageFor(pkBytes)
		if err != nil {
			return nil, err
		}
		single, err := db.BatchGetItem(table, ks, []types.Item{k})
		if err != nil {
			return nil, err
		}
		if len(single) == 1 {
			out[i] = single[0]
		}
		approxBytes := uint64(len(pkBytes))
		if out[i] != nil {
			approxBytes = estimatedItemBytes(out[i])
		}
		s.observeRangeMetric(rangeMetricRead, pkBytes, approxBytes, started)
	}
	return out, nil
}

// spatialAllShards scatter-gathers a spatial query across every
// shard. Spatial indexes are partitioned by the item's PK (same as
// every other write), so the matching rows can live on any shard.
// We merge results client-side; honouring the limit globally.
func (s *Server) spatialAllShards(td types.TableDescriptor, idxName string, q pebble.SpatialQuery) ([]types.Item, error) {
	if s.manager == nil {
		return s.db.SpatialQueryItems(td, idxName, q)
	}
	dbs, err := s.readShardStores()
	if err != nil {
		return nil, err
	}
	var out []types.Item
	for _, db := range dbs {
		got, err := db.SpatialQueryItems(td, idxName, q)
		if err != nil {
			return nil, err
		}
		out = append(out, got...)
		if q.Limit > 0 && len(out) >= q.Limit {
			out = out[:q.Limit]
			break
		}
	}
	return out, nil
}

// fanOutCatalog mirrors the descriptor to every shard except shard 0
// (which catalog.Create already touched). Single-shard mode is a
// no-op.
func (s *Server) fanOutCatalog(td types.TableDescriptor) {
	if s.manager == nil {
		return
	}
	for i, sh := range s.manager.Shards() {
		if i == 0 {
			continue
		}
		cat, err := catalog.New(sh.Storage)
		if err != nil {
			continue
		}
		_ = cat.Create(td)
	}
}

// instrument wraps an http.Handler with Prometheus latency +
// outcome reporting. Op label is the trailing segment of the URL
// path (e.g. "PutItem"); table label is left empty because the
// request-body-aware, PK-bearing handlers record bounded range metrics
// separately after they resolve the table and partition key.
func (s *Server) instrument(path string, h http.Handler) http.Handler {
	op := path
	if idx := strings.LastIndex(path, "/"); idx >= 0 && idx+1 < len(path) {
		op = path[idx+1:]
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		h.ServeHTTP(rw, r)
		outcome := classify(rw.status)
		s.metrics.Observe(op, "", outcome, time.Since(start).Seconds())
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func classify(status int) string {
	switch {
	case status >= 500:
		return "err"
	case status == 401:
		return "unauth"
	case status == 403:
		return "forbidden"
	case status == 412:
		return "precondition_failed"
	case status == 404:
		return "notfound"
	case status == 307:
		return "notleader"
	case status >= 400:
		return "client_err"
	}
	return "ok"
}

// pkBytesFromItem extracts the canonical PK byte form from `item`
// under the supplied key schema. Shared by every router decision.
func pkBytesFromItem(item types.Item, ks types.KeySchema) ([]byte, error) {
	pkAttr, ok := item[ks.PK]
	if !ok {
		return nil, fmt.Errorf("%w: %q", types.ErrMissingKey, ks.PK)
	}
	return storage.AttrCanonicalBytes(pkAttr)
}

// ---------- helpers ----------

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeErr(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

func mapWriteErr(err error) int {
	if errors.Is(err, types.ErrMissingKey) || errors.Is(err, types.ErrInvalidKeyType) {
		return http.StatusBadRequest
	}
	if errors.Is(err, types.ErrTableAlreadyExists) {
		return http.StatusConflict
	}
	if errors.Is(err, types.ErrStreamNotFound) || errors.Is(err, types.ErrStreamShardNotFound) {
		return http.StatusNotFound
	}
	if errors.Is(err, types.ErrStreamIteratorInvalid) {
		return http.StatusBadRequest
	}
	if errors.Is(err, types.ErrStreamIteratorExpired) || errors.Is(err, types.ErrStreamTrimmed) {
		return http.StatusPreconditionFailed
	}
	if errors.Is(err, pebble.ErrBackupNotFound) {
		return http.StatusNotFound
	}
	if errors.Is(err, pebble.ErrBackupInUse) {
		return http.StatusConflict
	}
	if errors.Is(err, pebble.ErrInvalidBackupRetention) {
		return http.StatusBadRequest
	}
	if errors.Is(err, placement.ErrInvalidPlacementPlan) {
		return http.StatusBadRequest
	}
	if errors.Is(err, cluster.ErrStaleRoute) {
		return http.StatusConflict
	}
	if errors.Is(err, storage.ErrConditionFailed) {
		// 412 Precondition Failed is the canonical status for an
		// optimistic-concurrency check that did not hold.
		return http.StatusPreconditionFailed
	}
	if errors.Is(err, pebble.ErrNotLeader) {
		// Handled separately in writeWriteErr — we want 307 with a
		// Location header, not a JSON body.
		return http.StatusTemporaryRedirect
	}
	if errors.Is(err, craft.ErrNotLeader) {
		return http.StatusServiceUnavailable
	}
	if errors.Is(err, pebble.ErrDraining) {
		return http.StatusServiceUnavailable
	}
	if errors.Is(err, pebble.ErrThrottled) {
		return http.StatusTooManyRequests
	}
	return http.StatusInternalServerError
}

// writeWriteErr is the shared error-emit path for write handlers. It
// turns a NotLeaderError into a real 307 redirect when the leader's
// HTTP URL is known; falls back to JSON otherwise.
func writeWriteErr(w http.ResponseWriter, r *http.Request, err error) {
	if errors.Is(err, pebble.ErrNotLeader) {
		var nle *pebble.NotLeaderError
		leader := ""
		if errors.As(err, &nle) {
			leader = nle.LeaderURL
		}
		if leader != "" {
			// Redirect preserves method + body on 307. The client
			// resubmits the same POST to the leader's HTTP listener.
			w.Header().Set("Location", leader+r.URL.RequestURI())
			http.Error(w, "not leader; redirected", http.StatusTemporaryRedirect)
			return
		}
		// Leader unknown (election in progress). 503 lets clients
		// back off and retry — there is no useful URL to redirect to.
		writeErr(w, http.StatusServiceUnavailable, err)
		return
	}
	writeErr(w, mapWriteErr(err), err)
}
