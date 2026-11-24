package fixture

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"math"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	recoverymodel "github.com/CefasDb/cefasdb/internal/recovery/model"
	"github.com/CefasDb/cefasdb/internal/recovery/repository"
	"github.com/CefasDb/cefasdb/pkg/client"
	cefaspb "github.com/CefasDb/cefasdb/pkg/protocol"
	"github.com/CefasDb/cefasdb/pkg/types"
)

const (
	sourceEvidenceVersion      = uint32(1)
	typedClientDigestNamespace = "recovery-fixture-v1/typed-client-item-v1"
)

var fnv64Pattern = regexp.MustCompile(`^[0-9a-f]{16}$`)

type TTLVisibilitySummary struct {
	Attribute               string `json:"attribute"`
	ClockSource             string `json:"clockSource"`
	ObservationEpochSeconds uint64 `json:"observationEpochSeconds"`
	ExpectedExpiredRows     uint64 `json:"expectedExpiredRows"`
	ExpectedVisibleRows     uint64 `json:"expectedVisibleRows"`
	SHA256                  string `json:"sha256"`
}

type TableSummary struct {
	Table                  string                `json:"table"`
	PhysicalRows           uint64                `json:"physicalRows"`
	LogicalRows            uint64                `json:"logicalRows"`
	LogicalBytes           uint64                `json:"logicalBytes"`
	DescriptorSHA256       string                `json:"descriptorSha256"`
	CanonicalItemSHA256    string                `json:"canonicalItemSha256"`
	CanonicalItemFNV1A64   string                `json:"canonicalItemFnv1a64"`
	TTLSHA256              string                `json:"ttlSha256"`
	TTLVisibility          *TTLVisibilitySummary `json:"ttlVisibility,omitempty"`
	StreamDescriptorSHA256 string                `json:"streamDescriptorSha256"`
}

type QuerySummary struct {
	ID     string `json:"id"`
	Count  uint64 `json:"count"`
	SHA256 string `json:"sha256"`
}

type SourceSummary struct {
	SchemaVersion          uint32                    `json:"schemaVersion"`
	FixtureID              string                    `json:"fixtureId"`
	ManifestSHA256         string                    `json:"manifestSha256"`
	Seed                   uint64                    `json:"seed"`
	RequestedLogicalBytes  uint64                    `json:"requestedLogicalBytes"`
	LogicalBytes           uint64                    `json:"logicalBytes"`
	ContractRef            recoverymodel.ContractRef `json:"contractRef"`
	ScopeID                string                    `json:"scopeId"`
	SourceEnvironmentID    string                    `json:"sourceEnvironmentId"`
	RecoveryPointSHA256    string                    `json:"recoveryPointSha256"`
	StreamCheckpointSHA256 string                    `json:"streamCheckpointSha256"`
	ShardCount             uint32                    `json:"shardCount"`
	ReplicationFactor      uint32                    `json:"replicationFactor"`
	PlacementEpoch         uint64                    `json:"placementEpoch"`
	ShardIDs               []uint32                  `json:"shardIds"`
	TopologySHA256         string                    `json:"topologySha256"`
	Tables                 []TableSummary            `json:"tables"`
	Queries                []QuerySummary            `json:"queries"`
}

// MarshalJSON refuses to serialize an unbound source summary as final evidence.
func (SourceSummary) MarshalJSON() ([]byte, error) {
	return nil, errors.New("fixture: unbound SourceSummary is not serializable")
}

type SourceEvidence struct {
	SchemaVersion              uint32                    `json:"schemaVersion"`
	FixtureID                  string                    `json:"fixtureId"`
	ManifestSHA256             string                    `json:"manifestSha256"`
	Seed                       uint64                    `json:"seed"`
	RequestedLogicalBytes      uint64                    `json:"requestedLogicalBytes"`
	LogicalBytes               uint64                    `json:"logicalBytes"`
	ContractRef                recoverymodel.ContractRef `json:"contractRef"`
	ScopeID                    string                    `json:"scopeId"`
	SourceEnvironmentID        string                    `json:"sourceEnvironmentId"`
	BackupID                   string                    `json:"backupId"`
	BackupSetManifestSHA256    string                    `json:"backupSetManifestSha256"`
	RecoveryPointSHA256        string                    `json:"recoveryPointSha256"`
	RecoveryPointCatalogSHA256 string                    `json:"recoveryPointCatalogSha256"`
	StreamCheckpointSHA256     string                    `json:"streamCheckpointSha256"`
	ShardCount                 uint32                    `json:"shardCount"`
	ReplicationFactor          uint32                    `json:"replicationFactor"`
	PlacementEpoch             uint64                    `json:"placementEpoch"`
	ShardIDs                   []uint32                  `json:"shardIds"`
	TopologySHA256             string                    `json:"topologySha256"`
	Tables                     []TableSummary            `json:"tables"`
	Queries                    []QuerySummary            `json:"queries"`
	SHA256                     string                    `json:"sha256,omitempty"`
}

type CollectRequest struct {
	Manifest            FixtureManifest
	Contract            recoverymodel.RecoveryContract
	ContractRef         recoverymodel.ContractRef
	SourceEnvironmentID string
	RecoveryPoint       repository.RecoveryPoint
}

type scanQueryPort interface {
	ScanItems(context.Context, string, func(types.Item) error) error
	QueryItems(context.Context, QueryExpectation, func(types.Item) error) error
}

type streamCatalogPort interface {
	ListStreams(context.Context, client.ListStreamsOptions) (client.ListStreamsResult, error)
	DescribeStream(context.Context, string, client.DescribeStreamOptions) (client.StreamDescription, error)
}

type streamRecordPort interface {
	GetShardIterator(context.Context, client.GetShardIteratorOptions) (string, error)
	GetRecords(context.Context, string, client.GetRecordsOptions) (client.GetRecordsResult, error)
}

type streamEvidencePorts struct {
	catalog streamCatalogPort
	records streamRecordPort
}

type collectPorts struct {
	schema   schemaPort
	ttl      ttlPort
	topology topologyPort
	source   scanQueryPort
	streams  streamEvidencePorts
}

// CollectSourceSummary hashes fixture state only while the typed stream high-water is fenced at req.RecoveryPoint.
func CollectSourceSummary(ctx context.Context, cli *client.Client, req CollectRequest) (SourceSummary, error) {
	if cli == nil {
		return SourceSummary{}, failure(CodeSeedMismatch, "CollectSourceSummary", "client", "NIL_CLIENT", nil)
	}
	adapter := clientAdapter{cli: cli}
	return collectSourceSummary(ctx, collectPorts{
		schema: adapter, ttl: adapter, topology: adapter, source: adapter,
		streams: streamEvidencePorts{catalog: adapter, records: adapter},
	}, req)
}

func collectSourceSummary(ctx context.Context, ports collectPorts, req CollectRequest) (SourceSummary, error) {
	seedReq := SeedRequest{
		Manifest: req.Manifest, Seed: req.Manifest.Seed, Contract: req.Contract, ContractRef: req.ContractRef,
	}
	if err := validateSeedRequest(seedReq); err != nil {
		return SourceSummary{}, err
	}
	if !validID(req.SourceEnvironmentID) {
		return SourceSummary{}, failure(CodeSeedMismatch, "CollectSourceSummary", "sourceEnvironmentId", "INVALID_IDENTITY", nil)
	}
	statusValue, err := ports.topology.ClusterStatus(ctx)
	if err != nil {
		return SourceSummary{}, failure(CodeUnsupportedTopology, "CollectSourceSummary", "topology", "STATUS_FAILED", err)
	}
	topology, err := normalizeTopology(statusValue, req.Contract.Workload)
	if err != nil {
		return SourceSummary{}, err
	}
	recoveryPointDigest, checkpointDigest, err := validateCollectionPoint(req, topology)
	if err != nil {
		return SourceSummary{}, err
	}
	streamRequest := streamObservationRequest{
		Manifest: req.Manifest, Topology: topology, RecoveryPoint: req.RecoveryPoint,
		RecoveryPointSHA256: recoveryPointDigest, StreamCheckpointSHA256: checkpointDigest,
	}
	streamsBefore, streamFenceBefore, err := observeRecoveryStreams(ctx, ports.streams, streamRequest)
	if err != nil {
		return SourceSummary{}, err
	}
	summary := SourceSummary{
		SchemaVersion: sourceEvidenceVersion, FixtureID: req.Manifest.FixtureID,
		ManifestSHA256: req.Manifest.SHA256, Seed: req.Manifest.Seed,
		RequestedLogicalBytes: req.Contract.Workload.LogicalBytes, ContractRef: req.ContractRef,
		ScopeID: req.Contract.ScopeID, SourceEnvironmentID: req.SourceEnvironmentID,
		RecoveryPointSHA256: recoveryPointDigest, StreamCheckpointSHA256: checkpointDigest,
		ShardCount: topology.ShardCount, ReplicationFactor: topology.ReplicationFactor,
		PlacementEpoch: topology.PlacementEpoch, ShardIDs: append([]uint32(nil), topology.ShardIDs...),
		TopologySHA256: topology.SHA256,
	}
	plan, err := planScale(req.Manifest, req.Contract.Workload.LogicalBytes)
	if err != nil {
		return SourceSummary{}, err
	}
	for _, table := range req.Manifest.Tables {
		expectedRows := table.BaseRows
		if table.ID == req.Manifest.ScalePolicy.EventTableID {
			expectedRows = plan.EventRows
		}
		tableSummary, collectErr := collectTable(
			ctx, ports, req.Manifest, req.Contract.Measurement.ClockSource, table, expectedRows,
			streamsBefore[table.Name],
		)
		if collectErr != nil {
			return SourceSummary{}, collectErr
		}
		if tableSummary.LogicalBytes > ^uint64(0)-summary.LogicalBytes {
			return SourceSummary{}, failure(CodeFixtureConflict, "CollectSourceSummary", table.Name, "LOGICAL_BYTES_OVERFLOW", nil)
		}
		summary.LogicalBytes += tableSummary.LogicalBytes
		summary.Tables = append(summary.Tables, tableSummary)
	}
	if summary.LogicalBytes < plan.LogicalBytes ||
		summary.LogicalBytes < summary.RequestedLogicalBytes {
		return SourceSummary{}, failure(CodeFixtureConflict, "CollectSourceSummary", "logicalBytes", "LOGICAL_BYTES_MISMATCH", nil)
	}
	for _, query := range req.Manifest.ExpectedQueries {
		table, ok := manifestTable(req.Manifest, query.Table)
		if !ok {
			return SourceSummary{}, failure(CodeSeedMismatch, "CollectSourceSummary", query.ID, "UNKNOWN_TABLE", nil)
		}
		collector := newDigestCollector(table.KeySchema)
		if err := ports.source.QueryItems(ctx, query, collector.Add); err != nil {
			return SourceSummary{}, failure(CodeFixtureConflict, "CollectSourceSummary", query.ID, "QUERY_FAILED", err)
		}
		count, digest, _, _, err := collector.Finish("query:" + query.ID)
		if err != nil {
			return SourceSummary{}, err
		}
		if count != expectedQueryCount(query.ID, plan.EventRows, req.Manifest.Seed) {
			return SourceSummary{}, failure(CodeFixtureConflict, "CollectSourceSummary", query.ID, "QUERY_COUNT_MISMATCH", nil)
		}
		summary.Queries = append(summary.Queries, QuerySummary{ID: query.ID, Count: count, SHA256: digest})
	}
	streamsAfter, streamFenceAfter, err := observeRecoveryStreams(ctx, ports.streams, streamRequest)
	if err != nil {
		return SourceSummary{}, err
	}
	if streamFenceBefore != streamFenceAfter {
		return SourceSummary{}, collectionBindingFailure("recoveryPoint", "CAPTURE_WINDOW_CHANGED", nil)
	}
	if err := verifyCatalogStable(ctx, ports, summary.Tables, streamsAfter); err != nil {
		return SourceSummary{}, err
	}
	sort.Slice(summary.Tables, func(i, j int) bool { return summary.Tables[i].Table < summary.Tables[j].Table })
	sort.Slice(summary.Queries, func(i, j int) bool { return summary.Queries[i].ID < summary.Queries[j].ID })
	return summary, nil
}

func collectTable(
	ctx context.Context,
	ports collectPorts,
	manifest FixtureManifest,
	clockSource string,
	table FixtureTable,
	expectedRows uint64,
	stream observedStream,
) (TableSummary, error) {
	descriptor, err := ports.schema.DescribeTable(ctx, table.Name)
	if err != nil {
		return TableSummary{}, failure(CodeFixtureConflict, "CollectSourceSummary", table.Name, "DESCRIBE_FAILED", err)
	}
	ttl, err := ports.ttl.DescribeTTL(ctx, table.Name)
	if err != nil {
		return TableSummary{}, failure(CodeFixtureConflict, "CollectSourceSummary", table.Name+".ttl", "DESCRIBE_FAILED", err)
	}
	observation := tableObservation{table: table, descriptor: descriptor, ttl: ttl, stream: stream, exists: true}
	if err := compatibleObservation(observation); err != nil {
		return TableSummary{}, err
	}
	descriptorDigest, err := digestJSON(normalizeDescriptor(descriptor))
	if err != nil {
		return TableSummary{}, err
	}
	ttlDigest, err := digestJSON(normalizeTTL(ttl))
	if err != nil {
		return TableSummary{}, err
	}
	streamDigest, err := digestJSON(normalizeStream(stream))
	if err != nil {
		return TableSummary{}, err
	}
	ttlCounter, err := newTTLVisibilityCounter(manifest, clockSource, table, expectedRows)
	if err != nil {
		return TableSummary{}, err
	}
	collector := newDigestCollector(table.KeySchema)
	visit := func(item types.Item) error {
		if err := collector.Add(item); err != nil {
			return err
		}
		return ttlCounter.Add(item)
	}
	if err := ports.source.ScanItems(ctx, table.Name, visit); err != nil {
		return TableSummary{}, failure(CodeFixtureConflict, "CollectSourceSummary", table.Name, "SCAN_FAILED", err)
	}
	count, itemDigest, fnvDigest, logicalBytes, err := collector.Finish("table:" + table.Name)
	if err != nil {
		return TableSummary{}, err
	}
	if count != expectedRows {
		return TableSummary{}, failure(CodeFixtureConflict, "CollectSourceSummary", table.Name, "TABLE_COUNT_MISMATCH", nil)
	}
	ttlVisibility, err := ttlCounter.Finish()
	if err != nil {
		return TableSummary{}, err
	}
	return TableSummary{
		Table: table.Name, PhysicalRows: count, LogicalRows: count, LogicalBytes: logicalBytes,
		DescriptorSHA256: descriptorDigest, CanonicalItemSHA256: itemDigest,
		CanonicalItemFNV1A64: fnvDigest, TTLSHA256: ttlDigest, TTLVisibility: ttlVisibility,
		StreamDescriptorSHA256: streamDigest,
	}, nil
}

type ttlVisibilityCounter struct {
	table                   FixtureTable
	attribute               string
	clockSource             string
	observationEpochSeconds uint64
	expectedExpiredRows     uint64
	expectedVisibleRows     uint64
	observedExpiredRows     uint64
	observedVisibleRows     uint64
}

func newTTLVisibilityCounter(
	manifest FixtureManifest,
	clockSource string,
	table FixtureTable,
	expectedRows uint64,
) (*ttlVisibilityCounter, error) {
	counter := &ttlVisibilityCounter{table: table}
	if table.TTLAttribute == "" {
		return counter, nil
	}
	expectation := manifest.TTLExpectation
	if expectation.TableID != table.ID || expectation.Attribute != table.TTLAttribute ||
		expectation.ClockSource != "utc" || clockSource != expectation.ClockSource ||
		expectation.ObservationOffsetSeconds >= manifest.EpochWindowSeconds ||
		expectation.ExpiredOffsetSeconds > expectation.ObservationOffsetSeconds ||
		expectation.VisibleOffsetSeconds <= expectation.ObservationOffsetSeconds ||
		expectation.ExpectedExpiredBaseRows != (table.BaseRows+1)/2 ||
		expectation.ExpectedVisibleBaseRows != table.BaseRows/2 {
		return nil, failure(CodeSeedMismatch, "CollectSourceSummary", table.Name+".ttl", "INVALID_TTL_EXPECTATION", nil)
	}
	if manifest.EpochSeconds > ^uint64(0)-expectation.ObservationOffsetSeconds {
		return nil, failure(CodeSeedMismatch, "CollectSourceSummary", table.Name+".ttl", "TTL_EPOCH_OVERFLOW", nil)
	}
	counter.attribute = expectation.Attribute
	counter.clockSource = expectation.ClockSource
	counter.observationEpochSeconds = manifest.EpochSeconds + expectation.ObservationOffsetSeconds
	counter.expectedExpiredRows = (expectedRows + 1) / 2
	counter.expectedVisibleRows = expectedRows / 2
	return counter, nil
}

func (counter *ttlVisibilityCounter) Add(item types.Item) error {
	if counter.attribute == "" {
		return nil
	}
	value, ok := item[counter.attribute]
	if !ok || value.T != types.AttrN {
		return failure(CodeFixtureConflict, "CollectSourceSummary", counter.table.Name+".ttl", "INVALID_TTL_VALUE", nil)
	}
	expiry, err := strconv.ParseUint(value.N, 10, 64)
	if err != nil {
		return failure(CodeFixtureConflict, "CollectSourceSummary", counter.table.Name+".ttl", "INVALID_TTL_VALUE", err)
	}
	if expiry <= counter.observationEpochSeconds {
		counter.observedExpiredRows++
	} else {
		counter.observedVisibleRows++
	}
	return nil
}

func (counter *ttlVisibilityCounter) Finish() (*TTLVisibilitySummary, error) {
	if counter.attribute == "" {
		return nil, nil
	}
	if counter.observedExpiredRows != counter.expectedExpiredRows ||
		counter.observedVisibleRows != counter.expectedVisibleRows {
		return nil, failure(
			CodeFixtureConflict, "CollectSourceSummary", counter.table.Name+".ttl", "TTL_VISIBILITY_MISMATCH", nil,
		)
	}
	summary := &TTLVisibilitySummary{
		Attribute: counter.attribute, ClockSource: counter.clockSource,
		ObservationEpochSeconds: counter.observationEpochSeconds,
		ExpectedExpiredRows:     counter.expectedExpiredRows, ExpectedVisibleRows: counter.expectedVisibleRows,
	}
	digest, err := ttlVisibilityDigest(*summary)
	if err != nil {
		return nil, err
	}
	summary.SHA256 = digest
	return summary, nil
}

func ttlVisibilityDigest(summary TTLVisibilitySummary) (string, error) {
	summary.SHA256 = ""
	return digestJSON(summary)
}

func manifestTable(manifest FixtureManifest, name string) (FixtureTable, bool) {
	for _, table := range manifest.Tables {
		if table.Name == name {
			return table, true
		}
	}
	return FixtureTable{}, false
}

func expectedQueryCount(id string, eventRows, seed uint64) uint64 {
	switch id {
	case "events.gsi.type-v1":
		return residueCount(eventRows, 4, (4-seed%4)%4)
	case "events.lsi.expiry-window-v1", "events.primary.user-v1":
		return residueCount(eventRows, 1000, 0)
	case "users.gsi.region-v1":
		return 250
	case "users.lsi.email-range-v1", "users.primary.account-v1":
		return 10
	default:
		return 0
	}
}

func residueCount(total, modulus, residue uint64) uint64 {
	if total <= residue {
		return 0
	}
	return 1 + (total-1-residue)/modulus
}

func (a clientAdapter) ScanItems(ctx context.Context, table string, visit func(types.Item) error) error {
	stream, err := a.cli.ScanStream(ctx, table, client.ScanOptions{Strong: true})
	if err != nil {
		return err
	}
	for {
		item, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := visit(itemFromProto(item.GetAttributes())); err != nil {
			return err
		}
	}
}

func (a clientAdapter) QueryItems(ctx context.Context, query QueryExpectation, visit func(types.Item) error) error {
	builder := a.cli.Query(ctx, query.Table).PK(fixtureValue(query.Partition)).Strong()
	if query.Index != "" {
		builder.Index(query.Index)
	}
	if query.SortLow != nil || query.SortHigh != nil {
		var low, high types.AttributeValue
		if query.SortLow != nil {
			low = fixtureValue(*query.SortLow)
		}
		if query.SortHigh != nil {
			high = fixtureValue(*query.SortHigh)
		}
		builder.SKBetween(low, high)
	}
	stream, err := builder.Stream(ctx)
	if err != nil {
		return err
	}
	for {
		item, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := visit(itemFromProto(item.GetAttributes())); err != nil {
			return err
		}
	}
}

func (a clientAdapter) ListStreams(
	ctx context.Context,
	opts client.ListStreamsOptions,
) (client.ListStreamsResult, error) {
	return a.cli.ListStreams(ctx, opts)
}

func (a clientAdapter) DescribeStream(
	ctx context.Context,
	streamARN string,
	opts client.DescribeStreamOptions,
) (client.StreamDescription, error) {
	return a.cli.DescribeStream(ctx, streamARN, opts)
}

func (a clientAdapter) GetShardIterator(
	ctx context.Context,
	opts client.GetShardIteratorOptions,
) (string, error) {
	return a.cli.GetShardIterator(ctx, opts)
}

func (a clientAdapter) GetRecords(
	ctx context.Context,
	iterator string,
	opts client.GetRecordsOptions,
) (client.GetRecordsResult, error) {
	return a.cli.GetRecords(ctx, iterator, opts)
}

type streamObservationRequest struct {
	Manifest               FixtureManifest
	Topology               topologySummary
	RecoveryPoint          repository.RecoveryPoint
	RecoveryPointSHA256    string
	StreamCheckpointSHA256 string
}

type recoveryStreamTable struct {
	Table  string         `json:"table"`
	Stream observedStream `json:"stream"`
}

type recoveryStreamFence struct {
	RecoveryPointSHA256    string                `json:"recoveryPointSha256"`
	StreamCheckpointSHA256 string                `json:"streamCheckpointSha256"`
	Tables                 []recoveryStreamTable `json:"tables"`
}

func validateCollectionPoint(
	req CollectRequest,
	topology topologySummary,
) (string, string, error) {
	point := req.RecoveryPoint
	if topology.ShardCount != 1 {
		return "", "", failure(
			CodeUnsupportedTopology, "CollectSourceSummary", "topology",
			"POINT_BOUND_STREAM_EVIDENCE_UNAVAILABLE", nil,
		)
	}
	if !validID(point.BarrierID) || point.SourceEnvironmentID != req.SourceEnvironmentID ||
		point.PlacementEpoch != topology.PlacementEpoch || point.AdmittedAt.IsZero() ||
		point.RecoveryPointAt.IsZero() || point.RecoveryPointAt.Before(point.AdmittedAt) {
		return "", "", collectionBindingFailure("recoveryPoint", "INVALID_RECOVERY_POINT", nil)
	}
	if !validRecoveryPointShards(point.Shards, topology.ShardIDs, topology.PlacementEpoch) {
		return "", "", collectionBindingFailure("recoveryPoint.shards", "SHARD_COVERAGE_MISMATCH", nil)
	}
	recoveryPointDigest, err := recoveryPointSHA256(point)
	if err != nil {
		return "", "", err
	}
	checkpointDigest, err := streamCheckpointSHA256(point)
	if err != nil {
		return "", "", err
	}
	return recoveryPointDigest, checkpointDigest, nil
}

func observeRecoveryStreams(
	ctx context.Context,
	ports streamEvidencePorts,
	req streamObservationRequest,
) (map[string]observedStream, string, error) {
	if ports.catalog == nil || ports.records == nil {
		return nil, "", failure(
			CodeUnsupportedTopology, "CollectSourceSummary", "streams", "STREAM_PORT_UNAVAILABLE", nil,
		)
	}
	pointIndex := req.RecoveryPoint.Shards[0].ChangeIndex
	byTable := make(map[string]observedStream, len(req.Manifest.Tables))
	fence := recoveryStreamFence{
		RecoveryPointSHA256:    req.RecoveryPointSHA256,
		StreamCheckpointSHA256: req.StreamCheckpointSHA256,
	}
	for _, table := range req.Manifest.Tables {
		stream, err := observeTableStream(ctx, ports, table, pointIndex)
		if err != nil {
			return nil, "", err
		}
		byTable[table.Name] = stream
		fence.Tables = append(fence.Tables, recoveryStreamTable{Table: table.Name, Stream: stream})
	}
	digest, err := digestJSON(fence)
	if err != nil {
		return nil, "", err
	}
	return byTable, digest, nil
}

func observeTableStream(
	ctx context.Context,
	ports streamEvidencePorts,
	table FixtureTable,
	pointIndex uint64,
) (observedStream, error) {
	if table.Stream == nil || !table.Stream.StreamEnabled {
		return observedStream{}, nil
	}
	listed, err := ports.catalog.ListStreams(ctx, client.ListStreamsOptions{TableName: table.Name, Limit: 2})
	if err != nil {
		return observedStream{}, collectionUnsupportedStream(table.Name, "LIST_STREAMS_FAILED", err)
	}
	if len(listed.Streams) != 1 || listed.LastEvaluatedStreamARN != "" ||
		listed.Streams[0].TableName != table.Name || listed.Streams[0].StreamArn == "" {
		return observedStream{}, collectionBindingFailure(table.Name+".stream", "STREAM_IDENTITY_MISMATCH", nil)
	}
	description, err := ports.catalog.DescribeStream(
		ctx, listed.Streams[0].StreamArn, client.DescribeStreamOptions{Limit: 2},
	)
	if err != nil {
		return observedStream{}, collectionUnsupportedStream(table.Name, "DESCRIBE_STREAM_FAILED", err)
	}
	stream := observedStream{
		Status:   description.StreamStatus,
		Enabled:  strings.EqualFold(description.StreamStatus, types.StreamStatusEnabled),
		ViewType: description.StreamViewType,
	}
	for _, shard := range description.Shards {
		stream.Shards = append(stream.Shards, observedStreamShard{
			ShardID:                shard.ShardID,
			StartingSequenceNumber: shard.SequenceNumberRange.StartingSequenceNumber,
			EndingSequenceNumber:   shard.SequenceNumberRange.EndingSequenceNumber,
			LatestSequenceNumber:   shard.LatestSequenceNumber,
		})
	}
	stream = normalizeStream(stream)
	want := expectedStream(table)
	if description.TableName != table.Name || description.LastEvaluatedShardID != "" ||
		stream.Status != want.Status || stream.Enabled != want.Enabled || stream.ViewType != want.ViewType ||
		len(stream.Shards) != 1 {
		return observedStream{}, collectionBindingFailure(table.Name+".stream", "STREAM_DESCRIPTOR_MISMATCH", nil)
	}
	latest, err := strconv.ParseUint(stream.Shards[0].LatestSequenceNumber, 10, 64)
	if err != nil || latest != pointIndex {
		return observedStream{}, collectionBindingFailure(table.Name+".stream", "STREAM_HIGH_WATER_MISMATCH", err)
	}
	iterator, err := ports.records.GetShardIterator(ctx, client.GetShardIteratorOptions{
		StreamArn: listed.Streams[0].StreamArn, ShardID: stream.Shards[0].ShardID,
		ShardIteratorType: "AFTER_SEQUENCE_NUMBER",
		SequenceNumber:    strconv.FormatUint(pointIndex, 10),
	})
	if err != nil {
		return observedStream{}, collectionUnsupportedStream(table.Name, "GET_ITERATOR_FAILED", err)
	}
	records, err := ports.records.GetRecords(ctx, iterator, client.GetRecordsOptions{Limit: 1})
	if err != nil {
		return observedStream{}, collectionUnsupportedStream(table.Name, "GET_RECORDS_FAILED", err)
	}
	if len(records.Records) != 0 ||
		records.LastScannedSequenceNumber != strconv.FormatUint(pointIndex, 10) {
		return observedStream{}, collectionBindingFailure(table.Name+".stream", "STREAM_NOT_FENCED", nil)
	}
	return stream, nil
}

func verifyCatalogStable(
	ctx context.Context,
	ports collectPorts,
	tables []TableSummary,
	streams map[string]observedStream,
) error {
	for _, table := range tables {
		descriptor, err := ports.schema.DescribeTable(ctx, table.Table)
		if err != nil {
			return collectionBindingFailure(table.Table, "DESCRIPTOR_RECHECK_FAILED", err)
		}
		descriptorDigest, err := digestJSON(normalizeDescriptor(descriptor))
		if err != nil {
			return err
		}
		ttl, err := ports.ttl.DescribeTTL(ctx, table.Table)
		if err != nil {
			return collectionBindingFailure(table.Table+".ttl", "TTL_RECHECK_FAILED", err)
		}
		ttlDigest, err := digestJSON(normalizeTTL(ttl))
		if err != nil {
			return err
		}
		streamDigest, err := digestJSON(normalizeStream(streams[table.Table]))
		if err != nil {
			return err
		}
		if descriptorDigest != table.DescriptorSHA256 || ttlDigest != table.TTLSHA256 ||
			streamDigest != table.StreamDescriptorSHA256 {
			return collectionBindingFailure(table.Table, "CATALOG_CHANGED_DURING_CAPTURE", nil)
		}
	}
	return nil
}

func collectionUnsupportedStream(table, reason string, cause error) error {
	return failure(CodeUnsupportedTopology, "CollectSourceSummary", table+".stream", reason, cause)
}

func collectionBindingFailure(field, reason string, cause error) error {
	return failure(CodeEvidenceBinding, "CollectSourceSummary", field, reason, cause)
}

func fixtureValue(value FixtureValue) types.AttributeValue {
	switch value.Type {
	case "S":
		return stringValue(value.Value)
	case "N":
		return numberValue(value.Value)
	default:
		return types.AttributeValue{}
	}
}

func itemFromProto(in map[string]*cefaspb.AttributeValue) types.Item {
	out := make(types.Item, len(in))
	for key, value := range in {
		out[key] = attrFromProto(value)
	}
	return out
}

func attrFromProto(attribute *cefaspb.AttributeValue) types.AttributeValue {
	if attribute == nil {
		return types.AttributeValue{}
	}
	switch value := attribute.GetValue().(type) {
	case *cefaspb.AttributeValue_S:
		return stringValue(value.S)
	case *cefaspb.AttributeValue_N:
		return numberValue(value.N)
	case *cefaspb.AttributeValue_B:
		return types.AttributeValue{T: types.AttrB, B: append([]byte(nil), value.B...)}
	case *cefaspb.AttributeValue_BoolVal:
		return types.AttributeValue{T: types.AttrBOOL, BOOL: value.BoolVal}
	case *cefaspb.AttributeValue_NullVal:
		return types.AttributeValue{T: types.AttrNull}
	case *cefaspb.AttributeValue_Ss:
		return types.AttributeValue{T: types.AttrSS, SS: append([]string(nil), value.Ss.GetValues()...)}
	case *cefaspb.AttributeValue_Ns:
		return types.AttributeValue{T: types.AttrNS, NS: append([]string(nil), value.Ns.GetValues()...)}
	case *cefaspb.AttributeValue_Bs:
		values := value.Bs.GetValues()
		out := make([][]byte, len(values))
		for i := range values {
			out[i] = append([]byte(nil), values[i]...)
		}
		return types.AttributeValue{T: types.AttrBS, BS: out}
	case *cefaspb.AttributeValue_L:
		values := value.L.GetValues()
		out := make([]types.AttributeValue, len(values))
		for i := range values {
			out[i] = attrFromProto(values[i])
		}
		return types.AttributeValue{T: types.AttrL, L: out}
	case *cefaspb.AttributeValue_M:
		out := make(map[string]types.AttributeValue, len(value.M.GetValues()))
		for key, nested := range value.M.GetValues() {
			out[key] = attrFromProto(nested)
		}
		return types.AttributeValue{T: types.AttrM, M: out}
	case *cefaspb.AttributeValue_V:
		return types.AttributeValue{T: types.AttrVec, Vec: append([]float64(nil), value.V.GetValues()...)}
	default:
		return types.AttributeValue{}
	}
}

type itemFingerprint struct {
	key []byte
	sha [sha256.Size]byte
	fnv uint64
}

type digestCollector struct {
	schema       types.KeySchema
	items        []itemFingerprint
	logicalBytes uint64
}

func newDigestCollector(schema types.KeySchema) *digestCollector {
	return &digestCollector{schema: schema}
}

func (collector *digestCollector) Add(item types.Item) error {
	key, err := keyForItem(collector.schema, item)
	if err != nil {
		return err
	}
	keyBytes, err := canonicalItemBytes(key)
	if err != nil {
		return err
	}
	itemBytes, err := canonicalItemBytes(item)
	if err != nil {
		return err
	}
	if uint64(len(itemBytes)) > ^uint64(0)-collector.logicalBytes {
		return failure(CodeFixtureConflict, "CollectSourceSummary", "logicalBytes", "LOGICAL_BYTES_OVERFLOW", nil)
	}
	collector.logicalBytes += uint64(len(itemBytes))
	itemFNV := fnv.New64a()
	_, _ = itemFNV.Write(itemBytes)
	collector.items = append(collector.items, itemFingerprint{
		key: append([]byte(nil), keyBytes...), sha: sha256.Sum256(itemBytes), fnv: itemFNV.Sum64(),
	})
	return nil
}

func (collector *digestCollector) Finish(domain string) (uint64, string, string, uint64, error) {
	sort.Slice(collector.items, func(i, j int) bool {
		return bytes.Compare(collector.items[i].key, collector.items[j].key) < 0
	})
	shaHasher := sha256.New()
	fnvHasher := fnv.New64a()
	writeDigestFrame(shaHasher, []byte(typedClientDigestNamespace))
	writeDigestFrame(fnvHasher, []byte(typedClientDigestNamespace))
	writeDigestFrame(shaHasher, []byte(domain))
	writeDigestFrame(fnvHasher, []byte(domain))
	for i, item := range collector.items {
		if i > 0 && bytes.Equal(item.key, collector.items[i-1].key) {
			return 0, "", "", 0, failure(CodeFixtureConflict, "canonicalItems", "$", "DUPLICATE_PRIMARY_KEY", nil)
		}
		writeDigestFrame(shaHasher, item.key)
		writeDigestFrame(shaHasher, item.sha[:])
		var fnvBytes [8]byte
		binary.BigEndian.PutUint64(fnvBytes[:], item.fnv)
		writeDigestFrame(shaHasher, fnvBytes[:])
		writeDigestFrame(fnvHasher, item.key)
		writeDigestFrame(fnvHasher, item.sha[:])
		writeDigestFrame(fnvHasher, fnvBytes[:])
	}
	return uint64(len(collector.items)),
		"sha256:" + hex.EncodeToString(shaHasher.Sum(nil)),
		fmt.Sprintf("%016x", fnvHasher.Sum64()), collector.logicalBytes, nil
}

func writeDigestFrame(writer io.Writer, value []byte) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = writer.Write(length[:])
	_, _ = writer.Write(value)
}

func canonicalItemBytes(item types.Item) ([]byte, error) {
	var buffer bytes.Buffer
	keys := make([]string, 0, len(item))
	for key := range item {
		if !utf8.ValidString(key) {
			return nil, failure(CodeFixtureConflict, "canonicalItem", key, "INVALID_UTF8", nil)
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	writeUint64(&buffer, uint64(len(keys)))
	for _, key := range keys {
		writeFrame(&buffer, []byte(key))
		if err := writeAttribute(&buffer, item[key]); err != nil {
			return nil, err
		}
	}
	return buffer.Bytes(), nil
}

func writeAttribute(buffer *bytes.Buffer, attribute types.AttributeValue) error {
	var payload bytes.Buffer
	switch attribute.T {
	case types.AttrNull:
	case types.AttrS:
		if !utf8.ValidString(attribute.S) {
			return failure(CodeFixtureConflict, "canonicalItem", "$", "INVALID_UTF8", nil)
		}
		payload.WriteString(attribute.S)
	case types.AttrN:
		payload.WriteString(attribute.N)
	case types.AttrB:
		payload.Write(attribute.B)
	case types.AttrBOOL:
		if attribute.BOOL {
			payload.WriteByte(1)
		} else {
			payload.WriteByte(0)
		}
	case types.AttrSS, types.AttrNS:
		values := append([]string(nil), attribute.SS...)
		if attribute.T == types.AttrNS {
			values = append([]string(nil), attribute.NS...)
		}
		sort.Strings(values)
		writeUint64(&payload, uint64(len(values)))
		for _, value := range values {
			if attribute.T == types.AttrSS && !utf8.ValidString(value) {
				return failure(CodeFixtureConflict, "canonicalItem", "$", "INVALID_UTF8", nil)
			}
			writeFrame(&payload, []byte(value))
		}
	case types.AttrBS:
		values := make([][]byte, len(attribute.BS))
		for i := range attribute.BS {
			values[i] = append([]byte(nil), attribute.BS[i]...)
		}
		sort.Slice(values, func(i, j int) bool { return bytes.Compare(values[i], values[j]) < 0 })
		writeUint64(&payload, uint64(len(values)))
		for _, value := range values {
			writeFrame(&payload, value)
		}
	case types.AttrL:
		writeUint64(&payload, uint64(len(attribute.L)))
		for _, value := range attribute.L {
			if err := writeAttribute(&payload, value); err != nil {
				return err
			}
		}
	case types.AttrM:
		keys := make([]string, 0, len(attribute.M))
		for key := range attribute.M {
			if !utf8.ValidString(key) {
				return failure(CodeFixtureConflict, "canonicalItem", key, "INVALID_UTF8", nil)
			}
			keys = append(keys, key)
		}
		sort.Strings(keys)
		writeUint64(&payload, uint64(len(keys)))
		for _, key := range keys {
			writeFrame(&payload, []byte(key))
			if err := writeAttribute(&payload, attribute.M[key]); err != nil {
				return err
			}
		}
	case types.AttrVec:
		writeUint64(&payload, uint64(len(attribute.Vec)))
		var raw [8]byte
		for _, value := range attribute.Vec {
			binary.BigEndian.PutUint64(raw[:], math.Float64bits(value))
			payload.Write(raw[:])
		}
	default:
		return failure(CodeFixtureConflict, "canonicalItem", "$", "UNKNOWN_ATTRIBUTE_TYPE", nil)
	}
	buffer.WriteByte(byte(attribute.T))
	writeFrame(buffer, payload.Bytes())
	return nil
}

func writeUint64(buffer *bytes.Buffer, value uint64) {
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], value)
	buffer.Write(raw[:])
}

func writeFrame(buffer *bytes.Buffer, value []byte) {
	writeUint64(buffer, uint64(len(value)))
	buffer.Write(value)
}

func digestJSON(value any) (string, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return "", failure(CodeFixtureConflict, "digestJSON", "$", "ENCODING_FAILED", err)
	}
	sum := sha256.Sum256(append(raw, '\n'))
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

type recoveryPointCatalogTable struct {
	Table                  string                `json:"table"`
	PhysicalRows           uint64                `json:"physicalRows"`
	LogicalRows            uint64                `json:"logicalRows"`
	LogicalBytes           uint64                `json:"logicalBytes"`
	DescriptorSHA256       string                `json:"descriptorSha256"`
	CanonicalItemSHA256    string                `json:"canonicalItemSha256"`
	CanonicalItemFNV1A64   string                `json:"canonicalItemFnv1a64"`
	TTLSHA256              string                `json:"ttlSha256"`
	TTLVisibility          *TTLVisibilitySummary `json:"ttlVisibility,omitempty"`
	StreamDescriptorSHA256 string                `json:"streamDescriptorSha256"`
}

type recoveryPointCatalog struct {
	SchemaVersion          uint32                      `json:"schemaVersion"`
	FixtureID              string                      `json:"fixtureId"`
	ManifestSHA256         string                      `json:"manifestSha256"`
	Seed                   uint64                      `json:"seed"`
	RequestedLogicalBytes  uint64                      `json:"requestedLogicalBytes"`
	LogicalBytes           uint64                      `json:"logicalBytes"`
	ContractRef            recoverymodel.ContractRef   `json:"contractRef"`
	ScopeID                string                      `json:"scopeId"`
	BackupID               string                      `json:"backupId"`
	SourceEnvironmentID    string                      `json:"sourceEnvironmentId"`
	RecoveryPointSHA256    string                      `json:"recoveryPointSha256"`
	StreamCheckpointSHA256 string                      `json:"streamCheckpointSha256"`
	ShardCount             uint32                      `json:"shardCount"`
	ReplicationFactor      uint32                      `json:"replicationFactor"`
	PlacementEpoch         uint64                      `json:"placementEpoch"`
	ShardIDs               []uint32                    `json:"shardIds"`
	TopologySHA256         string                      `json:"topologySha256"`
	Tables                 []recoveryPointCatalogTable `json:"tables"`
	Queries                []QuerySummary              `json:"queries"`
}

// RecoveryPointCatalogObject is the canonical row-free catalog payload and its content-addressed reference.
type RecoveryPointCatalogObject struct {
	reference              repository.ObjectRef
	payload                []byte
	recoveryPointSHA256    string
	streamCheckpointSHA256 string
}

// Reference returns the immutable object identity callers place in BackupSet.CatalogObject.
func (object RecoveryPointCatalogObject) Reference() repository.ObjectRef {
	return object.reference
}

// Bytes returns a copy of the canonical catalog bytes for durable upload.
func (object RecoveryPointCatalogObject) Bytes() []byte {
	return append([]byte(nil), object.payload...)
}

// Upload returns a repository upload whose body contains the exact bytes committed by Reference.
func (object RecoveryPointCatalogObject) Upload(attemptID string) repository.CatalogUpload {
	return repository.CatalogUpload{
		AttemptID: attemptID, SHA256: object.reference.SHA256,
		SizeBytes: object.reference.SizeBytes, MediaType: object.reference.MediaType,
		Body: bytes.NewReader(object.payload),
	}
}

// BuildRecoveryPointCatalog constructs the exact catalog object required by BindSourceEvidence.
func BuildRecoveryPointCatalog(
	summary SourceSummary,
	set repository.BackupSet,
) (RecoveryPointCatalogObject, error) {
	recoveryPointDigest, checkpointDigest, err := validateRecoveryPointBinding(summary, set)
	if err != nil {
		return RecoveryPointCatalogObject{}, err
	}
	catalog := recoveryPointCatalog{
		SchemaVersion: sourceEvidenceVersion, FixtureID: summary.FixtureID,
		ManifestSHA256: summary.ManifestSHA256, Seed: summary.Seed,
		RequestedLogicalBytes: summary.RequestedLogicalBytes, LogicalBytes: summary.LogicalBytes,
		ContractRef: summary.ContractRef, ScopeID: summary.ScopeID, BackupID: set.BackupID,
		SourceEnvironmentID: summary.SourceEnvironmentID,
		RecoveryPointSHA256: recoveryPointDigest, StreamCheckpointSHA256: checkpointDigest,
		ShardCount: summary.ShardCount, ReplicationFactor: summary.ReplicationFactor,
		PlacementEpoch: summary.PlacementEpoch, ShardIDs: append([]uint32(nil), summary.ShardIDs...),
		TopologySHA256: summary.TopologySHA256,
		Tables:         make([]recoveryPointCatalogTable, 0, len(summary.Tables)),
		Queries:        append([]QuerySummary(nil), summary.Queries...),
	}
	for _, table := range summary.Tables {
		var visibility *TTLVisibilitySummary
		if table.TTLVisibility != nil {
			copyValue := *table.TTLVisibility
			visibility = &copyValue
		}
		catalog.Tables = append(catalog.Tables, recoveryPointCatalogTable{
			Table: table.Table, PhysicalRows: table.PhysicalRows, LogicalRows: table.LogicalRows,
			LogicalBytes: table.LogicalBytes, DescriptorSHA256: table.DescriptorSHA256,
			CanonicalItemSHA256:  table.CanonicalItemSHA256,
			CanonicalItemFNV1A64: table.CanonicalItemFNV1A64,
			TTLSHA256:            table.TTLSHA256, TTLVisibility: visibility,
			StreamDescriptorSHA256: table.StreamDescriptorSHA256,
		})
	}
	raw, err := json.Marshal(catalog)
	if err != nil {
		return RecoveryPointCatalogObject{}, failure(
			CodeEvidenceBinding, "BuildRecoveryPointCatalog", "catalogObject", "ENCODING_FAILED", err,
		)
	}
	raw = append(raw, '\n')
	sum := sha256.Sum256(raw)
	digest := "sha256:" + hex.EncodeToString(sum[:])
	ref := repository.ObjectRef{
		ObjectID: "scopes/" + set.ScopeID + "/backups/" + set.BackupID +
			"/catalog/" + strings.TrimPrefix(digest, "sha256:"),
		SHA256: digest, SizeBytes: uint64(len(raw)), MediaType: repository.MediaTypeCatalog,
	}
	return RecoveryPointCatalogObject{
		reference: ref, payload: raw, recoveryPointSHA256: recoveryPointDigest,
		streamCheckpointSHA256: checkpointDigest,
	}, nil
}

func validateRecoveryPointBinding(
	summary SourceSummary,
	set repository.BackupSet,
) (string, string, error) {
	if err := validateSummary(summary); err != nil {
		return "", "", err
	}
	if set.ContractRef != summary.ContractRef {
		return "", "", bindingFailure("contractRef", "CONTRACT_REF_MISMATCH")
	}
	if set.ScopeID != summary.ScopeID {
		return "", "", bindingFailure("scopeId", "SCOPE_MISMATCH")
	}
	if set.SourceEnvironmentID != summary.SourceEnvironmentID ||
		set.RecoveryPoint.SourceEnvironmentID != summary.SourceEnvironmentID {
		return "", "", bindingFailure("sourceEnvironmentId", "SOURCE_ENVIRONMENT_MISMATCH")
	}
	if !validID(set.BackupID) {
		return "", "", bindingFailure("backupSet", "INVALID_BACKUP_SET_IDENTITY")
	}
	point := set.RecoveryPoint
	if !validID(point.BarrierID) || point.AdmittedAt.IsZero() || point.RecoveryPointAt.IsZero() ||
		point.RecoveryPointAt.Before(point.AdmittedAt) {
		return "", "", bindingFailure("recoveryPoint", "INVALID_RECOVERY_POINT")
	}
	if point.PlacementEpoch != summary.PlacementEpoch {
		return "", "", bindingFailure("recoveryPoint.placementEpoch", "PLACEMENT_EPOCH_MISMATCH")
	}
	if !sameUint32Set(set.ExpectedShardIDs, summary.ShardIDs) ||
		!validRecoveryPointShards(point.Shards, summary.ShardIDs, summary.PlacementEpoch) {
		return "", "", bindingFailure("recoveryPoint.shards", "SHARD_COVERAGE_MISMATCH")
	}
	recoveryPointDigest, err := recoveryPointSHA256(point)
	if err != nil {
		return "", "", err
	}
	checkpointDigest, err := streamCheckpointSHA256(point)
	if err != nil {
		return "", "", err
	}
	if recoveryPointDigest != summary.RecoveryPointSHA256 {
		return "", "", bindingFailure("recoveryPoint", "RECOVERY_POINT_DIGEST_MISMATCH")
	}
	if checkpointDigest != summary.StreamCheckpointSHA256 {
		return "", "", bindingFailure("streamCheckpoint", "STREAM_CHECKPOINT_DIGEST_MISMATCH")
	}
	return recoveryPointDigest, checkpointDigest, nil
}

// BindSourceEvidence accepts only the catalog bytes produced by BuildRecoveryPointCatalog.
func BindSourceEvidence(summary SourceSummary, set repository.BackupSet) (SourceEvidence, error) {
	catalog, err := BuildRecoveryPointCatalog(summary, set)
	if err != nil {
		return SourceEvidence{}, err
	}
	if !digestPattern.MatchString(set.ManifestSHA256) {
		return SourceEvidence{}, bindingFailure("backupSet", "INVALID_BACKUP_SET_IDENTITY")
	}
	if set.CatalogObject != catalog.Reference() {
		return SourceEvidence{}, bindingFailure("catalogObject", "RECOVERY_POINT_CONTENT_MISMATCH")
	}
	if uint32(len(summary.ShardIDs)) != summary.ShardCount || summary.ReplicationFactor == 0 {
		return SourceEvidence{}, bindingFailure("topology", "TOPOLOGY_MISMATCH")
	}
	if !backupMatchesFixture(set, summary) {
		return SourceEvidence{}, bindingFailure("artifacts", "FIXTURE_COVERAGE_MISMATCH")
	}
	evidence := SourceEvidence{
		SchemaVersion: summary.SchemaVersion, FixtureID: summary.FixtureID,
		ManifestSHA256: summary.ManifestSHA256, Seed: summary.Seed,
		RequestedLogicalBytes: summary.RequestedLogicalBytes, LogicalBytes: summary.LogicalBytes,
		ContractRef: summary.ContractRef,
		ScopeID:     summary.ScopeID, SourceEnvironmentID: summary.SourceEnvironmentID,
		BackupID: set.BackupID, BackupSetManifestSHA256: set.ManifestSHA256,
		RecoveryPointSHA256:        catalog.recoveryPointSHA256,
		RecoveryPointCatalogSHA256: catalog.reference.SHA256,
		StreamCheckpointSHA256:     catalog.streamCheckpointSHA256,
		ShardCount:                 summary.ShardCount, ReplicationFactor: summary.ReplicationFactor,
		PlacementEpoch: summary.PlacementEpoch, ShardIDs: append([]uint32(nil), summary.ShardIDs...),
		TopologySHA256: summary.TopologySHA256,
		Tables:         append([]TableSummary(nil), summary.Tables...),
		Queries:        append([]QuerySummary(nil), summary.Queries...),
	}
	_, digest, err := canonicalEvidenceBytes(evidence)
	if err != nil {
		return SourceEvidence{}, err
	}
	evidence.SHA256 = digest
	return evidence, nil
}

func bindingFailure(field, reason string) error {
	return failure(CodeEvidenceBinding, "BindSourceEvidence", field, reason, nil)
}

func validateSummary(summary SourceSummary) error {
	switch {
	case summary.SchemaVersion != sourceEvidenceVersion:
		return bindingFailure("schemaVersion", "UNSUPPORTED_VERSION")
	case summary.FixtureID != FixtureID || summary.Seed != ManifestSeed:
		return bindingFailure("fixtureId", "FIXTURE_IDENTITY_MISMATCH")
	case summary.RequestedLogicalBytes == 0 ||
		summary.LogicalBytes < summary.RequestedLogicalBytes:
		return bindingFailure("logicalBytes", "INVALID_LOGICAL_BYTES")
	case summary.ManifestSHA256 != ManifestSHA256:
		return bindingFailure("manifestSha256", "FIXTURE_MANIFEST_MISMATCH")
	case !validID(summary.ContractRef.ID) || summary.ContractRef.Revision == 0 || !digestPattern.MatchString(summary.ContractRef.SHA256):
		return bindingFailure("contractRef", "INVALID_CONTRACT_REF")
	case !validID(summary.ScopeID) || !validID(summary.SourceEnvironmentID):
		return bindingFailure("source", "INVALID_IDENTITY")
	case !digestPattern.MatchString(summary.RecoveryPointSHA256) ||
		!digestPattern.MatchString(summary.StreamCheckpointSHA256):
		return bindingFailure("recoveryPoint", "INVALID_RECOVERY_POINT_DIGEST")
	case summary.ShardCount == 0 || summary.ReplicationFactor == 0 || summary.PlacementEpoch == 0:
		return bindingFailure("topology", "INVALID_TOPOLOGY")
	case !digestPattern.MatchString(summary.TopologySHA256):
		return bindingFailure("topologySha256", "INVALID_DIGEST")
	case len(summary.ShardIDs) != int(summary.ShardCount):
		return bindingFailure("shardIds", "SHARD_COUNT_MISMATCH")
	}
	if !sort.SliceIsSorted(summary.ShardIDs, func(i, j int) bool { return summary.ShardIDs[i] < summary.ShardIDs[j] }) || hasDuplicateUint32(summary.ShardIDs) {
		return bindingFailure("shardIds", "UNSORTED_OR_DUPLICATE_SHARDS")
	}
	expectedTables := []string{"recovery_events_v1", "recovery_users_v1"}
	expectedQueries := []string{
		"events.gsi.type-v1", "events.lsi.expiry-window-v1", "events.primary.user-v1",
		"users.gsi.region-v1", "users.lsi.email-range-v1", "users.primary.account-v1",
	}
	if len(summary.Tables) != len(expectedTables) || len(summary.Queries) != len(expectedQueries) {
		return bindingFailure("summary", "INCOMPLETE_FIXTURE_COVERAGE")
	}
	var logicalBytes uint64
	for i, table := range summary.Tables {
		if table.Table != expectedTables[i] ||
			table.PhysicalRows == 0 ||
			table.LogicalRows != table.PhysicalRows ||
			table.LogicalBytes == 0 ||
			!digestPattern.MatchString(table.DescriptorSHA256) ||
			!digestPattern.MatchString(table.CanonicalItemSHA256) ||
			!fnv64Pattern.MatchString(table.CanonicalItemFNV1A64) ||
			!digestPattern.MatchString(table.TTLSHA256) ||
			!digestPattern.MatchString(table.StreamDescriptorSHA256) {
			return bindingFailure("tables", "INVALID_TABLE_SUMMARY")
		}
		if !validTTLVisibilitySummary(table) {
			return bindingFailure("tables.ttlVisibility", "INVALID_TTL_VISIBILITY_SUMMARY")
		}
		if table.LogicalBytes > ^uint64(0)-logicalBytes {
			return bindingFailure("logicalBytes", "LOGICAL_BYTES_OVERFLOW")
		}
		logicalBytes += table.LogicalBytes
	}
	if logicalBytes != summary.LogicalBytes {
		return bindingFailure("logicalBytes", "LOGICAL_BYTES_MISMATCH")
	}
	for i, query := range summary.Queries {
		if query.ID != expectedQueries[i] || query.Count == 0 || !digestPattern.MatchString(query.SHA256) {
			return bindingFailure("queries", "INVALID_QUERY_SUMMARY")
		}
	}
	return nil
}

func validTTLVisibilitySummary(table TableSummary) bool {
	if table.Table != "recovery_events_v1" {
		return table.TTLVisibility == nil
	}
	visibility := table.TTLVisibility
	if visibility == nil || visibility.Attribute != "expires_at" || visibility.ClockSource != "utc" ||
		visibility.ObservationEpochSeconds != FixtureEpoch+FixtureTTLObservation ||
		visibility.ExpectedExpiredRows != (table.PhysicalRows+1)/2 ||
		visibility.ExpectedVisibleRows != table.PhysicalRows/2 ||
		!digestPattern.MatchString(visibility.SHA256) {
		return false
	}
	digest, err := ttlVisibilityDigest(*visibility)
	return err == nil && digest == visibility.SHA256
}

func sameUint32Set(left, right []uint32) bool {
	leftCopy, rightCopy := append([]uint32(nil), left...), append([]uint32(nil), right...)
	sort.Slice(leftCopy, func(i, j int) bool { return leftCopy[i] < leftCopy[j] })
	sort.Slice(rightCopy, func(i, j int) bool { return rightCopy[i] < rightCopy[j] })
	return slices.Equal(leftCopy, rightCopy) && !hasDuplicateUint32(leftCopy)
}

func hasDuplicateUint32(values []uint32) bool {
	for i := 1; i < len(values); i++ {
		if values[i] == values[i-1] {
			return true
		}
	}
	return false
}

func validRecoveryPointShards(
	points []repository.ShardPoint,
	expected []uint32,
	placementEpoch uint64,
) bool {
	ids := make([]uint32, len(points))
	seen := make(map[uint32]struct{}, len(points))
	for index, point := range points {
		if _, duplicate := seen[point.ShardID]; duplicate ||
			point.PlacementEpoch != placementEpoch || !validID(point.ReplicaID) ||
			point.RaftAppliedIndex == 0 || point.ChangeIndex == 0 {
			return false
		}
		seen[point.ShardID] = struct{}{}
		ids[index] = point.ShardID
	}
	return sameUint32Set(ids, expected)
}

func backupMatchesFixture(set repository.BackupSet, summary SourceSummary) bool {
	if set.SchemaVersion != 1 || len(set.Artifacts) != int(summary.ShardCount) {
		return false
	}
	points := make(map[uint32]repository.ShardPoint, len(set.RecoveryPoint.Shards))
	for _, point := range set.RecoveryPoint.Shards {
		if point.PlacementEpoch != summary.PlacementEpoch || !validID(point.ReplicaID) ||
			point.RaftAppliedIndex == 0 || point.ChangeIndex == 0 {
			return false
		}
		if _, duplicate := points[point.ShardID]; duplicate {
			return false
		}
		points[point.ShardID] = point
	}
	artifactIDs := make([]uint32, 0, len(set.Artifacts))
	coveredTables := make(map[string]struct{}, len(summary.Tables))
	tableRows := make(map[string]uint64, len(summary.Tables))
	for _, artifact := range set.Artifacts {
		point, ok := points[artifact.ShardID]
		if !ok || artifact.PlacementEpoch != summary.PlacementEpoch || artifact.ReplicaID != point.ReplicaID {
			return false
		}
		artifactIDs = append(artifactIDs, artifact.ShardID)
		perArtifact := make(map[string]struct{}, len(artifact.RowStats))
		for _, rowStat := range artifact.RowStats {
			if _, duplicate := perArtifact[rowStat.Table]; duplicate {
				return false
			}
			if !validID(rowStat.Table) || !fnv64Pattern.MatchString(rowStat.FNV1A64) {
				return false
			}
			if ^uint64(0)-tableRows[rowStat.Table] < rowStat.Rows {
				return false
			}
			perArtifact[rowStat.Table] = struct{}{}
			coveredTables[rowStat.Table] = struct{}{}
			tableRows[rowStat.Table] += rowStat.Rows
		}
	}
	for _, table := range summary.Tables {
		if _, ok := coveredTables[table.Table]; !ok || tableRows[table.Table] != table.PhysicalRows {
			return false
		}
	}
	return sameUint32Set(artifactIDs, summary.ShardIDs)
}

type streamCheckpoint struct {
	ShardID          uint32 `json:"shardId"`
	PlacementEpoch   uint64 `json:"placementEpoch"`
	RaftAppliedIndex uint64 `json:"raftAppliedIndex"`
	ChangeIndex      uint64 `json:"changeIndex"`
}

func streamCheckpointSHA256(point repository.RecoveryPoint) (string, error) {
	checkpoints := make([]streamCheckpoint, 0, len(point.Shards))
	for _, shard := range point.Shards {
		checkpoints = append(checkpoints, streamCheckpoint{
			ShardID: shard.ShardID, PlacementEpoch: shard.PlacementEpoch,
			RaftAppliedIndex: shard.RaftAppliedIndex, ChangeIndex: shard.ChangeIndex,
		})
	}
	sort.Slice(checkpoints, func(i, j int) bool { return checkpoints[i].ShardID < checkpoints[j].ShardID })
	digest, err := digestJSON(checkpoints)
	if err != nil {
		return "", failure(
			CodeEvidenceBinding, "BuildRecoveryPointCatalog", "streamCheckpoint", "ENCODING_FAILED", err,
		)
	}
	return digest, nil
}

func recoveryPointSHA256(point repository.RecoveryPoint) (string, error) {
	point.AdmittedAt = point.AdmittedAt.UTC().Round(0)
	point.RecoveryPointAt = point.RecoveryPointAt.UTC().Round(0)
	point.Shards = append([]repository.ShardPoint(nil), point.Shards...)
	sort.Slice(point.Shards, func(i, j int) bool { return point.Shards[i].ShardID < point.Shards[j].ShardID })
	raw, err := json.Marshal(point)
	if err != nil {
		return "", failure(CodeEvidenceBinding, "BindSourceEvidence", "recoveryPoint", "ENCODING_FAILED", err)
	}
	sum := sha256.Sum256(append(raw, '\n'))
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func canonicalEvidenceBytes(evidence SourceEvidence) ([]byte, string, error) {
	evidence.SHA256 = ""
	raw, err := json.Marshal(evidence)
	if err != nil {
		return nil, "", failure(CodeEvidenceBinding, "WriteSourceEvidence", "$", "ENCODING_FAILED", err)
	}
	raw = append(raw, '\n')
	sum := sha256.Sum256(raw)
	return raw, "sha256:" + hex.EncodeToString(sum[:]), nil
}

func storedEvidenceBytes(evidence SourceEvidence) ([]byte, error) {
	raw, err := json.Marshal(evidence)
	if err != nil {
		return nil, failure(CodeEvidenceBinding, "WriteSourceEvidence", "$", "ENCODING_FAILED", err)
	}
	return append(raw, '\n'), nil
}

// WriteSourceEvidence validates and writes compact bound evidence followed by one LF.
func WriteSourceEvidence(writer io.Writer, evidence SourceEvidence) error {
	if writer == nil {
		return failure(CodeEvidenceBinding, "WriteSourceEvidence", "writer", "NIL_WRITER", nil)
	}
	if err := validateEvidence(evidence); err != nil {
		return err
	}
	_, digest, err := canonicalEvidenceBytes(evidence)
	if err != nil {
		return err
	}
	if digest != evidence.SHA256 {
		return failure(CodeEvidenceBinding, "WriteSourceEvidence", "sha256", "HASH_MISMATCH", nil)
	}
	raw, err := storedEvidenceBytes(evidence)
	if err != nil {
		return err
	}
	if unsafeEvidenceBytes(raw) {
		return failure(CodeEvidenceRedaction, "WriteSourceEvidence", "$", "FORBIDDEN_DISCLOSURE", nil)
	}
	written, err := writer.Write(raw)
	if err != nil {
		return failure(CodeEvidenceBinding, "WriteSourceEvidence", "writer", "WRITE_FAILED", err)
	}
	if written != len(raw) {
		return failure(CodeEvidenceBinding, "WriteSourceEvidence", "writer", "SHORT_WRITE", io.ErrShortWrite)
	}
	return nil
}

func validateEvidence(evidence SourceEvidence) error {
	summary := SourceSummary{
		SchemaVersion: evidence.SchemaVersion, FixtureID: evidence.FixtureID,
		ManifestSHA256: evidence.ManifestSHA256, Seed: evidence.Seed,
		RequestedLogicalBytes: evidence.RequestedLogicalBytes, LogicalBytes: evidence.LogicalBytes,
		ContractRef: evidence.ContractRef,
		ScopeID:     evidence.ScopeID, SourceEnvironmentID: evidence.SourceEnvironmentID,
		RecoveryPointSHA256:    evidence.RecoveryPointSHA256,
		StreamCheckpointSHA256: evidence.StreamCheckpointSHA256,
		ShardCount:             evidence.ShardCount, ReplicationFactor: evidence.ReplicationFactor,
		PlacementEpoch: evidence.PlacementEpoch, ShardIDs: evidence.ShardIDs,
		TopologySHA256: evidence.TopologySHA256, Tables: evidence.Tables, Queries: evidence.Queries,
	}
	if err := validateSummary(summary); err != nil {
		return err
	}
	if !validID(evidence.BackupID) ||
		!digestPattern.MatchString(evidence.BackupSetManifestSHA256) ||
		!digestPattern.MatchString(evidence.RecoveryPointSHA256) ||
		!digestPattern.MatchString(evidence.RecoveryPointCatalogSHA256) ||
		!digestPattern.MatchString(evidence.StreamCheckpointSHA256) ||
		!digestPattern.MatchString(evidence.SHA256) {
		return failure(CodeEvidenceBinding, "WriteSourceEvidence", "binding", "INVALID_BINDING", nil)
	}
	return nil
}

var (
	evidenceCredentialPattern = regexp.MustCompile(
		`(?i)(^|[^a-z0-9])(?:authorization|password|passwd|secret|credentials?|api[-_ ]?key|` +
			`access[-_ ]?key|secret[-_ ]?access[-_ ]?key|private[-_ ]?key|client[-_ ]?secret|` +
			`session[-_ ]?token|refresh[-_ ]?token|cookies?|set[-_ ]?cookie)([^a-z0-9]|$)`,
	)
	evidenceBearerPattern = regexp.MustCompile(`(?i)\bbearer[ \t]+[a-z0-9._~+/=-]+`)
	evidenceJWT           = regexp.MustCompile(
		`(?:^|[^A-Za-z0-9_-])eyJ[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+(?:$|[^A-Za-z0-9_-])`,
	)
	evidenceSignedPattern = regexp.MustCompile(
		`(?i)(?:x-amz-(?:credential|security-token|signature)|x-goog-(?:credential|signature)|` +
			`awsaccesskeyid|signature|access_token|token|sig)\s*=`,
	)
	evidencePayloadPattern = regexp.MustCompile(`(?i)[{[,]\s*"(?:row|rows|item|items|payload)"\s*:`)
	evidenceURLPattern     = regexp.MustCompile(`(?i)\bhttps?://`)
)

func unsafeEvidenceBytes(raw []byte) bool {
	value := string(raw)
	return evidenceCredentialPattern.MatchString(value) ||
		evidenceBearerPattern.MatchString(value) ||
		evidenceJWT.MatchString(value) ||
		evidenceSignedPattern.MatchString(value) ||
		evidencePayloadPattern.MatchString(value) ||
		evidenceURLPattern.MatchString(value)
}
