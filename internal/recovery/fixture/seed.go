package fixture

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	recoverymodel "github.com/CefasDb/cefasdb/internal/recovery/model"
	"github.com/CefasDb/cefasdb/pkg/client"
	"github.com/CefasDb/cefasdb/pkg/types"
)

const (
	FixtureID                = "recovery-fixture-v1"
	ManifestVersion          = uint32(1)
	ManifestSeed             = uint64(20261031)
	ManifestSHA256           = "sha256:4a54e72121c30835e8db3987db4e29b80a19f5a035631e2c3a0a549919f25331"
	FixtureEpoch             = uint64(4102444800)
	FixtureEpochRange        = uint64(86400)
	FixtureTTLObservation    = uint64(43200)
	FixtureTTLExpiredOffset  = uint64(43199)
	FixtureTTLVisibleOffset  = uint64(43201)
	maxManifestBytes         = 1 << 20
	maxJSONDepth             = 64
	maxBatchItems            = 25
	maxBatchWireBytes        = 3 << 20
	maxFixturePayloadBytes   = 512 << 10
	mutationReconcileTimeout = 5 * time.Second
)

type ErrorCode string

const (
	CodeInvalidManifest     ErrorCode = "INVALID_MANIFEST"
	CodeFixtureConflict     ErrorCode = "FIXTURE_CONFLICT"
	CodeUnsupportedTopology ErrorCode = "UNSUPPORTED_TOPOLOGY"
	CodeSeedMismatch        ErrorCode = "SEED_MISMATCH"
	CodeEvidenceBinding     ErrorCode = "EVIDENCE_BINDING"
	CodeEvidenceRedaction   ErrorCode = "EVIDENCE_REDACTION"
	CodeWorkloadFailed      ErrorCode = "WORKLOAD_FAILED"
)

// Error carries a stable fixture failure code without exposing transport bodies.
type Error struct {
	Code      ErrorCode
	Operation string
	Field     string
	Reason    string
	Cause     error
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	out := string(e.Code)
	if e.Operation != "" {
		out += ": " + e.Operation
	}
	if e.Field != "" {
		out += ": " + e.Field
	}
	if e.Reason != "" {
		out += ": " + e.Reason
	}
	return out
}

func (e *Error) Unwrap() error { return e.Cause }

func (e *Error) Is(target error) bool {
	other, ok := target.(*Error)
	return ok && (other.Code == "" || e.Code == other.Code)
}

// IsCode reports whether err contains the requested stable fixture code.
func IsCode(err error, code ErrorCode) bool {
	var failure *Error
	return errors.As(err, &failure) && failure.Code == code
}

func failure(code ErrorCode, op, field, reason string, cause error) error {
	return &Error{Code: code, Operation: op, Field: field, Reason: reason, Cause: cause}
}

type FixtureValue struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

type QueryExpectation struct {
	ID        string        `json:"id"`
	Table     string        `json:"table"`
	Index     string        `json:"index,omitempty"`
	Partition FixtureValue  `json:"partition"`
	SortLow   *FixtureValue `json:"sortLow,omitempty"`
	SortHigh  *FixtureValue `json:"sortHigh,omitempty"`
	BaseCount uint64        `json:"baseCount"`
}

type FixtureTable struct {
	ID                   string                      `json:"id"`
	Name                 string                      `json:"name"`
	KeySchema            types.KeySchema             `json:"keySchema"`
	AttributeDefinitions []types.AttributeDefinition `json:"attributeDefinitions"`
	GSIs                 []types.GSIDescriptor       `json:"gsis"`
	LSIs                 []types.LSIDescriptor       `json:"lsis"`
	BaseRows             uint64                      `json:"baseRows"`
	TTLAttribute         string                      `json:"ttlAttribute,omitempty"`
	Stream               *types.StreamSpecification  `json:"stream,omitempty"`
}

type ScalePolicy struct {
	LogicalBytesSource string `json:"logicalBytesSource"`
	EventTableID       string `json:"eventTableId"`
	BaseLogicalRows    uint64 `json:"baseLogicalRows"`
	BasePayloadBytes   uint32 `json:"basePayloadBytes"`
	EventOrdinalBytes  uint64 `json:"eventOrdinalBytes"`
}

type TTLExpectation struct {
	TableID                  string `json:"tableId"`
	Attribute                string `json:"attribute"`
	ClockSource              string `json:"clockSource"`
	ObservationOffsetSeconds uint64 `json:"observationOffsetSeconds"`
	ExpiredOffsetSeconds     uint64 `json:"expiredOffsetSeconds"`
	VisibleOffsetSeconds     uint64 `json:"visibleOffsetSeconds"`
	ExpectedExpiredBaseRows  uint64 `json:"expectedExpiredBaseRows"`
	ExpectedVisibleBaseRows  uint64 `json:"expectedVisibleBaseRows"`
}

type FixtureManifest struct {
	FixtureID          string             `json:"fixtureId"`
	SchemaVersion      uint32             `json:"schemaVersion"`
	Seed               uint64             `json:"seed"`
	EpochSeconds       uint64             `json:"epochSeconds"`
	EpochWindowSeconds uint64             `json:"epochWindowSeconds"`
	TTLExpectation     TTLExpectation     `json:"ttlExpectation"`
	Tables             []FixtureTable     `json:"tables"`
	ExpectedQueries    []QueryExpectation `json:"expectedQueries"`
	RequiredChecks     []string           `json:"requiredChecks"`
	ScalePolicy        ScalePolicy        `json:"scalePolicy"`
	SHA256             string             `json:"sha256,omitempty"`
}

var (
	digestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	idPattern     = regexp.MustCompile(`^[A-Za-z0-9](?:[A-Za-z0-9._-]{0,191})$`)
	manifestKeys  = map[string]string{
		"$":                                 "|fixtureId|schemaVersion|seed|epochSeconds|epochWindowSeconds|ttlExpectation|tables|expectedQueries|requiredChecks|scalePolicy|sha256|",
		"$.tables[]":                        "|id|name|keySchema|attributeDefinitions|gsis|lsis|baseRows|ttlAttribute|stream|",
		"$.tables[].keySchema":              "|pk|sk|",
		"$.tables[].attributeDefinitions[]": "|name|type|vectorDimensions|",
		"$.tables[].gsis[]":                 "|name|keySchema|projection|projected|",
		"$.tables[].gsis[].keySchema":       "|pk|sk|",
		"$.tables[].gsis[].projection":      "|mode|include|",
		"$.tables[].lsis[]":                 "|name|sk|projection|",
		"$.tables[].lsis[].projection":      "|mode|include|",
		"$.tables[].stream":                 "|streamEnabled|streamViewType|retentionSeconds|",
		"$.ttlExpectation":                  "|tableId|attribute|clockSource|observationOffsetSeconds|expiredOffsetSeconds|visibleOffsetSeconds|expectedExpiredBaseRows|expectedVisibleBaseRows|",
		"$.expectedQueries[]":               "|id|table|index|partition|sortLow|sortHigh|baseCount|",
		"$.expectedQueries[].partition":     "|type|value|",
		"$.expectedQueries[].sortLow":       "|type|value|",
		"$.expectedQueries[].sortHigh":      "|type|value|",
		"$.scalePolicy":                     "|logicalBytesSource|eventTableId|baseLogicalRows|basePayloadBytes|eventOrdinalBytes|",
	}
)

// LoadManifest reads, validates, and self-digest verifies one canonical fixture manifest.
func LoadManifest(path string) (FixtureManifest, error) {
	// #nosec G304 -- selecting the manifest is this API's caller-controlled operation.
	file, err := os.Open(path)
	if err != nil {
		return FixtureManifest{}, failure(CodeInvalidManifest, "LoadManifest", "path", "READ_FAILED", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return FixtureManifest{}, failure(CodeInvalidManifest, "LoadManifest", "path", "READ_FAILED", err)
	}
	if info.Size() > maxManifestBytes {
		return FixtureManifest{}, failure(CodeInvalidManifest, "LoadManifest", "$", "INPUT_TOO_LARGE", nil)
	}
	raw, err := io.ReadAll(io.LimitReader(file, maxManifestBytes+1))
	if err != nil {
		return FixtureManifest{}, failure(CodeInvalidManifest, "LoadManifest", "path", "READ_FAILED", err)
	}
	return decodeManifest(raw)
}

func decodeManifest(raw []byte) (FixtureManifest, error) {
	switch {
	case len(raw) > maxManifestBytes:
		return FixtureManifest{}, failure(CodeInvalidManifest, "decodeManifest", "$", "INPUT_TOO_LARGE", nil)
	case !utf8.Valid(raw):
		return FixtureManifest{}, failure(CodeInvalidManifest, "decodeManifest", "$", "INVALID_UTF8", nil)
	case bytes.IndexByte(raw, '\r') >= 0:
		return FixtureManifest{}, failure(CodeInvalidManifest, "decodeManifest", "$", "INVALID_LINE_ENDING", nil)
	}
	if err := scanManifestJSON(raw); err != nil {
		return FixtureManifest{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var manifest FixtureManifest
	if err := decoder.Decode(&manifest); err != nil {
		return FixtureManifest{}, failure(CodeInvalidManifest, "decodeManifest", "$", "INVALID_JSON", err)
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return FixtureManifest{}, failure(CodeInvalidManifest, "decodeManifest", "$", "TRAILING_JSON", nil)
	}
	if err := validateManifest(manifest); err != nil {
		return FixtureManifest{}, err
	}
	stored, err := storedManifestBytes(manifest)
	if err != nil || !bytes.Equal(raw, stored) {
		return FixtureManifest{}, failure(CodeInvalidManifest, "decodeManifest", "$", "NON_CANONICAL_JSON", err)
	}
	return manifest, nil
}

func canonicalManifestBytes(manifest FixtureManifest) ([]byte, string, error) {
	manifest.SHA256 = ""
	raw, err := json.Marshal(manifest)
	if err != nil {
		return nil, "", failure(CodeInvalidManifest, "canonicalManifestBytes", "$", "ENCODING_FAILED", err)
	}
	raw = append(raw, '\n')
	sum := sha256.Sum256(raw)
	return raw, "sha256:" + hex.EncodeToString(sum[:]), nil
}

func storedManifestBytes(manifest FixtureManifest) ([]byte, error) {
	raw, err := json.Marshal(manifest)
	if err != nil {
		return nil, failure(CodeInvalidManifest, "storedManifestBytes", "$", "ENCODING_FAILED", err)
	}
	return append(raw, '\n'), nil
}

func validateManifest(manifest FixtureManifest) error {
	const op = "validateManifest"
	if !validID(manifest.FixtureID) || manifest.FixtureID != FixtureID {
		return failure(CodeInvalidManifest, op, "fixtureId", "INVALID_IDENTITY", nil)
	}
	if manifest.SchemaVersion != ManifestVersion {
		return failure(CodeInvalidManifest, op, "schemaVersion", "UNSUPPORTED_VERSION", nil)
	}
	if manifest.Seed != ManifestSeed {
		return failure(CodeInvalidManifest, op, "seed", "SEED_MISMATCH", nil)
	}
	if manifest.EpochSeconds != FixtureEpoch || manifest.EpochWindowSeconds != FixtureEpochRange {
		return failure(CodeInvalidManifest, op, "epochSeconds", "INVALID_EPOCH", nil)
	}
	if err := validateSortedManifestIDs(manifest); err != nil {
		return err
	}
	profile := baseManifest()
	provided := manifest.SHA256
	manifest.SHA256 = ""
	if !reflect.DeepEqual(manifest, profile) {
		return failure(CodeInvalidManifest, op, "$", "PROFILE_MISMATCH", nil)
	}
	if !digestPattern.MatchString(provided) {
		return failure(CodeInvalidManifest, op, "sha256", "INVALID_HASH", nil)
	}
	_, digest, err := canonicalManifestBytes(manifest)
	if err != nil {
		return err
	}
	if digest != provided || digest != ManifestSHA256 {
		return failure(CodeInvalidManifest, op, "sha256", "HASH_MISMATCH", nil)
	}
	return nil
}

func validateSortedManifestIDs(manifest FixtureManifest) error {
	tableIDs := make([]string, len(manifest.Tables))
	for i, table := range manifest.Tables {
		if !validID(table.ID) || !validID(table.Name) {
			return failure(CodeInvalidManifest, "validateManifest", "tables", "INVALID_IDENTITY", nil)
		}
		tableIDs[i] = table.ID
	}
	if err := requireSortedUnique(tableIDs); err != nil {
		return failure(CodeInvalidManifest, "validateManifest", "tables", err.Error(), nil)
	}
	queryIDs := make([]string, len(manifest.ExpectedQueries))
	for i, query := range manifest.ExpectedQueries {
		if !validID(query.ID) || !validID(query.Table) || (query.Index != "" && !validID(query.Index)) {
			return failure(CodeInvalidManifest, "validateManifest", "expectedQueries", "INVALID_IDENTITY", nil)
		}
		queryIDs[i] = query.ID
	}
	if err := requireSortedUnique(queryIDs); err != nil {
		return failure(CodeInvalidManifest, "validateManifest", "expectedQueries", err.Error(), nil)
	}
	if err := requireSortedUnique(manifest.RequiredChecks); err != nil {
		return failure(CodeInvalidManifest, "validateManifest", "requiredChecks", err.Error(), nil)
	}
	return nil
}

func requireSortedUnique(values []string) error {
	if !sort.StringsAreSorted(values) {
		return errors.New("UNSORTED_IDS")
	}
	for i, value := range values {
		if !validID(value) {
			return errors.New("INVALID_IDENTITY")
		}
		if i > 0 && value == values[i-1] {
			return errors.New("DUPLICATE_ID")
		}
	}
	return nil
}

func validID(value string) bool {
	return idPattern.MatchString(value) && value != "." && value != ".." && !strings.Contains(value, "..")
}

func scanManifestJSON(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	token, err := decoder.Token()
	if err != nil {
		return failure(CodeInvalidManifest, "decodeManifest", "$", "INVALID_JSON", err)
	}
	delim, ok := token.(json.Delim)
	if !ok || delim != '{' {
		return failure(CodeInvalidManifest, "decodeManifest", "$", "TOP_LEVEL_NOT_OBJECT", nil)
	}
	if err := scanJSONObject(decoder, "$", 1); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return failure(CodeInvalidManifest, "decodeManifest", "$", "TRAILING_JSON", nil)
	}
	return nil
}

func scanJSONObject(decoder *json.Decoder, path string, depth int) error {
	if depth > maxJSONDepth {
		return failure(CodeInvalidManifest, "decodeManifest", path, "JSON_TOO_DEEP", nil)
	}
	allowed, ok := manifestKeys[path]
	if !ok {
		return failure(CodeInvalidManifest, "decodeManifest", path, "UNKNOWN_OBJECT", nil)
	}
	seen := map[string]struct{}{}
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return failure(CodeInvalidManifest, "decodeManifest", path, "INVALID_JSON", err)
		}
		key, ok := token.(string)
		if !ok {
			return failure(CodeInvalidManifest, "decodeManifest", path, "INVALID_OBJECT_KEY", nil)
		}
		if _, duplicate := seen[key]; duplicate {
			return failure(CodeInvalidManifest, "decodeManifest", childPath(path, key), "DUPLICATE_FIELD", nil)
		}
		seen[key] = struct{}{}
		if !strings.Contains(allowed, "|"+key+"|") {
			return failure(CodeInvalidManifest, "decodeManifest", childPath(path, key), "UNKNOWN_FIELD", nil)
		}
		if err := scanJSONValue(decoder, childPath(path, key), depth+1); err != nil {
			return err
		}
	}
	if _, err := decoder.Token(); err != nil {
		return failure(CodeInvalidManifest, "decodeManifest", path, "INVALID_JSON", err)
	}
	return nil
}

func scanJSONValue(decoder *json.Decoder, path string, depth int) error {
	if depth > maxJSONDepth {
		return failure(CodeInvalidManifest, "decodeManifest", path, "JSON_TOO_DEEP", nil)
	}
	token, err := decoder.Token()
	if err != nil {
		return failure(CodeInvalidManifest, "decodeManifest", path, "INVALID_JSON", err)
	}
	switch value := token.(type) {
	case json.Delim:
		switch value {
		case '{':
			return scanJSONObject(decoder, path, depth)
		case '[':
			for decoder.More() {
				if err := scanJSONValue(decoder, path+"[]", depth+1); err != nil {
					return err
				}
			}
			if _, err := decoder.Token(); err != nil {
				return failure(CodeInvalidManifest, "decodeManifest", path, "INVALID_JSON", err)
			}
		default:
			return failure(CodeInvalidManifest, "decodeManifest", path, "INVALID_JSON", nil)
		}
	case json.Number:
		if !integerJSON(value.String()) {
			return failure(CodeInvalidManifest, "decodeManifest", path, "NON_INTEGER_NUMBER", nil)
		}
	}
	return nil
}

func integerJSON(value string) bool {
	if value == "" || strings.ContainsAny(value, ".eE+") || value == "-0" {
		return false
	}
	_, err := strconv.ParseUint(value, 10, 64)
	return err == nil
}

func childPath(parent, key string) string {
	if parent == "$" {
		return "$." + key
	}
	return parent + "." + key
}

func baseManifest() FixtureManifest {
	value := func(kind, raw string) *FixtureValue { return &FixtureValue{Type: kind, Value: raw} }
	events := FixtureTable{
		ID: "recovery_events_v1", Name: "recovery_events_v1",
		KeySchema: types.KeySchema{PK: "user_id", SK: "event_id"},
		AttributeDefinitions: []types.AttributeDefinition{
			{Name: "event_id", Type: types.AttributeTypeString},
			{Name: "event_time_id", Type: types.AttributeTypeString},
			{Name: "event_type", Type: types.AttributeTypeString},
			{Name: "expires_at", Type: types.AttributeTypeNumber},
			{Name: "user_id", Type: types.AttributeTypeString},
		},
		GSIs: []types.GSIDescriptor{{
			Name: "events_by_type_v1", KeySchema: types.KeySchema{PK: "event_type", SK: "event_time_id"},
			Projection: types.IndexProjection{Mode: "ALL"},
		}},
		LSIs: []types.LSIDescriptor{{
			Name: "events_by_expiry_v1", SK: "expires_at",
			Projection: types.IndexProjection{Mode: "KEYS_ONLY"},
		}},
		BaseRows: 9000, TTLAttribute: "expires_at",
		Stream: &types.StreamSpecification{StreamEnabled: true, StreamViewType: types.StreamViewTypeNewAndOldImages},
	}
	users := FixtureTable{
		ID: "recovery_users_v1", Name: "recovery_users_v1",
		KeySchema: types.KeySchema{PK: "account_id", SK: "user_id"},
		AttributeDefinitions: []types.AttributeDefinition{
			{Name: "account_id", Type: types.AttributeTypeString},
			{Name: "email", Type: types.AttributeTypeString},
			{Name: "region", Type: types.AttributeTypeString},
			{Name: "user_id", Type: types.AttributeTypeString},
		},
		GSIs: []types.GSIDescriptor{{
			Name: "users_by_region_v1", KeySchema: types.KeySchema{PK: "region", SK: "user_id"},
			Projection: types.IndexProjection{Mode: "ALL"},
		}},
		LSIs: []types.LSIDescriptor{{
			Name: "users_by_email_v1", SK: "email",
			Projection: types.IndexProjection{Mode: "INCLUDE", Include: nil},
		}},
		BaseRows: 1000,
	}
	queries := []QueryExpectation{
		{ID: "events.gsi.type-v1", Table: events.ID, Index: "events_by_type_v1", Partition: *value("S", "type-00"), BaseCount: 2250},
		{ID: "events.lsi.expiry-window-v1", Table: events.ID, Index: "events_by_expiry_v1", Partition: *value("S", "user-000000"), SortLow: value("N", strconv.FormatUint(FixtureEpoch, 10)), SortHigh: value("N", strconv.FormatUint(FixtureEpoch+FixtureEpochRange-1, 10)), BaseCount: 9},
		{ID: "events.primary.user-v1", Table: events.ID, Partition: *value("S", "user-000000"), BaseCount: 9},
		{ID: "users.gsi.region-v1", Table: users.ID, Index: "users_by_region_v1", Partition: *value("S", "region-00"), BaseCount: 250},
		{ID: "users.lsi.email-range-v1", Table: users.ID, Index: "users_by_email_v1", Partition: *value("S", "acct-0000"), SortLow: value("S", "user-000000@example.test"), SortHigh: value("S", "user-000009@example.test"), BaseCount: 10},
		{ID: "users.primary.account-v1", Table: users.ID, Partition: *value("S", "acct-0000"), BaseCount: 10},
	}
	return FixtureManifest{
		FixtureID: FixtureID, SchemaVersion: ManifestVersion, Seed: ManifestSeed,
		EpochSeconds: FixtureEpoch, EpochWindowSeconds: FixtureEpochRange,
		TTLExpectation: TTLExpectation{
			TableID: events.ID, Attribute: events.TTLAttribute, ClockSource: "utc",
			ObservationOffsetSeconds: FixtureTTLObservation,
			ExpiredOffsetSeconds:     FixtureTTLExpiredOffset, VisibleOffsetSeconds: FixtureTTLVisibleOffset,
			ExpectedExpiredBaseRows: events.BaseRows / 2, ExpectedVisibleBaseRows: events.BaseRows / 2,
		},
		Tables: []FixtureTable{events, users}, ExpectedQueries: queries,
		RequiredChecks: []string{"REC-01", "REC-02", "REC-03", "REC-04", "REC-05"},
		ScalePolicy: ScalePolicy{
			LogicalBytesSource: "RecoveryContract.Workload.LogicalBytes",
			EventTableID:       events.ID, BaseLogicalRows: 10000, BasePayloadBytes: 64,
			EventOrdinalBytes: 1 << 20,
		},
	}
}

type schemaPort interface {
	DescribeTable(context.Context, string) (types.TableDescriptor, error)
	CreateTable(context.Context, types.TableDescriptor) (types.TableDescriptor, error)
}

type ttlPort interface {
	DescribeTTL(context.Context, string) (client.TTLState, error)
	UpdateTTL(context.Context, string, string, bool) (client.TTLState, error)
}

type itemPort interface {
	WriteBatch(context.Context, string, []client.BatchWriteOp) error
	StrongGet(context.Context, string, types.Item) (types.Item, error)
}

type seedItemPort interface {
	itemPort
	ScanItems(context.Context, string, func(types.Item) error) error
}

type topologyPort interface {
	ClusterStatus(context.Context) (client.ClusterStatus, error)
}

type seedPorts struct {
	schema   schemaPort
	ttl      ttlPort
	items    seedItemPort
	topology topologyPort
}

type clientAdapter struct{ cli *client.Client }

func (a clientAdapter) DescribeTable(ctx context.Context, table string) (types.TableDescriptor, error) {
	return a.cli.DescribeTable(ctx, table)
}

func (a clientAdapter) CreateTable(ctx context.Context, descriptor types.TableDescriptor) (types.TableDescriptor, error) {
	return a.cli.CreateTableWithRequiredLSIs(ctx, descriptor)
}

func (a clientAdapter) DescribeTTL(ctx context.Context, table string) (client.TTLState, error) {
	return a.cli.DescribeTimeToLive(ctx, table)
}

func (a clientAdapter) UpdateTTL(ctx context.Context, table, attribute string, enabled bool) (client.TTLState, error) {
	return a.cli.UpdateTimeToLive(ctx, table, attribute, enabled)
}

func (a clientAdapter) WriteBatch(ctx context.Context, table string, ops []client.BatchWriteOp) error {
	return a.cli.BatchWriteItem(ctx, table, ops)
}

func (a clientAdapter) StrongGet(ctx context.Context, table string, key types.Item) (types.Item, error) {
	return a.cli.GetItem(ctx, table, key, client.GetOptions{Strong: true})
}

func (a clientAdapter) ClusterStatus(ctx context.Context) (client.ClusterStatus, error) {
	return a.cli.Status(ctx)
}

type SeedRequest struct {
	Manifest    FixtureManifest
	Seed        uint64
	Contract    recoverymodel.RecoveryContract
	ContractRef recoverymodel.ContractRef
}

type SeedResult struct {
	FixtureID        string   `json:"fixtureId"`
	ManifestSHA256   string   `json:"manifestSha256"`
	CreatedTables    []string `json:"createdTables"`
	RowsAttempted    uint64   `json:"rowsAttempted"`
	RowsAcknowledged uint64   `json:"rowsAcknowledged"`
}

// Seed validates and writes the deterministic fixture through a concrete client.
// Environmental cleanup is intentionally not implemented here. A later cleanup
// operation must prove this exact fixture id and manifest digest and stop on mismatch.
func Seed(ctx context.Context, cli *client.Client, req SeedRequest) (SeedResult, error) {
	if cli == nil {
		return SeedResult{}, failure(CodeSeedMismatch, "Seed", "client", "NIL_CLIENT", nil)
	}
	adapter := clientAdapter{cli: cli}
	return seedWithPorts(ctx, seedPorts{
		schema: adapter, ttl: adapter, items: adapter, topology: adapter,
	}, req)
}

type tableObservation struct {
	table      FixtureTable
	descriptor types.TableDescriptor
	ttl        client.TTLState
	stream     observedStream
	exists     bool
}

type observedStreamShard struct {
	ShardID                string
	StartingSequenceNumber string
	EndingSequenceNumber   string
	LatestSequenceNumber   string
}

type observedStream struct {
	Status   string
	Enabled  bool
	ViewType string
	Shards   []observedStreamShard `json:",omitempty"`
}

func seedWithPorts(ctx context.Context, ports seedPorts, req SeedRequest) (SeedResult, error) {
	if err := validateSeedRequest(req); err != nil {
		return SeedResult{}, err
	}
	statusValue, err := ports.topology.ClusterStatus(ctx)
	if err != nil {
		return SeedResult{}, failure(CodeUnsupportedTopology, "Seed", "topology", "STATUS_FAILED", err)
	}
	if _, err := normalizeTopology(statusValue, req.Contract.Workload); err != nil {
		return SeedResult{}, err
	}
	plan, err := planScale(req.Manifest, req.Contract.Workload.LogicalBytes)
	if err != nil {
		return SeedResult{}, err
	}
	observations := make([]tableObservation, len(req.Manifest.Tables))
	for i, table := range req.Manifest.Tables {
		observations[i], err = observeTable(ctx, ports, table)
		if err != nil {
			return SeedResult{}, err
		}
		if observations[i].exists {
			if err := compatibleObservation(observations[i]); err != nil {
				return SeedResult{}, err
			}
		}
	}
	for _, observation := range observations {
		if !observation.exists {
			continue
		}
		rows := observation.table.BaseRows
		if observation.table.ID == req.Manifest.ScalePolicy.EventTableID {
			rows = plan.EventRows
		}
		if err := validateExistingRows(
			ctx, ports.items, req.Manifest, observation.table, rows, plan.PayloadBytes,
		); err != nil {
			return SeedResult{}, err
		}
	}
	created := make([]string, 0, len(observations))
	for i := range observations {
		if observations[i].exists {
			continue
		}
		observed, createErr := createAndObserve(ctx, ports, observations[i].table)
		if createErr != nil {
			return SeedResult{}, createErr
		}
		observations[i] = observed
		created = append(created, observed.table.Name)
	}
	result := SeedResult{FixtureID: req.Manifest.FixtureID, ManifestSHA256: req.Manifest.SHA256, CreatedTables: created}
	for _, table := range req.Manifest.Tables {
		rows := table.BaseRows
		if table.ID == req.Manifest.ScalePolicy.EventTableID {
			rows = plan.EventRows
		}
		attempted, acknowledged, err := seedTable(ctx, ports.items, req.Manifest, table, rows, plan.PayloadBytes)
		result.RowsAttempted += attempted
		result.RowsAcknowledged += acknowledged
		if err != nil {
			return result, err
		}
	}
	return result, nil
}

func validateSeedRequest(req SeedRequest) error {
	if err := req.Contract.Validate(); err != nil {
		return failure(CodeSeedMismatch, "Seed", "contract", "INVALID_CONTRACT", err)
	}
	if err := validateManifest(req.Manifest); err != nil {
		return failure(CodeSeedMismatch, "Seed", "manifest", "INVALID_MANIFEST", err)
	}
	if req.Seed != req.Manifest.Seed {
		return failure(CodeSeedMismatch, "Seed", "seed", "SEED_MISMATCH", nil)
	}
	expectedRef := recoverymodel.ContractRef{ID: req.Contract.ContractID, Revision: req.Contract.Revision, SHA256: req.Contract.SHA256}
	if req.ContractRef != expectedRef {
		return failure(CodeSeedMismatch, "Seed", "contractRef", "CONTRACT_REF_MISMATCH", nil)
	}
	if !slices.Equal(req.Contract.RequiredChecks, req.Manifest.RequiredChecks) {
		return failure(CodeSeedMismatch, "Seed", "requiredChecks", "CHECK_SET_MISMATCH", nil)
	}
	if !slices.Contains(req.Contract.ManifestVersions, req.Manifest.SchemaVersion) {
		return failure(CodeSeedMismatch, "Seed", "manifestVersions", "UNSUPPORTED_MANIFEST_VERSION", nil)
	}
	if req.Contract.Workload.LogicalBytes == 0 || req.Contract.Workload.Shards == 0 || req.Contract.Workload.ReplicationFactor == 0 {
		return failure(CodeUnsupportedTopology, "Seed", "workload", "ZERO_TOPOLOGY", nil)
	}
	return nil
}

func observeTable(ctx context.Context, ports seedPorts, table FixtureTable) (tableObservation, error) {
	descriptor, err := ports.schema.DescribeTable(ctx, table.Name)
	if err != nil {
		if tableMissing(err) {
			return tableObservation{table: table}, nil
		}
		return tableObservation{}, failure(CodeFixtureConflict, "Seed", table.Name, "DESCRIBE_FAILED", err)
	}
	ttl, err := ports.ttl.DescribeTTL(ctx, table.Name)
	if err != nil {
		return tableObservation{}, failure(CodeFixtureConflict, "Seed", table.Name+".ttl", "DESCRIBE_FAILED", err)
	}
	return tableObservation{
		table: table, descriptor: descriptor, ttl: ttl,
		stream: streamFromDescriptor(descriptor), exists: true,
	}, nil
}

func compatibleObservation(observation tableObservation) error {
	want := normalizeDescriptor(descriptorFor(observation.table))
	got := normalizeDescriptor(observation.descriptor)
	if !reflect.DeepEqual(got, want) {
		return failure(CodeFixtureConflict, "Seed", observation.table.Name, "INCOMPATIBLE_DESCRIPTOR", nil)
	}
	wantTTL := expectedTTL(observation.table)
	gotTTL := normalizeTTL(observation.ttl)
	if gotTTL != wantTTL {
		return failure(CodeFixtureConflict, "Seed", observation.table.Name+".ttl", "INCOMPATIBLE_TTL", nil)
	}
	wantStream := expectedStream(observation.table)
	gotStream := normalizeStream(observation.stream)
	if gotStream.Status != wantStream.Status ||
		gotStream.Enabled != wantStream.Enabled ||
		gotStream.ViewType != wantStream.ViewType {
		return failure(CodeFixtureConflict, "Seed", observation.table.Name+".stream", "INCOMPATIBLE_STREAM", nil)
	}
	return nil
}

func createAndObserve(ctx context.Context, ports seedPorts, table FixtureTable) (tableObservation, error) {
	want := descriptorFor(table)
	_, err := ports.schema.CreateTable(ctx, want)
	if err != nil {
		stored, observedErr := ports.schema.DescribeTable(ctx, table.Name)
		if observedErr != nil {
			return tableObservation{}, failure(CodeFixtureConflict, "Seed", table.Name, "CREATE_UNRESOLVED", err)
		}
		if !reflect.DeepEqual(normalizeDescriptor(stored), normalizeDescriptor(want)) {
			return tableObservation{}, failure(CodeFixtureConflict, "Seed", table.Name, "CREATE_CONFLICT", err)
		}
	}
	stored, err := ports.schema.DescribeTable(ctx, table.Name)
	if err != nil {
		return tableObservation{}, failure(CodeFixtureConflict, "Seed", table.Name, "READBACK_FAILED", err)
	}
	if !reflect.DeepEqual(normalizeDescriptor(stored), normalizeDescriptor(want)) {
		return tableObservation{}, failure(CodeFixtureConflict, "Seed", table.Name, "READBACK_CONFLICT", nil)
	}
	if table.TTLAttribute != "" {
		if _, err := ports.ttl.UpdateTTL(ctx, table.Name, table.TTLAttribute, true); err != nil {
			observed, observedErr := ports.ttl.DescribeTTL(ctx, table.Name)
			if observedErr != nil || normalizeTTL(observed) != expectedTTL(table) {
				return tableObservation{}, failure(CodeFixtureConflict, "Seed", table.Name+".ttl", "TTL_UNRESOLVED", err)
			}
		}
	}
	ttl, err := ports.ttl.DescribeTTL(ctx, table.Name)
	if err != nil || normalizeTTL(ttl) != expectedTTL(table) {
		return tableObservation{}, failure(CodeFixtureConflict, "Seed", table.Name+".ttl", "TTL_READBACK_CONFLICT", err)
	}
	stream := streamFromDescriptor(stored)
	observation := tableObservation{table: table, descriptor: stored, ttl: ttl, stream: stream, exists: true}
	if err := compatibleObservation(observation); err != nil {
		return tableObservation{}, err
	}
	return observation, nil
}

func tableMissing(err error) bool {
	return errors.Is(err, types.ErrTableNotFound) || status.Code(err) == codes.NotFound
}

type descriptorShape struct {
	Name                 string
	KeySchema            types.KeySchema
	AttributeDefinitions []types.AttributeDefinition
	GSIs                 []types.GSIDescriptor
	LSIs                 []types.LSIDescriptor
	SpatialIndexes       []types.SpatialIndexDescriptor
	TextIndexes          []types.TextIndexDescriptor
	StorageClass         string
	MemoryFootprintBytes int64
	TTLAttribute         string
	StreamEnabled        bool
	StreamViewType       string
	MaterializedViews    []string
	GlobalIndexes        []string
}

func descriptorFor(table FixtureTable) types.TableDescriptor {
	descriptor := types.TableDescriptor{
		Name: table.Name, KeySchema: table.KeySchema, TTLAttribute: table.TTLAttribute,
		StorageClass:         types.StorageClassDisk,
		AttributeDefinitions: append([]types.AttributeDefinition(nil), table.AttributeDefinitions...),
		GSIs:                 append([]types.GSIDescriptor(nil), table.GSIs...),
		LSIs:                 append([]types.LSIDescriptor(nil), table.LSIs...),
	}
	if table.Stream != nil {
		copyValue := *table.Stream
		descriptor.StreamSpecification = &copyValue
	}
	return descriptor
}

func normalizeDescriptor(descriptor types.TableDescriptor) descriptorShape {
	storageClass := strings.ToLower(strings.TrimSpace(descriptor.StorageClass))
	if storageClass == "" {
		storageClass = types.StorageClassDisk
	}
	shape := descriptorShape{
		Name: descriptor.Name, KeySchema: descriptor.KeySchema,
		AttributeDefinitions: append([]types.AttributeDefinition(nil), descriptor.AttributeDefinitions...),
		GSIs:                 append([]types.GSIDescriptor(nil), descriptor.GSIs...),
		LSIs:                 append([]types.LSIDescriptor(nil), descriptor.LSIs...),
		SpatialIndexes:       append([]types.SpatialIndexDescriptor(nil), descriptor.SpatialIndexes...),
		TextIndexes:          append([]types.TextIndexDescriptor(nil), descriptor.TextIndexes...),
		StorageClass:         storageClass,
		MemoryFootprintBytes: descriptor.MemoryFootprintBytes,
		TTLAttribute:         descriptor.TTLAttribute,
		MaterializedViews:    append([]string(nil), descriptor.MaterializedViews...),
		GlobalIndexes:        append([]string(nil), descriptor.GlobalIndexes...),
	}
	if descriptor.StreamSpecification != nil {
		shape.StreamEnabled = descriptor.StreamSpecification.StreamEnabled
		shape.StreamViewType = types.NormalizeStreamViewType(descriptor.StreamSpecification.StreamViewType)
	}
	sort.Slice(shape.AttributeDefinitions, func(i, j int) bool { return shape.AttributeDefinitions[i].Name < shape.AttributeDefinitions[j].Name })
	sort.Slice(shape.GSIs, func(i, j int) bool { return shape.GSIs[i].Name < shape.GSIs[j].Name })
	sort.Slice(shape.LSIs, func(i, j int) bool { return shape.LSIs[i].Name < shape.LSIs[j].Name })
	sort.Slice(shape.SpatialIndexes, func(i, j int) bool { return shape.SpatialIndexes[i].Name < shape.SpatialIndexes[j].Name })
	sort.Slice(shape.TextIndexes, func(i, j int) bool { return shape.TextIndexes[i].Name < shape.TextIndexes[j].Name })
	sort.Strings(shape.MaterializedViews)
	sort.Strings(shape.GlobalIndexes)
	for i := range shape.GSIs {
		shape.GSIs[i].Projection.Include = append([]string(nil), shape.GSIs[i].Projection.Include...)
		shape.GSIs[i].Projected = append([]string(nil), shape.GSIs[i].Projected...)
		shape.GSIs[i].Projection.Mode = strings.ToUpper(shape.GSIs[i].Projection.Mode)
		sort.Strings(shape.GSIs[i].Projection.Include)
		sort.Strings(shape.GSIs[i].Projected)
	}
	for i := range shape.LSIs {
		shape.LSIs[i].Projection.Include = append([]string(nil), shape.LSIs[i].Projection.Include...)
		shape.LSIs[i].Projection.Mode = strings.ToUpper(shape.LSIs[i].Projection.Mode)
		sort.Strings(shape.LSIs[i].Projection.Include)
	}
	return shape
}

func expectedTTL(table FixtureTable) client.TTLState {
	return client.TTLState{Enabled: table.TTLAttribute != "", AttributeName: table.TTLAttribute}
}

func normalizeTTL(state client.TTLState) client.TTLState {
	if !state.Enabled {
		state.AttributeName = ""
	}
	return state
}

func expectedStream(table FixtureTable) observedStream {
	if table.Stream == nil || !table.Stream.StreamEnabled {
		return observedStream{}
	}
	return observedStream{
		Status: types.StreamStatusEnabled, Enabled: true,
		ViewType: types.NormalizeStreamViewType(table.Stream.StreamViewType),
	}
}

func streamFromDescriptor(descriptor types.TableDescriptor) observedStream {
	if descriptor.StreamSpecification == nil || !descriptor.StreamSpecification.StreamEnabled {
		return observedStream{}
	}
	statusValue := strings.ToUpper(strings.TrimSpace(descriptor.StreamStatus))
	if statusValue == "" {
		statusValue = types.StreamStatusEnabled
	}
	return normalizeStream(observedStream{
		Status: statusValue, Enabled: statusValue == types.StreamStatusEnabled,
		ViewType: descriptor.StreamSpecification.StreamViewType,
	})
}

func normalizeStream(stream observedStream) observedStream {
	stream.Status = strings.ToUpper(strings.TrimSpace(stream.Status))
	stream.ViewType = types.NormalizeStreamViewType(stream.ViewType)
	stream.Shards = append([]observedStreamShard(nil), stream.Shards...)
	sort.Slice(stream.Shards, func(i, j int) bool {
		return stream.Shards[i].ShardID < stream.Shards[j].ShardID
	})
	if !stream.Enabled {
		stream.Status = ""
		stream.ViewType = ""
		stream.Shards = nil
	}
	return stream
}

type topologySummary struct {
	ShardCount        uint32   `json:"shardCount"`
	ReplicationFactor uint32   `json:"replicationFactor"`
	PlacementEpoch    uint64   `json:"placementEpoch"`
	ShardIDs          []uint32 `json:"shardIds"`
	SHA256            string   `json:"sha256"`
}

func normalizeTopology(statusValue client.ClusterStatus, workload recoverymodel.Workload) (topologySummary, error) {
	if statusValue.ShardCount != int(workload.Shards) || len(statusValue.Shards) != int(workload.Shards) {
		return topologySummary{}, failure(CodeUnsupportedTopology, "topology", "shards", "SHARD_COUNT_MISMATCH", nil)
	}
	summary := topologySummary{ShardCount: workload.Shards, ReplicationFactor: workload.ReplicationFactor}
	seen := map[uint32]struct{}{}
	for _, shard := range statusValue.Shards {
		if _, duplicate := seen[shard.ID]; duplicate {
			return topologySummary{}, failure(CodeUnsupportedTopology, "topology", "shards", "DUPLICATE_SHARD", nil)
		}
		seen[shard.ID] = struct{}{}
		if len(shard.Voters) != int(workload.ReplicationFactor) {
			return topologySummary{}, failure(CodeUnsupportedTopology, "topology", "replicationFactor", "REPLICATION_FACTOR_MISMATCH", nil)
		}
		voters := make(map[string]struct{}, len(shard.Voters))
		for _, voter := range shard.Voters {
			if !validID(voter) {
				return topologySummary{}, failure(CodeUnsupportedTopology, "topology", "voters", "INVALID_VOTER", nil)
			}
			if _, duplicate := voters[voter]; duplicate {
				return topologySummary{}, failure(CodeUnsupportedTopology, "topology", "voters", "DUPLICATE_VOTER", nil)
			}
			voters[voter] = struct{}{}
		}
		summary.ShardIDs = append(summary.ShardIDs, shard.ID)
		if shard.Epoch > summary.PlacementEpoch {
			summary.PlacementEpoch = shard.Epoch
		}
	}
	if statusValue.PlacementVersion > summary.PlacementEpoch {
		summary.PlacementEpoch = statusValue.PlacementVersion
	}
	if summary.PlacementEpoch == 0 {
		return topologySummary{}, failure(CodeUnsupportedTopology, "topology", "placementEpoch", "ZERO_PLACEMENT_EPOCH", nil)
	}
	sort.Slice(summary.ShardIDs, func(i, j int) bool { return summary.ShardIDs[i] < summary.ShardIDs[j] })
	raw, _ := json.Marshal(struct {
		ShardCount        uint32   `json:"shardCount"`
		ReplicationFactor uint32   `json:"replicationFactor"`
		PlacementEpoch    uint64   `json:"placementEpoch"`
		ShardIDs          []uint32 `json:"shardIds"`
	}{summary.ShardCount, summary.ReplicationFactor, summary.PlacementEpoch, summary.ShardIDs})
	sum := sha256.Sum256(append(raw, '\n'))
	summary.SHA256 = "sha256:" + hex.EncodeToString(sum[:])
	return summary, nil
}

type scalePlan struct {
	EventRows    uint64
	PayloadBytes int
	LogicalBytes uint64
}

func planScale(manifest FixtureManifest, logicalBytes uint64) (scalePlan, error) {
	base := manifest.ScalePolicy.BaseLogicalRows * 256
	plan := scalePlan{EventRows: 9000, PayloadBytes: int(manifest.ScalePolicy.BasePayloadBytes)}
	if logicalBytes > base {
		extra := logicalBytes - base
		ordinalRows := extra / manifest.ScalePolicy.EventOrdinalBytes
		if extra%manifest.ScalePolicy.EventOrdinalBytes != 0 {
			ordinalRows++
		}
		if ordinalRows > ^uint64(0)-plan.EventRows {
			return scalePlan{}, failure(CodeSeedMismatch, "planScale", "logicalBytes", "SCALE_OVERFLOW", nil)
		}
		plan.EventRows += ordinalRows
		availablePayload := uint64(maxFixturePayloadBytes) - uint64(plan.PayloadBytes)
		rowsForWireBound := extra / availablePayload
		if extra%availablePayload != 0 {
			rowsForWireBound++
		}
		if rowsForWireBound > plan.EventRows {
			plan.EventRows = rowsForWireBound
		}
		payloadExtra := extra / plan.EventRows
		if extra%plan.EventRows != 0 {
			payloadExtra++
		}
		maxInt := uint64(^uint(0) >> 1)
		if payloadExtra > maxInt-uint64(plan.PayloadBytes) ||
			payloadExtra+uint64(plan.PayloadBytes) > maxFixturePayloadBytes {
			return scalePlan{}, failure(CodeSeedMismatch, "planScale", "logicalBytes", "SCALE_OVERFLOW", nil)
		}
		plan.PayloadBytes += int(payloadExtra)
	}
	measured, err := measureFixtureLogicalBytes(manifest, plan)
	if err != nil {
		return scalePlan{}, err
	}
	if measured < logicalBytes {
		return scalePlan{}, failure(CodeSeedMismatch, "planScale", "logicalBytes", "LOGICAL_BYTES_UNDERSIZED", nil)
	}
	plan.LogicalBytes = measured
	return plan, nil
}

func measureFixtureLogicalBytes(manifest FixtureManifest, plan scalePlan) (uint64, error) {
	var total uint64
	for _, table := range manifest.Tables {
		rows := table.BaseRows
		payloadBytes := 0
		if table.ID == manifest.ScalePolicy.EventTableID {
			rows = plan.EventRows
			payloadBytes = plan.PayloadBytes
		}
		for ordinal := uint64(0); ordinal < rows; ordinal++ {
			raw, err := canonicalItemBytes(generateRow(manifest, table.ID, ordinal, 0))
			if err != nil {
				return 0, failure(CodeSeedMismatch, "planScale", table.Name, "LOGICAL_BYTES_ENCODING_FAILED", err)
			}
			rowBytes := uint64(len(raw))
			if uint64(payloadBytes) > ^uint64(0)-rowBytes {
				return 0, failure(CodeSeedMismatch, "planScale", "logicalBytes", "SCALE_OVERFLOW", nil)
			}
			rowBytes += uint64(payloadBytes)
			if rowBytes > ^uint64(0)-total {
				return 0, failure(CodeSeedMismatch, "planScale", "logicalBytes", "SCALE_OVERFLOW", nil)
			}
			total += rowBytes
		}
	}
	return total, nil
}

func validateExistingRows(
	ctx context.Context,
	items seedItemPort,
	manifest FixtureManifest,
	table FixtureTable,
	rows uint64,
	payloadBytes int,
) error {
	var observed uint64
	err := items.ScanItems(ctx, table.Name, func(item types.Item) error {
		ordinal, ordinalErr := fixtureRowOrdinal(table, item)
		if ordinalErr != nil || ordinal >= rows {
			return failure(CodeFixtureConflict, "Seed", table.Name, "PREEXISTING_ROW_CONFLICT", ordinalErr)
		}
		expected := generateRow(manifest, table.ID, ordinal, payloadBytes)
		equal, equalErr := equalWorkloadRow(item, expected)
		if equalErr != nil {
			return failure(CodeFixtureConflict, "Seed", table.Name, "PREEXISTING_ROW_CONFLICT", equalErr)
		}
		if !equal {
			return failure(CodeFixtureConflict, "Seed", table.Name, "PREEXISTING_ROW_CONFLICT", nil)
		}
		observed++
		return nil
	})
	if err != nil {
		var fixtureErr *Error
		if errors.As(err, &fixtureErr) {
			return err
		}
		return failure(CodeFixtureConflict, "Seed", table.Name, "PREFLIGHT_SCAN_FAILED", err)
	}
	if observed > rows {
		return failure(CodeFixtureConflict, "Seed", table.Name, "PREEXISTING_ROW_CONFLICT", nil)
	}
	return nil
}

func fixtureRowOrdinal(table FixtureTable, item types.Item) (uint64, error) {
	attribute, prefix, width := "event_id", "event-", 8
	if table.ID == "recovery_users_v1" {
		attribute, prefix, width = "user_id", "user-", 6
	}
	value, ok := item[attribute]
	if !ok || value.T != types.AttrS || !strings.HasPrefix(value.S, prefix) {
		return 0, failure(CodeFixtureConflict, "Seed", table.Name, "INVALID_FIXTURE_KEY", nil)
	}
	ordinal, err := strconv.ParseUint(strings.TrimPrefix(value.S, prefix), 10, 64)
	if err != nil || value.S != fmt.Sprintf("%s%0*d", prefix, width, ordinal) {
		return 0, failure(CodeFixtureConflict, "Seed", table.Name, "INVALID_FIXTURE_KEY", err)
	}
	return ordinal, nil
}

func seedTable(
	ctx context.Context,
	items itemPort,
	manifest FixtureManifest,
	table FixtureTable,
	rows uint64,
	payloadBytes int,
) (uint64, uint64, error) {
	var attempted, acknowledged uint64
	batch := make([]client.BatchWriteOp, 0, maxBatchItems)
	flush := func() error {
		attempted += uint64(len(batch))
		if err := writeBatchReconciled(ctx, items, table, batch); err != nil {
			return err
		}
		acknowledged += uint64(len(batch))
		batch = batch[:0]
		return nil
	}
	for ordinal := uint64(0); ordinal < rows; ordinal++ {
		operation := client.BatchWriteOp{Put: generateRow(manifest, table.ID, ordinal, payloadBytes)}
		if len(batch) == maxBatchItems {
			if err := flush(); err != nil {
				return attempted, acknowledged, err
			}
		}
		candidate := append(batch, operation)
		size, err := batchWriteRequestSize(table.Name, candidate)
		if err != nil {
			return attempted, acknowledged, err
		}
		if size > maxBatchWireBytes && len(batch) > 0 {
			if err := flush(); err != nil {
				return attempted, acknowledged, err
			}
			candidate = append(batch, operation)
			size, err = batchWriteRequestSize(table.Name, candidate)
			if err != nil {
				return attempted, acknowledged, err
			}
		}
		if size > maxBatchWireBytes {
			return attempted, acknowledged, failure(
				CodeSeedMismatch, "Seed", table.Name, "ITEM_EXCEEDS_TRANSPORT_LIMIT", nil,
			)
		}
		batch = candidate
	}
	if len(batch) > 0 {
		if err := flush(); err != nil {
			return attempted, acknowledged, err
		}
	}
	return attempted, acknowledged, nil
}

func writeBatchReconciled(ctx context.Context, items itemPort, table FixtureTable, batch []client.BatchWriteOp) error {
	if len(batch) == 0 || len(batch) > maxBatchItems {
		return failure(CodeFixtureConflict, "Seed", table.Name, "BATCH_TOO_LARGE", nil)
	}
	size, err := batchWriteRequestSize(table.Name, batch)
	if err != nil {
		return err
	}
	if size > maxBatchWireBytes {
		return failure(CodeFixtureConflict, "Seed", table.Name, "BATCH_BYTES_EXCEEDED", nil)
	}
	writeErr := items.WriteBatch(ctx, table.Name, batch)
	if writeErr == nil {
		return nil
	}
	if !ambiguousMutation(writeErr) {
		return failure(CodeFixtureConflict, "Seed", table.Name, "BATCH_WRITE_REJECTED", writeErr)
	}
	reconcileCtx, cancel := mutationReconcileContext(ctx)
	state, reconcileErr := reconcileBatch(reconcileCtx, items, table, batch)
	cancel()
	if reconcileErr != nil {
		return reconcileErr
	}
	switch state {
	case batchCommitted:
		return nil
	case batchAbsent:
		if canceledMutation(writeErr) || ctx.Err() != nil {
			return failure(CodeFixtureConflict, "Seed", table.Name, "BATCH_CANCELED_UNCOMMITTED", writeErr)
		}
		retryErr := items.WriteBatch(ctx, table.Name, batch)
		if retryErr == nil {
			return nil
		}
		if !ambiguousMutation(retryErr) {
			return failure(CodeFixtureConflict, "Seed", table.Name, "BATCH_RETRY_REJECTED", retryErr)
		}
		retryCtx, retryCancel := mutationReconcileContext(ctx)
		retryState, secondErr := reconcileBatch(retryCtx, items, table, batch)
		retryCancel()
		if secondErr != nil {
			return secondErr
		}
		if retryState == batchCommitted {
			return nil
		}
		if retryState == batchAbsent {
			return failure(CodeFixtureConflict, "Seed", table.Name, "BATCH_RETRY_UNACKNOWLEDGED", retryErr)
		}
		return failure(CodeFixtureConflict, "Seed", table.Name, "PARTIAL_OR_DIFFERENT_BATCH", retryErr)
	default:
		return failure(CodeFixtureConflict, "Seed", table.Name, "PARTIAL_OR_DIFFERENT_BATCH", writeErr)
	}
}

func ambiguousMutation(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return true
	}
	switch status.Code(err) {
	case codes.Canceled, codes.Unavailable, codes.DeadlineExceeded, codes.Unknown, codes.Internal:
		return true
	default:
		return false
	}
}

func canceledMutation(err error) bool {
	return errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled
}

func mutationReconcileContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(parent), mutationReconcileTimeout)
}

type batchState uint8

const (
	batchAbsent batchState = iota
	batchCommitted
	batchConflict
)

func reconcileBatch(ctx context.Context, items itemPort, table FixtureTable, batch []client.BatchWriteOp) (batchState, error) {
	matching, absent := 0, 0
	for _, operation := range batch {
		key, err := keyForItem(table.KeySchema, operation.Put)
		if err != nil {
			return batchConflict, err
		}
		got, err := items.StrongGet(ctx, table.Name, key)
		if err != nil {
			return batchConflict, failure(CodeFixtureConflict, "Seed", table.Name, "RECONCILE_READ_FAILED", err)
		}
		if got == nil {
			absent++
			continue
		}
		equal, err := equalItem(got, operation.Put)
		if err != nil {
			return batchConflict, err
		}
		if !equal {
			return batchConflict, nil
		}
		matching++
	}
	switch {
	case matching == len(batch):
		return batchCommitted, nil
	case absent == len(batch):
		return batchAbsent, nil
	default:
		return batchConflict, nil
	}
}

func keyForItem(schema types.KeySchema, item types.Item) (types.Item, error) {
	key := make(types.Item, 2)
	pk, ok := item[schema.PK]
	if !ok {
		return nil, failure(CodeFixtureConflict, "keyForItem", schema.PK, "MISSING_KEY", nil)
	}
	key[schema.PK] = pk
	if schema.SK != "" {
		sk, ok := item[schema.SK]
		if !ok {
			return nil, failure(CodeFixtureConflict, "keyForItem", schema.SK, "MISSING_KEY", nil)
		}
		key[schema.SK] = sk
	}
	return key, nil
}

func equalItem(left, right types.Item) (bool, error) {
	leftBytes, err := canonicalItemBytes(left)
	if err != nil {
		return false, err
	}
	rightBytes, err := canonicalItemBytes(right)
	return bytes.Equal(leftBytes, rightBytes), err
}

func generateRow(manifest FixtureManifest, tableID string, ordinal uint64, payloadBytes int) types.Item {
	state := deterministicState(manifest.Seed, tableID, ordinal)
	common := types.Item{
		"fixture_id":              stringValue(manifest.FixtureID),
		"fixture_schema_version":  numberValue(strconv.FormatUint(uint64(manifest.SchemaVersion), 10)),
		"fixture_manifest_sha256": stringValue(manifest.SHA256),
		"fixture_seed":            numberValue(strconv.FormatUint(manifest.Seed, 10)),
	}
	switch tableID {
	case "recovery_users_v1":
		common["account_id"] = stringValue(fmt.Sprintf("acct-%04d", ordinal/10))
		common["user_id"] = stringValue(fmt.Sprintf("user-%06d", ordinal))
		common["email"] = stringValue(fmt.Sprintf("user-%06d@example.test", ordinal))
		common["region"] = stringValue(fmt.Sprintf("region-%02d", (ordinal+manifest.Seed%4)%4))
		common["profile"] = types.AttributeValue{T: types.AttrM, M: map[string]types.AttributeValue{
			"active": {T: types.AttrBOOL, BOOL: state&1 == 0},
			"rank":   numberValue(strconv.FormatUint(state%1000, 10)),
		}}
	default:
		offset := state % manifest.EpochWindowSeconds
		eventTime := manifest.EpochSeconds + offset
		expiryOffset := manifest.TTLExpectation.ExpiredOffsetSeconds
		if ordinal%2 == 1 {
			expiryOffset = manifest.TTLExpectation.VisibleOffsetSeconds
		}
		common["user_id"] = stringValue(fmt.Sprintf("user-%06d", ordinal%1000))
		common["event_id"] = stringValue(fmt.Sprintf("event-%08d", ordinal))
		common["event_type"] = stringValue(fmt.Sprintf("type-%02d", (ordinal+manifest.Seed%4)%4))
		common["event_time_id"] = stringValue(fmt.Sprintf("%010d#%08d", eventTime, ordinal))
		common["expires_at"] = numberValue(strconv.FormatUint(manifest.EpochSeconds+expiryOffset, 10))
		common["payload"] = stringValue(deterministicPayload(state, payloadBytes))
	}
	return common
}

func deterministicState(seed uint64, tableID string, ordinal uint64) uint64 {
	hasher := fnv.New64a()
	_, _ = hasher.Write([]byte(tableID))
	value := seed ^ hasher.Sum64() ^ ordinal*0x9e3779b97f4a7c15
	value += 0x9e3779b97f4a7c15
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	return value ^ (value >> 31)
}

func deterministicPayload(state uint64, size int) string {
	if size <= 0 {
		return ""
	}
	const digits = "0123456789abcdef"
	out := make([]byte, size)
	for i := range out {
		state += 0x9e3779b97f4a7c15
		value := state
		value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
		value = (value ^ (value >> 27)) * 0x94d049bb133111eb
		out[i] = digits[(value^(value>>31))&15]
	}
	return string(out)
}

func stringValue(value string) types.AttributeValue {
	return types.AttributeValue{T: types.AttrS, S: value}
}

func numberValue(value string) types.AttributeValue {
	return types.AttributeValue{T: types.AttrN, N: value}
}
