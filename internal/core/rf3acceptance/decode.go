package rf3acceptance

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	maxJSONDocumentBytes = 1 << 20
	maxJSONDepth         = 16
	maxJSONObjectMembers = 128
	maxJSONArrayItems    = 128
	maxJSONTotalValues   = 4096
	maxJSONStringBytes   = 4096
	maxSafeJSONInteger   = uint64(9_007_199_254_740_991)
	maxOperationalCount  = uint64(1_000_000_000_000)
)

var (
	requiredWorkloadProfiles = [...]string{"write", "point-read", "scan", "mixed"}
	requiredScenarios        = [...]string{
		"healthy-baseline",
		"pod-kill",
		"node-drain",
		"node-shutdown",
		"orphan-process",
		"disk-pressure",
		"network-partition",
		"two-node-failure",
	}
	allowedGRPCCodes = map[string]struct{}{
		"OK":                 {},
		"Unavailable":        {},
		"FailedPrecondition": {},
	}
	allowedDoctorClasses = map[string]struct{}{
		"healthy":    {},
		"degraded":   {},
		"repairable": {},
		"unsafe":     {},
	}
	approvalRoles = map[string]struct{}{
		"product-owner":     {},
		"database-operator": {},
		"platform-owner":    {},
	}
)

// DecodeBundle strictly decodes and validates a complete schema-1.0 bundle.
//
// It returns Bundle{} for every malformed, unsupported, or semantically invalid
// input and performs no filesystem or network I/O.
func DecodeBundle(root, workload, topology Source) (Bundle, error) {
	if err := validateBundleSources(root, workload, topology); err != nil {
		return Bundle{}, err
	}

	var rootDocument rootJSON
	if err := decodeJSONSource(root, "acceptance root", &rootDocument); err != nil {
		return Bundle{}, err
	}
	var workloadDocument workloadJSON
	if err := decodeJSONSource(workload, "workload profile", &workloadDocument); err != nil {
		return Bundle{}, err
	}
	var topologyDocument topologyJSON
	if err := decodeJSONSource(topology, "topology profile", &topologyDocument); err != nil {
		return Bundle{}, err
	}

	decodedWorkload, err := buildWorkload(workloadDocument)
	if err != nil {
		return Bundle{}, fmt.Errorf("workload profile %q: %w", workload.path, err)
	}
	decodedTopology, err := buildTopology(topologyDocument)
	if err != nil {
		return Bundle{}, fmt.Errorf("topology profile %q: %w", topology.path, err)
	}
	decodedRoot, err := buildAcceptance(
		rootDocument,
		workload,
		topology,
		decodedWorkload,
		decodedTopology,
	)
	if err != nil {
		return Bundle{}, fmt.Errorf("acceptance root %q: %w", root.path, err)
	}

	return Bundle{
		acceptance:       decodedRoot,
		workload:         decodedWorkload,
		topology:         decodedTopology,
		acceptanceDigest: root.Digest(),
		workloadDigest:   workload.Digest(),
		topologyDigest:   topology.Digest(),
	}, nil
}

func validateBundleSources(root, workload, topology Source) error {
	sources := []struct {
		role   string
		source Source
	}{
		{role: "acceptance root", source: root},
		{role: "workload profile", source: workload},
		{role: "topology profile", source: topology},
	}
	paths := make(map[string]string, len(sources))
	for _, item := range sources {
		if err := validateSourcePath(item.source.path); err != nil {
			return fmt.Errorf("%s source: %w", item.role, err)
		}
		if len(item.source.data) == 0 {
			return fmt.Errorf("%s source %q is empty", item.role, item.source.path)
		}
		if len(item.source.data) > maxJSONDocumentBytes {
			return fmt.Errorf(
				"%s source %q exceeds %d bytes",
				item.role,
				item.source.path,
				maxJSONDocumentBytes,
			)
		}
		if prior, exists := paths[item.source.path]; exists {
			return fmt.Errorf("%s and %s use the same source path %q", prior, item.role, item.source.path)
		}
		paths[item.source.path] = item.role
	}
	return nil
}

type jsonScanState struct {
	values int
}

func decodeJSONSource[T any](source Source, role string, target *T) error {
	if err := validateJSONStructure(source.data); err != nil {
		return fmt.Errorf("%s %q: %w", role, source.path, err)
	}

	decoder := json.NewDecoder(bytes.NewReader(source.data))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("%s %q: decode JSON: %w", role, source.path, err)
	}
	var trailing json.RawMessage
	switch err := decoder.Decode(&trailing); {
	case errors.Is(err, io.EOF):
		return nil
	case err != nil:
		return fmt.Errorf("%s %q: decode trailing JSON: %w", role, source.path, err)
	default:
		return fmt.Errorf("%s %q: trailing JSON document", role, source.path)
	}
}

func validateJSONStructure(data []byte) error {
	if !utf8.Valid(data) {
		return fmt.Errorf("JSON document is not valid UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	state := &jsonScanState{}

	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("decode JSON token: %w", err)
	}
	delimiter, ok := token.(json.Delim)
	if !ok || delimiter != '{' {
		return fmt.Errorf("top-level JSON value must be an object")
	}
	state.values++
	if err := scanJSONObject(decoder, 1, state); err != nil {
		return err
	}
	if token, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err != nil {
			return fmt.Errorf("decode trailing JSON token: %w", err)
		}
		return fmt.Errorf("trailing JSON token %v", token)
	}
	return nil
}

func scanJSONValue(decoder *json.Decoder, depth int, state *jsonScanState) error {
	if depth > maxJSONDepth {
		return fmt.Errorf("JSON nesting exceeds depth limit %d", maxJSONDepth)
	}
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("decode JSON value: %w", err)
	}
	state.values++
	if state.values > maxJSONTotalValues {
		return fmt.Errorf("JSON value count exceeds limit %d", maxJSONTotalValues)
	}

	switch value := token.(type) {
	case json.Delim:
		switch value {
		case '{':
			return scanJSONObject(decoder, depth, state)
		case '[':
			return scanJSONArray(decoder, depth, state)
		default:
			return fmt.Errorf("unexpected opening delimiter %q", value)
		}
	case string:
		if len(value) > maxJSONStringBytes {
			return fmt.Errorf("JSON string exceeds %d bytes", maxJSONStringBytes)
		}
	case json.Number:
		if len(value.String()) > 32 {
			return fmt.Errorf("JSON number exceeds 32 bytes")
		}
	}
	return nil
}

func scanJSONObject(decoder *json.Decoder, depth int, state *jsonScanState) error {
	if depth > maxJSONDepth {
		return fmt.Errorf("JSON nesting exceeds depth limit %d", maxJSONDepth)
	}
	seen := make(map[string]struct{})
	members := 0
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("decode JSON object key: %w", err)
		}
		key, ok := keyToken.(string)
		if !ok {
			return fmt.Errorf("JSON object key has type %T", keyToken)
		}
		if len(key) == 0 || len(key) > maxJSONStringBytes {
			return fmt.Errorf("JSON object key length is outside 1..%d bytes", maxJSONStringBytes)
		}
		if !isSchemaV1JSONKey(key) {
			return fmt.Errorf("unknown or non-canonical JSON key %q", key)
		}
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("duplicate JSON key %q", key)
		}
		seen[key] = struct{}{}
		members++
		if members > maxJSONObjectMembers {
			return fmt.Errorf("JSON object member count exceeds limit %d", maxJSONObjectMembers)
		}
		if err := scanJSONValue(decoder, depth+1, state); err != nil {
			return err
		}
	}
	return expectJSONDelimiter(decoder, '}')
}

func scanJSONArray(decoder *json.Decoder, depth int, state *jsonScanState) error {
	items := 0
	for decoder.More() {
		items++
		if items > maxJSONArrayItems {
			return fmt.Errorf("JSON array item count exceeds limit %d", maxJSONArrayItems)
		}
		if err := scanJSONValue(decoder, depth+1, state); err != nil {
			return err
		}
	}
	return expectJSONDelimiter(decoder, ']')
}

func isSchemaV1JSONKey(key string) bool {
	switch key {
	case "accessMode", "aggregation", "allowedGrpcCodes", "antiColocation", "approvals",
		"approvedAt", "approver", "batchSize", "bytes", "capacityMethod", "class",
		"consistency", "contractId", "cpu", "dataClass", "dataset", "defaultRule",
		"digest", "documentType", "durable", "duration", "environment", "evidence",
		"failureDomains", "headroom", "itemCount", "keyspace", "kind",
		"latencySampleEvery", "limit", "maxAcknowledgedWritesDuringNoQuorum",
		"maxErrorRate", "maxP99Latency", "maxRecovery", "memory", "method",
		"minimumAttempts", "minimumRejectedWritesDuringNoQuorum", "minimumRuns",
		"minimumThroughput", "mode", "name", "network", "nodeCount",
		"observationWindow", "operations", "owner", "path", "payload", "peakUnit",
		"phaseOrder", "population", "profileId", "profiles", "provenance",
		"publishedAt", "rate", "rationale", "readRate", "readWorkers", "references",
		"replicas", "replicationFactor", "requireBaselineAfterRecovery",
		"requireSafeRejection", "requiredDoctorClasses", "resourcesPerNode", "role",
		"rounding", "rpcTimeout", "saturationSignal", "scenarios", "schemaVersion",
		"shards", "size", "status", "storage", "tableNameTemplate", "tolerance",
		"topology", "topologyDigest", "topologyProfileId", "unit", "users", "value",
		"variance", "volume", "workers", "workload", "workloadDigest",
		"workloadProfileId", "writeRate", "writeWorkers":
		return true
	default:
		return false
	}
}

func expectJSONDelimiter(decoder *json.Decoder, expected json.Delim) error {
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("decode JSON closing delimiter: %w", err)
	}
	if token != expected {
		return fmt.Errorf("JSON closing delimiter %v does not match %q", token, expected)
	}
	return nil
}

type provenanceJSON struct {
	Kind      string
	Owner     string
	Evidence  string
	Rationale string
}

type quantityJSON struct {
	Value      json.Number
	Unit       string
	Provenance *provenanceJSON
}

type referenceJSON struct {
	Path      string
	ProfileID string
	Digest    string
}

type referencesJSON struct {
	Workload *referenceJSON
	Topology *referenceJSON
}

type approvalJSON struct {
	Role              string
	Approver          string
	ApprovedAt        string
	ContractID        string
	WorkloadProfileID string
	WorkloadDigest    string
	TopologyProfileID string
	TopologyDigest    string
}

type acceptanceRuleJSON struct {
	ObservationWindow *quantityJSON
	MinimumAttempts   *quantityJSON
	MaxErrorRate      *quantityJSON
	MaxP99Latency     *quantityJSON
	MinimumThroughput *quantityJSON
	MaxRecovery       *quantityJSON
}

type scenarioJSON struct {
	Name                                string
	AllowedGRPCCodes                    []string
	RequiredDoctorClasses               []string
	RequireBaselineAfterRecovery        *bool
	RequireSafeRejection                *bool
	MaxAcknowledgedWritesDuringNoQuorum *quantityJSON
	MinimumRejectedWritesDuringNoQuorum *quantityJSON
}

type varianceJSON struct {
	Method      string
	Population  string
	Aggregation string
	MinimumRuns *quantityJSON
	Tolerance   *quantityJSON
}

type capacityMethodJSON struct {
	Status            string
	Method            string
	PeakUnit          string
	SaturationSignal  string
	Rounding          string
	ObservationWindow *quantityJSON
	Headroom          *quantityJSON
}

type rootJSON struct {
	DocumentType   string
	SchemaVersion  string
	ContractID     string
	ApprovedAt     string
	References     *referencesJSON
	Approvals      []approvalJSON
	DefaultRule    *acceptanceRuleJSON
	Scenarios      []scenarioJSON
	Variance       *varianceJSON
	CapacityMethod *capacityMethodJSON
}

type payloadJSON struct {
	Mode  string
	Bytes *quantityJSON
}

type datasetJSON struct {
	TableNameTemplate string
	ItemCount         *quantityJSON
	Keyspace          *quantityJSON
	Users             *quantityJSON
	Payload           *payloadJSON
}

type workloadPhaseJSON struct {
	Name               string
	PhaseOrder         *quantityJSON
	Operations         *quantityJSON
	Duration           *quantityJSON
	Rate               *quantityJSON
	Workers            *quantityJSON
	BatchSize          *quantityJSON
	Limit              *quantityJSON
	WriteRate          *quantityJSON
	ReadRate           *quantityJSON
	WriteWorkers       *quantityJSON
	ReadWorkers        *quantityJSON
	Consistency        string
	RPCTimeout         *quantityJSON
	LatencySampleEvery *quantityJSON
}

type workloadJSON struct {
	DocumentType  string
	SchemaVersion string
	ProfileID     string
	PublishedAt   string
	DataClass     string
	Dataset       *datasetJSON
	Profiles      []workloadPhaseJSON
}

type volumeJSON struct {
	Durable    *bool
	Class      string
	AccessMode string
	Size       *quantityJSON
}

type resourcesPerNodeJSON struct {
	CPU     *quantityJSON
	Memory  *quantityJSON
	Storage *quantityJSON
	Network *quantityJSON
}

type topologyJSON struct {
	DocumentType      string
	SchemaVersion     string
	ProfileID         string
	PublishedAt       string
	Environment       string
	NodeCount         *quantityJSON
	Replicas          *quantityJSON
	ReplicationFactor *quantityJSON
	Shards            *quantityJSON
	FailureDomains    []string
	AntiColocation    *bool
	Volume            *volumeJSON
	ResourcesPerNode  *resourcesPerNodeJSON
}

func buildWorkload(document workloadJSON) (WorkloadProfileV1, error) {
	if err := validateDocumentHeader(
		document.DocumentType,
		"rf3-workload-profile",
		document.SchemaVersion,
	); err != nil {
		return WorkloadProfileV1{}, err
	}
	id, err := NewProfileID(document.ProfileID)
	if err != nil {
		return WorkloadProfileV1{}, err
	}
	publishedAt, err := parseTimestamp("workload publishedAt", document.PublishedAt)
	if err != nil {
		return WorkloadProfileV1{}, err
	}
	if document.DataClass != "synthetic" {
		return WorkloadProfileV1{}, fmt.Errorf("dataClass must be synthetic, got %q", document.DataClass)
	}
	dataset, err := buildDataset(document.Dataset)
	if err != nil {
		return WorkloadProfileV1{}, err
	}
	if len(document.Profiles) != len(requiredWorkloadProfiles) {
		return WorkloadProfileV1{}, fmt.Errorf(
			"profiles must contain exactly %d entries",
			len(requiredWorkloadProfiles),
		)
	}

	profiles := make([]workloadPhaseV1, len(requiredWorkloadProfiles))
	for index, expected := range requiredWorkloadProfiles {
		profile, err := buildWorkloadPhase(document.Profiles[index], expected, uint64(index+1))
		if err != nil {
			return WorkloadProfileV1{}, fmt.Errorf("profile %q: %w", expected, err)
		}
		profiles[index] = profile
	}
	return WorkloadProfileV1{
		id:          id,
		publishedAt: publishedAt,
		dataClass:   document.DataClass,
		dataset:     dataset,
		profiles:    profiles,
	}, nil
}

func buildDataset(document *datasetJSON) (datasetV1, error) {
	if document == nil {
		return datasetV1{}, fmt.Errorf("dataset is required")
	}
	if err := validateTableNameTemplate(document.TableNameTemplate); err != nil {
		return datasetV1{}, err
	}
	itemCount, err := decodePositiveNumber(document.ItemCount, "dataset itemCount", "items")
	if err != nil {
		return datasetV1{}, err
	}
	keyspace, err := decodePositiveNumber(document.Keyspace, "dataset keyspace", "keys")
	if err != nil {
		return datasetV1{}, err
	}
	users, err := decodePositiveNumber(document.Users, "dataset users", "users")
	if err != nil {
		return datasetV1{}, err
	}
	if document.Payload == nil {
		return datasetV1{}, fmt.Errorf("dataset payload is required")
	}
	if document.Payload.Mode != "repeat" {
		return datasetV1{}, fmt.Errorf("dataset payload mode must be repeat, got %q", document.Payload.Mode)
	}
	payloadBytes, err := decodePositiveNumber(document.Payload.Bytes, "dataset payload bytes", "bytes")
	if err != nil {
		return datasetV1{}, err
	}
	if keyspace.value < itemCount.value {
		return datasetV1{}, fmt.Errorf("dataset keyspace must be at least itemCount")
	}
	return datasetV1{
		tableNameTemplate: document.TableNameTemplate,
		itemCount:         itemCount,
		keyspace:          keyspace,
		users:             users,
		payloadMode:       document.Payload.Mode,
		payloadBytes:      payloadBytes,
	}, nil
}

func validateTableNameTemplate(value string) error {
	if err := validateCanonicalText("dataset tableNameTemplate", value, 128); err != nil {
		return err
	}
	if strings.Count(value, "%s") != 1 {
		return fmt.Errorf("dataset tableNameTemplate must contain exactly one %%s placeholder")
	}
	for _, character := range strings.Replace(value, "%s", "", 1) {
		if (character < 'a' || character > 'z') &&
			(character < '0' || character > '9') &&
			character != '_' {
			return fmt.Errorf("dataset tableNameTemplate contains non-canonical character %q", character)
		}
	}
	if value[0] < 'a' || value[0] > 'z' {
		return fmt.Errorf("dataset tableNameTemplate must start with a lowercase letter")
	}
	return nil
}

func buildWorkloadPhase(
	document workloadPhaseJSON,
	expectedName string,
	expectedOrder uint64,
) (workloadPhaseV1, error) {
	if document.Name != expectedName {
		return workloadPhaseV1{}, fmt.Errorf("name must be %q, got %q", expectedName, document.Name)
	}
	order, err := decodePositiveNumber(document.PhaseOrder, "phaseOrder", "ordinal")
	if err != nil {
		return workloadPhaseV1{}, err
	}
	if order.value != expectedOrder {
		return workloadPhaseV1{}, fmt.Errorf("phaseOrder must be %d, got %d", expectedOrder, order.value)
	}
	duration, err := decodeDuration(document.Duration, "duration")
	if err != nil {
		return workloadPhaseV1{}, err
	}
	timeout, err := decodeDuration(document.RPCTimeout, "rpcTimeout")
	if err != nil {
		return workloadPhaseV1{}, err
	}
	sampleEvery, err := decodePositiveNumber(
		document.LatencySampleEvery,
		"latencySampleEvery",
		"requests-per-sample",
	)
	if err != nil {
		return workloadPhaseV1{}, err
	}
	if document.Consistency != "strong" && document.Consistency != "eventual" {
		return workloadPhaseV1{}, fmt.Errorf("consistency %q is not strong or eventual", document.Consistency)
	}

	profile := workloadPhaseV1{
		name:               document.Name,
		phaseOrder:         order,
		duration:           duration,
		consistency:        document.Consistency,
		rpcTimeout:         timeout,
		latencySampleEvery: sampleEvery,
	}
	switch expectedName {
	case "write", "point-read":
		return buildOperationPhase(profile, document, expectedName == "write")
	case "scan":
		return buildScanPhase(profile, document)
	case "mixed":
		return buildMixedPhase(profile, document)
	default:
		return workloadPhaseV1{}, fmt.Errorf("unsupported profile %q", expectedName)
	}
}

func buildOperationPhase(
	profile workloadPhaseV1,
	document workloadPhaseJSON,
	requireBatch bool,
) (workloadPhaseV1, error) {
	operations, err := decodePositiveNumber(document.Operations, "operations", "operations")
	if err != nil {
		return workloadPhaseV1{}, err
	}
	rate, err := decodePositiveNumber(document.Rate, "rate", "operations-per-second")
	if err != nil {
		return workloadPhaseV1{}, err
	}
	workers, err := decodePositiveNumber(document.Workers, "workers", "workers")
	if err != nil {
		return workloadPhaseV1{}, err
	}
	profile.operations = &operations
	profile.rate = &rate
	profile.workers = &workers
	if requireBatch {
		batch, err := decodePositiveNumber(document.BatchSize, "batchSize", "items")
		if err != nil {
			return workloadPhaseV1{}, err
		}
		profile.batchSize = &batch
	} else if document.BatchSize != nil {
		return workloadPhaseV1{}, fmt.Errorf("point-read profile cannot declare batchSize")
	}
	if err := requireAbsentWorkloadFields(
		document,
		"limit",
		"writeRate",
		"readRate",
		"writeWorkers",
		"readWorkers",
	); err != nil {
		return workloadPhaseV1{}, err
	}
	if profile.name == "write" && profile.consistency != "strong" {
		return workloadPhaseV1{}, fmt.Errorf("write consistency must be strong")
	}
	return profile, nil
}

func buildScanPhase(
	profile workloadPhaseV1,
	document workloadPhaseJSON,
) (workloadPhaseV1, error) {
	rate, err := decodePositiveNumber(document.Rate, "rate", "operations-per-second")
	if err != nil {
		return workloadPhaseV1{}, err
	}
	workers, err := decodePositiveNumber(document.Workers, "workers", "workers")
	if err != nil {
		return workloadPhaseV1{}, err
	}
	limit, err := decodePositiveNumber(document.Limit, "limit", "items")
	if err != nil {
		return workloadPhaseV1{}, err
	}
	if err := requireAbsentWorkloadFields(
		document,
		"operations",
		"batchSize",
		"writeRate",
		"readRate",
		"writeWorkers",
		"readWorkers",
	); err != nil {
		return workloadPhaseV1{}, err
	}
	profile.rate = &rate
	profile.workers = &workers
	profile.limit = &limit
	return profile, nil
}

func buildMixedPhase(
	profile workloadPhaseV1,
	document workloadPhaseJSON,
) (workloadPhaseV1, error) {
	writeRate, err := decodePositiveNumber(document.WriteRate, "writeRate", "operations-per-second")
	if err != nil {
		return workloadPhaseV1{}, err
	}
	readRate, err := decodePositiveNumber(document.ReadRate, "readRate", "operations-per-second")
	if err != nil {
		return workloadPhaseV1{}, err
	}
	writeWorkers, err := decodePositiveNumber(document.WriteWorkers, "writeWorkers", "workers")
	if err != nil {
		return workloadPhaseV1{}, err
	}
	readWorkers, err := decodePositiveNumber(document.ReadWorkers, "readWorkers", "workers")
	if err != nil {
		return workloadPhaseV1{}, err
	}
	batchSize, err := decodePositiveNumber(document.BatchSize, "batchSize", "items")
	if err != nil {
		return workloadPhaseV1{}, err
	}
	if err := requireAbsentWorkloadFields(document, "operations", "rate", "workers", "limit"); err != nil {
		return workloadPhaseV1{}, err
	}
	if profile.consistency != "strong" {
		return workloadPhaseV1{}, fmt.Errorf("mixed consistency must be strong")
	}
	profile.writeRate = &writeRate
	profile.readRate = &readRate
	profile.writeWorkers = &writeWorkers
	profile.readWorkers = &readWorkers
	profile.batchSize = &batchSize
	return profile, nil
}

func requireAbsentWorkloadFields(document workloadPhaseJSON, names ...string) error {
	fields := map[string]*quantityJSON{
		"operations":   document.Operations,
		"rate":         document.Rate,
		"workers":      document.Workers,
		"batchSize":    document.BatchSize,
		"limit":        document.Limit,
		"writeRate":    document.WriteRate,
		"readRate":     document.ReadRate,
		"writeWorkers": document.WriteWorkers,
		"readWorkers":  document.ReadWorkers,
	}
	for _, name := range names {
		if fields[name] != nil {
			return fmt.Errorf("%s profile cannot declare %s", document.Name, name)
		}
	}
	return nil
}

func buildTopology(document topologyJSON) (TopologyProfileV1, error) {
	if err := validateDocumentHeader(
		document.DocumentType,
		"rf3-topology-profile",
		document.SchemaVersion,
	); err != nil {
		return TopologyProfileV1{}, err
	}
	id, err := NewProfileID(document.ProfileID)
	if err != nil {
		return TopologyProfileV1{}, err
	}
	publishedAt, err := parseTimestamp("topology publishedAt", document.PublishedAt)
	if err != nil {
		return TopologyProfileV1{}, err
	}
	if document.Environment != "non-production" {
		return TopologyProfileV1{}, fmt.Errorf("environment must be non-production, got %q", document.Environment)
	}

	nodeCount, err := decodePositiveNumber(document.NodeCount, "nodeCount", "nodes")
	if err != nil {
		return TopologyProfileV1{}, err
	}
	replicas, err := decodePositiveNumber(document.Replicas, "replicas", "replicas")
	if err != nil {
		return TopologyProfileV1{}, err
	}
	replicationFactor, err := decodePositiveNumber(
		document.ReplicationFactor,
		"replicationFactor",
		"replicas",
	)
	if err != nil {
		return TopologyProfileV1{}, err
	}
	shards, err := decodePositiveNumber(document.Shards, "shards", "shards")
	if err != nil {
		return TopologyProfileV1{}, err
	}
	if nodeCount.value < 3 || replicas.value != 3 || replicationFactor.value != 3 {
		return TopologyProfileV1{}, fmt.Errorf(
			"RF3 topology requires at least 3 nodes, exactly 3 replicas, and replicationFactor 3",
		)
	}
	if replicas.value > nodeCount.value {
		return TopologyProfileV1{}, fmt.Errorf("replicas cannot exceed nodeCount")
	}

	domains, err := decodeFailureDomains(document.FailureDomains)
	if err != nil {
		return TopologyProfileV1{}, err
	}
	if document.AntiColocation == nil || !*document.AntiColocation {
		return TopologyProfileV1{}, fmt.Errorf("antiColocation must be true")
	}
	volume, err := buildVolume(document.Volume)
	if err != nil {
		return TopologyProfileV1{}, err
	}
	resources, err := buildResources(document.ResourcesPerNode)
	if err != nil {
		return TopologyProfileV1{}, err
	}
	if volume.size.value > resources.storage.value {
		return TopologyProfileV1{}, fmt.Errorf("volume size cannot exceed per-node storage")
	}

	return TopologyProfileV1{
		id:                id,
		publishedAt:       publishedAt,
		environment:       document.Environment,
		nodeCount:         nodeCount,
		replicas:          replicas,
		replicationFactor: replicationFactor,
		shards:            shards,
		failureDomains:    domains,
		antiColocation:    *document.AntiColocation,
		volume:            volume,
		resourcesPerNode:  resources,
	}, nil
}

func decodeFailureDomains(values []string) ([]string, error) {
	if len(values) != 3 {
		return nil, fmt.Errorf("failureDomains must contain exactly three entries")
	}
	seen := make(map[string]struct{}, len(values))
	result := make([]string, len(values))
	for index, value := range values {
		if err := validateCanonicalText("failure domain", value, 128); err != nil {
			return nil, err
		}
		if _, exists := seen[value]; exists {
			return nil, fmt.Errorf("failureDomains contains duplicate %q", value)
		}
		seen[value] = struct{}{}
		result[index] = value
	}
	return result, nil
}

func buildVolume(document *volumeJSON) (volumeV1, error) {
	if document == nil {
		return volumeV1{}, fmt.Errorf("volume is required")
	}
	if document.Durable == nil || !*document.Durable {
		return volumeV1{}, fmt.Errorf("volume durable must be true")
	}
	if err := validateCanonicalText("volume class", document.Class, 128); err != nil {
		return volumeV1{}, err
	}
	if document.AccessMode != "read-write-once" {
		return volumeV1{}, fmt.Errorf("volume accessMode must be read-write-once")
	}
	size, err := decodePositiveNumber(document.Size, "volume size", "gibibytes")
	if err != nil {
		return volumeV1{}, err
	}
	return volumeV1{
		durable:    *document.Durable,
		class:      document.Class,
		accessMode: document.AccessMode,
		size:       size,
	}, nil
}

func buildResources(document *resourcesPerNodeJSON) (resourcesPerNodeV1, error) {
	if document == nil {
		return resourcesPerNodeV1{}, fmt.Errorf("resourcesPerNode is required")
	}
	cpu, err := decodePositiveNumber(document.CPU, "resourcesPerNode cpu", "cores")
	if err != nil {
		return resourcesPerNodeV1{}, err
	}
	memory, err := decodePositiveNumber(document.Memory, "resourcesPerNode memory", "gibibytes")
	if err != nil {
		return resourcesPerNodeV1{}, err
	}
	storage, err := decodePositiveNumber(document.Storage, "resourcesPerNode storage", "gibibytes")
	if err != nil {
		return resourcesPerNodeV1{}, err
	}
	network, err := decodePositiveNumber(
		document.Network,
		"resourcesPerNode network",
		"megabits-per-second",
	)
	if err != nil {
		return resourcesPerNodeV1{}, err
	}
	return resourcesPerNodeV1{
		cpu:     cpu,
		memory:  memory,
		storage: storage,
		network: network,
	}, nil
}

func buildAcceptance(
	document rootJSON,
	workloadSource Source,
	topologySource Source,
	workload WorkloadProfileV1,
	topology TopologyProfileV1,
) (AcceptanceContractV1, error) {
	if err := validateDocumentHeader(
		document.DocumentType,
		"rf3-acceptance-root",
		document.SchemaVersion,
	); err != nil {
		return AcceptanceContractV1{}, err
	}
	id, err := NewContractID(document.ContractID)
	if err != nil {
		return AcceptanceContractV1{}, err
	}
	approvedAt, err := parseTimestamp("contract approvedAt", document.ApprovedAt)
	if err != nil {
		return AcceptanceContractV1{}, err
	}
	workloadReference, topologyReference, err := buildReferences(
		document.References,
		workloadSource,
		topologySource,
		workload,
		topology,
	)
	if err != nil {
		return AcceptanceContractV1{}, err
	}
	approvals, err := buildApprovals(
		document.Approvals,
		id,
		workloadReference,
		topologyReference,
		workload.publishedAt,
		topology.publishedAt,
		approvedAt,
	)
	if err != nil {
		return AcceptanceContractV1{}, err
	}
	defaultRule, err := buildAcceptanceRule(document.DefaultRule)
	if err != nil {
		return AcceptanceContractV1{}, err
	}
	scenarios, err := buildScenarios(document.Scenarios)
	if err != nil {
		return AcceptanceContractV1{}, err
	}
	variance, err := buildVariance(document.Variance)
	if err != nil {
		return AcceptanceContractV1{}, err
	}
	capacity, err := buildCapacityMethod(document.CapacityMethod)
	if err != nil {
		return AcceptanceContractV1{}, err
	}
	if approvedAt.Before(workload.publishedAt) || approvedAt.Before(topology.publishedAt) {
		return AcceptanceContractV1{}, fmt.Errorf("contract approvedAt predates a referenced profile")
	}

	return AcceptanceContractV1{
		id:                id,
		approvedAt:        approvedAt,
		workloadReference: workloadReference,
		topologyReference: topologyReference,
		approvals:         approvals,
		defaultRule:       defaultRule,
		scenarios:         scenarios,
		variance:          variance,
		capacityMethod:    capacity,
	}, nil
}

func buildReferences(
	document *referencesJSON,
	workloadSource Source,
	topologySource Source,
	workload WorkloadProfileV1,
	topology TopologyProfileV1,
) (profileReference, profileReference, error) {
	if document == nil || document.Workload == nil || document.Topology == nil {
		return profileReference{}, profileReference{}, fmt.Errorf(
			"workload and topology references are required",
		)
	}
	workloadReference, err := buildReference(
		"workload",
		document.Workload,
		workloadSource,
		workload.id,
	)
	if err != nil {
		return profileReference{}, profileReference{}, err
	}
	topologyReference, err := buildReference(
		"topology",
		document.Topology,
		topologySource,
		topology.id,
	)
	if err != nil {
		return profileReference{}, profileReference{}, err
	}
	return workloadReference, topologyReference, nil
}

func buildReference(
	role string,
	document *referenceJSON,
	source Source,
	expectedID ProfileID,
) (profileReference, error) {
	if err := validateSourcePath(document.Path); err != nil {
		return profileReference{}, fmt.Errorf("%s reference path: %w", role, err)
	}
	if document.Path != source.path {
		return profileReference{}, fmt.Errorf(
			"%s reference path %q does not match source path %q",
			role,
			document.Path,
			source.path,
		)
	}
	id, err := NewProfileID(document.ProfileID)
	if err != nil {
		return profileReference{}, fmt.Errorf("%s reference: %w", role, err)
	}
	if id != expectedID {
		return profileReference{}, fmt.Errorf(
			"%s reference profileId %q does not match document %q",
			role,
			id.String(),
			expectedID.String(),
		)
	}
	digest, err := NewDigest(document.Digest)
	if err != nil {
		return profileReference{}, fmt.Errorf("%s reference: %w", role, err)
	}
	exactDigest := source.Digest()
	if !digest.Equal(exactDigest) {
		return profileReference{}, fmt.Errorf(
			"%s digest %q does not match exact source bytes %q",
			role,
			digest.String(),
			exactDigest.String(),
		)
	}
	return profileReference{path: document.Path, id: id, digest: digest}, nil
}

func buildApprovals(
	documents []approvalJSON,
	contractID ContractID,
	workload profileReference,
	topology profileReference,
	workloadPublishedAt time.Time,
	topologyPublishedAt time.Time,
	contractApprovedAt time.Time,
) ([]approval, error) {
	if len(documents) != len(approvalRoles) {
		return nil, fmt.Errorf("approvals must contain exactly three owner roles")
	}
	seen := make(map[string]struct{}, len(documents))
	result := make([]approval, len(documents))
	for index, document := range documents {
		decoded, err := buildApproval(
			document,
			contractID,
			workload,
			topology,
			workloadPublishedAt,
			topologyPublishedAt,
			contractApprovedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("approval[%d]: %w", index, err)
		}
		if _, duplicate := seen[decoded.role]; duplicate {
			return nil, fmt.Errorf("duplicate approval role %q", decoded.role)
		}
		seen[decoded.role] = struct{}{}
		result[index] = decoded
	}
	for role := range approvalRoles {
		if _, exists := seen[role]; !exists {
			return nil, fmt.Errorf("approval role %q is missing", role)
		}
	}
	return result, nil
}

func buildApproval(
	document approvalJSON,
	contractID ContractID,
	workload profileReference,
	topology profileReference,
	workloadPublishedAt time.Time,
	topologyPublishedAt time.Time,
	contractApprovedAt time.Time,
) (approval, error) {
	if _, allowed := approvalRoles[document.Role]; !allowed {
		return approval{}, fmt.Errorf("role %q is not an approval owner", document.Role)
	}
	if err := validateCanonicalText("approver", document.Approver, 256); err != nil {
		return approval{}, err
	}
	approvedAt, err := parseTimestamp("approval approvedAt", document.ApprovedAt)
	if err != nil {
		return approval{}, err
	}
	if approvedAt.Before(workloadPublishedAt) || approvedAt.Before(topologyPublishedAt) {
		return approval{}, fmt.Errorf("approval predates referenced profile bytes")
	}
	if approvedAt.After(contractApprovedAt) {
		return approval{}, fmt.Errorf("approval occurs after contract approvedAt")
	}
	boundContractID, err := NewContractID(document.ContractID)
	if err != nil {
		return approval{}, err
	}
	boundWorkloadID, err := NewProfileID(document.WorkloadProfileID)
	if err != nil {
		return approval{}, err
	}
	boundWorkloadDigest, err := NewDigest(document.WorkloadDigest)
	if err != nil {
		return approval{}, err
	}
	boundTopologyID, err := NewProfileID(document.TopologyProfileID)
	if err != nil {
		return approval{}, err
	}
	boundTopologyDigest, err := NewDigest(document.TopologyDigest)
	if err != nil {
		return approval{}, err
	}
	if boundContractID != contractID ||
		boundWorkloadID != workload.id ||
		!boundWorkloadDigest.Equal(workload.digest) ||
		boundTopologyID != topology.id ||
		!boundTopologyDigest.Equal(topology.digest) {
		return approval{}, fmt.Errorf(
			"approval is not bound to the decoded contract and exact profile bytes",
		)
	}
	return approval{
		role:              document.Role,
		approver:          document.Approver,
		approvedAt:        approvedAt,
		contractID:        boundContractID,
		workloadProfileID: boundWorkloadID,
		workloadDigest:    boundWorkloadDigest,
		topologyProfileID: boundTopologyID,
		topologyDigest:    boundTopologyDigest,
	}, nil
}

func buildAcceptanceRule(document *acceptanceRuleJSON) (AcceptanceRuleV1, error) {
	if document == nil {
		return AcceptanceRuleV1{}, fmt.Errorf("defaultRule is required")
	}
	observation, err := decodeDuration(document.ObservationWindow, "defaultRule observationWindow")
	if err != nil {
		return AcceptanceRuleV1{}, err
	}
	attempts, err := decodePositiveNumber(
		document.MinimumAttempts,
		"defaultRule minimumAttempts",
		"requests",
	)
	if err != nil {
		return AcceptanceRuleV1{}, err
	}
	errorRate, err := decodeRate(document.MaxErrorRate, "defaultRule maxErrorRate")
	if err != nil {
		return AcceptanceRuleV1{}, err
	}
	latency, err := decodeDuration(document.MaxP99Latency, "defaultRule maxP99Latency")
	if err != nil {
		return AcceptanceRuleV1{}, err
	}
	throughput, err := decodePositiveNumber(
		document.MinimumThroughput,
		"defaultRule minimumThroughput",
		"operations-per-second",
	)
	if err != nil {
		return AcceptanceRuleV1{}, err
	}
	recovery, err := decodeDuration(document.MaxRecovery, "defaultRule maxRecovery")
	if err != nil {
		return AcceptanceRuleV1{}, err
	}
	return AcceptanceRuleV1{
		observationWindow: observation,
		minimumAttempts:   attempts,
		maxErrorRate:      errorRate,
		maxP99Latency:     latency,
		minimumThroughput: throughput,
		maxRecovery:       recovery,
	}, nil
}

func buildScenarios(documents []scenarioJSON) ([]ScenarioAcceptanceV1, error) {
	if len(documents) != len(requiredScenarios) {
		return nil, fmt.Errorf("scenarios must contain exactly %d entries", len(requiredScenarios))
	}
	result := make([]ScenarioAcceptanceV1, len(documents))
	for index, expectedName := range requiredScenarios {
		scenario, err := buildScenario(documents[index], expectedName)
		if err != nil {
			return nil, fmt.Errorf("scenario %q: %w", expectedName, err)
		}
		result[index] = scenario
	}
	return result, nil
}

func buildScenario(document scenarioJSON, expectedName string) (ScenarioAcceptanceV1, error) {
	if document.Name != expectedName {
		return ScenarioAcceptanceV1{}, fmt.Errorf("name must be %q, got %q", expectedName, document.Name)
	}
	if document.RequireBaselineAfterRecovery == nil || document.RequireSafeRejection == nil {
		return ScenarioAcceptanceV1{}, fmt.Errorf("both scenario boolean rules are required")
	}
	codes, err := validateEnumSet("allowedGrpcCodes", document.AllowedGRPCCodes, allowedGRPCCodes)
	if err != nil {
		return ScenarioAcceptanceV1{}, err
	}
	classes, err := validateEnumSet(
		"requiredDoctorClasses",
		document.RequiredDoctorClasses,
		allowedDoctorClasses,
	)
	if err != nil {
		return ScenarioAcceptanceV1{}, err
	}
	scenario := ScenarioAcceptanceV1{
		name:                         document.Name,
		allowedGRPCCodes:             codes,
		requiredDoctorClasses:        classes,
		requireBaselineAfterRecovery: *document.RequireBaselineAfterRecovery,
		requireSafeRejection:         *document.RequireSafeRejection,
	}
	switch expectedName {
	case "healthy-baseline":
		if scenario.requireBaselineAfterRecovery || scenario.requireSafeRejection ||
			document.MaxAcknowledgedWritesDuringNoQuorum != nil ||
			document.MinimumRejectedWritesDuringNoQuorum != nil {
			return ScenarioAcceptanceV1{}, fmt.Errorf(
				"healthy baseline cannot declare recovery or safe-rejection rules",
			)
		}
		if !sameStringSet(codes, []string{"OK"}) ||
			!sameStringSet(classes, []string{"healthy"}) {
			return ScenarioAcceptanceV1{}, fmt.Errorf("healthy baseline must require OK and healthy")
		}
	case "two-node-failure":
		if err := applySafeRejectionRules(&scenario, document); err != nil {
			return ScenarioAcceptanceV1{}, err
		}
	default:
		if err := validateOneDomainServing(scenario, document); err != nil {
			return ScenarioAcceptanceV1{}, err
		}
	}
	return scenario, nil
}

func validateOneDomainServing(
	scenario ScenarioAcceptanceV1,
	document scenarioJSON,
) error {
	if !scenario.requireBaselineAfterRecovery {
		return fmt.Errorf("one-domain fault must require baseline after recovery")
	}
	if scenario.requireSafeRejection ||
		document.MaxAcknowledgedWritesDuringNoQuorum != nil ||
		document.MinimumRejectedWritesDuringNoQuorum != nil {
		return fmt.Errorf("one-domain fault cannot use two-node safe-rejection rules")
	}
	if !containsString(scenario.allowedGRPCCodes, "OK") {
		return fmt.Errorf("one-domain fault must permit serving code OK")
	}
	if containsString(scenario.allowedGRPCCodes, "FailedPrecondition") {
		return fmt.Errorf("one-domain fault cannot use quorum-loss rejection code")
	}
	if containsString(scenario.requiredDoctorClasses, "unsafe") {
		return fmt.Errorf("one-domain fault cannot require unsafe doctor class")
	}
	if !containsString(scenario.requiredDoctorClasses, "healthy") &&
		!containsString(scenario.requiredDoctorClasses, "degraded") {
		return fmt.Errorf("one-domain fault must require a serving doctor class")
	}
	return nil
}

func applySafeRejectionRules(
	scenario *ScenarioAcceptanceV1,
	document scenarioJSON,
) error {
	if !scenario.requireBaselineAfterRecovery || !scenario.requireSafeRejection {
		return fmt.Errorf("two-node failure must require safe rejection and recovered baseline")
	}
	if containsString(scenario.allowedGRPCCodes, "OK") {
		return fmt.Errorf("two-node failure cannot permit OK without quorum")
	}
	if !containsString(scenario.allowedGRPCCodes, "Unavailable") &&
		!containsString(scenario.allowedGRPCCodes, "FailedPrecondition") {
		return fmt.Errorf("two-node failure must permit an explicit quorum-loss rejection")
	}
	if !sameStringSet(scenario.requiredDoctorClasses, []string{"unsafe"}) {
		return fmt.Errorf("two-node failure must require only unsafe doctor class")
	}
	acknowledged, err := decodeNumber(
		document.MaxAcknowledgedWritesDuringNoQuorum,
		"maxAcknowledgedWritesDuringNoQuorum",
		"writes",
		0,
		0,
	)
	if err != nil {
		return err
	}
	rejected, err := decodePositiveNumber(
		document.MinimumRejectedWritesDuringNoQuorum,
		"minimumRejectedWritesDuringNoQuorum",
		"writes",
	)
	if err != nil {
		return err
	}
	scenario.maxAcknowledgedWritesDuringNoQuorum = &acknowledged
	scenario.minimumRejectedWritesDuringNoQuorum = &rejected
	return nil
}

func buildVariance(document *varianceJSON) (varianceRuleV1, error) {
	if document == nil {
		return varianceRuleV1{}, fmt.Errorf("variance is required")
	}
	if document.Method != "relative-range-ppm" {
		return varianceRuleV1{}, fmt.Errorf("variance method must be relative-range-ppm")
	}
	if err := validateCanonicalText("variance population", document.Population, 256); err != nil {
		return varianceRuleV1{}, err
	}
	if document.Aggregation != "per-profile-and-scenario" {
		return varianceRuleV1{}, fmt.Errorf("variance aggregation must be per-profile-and-scenario")
	}
	runs, err := decodeNumber(
		document.MinimumRuns,
		"variance minimumRuns",
		"runs",
		3,
		maxOperationalCount,
	)
	if err != nil {
		return varianceRuleV1{}, err
	}
	tolerance, err := decodeRate(document.Tolerance, "variance tolerance")
	if err != nil {
		return varianceRuleV1{}, err
	}
	if tolerance.value.Value() == 0 {
		return varianceRuleV1{}, fmt.Errorf("variance tolerance must be greater than zero")
	}
	return varianceRuleV1{
		method:      document.Method,
		population:  document.Population,
		aggregation: document.Aggregation,
		minimumRuns: runs,
		tolerance:   tolerance,
	}, nil
}

func buildCapacityMethod(document *capacityMethodJSON) (capacityMethodV1, error) {
	if document == nil {
		return capacityMethodV1{}, fmt.Errorf("capacityMethod is required")
	}
	if document.Status != "proposed" && document.Status != "approved" {
		return capacityMethodV1{}, fmt.Errorf(
			"capacityMethod status %q is not proposed or approved",
			document.Status,
		)
	}
	if document.Method != "bounded-binary-search" {
		return capacityMethodV1{}, fmt.Errorf("capacityMethod method must be bounded-binary-search")
	}
	if document.PeakUnit != "operations-per-second" {
		return capacityMethodV1{}, fmt.Errorf("capacityMethod peakUnit must be operations-per-second")
	}
	if document.SaturationSignal != "first-approved-limit-breach" {
		return capacityMethodV1{}, fmt.Errorf(
			"capacityMethod saturationSignal must be first-approved-limit-breach",
		)
	}
	if document.Rounding != "down-to-whole-unit" {
		return capacityMethodV1{}, fmt.Errorf("capacityMethod rounding must be down-to-whole-unit")
	}
	observation, err := decodeDuration(
		document.ObservationWindow,
		"capacityMethod observationWindow",
	)
	if err != nil {
		return capacityMethodV1{}, err
	}
	headroom, err := decodeRate(document.Headroom, "capacityMethod headroom")
	if err != nil {
		return capacityMethodV1{}, err
	}
	if document.Status == "proposed" && headroom.provenance.Kind() != "proposed-target" {
		return capacityMethodV1{}, fmt.Errorf(
			"proposed capacityMethod headroom must have proposed-target provenance",
		)
	}
	return capacityMethodV1{
		status:            document.Status,
		method:            document.Method,
		peakUnit:          document.PeakUnit,
		saturationSignal:  document.SaturationSignal,
		rounding:          document.Rounding,
		observationWindow: observation,
		headroom:          headroom,
	}, nil
}

func validateDocumentHeader(actualType, expectedType, version string) error {
	if actualType != expectedType {
		return fmt.Errorf("documentType must be %q, got %q", expectedType, actualType)
	}
	if version != schemaVersionV1 {
		return fmt.Errorf("schemaVersion must be %q, got %q", schemaVersionV1, version)
	}
	return nil
}

func decodePositiveNumber(
	document *quantityJSON,
	label string,
	unit string,
) (attributedNumber, error) {
	return decodeNumber(document, label, unit, 1, maxOperationalCount)
}

func decodeNumber(
	document *quantityJSON,
	label string,
	unit string,
	minimum uint64,
	maximum uint64,
) (attributedNumber, error) {
	if document == nil {
		return attributedNumber{}, fmt.Errorf("%s is required", label)
	}
	if document.Unit != unit {
		return attributedNumber{}, fmt.Errorf(
			"%s unit must be %q, got %q",
			label,
			unit,
			document.Unit,
		)
	}
	value, err := parseUnsignedInteger(label, document.Value)
	if err != nil {
		return attributedNumber{}, err
	}
	if value < minimum || value > maximum {
		return attributedNumber{}, fmt.Errorf(
			"%s value must be between %d and %d, got %d",
			label,
			minimum,
			maximum,
			value,
		)
	}
	provenance, err := decodeProvenance(document.Provenance)
	if err != nil {
		return attributedNumber{}, fmt.Errorf("%s: %w", label, err)
	}
	return attributedNumber{value: value, unit: unit, provenance: provenance}, nil
}

func decodeDuration(document *quantityJSON, label string) (attributedDuration, error) {
	number, err := decodeNumber(document, label, "milliseconds", 1, maxDurationMillis)
	if err != nil {
		return attributedDuration{}, err
	}
	duration, err := NewDurationMillis(number.value)
	if err != nil {
		return attributedDuration{}, fmt.Errorf("%s: %w", label, err)
	}
	return attributedDuration{value: duration, provenance: number.provenance}, nil
}

func decodeRate(document *quantityJSON, label string) (attributedRate, error) {
	number, err := decodeNumber(
		document,
		label,
		"parts-per-million",
		0,
		maxRatePartsPerMillion,
	)
	if err != nil {
		return attributedRate{}, err
	}
	rate, err := NewRatePPM(number.value)
	if err != nil {
		return attributedRate{}, fmt.Errorf("%s: %w", label, err)
	}
	return attributedRate{value: rate, provenance: number.provenance}, nil
}

func parseUnsignedInteger(label string, number json.Number) (uint64, error) {
	raw := number.String()
	if raw == "" {
		return 0, fmt.Errorf("%s value is required", label)
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf(
			"%s value %q must be an unsigned integer: %w",
			label,
			raw,
			err,
		)
	}
	if value > maxSafeJSONInteger {
		return 0, fmt.Errorf("%s value exceeds the safe JSON integer limit", label)
	}
	return value, nil
}

func decodeProvenance(document *provenanceJSON) (Provenance, error) {
	if document == nil {
		return Provenance{}, fmt.Errorf("provenance is required")
	}
	if _, known := approvalRoles[document.Owner]; !known {
		return Provenance{}, fmt.Errorf(
			"provenance owner %q is not an accountable role",
			document.Owner,
		)
	}
	return NewProvenance(
		document.Kind,
		document.Owner,
		document.Evidence,
		document.Rationale,
	)
}

func parseTimestamp(label, value string) (time.Time, error) {
	if strings.TrimSpace(value) != value || value == "" {
		return time.Time{}, fmt.Errorf("%s is required and must be canonical", label)
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("%s %q is not RFC3339: %w", label, value, err)
	}
	_, offset := parsed.Zone()
	if offset != 0 {
		return time.Time{}, fmt.Errorf("%s must use UTC", label)
	}
	return parsed.UTC(), nil
}

func validateEnumSet(
	label string,
	values []string,
	allowed map[string]struct{},
) ([]string, error) {
	if len(values) == 0 {
		return nil, fmt.Errorf("%s must not be empty", label)
	}
	seen := make(map[string]struct{}, len(values))
	result := make([]string, len(values))
	for index, value := range values {
		if _, known := allowed[value]; !known {
			return nil, fmt.Errorf("%s contains unknown value %q", label, value)
		}
		if _, duplicate := seen[value]; duplicate {
			return nil, fmt.Errorf("%s contains duplicate value %q", label, value)
		}
		seen[value] = struct{}{}
		result[index] = value
	}
	return result, nil
}

func containsString(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

func sameStringSet(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for _, value := range left {
		if !containsString(right, value) {
			return false
		}
	}
	return true
}
