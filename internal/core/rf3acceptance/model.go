// Package rf3acceptance validates immutable RF3 qualification contracts.
package rf3acceptance

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	schemaVersionV1        = "1.0"
	maxSourcePathBytes     = 512
	maxIdentityBytes       = 192
	maxDurationMillis      = uint64(7 * 24 * time.Hour / time.Millisecond)
	maxRatePartsPerMillion = uint64(1_000_000)
)

var (
	identityPattern      = regexp.MustCompile(`^[a-z][a-z0-9-]*(?:[.][a-z][a-z0-9-]*)+$`)
	digestPattern        = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	sourceSegmentPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._+-]*$`)
	mutableAlias         = regexp.MustCompile(`(?i)^(?:latest|current|stable|head|main|master|nightly|snapshot|edge)$`)
)

// Source is one immutable, canonical relative path and its exact bytes.
type Source struct {
	path string
	data []byte
}

// NewSource validates path and defensively copies data.
func NewSource(sourcePath string, data []byte) (Source, error) {
	if err := validateSourcePath(sourcePath); err != nil {
		return Source{}, err
	}
	return Source{
		path: sourcePath,
		data: append([]byte(nil), data...),
	}, nil
}

// Path returns the canonical relative path.
func (source Source) Path() string {
	return source.path
}

// Bytes returns a defensive copy of the exact source bytes.
func (source Source) Bytes() []byte {
	return append([]byte(nil), source.data...)
}

// Digest returns the SHA-256 digest of the exact source bytes.
func (source Source) Digest() Digest {
	return digestBytes(source.data)
}

func validateSourcePath(value string) error {
	if value == "" {
		return fmt.Errorf("source path is required")
	}
	if len(value) > maxSourcePathBytes {
		return fmt.Errorf("source path exceeds %d bytes", maxSourcePathBytes)
	}
	if !utf8.ValidString(value) || strings.TrimSpace(value) != value {
		return fmt.Errorf("source path %q is not canonical", value)
	}
	if strings.ContainsAny(value, "\\\x00\r\n\t") || path.IsAbs(value) {
		return fmt.Errorf("source path %q must be a slash-separated relative path", value)
	}
	if cleaned := path.Clean(value); cleaned != value || value == "." || strings.HasPrefix(value, "../") {
		return fmt.Errorf("source path %q is not canonical", value)
	}
	if path.Ext(value) != ".json" {
		return fmt.Errorf("source path %q must identify a .json document", value)
	}
	for _, segment := range strings.Split(value, "/") {
		if !sourceSegmentPattern.MatchString(segment) || segment == "." || segment == ".." {
			return fmt.Errorf("source path %q contains an invalid segment", value)
		}
		stem := strings.TrimSuffix(segment, path.Ext(segment))
		if mutableAlias.MatchString(stem) {
			return fmt.Errorf("source path %q contains mutable alias %q", value, segment)
		}
	}
	return nil
}

// ContractID is the immutable identity of an acceptance contract.
type ContractID struct {
	value string
}

// NewContractID validates and constructs a ContractID.
func NewContractID(value string) (ContractID, error) {
	if err := validateIdentity("contract id", value); err != nil {
		return ContractID{}, err
	}
	return ContractID{value: value}, nil
}

// String returns the canonical contract identity.
func (id ContractID) String() string {
	return id.value
}

// ProfileID is the immutable identity of a workload or topology profile.
type ProfileID struct {
	value string
}

// NewProfileID validates and constructs a ProfileID.
func NewProfileID(value string) (ProfileID, error) {
	if err := validateIdentity("profile id", value); err != nil {
		return ProfileID{}, err
	}
	return ProfileID{value: value}, nil
}

// String returns the canonical profile identity.
func (id ProfileID) String() string {
	return id.value
}

func validateIdentity(label, value string) error {
	if len(value) == 0 || len(value) > maxIdentityBytes || !identityPattern.MatchString(value) {
		return fmt.Errorf("%s %q is not canonical", label, value)
	}
	for _, segment := range strings.Split(value, ".") {
		if mutableAlias.MatchString(segment) {
			return fmt.Errorf("%s %q contains mutable alias %q", label, value, segment)
		}
	}
	return nil
}

// Digest is a lowercase SHA-256 digest over exact bytes.
type Digest struct {
	sum [sha256.Size]byte
}

// NewDigest validates and constructs a Digest.
func NewDigest(value string) (Digest, error) {
	if !digestPattern.MatchString(value) {
		return Digest{}, fmt.Errorf("digest %q must match sha256:<64 lowercase hex>", value)
	}
	decoded, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	if err != nil {
		return Digest{}, fmt.Errorf("decode digest: %w", err)
	}
	var digest Digest
	copy(digest.sum[:], decoded)
	return digest, nil
}

// String returns the canonical lowercase digest.
func (digest Digest) String() string {
	return "sha256:" + hex.EncodeToString(digest.sum[:])
}

// Equal reports whether two digests identify the same exact bytes.
func (digest Digest) Equal(other Digest) bool {
	return digest.sum == other.sum
}

func digestBytes(data []byte) Digest {
	return Digest{sum: sha256.Sum256(data)}
}

// DurationMillis is a positive bounded duration expressed in milliseconds.
type DurationMillis struct {
	value uint64
}

// NewDurationMillis validates and constructs a DurationMillis.
func NewDurationMillis(value uint64) (DurationMillis, error) {
	if value == 0 || value > maxDurationMillis {
		return DurationMillis{}, fmt.Errorf(
			"duration must be between 1 and %d milliseconds, got %d",
			maxDurationMillis,
			value,
		)
	}
	return DurationMillis{value: value}, nil
}

// Value returns the duration as an integer number of milliseconds.
func (duration DurationMillis) Value() uint64 {
	return duration.value
}

// RatePPM is a bounded rate expressed in parts per million.
type RatePPM struct {
	value uint32
}

// NewRatePPM validates and constructs a RatePPM.
func NewRatePPM(value uint64) (RatePPM, error) {
	if value > maxRatePartsPerMillion {
		return RatePPM{}, fmt.Errorf(
			"rate must be between 0 and %d parts per million, got %d",
			maxRatePartsPerMillion,
			value,
		)
	}
	return RatePPM{value: uint32(value)}, nil
}

// Value returns the rate as an integer number of parts per million.
func (rate RatePPM) Value() uint32 {
	return rate.value
}

// Provenance records the accountable origin of one decision-driving value.
type Provenance struct {
	kind      string
	owner     string
	evidence  string
	rationale string
}

// NewProvenance validates and constructs a measured or proposed-target provenance.
func NewProvenance(kind, owner, evidence, rationale string) (Provenance, error) {
	if err := validateCanonicalText("provenance owner", owner, 256); err != nil {
		return Provenance{}, err
	}
	switch kind {
	case "measured":
		if err := validateCanonicalText("measured evidence", evidence, 2048); err != nil {
			return Provenance{}, err
		}
		if rationale != "" {
			return Provenance{}, fmt.Errorf("measured provenance cannot carry a proposal rationale")
		}
	case "proposed-target":
		if err := validateCanonicalText("proposal rationale", rationale, 2048); err != nil {
			return Provenance{}, err
		}
		if evidence != "" {
			return Provenance{}, fmt.Errorf("proposed-target provenance cannot carry measured evidence")
		}
	default:
		return Provenance{}, fmt.Errorf("provenance kind %q is not measured or proposed-target", kind)
	}
	return Provenance{
		kind:      kind,
		owner:     owner,
		evidence:  evidence,
		rationale: rationale,
	}, nil
}

// Kind returns measured or proposed-target.
func (provenance Provenance) Kind() string {
	return provenance.kind
}

// Owner returns the accountable owner.
func (provenance Provenance) Owner() string {
	return provenance.owner
}

// Evidence returns the reproducible evidence locator for measured values.
func (provenance Provenance) Evidence() string {
	return provenance.evidence
}

// Rationale returns the rationale for proposed target values.
func (provenance Provenance) Rationale() string {
	return provenance.rationale
}

func validateCanonicalText(label, value string, maxBytes int) error {
	if value == "" || len(value) > maxBytes || !utf8.ValidString(value) {
		return fmt.Errorf("%s is required and must not exceed %d bytes", label, maxBytes)
	}
	if strings.TrimSpace(value) != value || strings.ContainsAny(value, "\x00\r\n\t") {
		return fmt.Errorf("%s %q is not canonical", label, value)
	}
	switch strings.ToLower(value) {
	case "unknown", "unresolved", "tbd", "todo", "n/a":
		return fmt.Errorf("%s %q is unresolved", label, value)
	}
	return nil
}

type attributedNumber struct {
	value      uint64
	unit       string
	provenance Provenance
}

type attributedDuration struct {
	value      DurationMillis
	provenance Provenance
}

type attributedRate struct {
	value      RatePPM
	provenance Provenance
}

type profileReference struct {
	path   string
	id     ProfileID
	digest Digest
}

type approval struct {
	role              string
	approver          string
	approvedAt        time.Time
	contractID        ContractID
	workloadProfileID ProfileID
	workloadDigest    Digest
	topologyProfileID ProfileID
	topologyDigest    Digest
}

// AcceptanceRuleV1 contains the default schema-1.0 measurement limits.
type AcceptanceRuleV1 struct {
	observationWindow attributedDuration
	minimumAttempts   attributedNumber
	maxErrorRate      attributedRate
	maxP99Latency     attributedDuration
	minimumThroughput attributedNumber
	maxRecovery       attributedDuration
}

// ObservationWindow returns the bounded measurement window.
func (rule AcceptanceRuleV1) ObservationWindow() DurationMillis {
	return rule.observationWindow.value
}

// MinimumAttempts returns the required request denominator.
func (rule AcceptanceRuleV1) MinimumAttempts() uint64 {
	return rule.minimumAttempts.value
}

// MaxErrorRate returns the maximum error rate.
func (rule AcceptanceRuleV1) MaxErrorRate() RatePPM {
	return rule.maxErrorRate.value
}

// MaxP99Latency returns the maximum p99 latency.
func (rule AcceptanceRuleV1) MaxP99Latency() DurationMillis {
	return rule.maxP99Latency.value
}

// MinimumThroughput returns the minimum operations per second.
func (rule AcceptanceRuleV1) MinimumThroughput() uint64 {
	return rule.minimumThroughput.value
}

// MaxRecovery returns the maximum recovery duration.
func (rule AcceptanceRuleV1) MaxRecovery() DurationMillis {
	return rule.maxRecovery.value
}

// ScenarioAcceptanceV1 contains one schema-1.0 resilience scenario rule.
type ScenarioAcceptanceV1 struct {
	name                                string
	allowedGRPCCodes                    []string
	requiredDoctorClasses               []string
	requireBaselineAfterRecovery        bool
	requireSafeRejection                bool
	maxAcknowledgedWritesDuringNoQuorum *attributedNumber
	minimumRejectedWritesDuringNoQuorum *attributedNumber
}

// Name returns the canonical resilience scenario name.
func (scenario ScenarioAcceptanceV1) Name() string {
	return scenario.name
}

// AllowedGRPCCodes returns a defensive copy of the allowed result codes.
func (scenario ScenarioAcceptanceV1) AllowedGRPCCodes() []string {
	return append([]string(nil), scenario.allowedGRPCCodes...)
}

// RequiredDoctorClasses returns a defensive copy of the required health classes.
func (scenario ScenarioAcceptanceV1) RequiredDoctorClasses() []string {
	return append([]string(nil), scenario.requiredDoctorClasses...)
}

// RequiresBaselineAfterRecovery reports whether baseline behavior must return after recovery.
func (scenario ScenarioAcceptanceV1) RequiresBaselineAfterRecovery() bool {
	return scenario.requireBaselineAfterRecovery
}

// RequiresSafeRejection reports whether writes without quorum must fail safely.
func (scenario ScenarioAcceptanceV1) RequiresSafeRejection() bool {
	return scenario.requireSafeRejection
}

// MaxAcknowledgedWritesDuringNoQuorum returns the limit and whether it is defined.
func (scenario ScenarioAcceptanceV1) MaxAcknowledgedWritesDuringNoQuorum() (uint64, bool) {
	if scenario.maxAcknowledgedWritesDuringNoQuorum == nil {
		return 0, false
	}
	return scenario.maxAcknowledgedWritesDuringNoQuorum.value, true
}

// MinimumRejectedWritesDuringNoQuorum returns the floor and whether it is defined.
func (scenario ScenarioAcceptanceV1) MinimumRejectedWritesDuringNoQuorum() (uint64, bool) {
	if scenario.minimumRejectedWritesDuringNoQuorum == nil {
		return 0, false
	}
	return scenario.minimumRejectedWritesDuringNoQuorum.value, true
}

type varianceRuleV1 struct {
	method      string
	population  string
	aggregation string
	minimumRuns attributedNumber
	tolerance   attributedRate
}

type capacityMethodV1 struct {
	status            string
	method            string
	peakUnit          string
	saturationSignal  string
	rounding          string
	observationWindow attributedDuration
	headroom          attributedRate
}

// AcceptanceContractV1 is the immutable schema-1.0 acceptance root.
type AcceptanceContractV1 struct {
	id                ContractID
	approvedAt        time.Time
	workloadReference profileReference
	topologyReference profileReference
	approvals         []approval
	defaultRule       AcceptanceRuleV1
	scenarios         []ScenarioAcceptanceV1
	variance          varianceRuleV1
	capacityMethod    capacityMethodV1
}

// ID returns the immutable contract identity.
func (contract AcceptanceContractV1) ID() ContractID {
	return contract.id
}

// ApprovedAt returns the contract approval timestamp.
func (contract AcceptanceContractV1) ApprovedAt() time.Time {
	return contract.approvedAt
}

// DefaultRule returns the schema-1.0 default acceptance rule.
func (contract AcceptanceContractV1) DefaultRule() AcceptanceRuleV1 {
	return contract.defaultRule
}

// Scenarios returns a defensive copy of the eight scenario values.
func (contract AcceptanceContractV1) Scenarios() []ScenarioAcceptanceV1 {
	return append([]ScenarioAcceptanceV1(nil), contract.scenarios...)
}

type datasetV1 struct {
	tableNameTemplate string
	itemCount         attributedNumber
	keyspace          attributedNumber
	users             attributedNumber
	payloadMode       string
	payloadBytes      attributedNumber
}

type workloadPhaseV1 struct {
	name               string
	phaseOrder         attributedNumber
	operations         *attributedNumber
	duration           attributedDuration
	rate               *attributedNumber
	workers            *attributedNumber
	batchSize          *attributedNumber
	limit              *attributedNumber
	writeRate          *attributedNumber
	readRate           *attributedNumber
	writeWorkers       *attributedNumber
	readWorkers        *attributedNumber
	consistency        string
	rpcTimeout         attributedDuration
	latencySampleEvery attributedNumber
}

// WorkloadProfileV1 is the immutable schema-1.0 dataset and four-profile contract.
type WorkloadProfileV1 struct {
	id          ProfileID
	publishedAt time.Time
	dataClass   string
	dataset     datasetV1
	profiles    []workloadPhaseV1
}

// ID returns the immutable workload profile identity.
func (profile WorkloadProfileV1) ID() ProfileID {
	return profile.id
}

// PublishedAt returns the workload publication timestamp.
func (profile WorkloadProfileV1) PublishedAt() time.Time {
	return profile.publishedAt
}

// ProfileNames returns the four canonical profile names.
func (profile WorkloadProfileV1) ProfileNames() []string {
	names := make([]string, len(profile.profiles))
	for index, phase := range profile.profiles {
		names[index] = phase.name
	}
	return names
}

type volumeV1 struct {
	durable    bool
	class      string
	accessMode string
	size       attributedNumber
}

type resourcesPerNodeV1 struct {
	cpu     attributedNumber
	memory  attributedNumber
	storage attributedNumber
	network attributedNumber
}

// TopologyQuantityV1 is one validated topology quantity and its canonical unit.
type TopologyQuantityV1 struct {
	Value uint64
	Unit  string
}

// TopologyVolumeV1 is the read-only durable-volume view of a topology profile.
type TopologyVolumeV1 struct {
	Durable    bool
	Class      string
	AccessMode string
	Size       TopologyQuantityV1
}

// TopologyResourcesPerNodeV1 is the read-only resource view for one topology node.
type TopologyResourcesPerNodeV1 struct {
	CPU     TopologyQuantityV1
	Memory  TopologyQuantityV1
	Storage TopologyQuantityV1
	Network TopologyQuantityV1
}

// TopologyProfileV1 is the immutable schema-1.0 RF3 topology contract.
type TopologyProfileV1 struct {
	id                ProfileID
	publishedAt       time.Time
	environment       string
	nodeCount         attributedNumber
	replicas          attributedNumber
	replicationFactor attributedNumber
	shards            attributedNumber
	failureDomains    []string
	antiColocation    bool
	volume            volumeV1
	resourcesPerNode  resourcesPerNodeV1
}

// ID returns the immutable topology profile identity.
func (profile TopologyProfileV1) ID() ProfileID {
	return profile.id
}

// PublishedAt returns the topology publication timestamp.
func (profile TopologyProfileV1) PublishedAt() time.Time {
	return profile.publishedAt
}

// Environment returns the validated deployment environment.
func (profile TopologyProfileV1) Environment() string {
	return profile.environment
}

// NodeCount returns the approved number of topology nodes.
func (profile TopologyProfileV1) NodeCount() uint64 {
	return profile.nodeCount.value
}

// Replicas returns the approved number of database replicas.
func (profile TopologyProfileV1) Replicas() uint64 {
	return profile.replicas.value
}

// ReplicationFactor returns the approved replication factor.
func (profile TopologyProfileV1) ReplicationFactor() uint64 {
	return profile.replicationFactor.value
}

// Shards returns the approved shard count.
func (profile TopologyProfileV1) Shards() uint64 {
	return profile.shards.value
}

// FailureDomains returns a defensive copy of the three distinct domains.
func (profile TopologyProfileV1) FailureDomains() []string {
	return append([]string(nil), profile.failureDomains...)
}

// AntiColocation reports whether replicas must occupy distinct nodes.
func (profile TopologyProfileV1) AntiColocation() bool {
	return profile.antiColocation
}

// Volume returns the approved durable-volume properties.
func (profile TopologyProfileV1) Volume() TopologyVolumeV1 {
	return TopologyVolumeV1{
		Durable:    profile.volume.durable,
		Class:      profile.volume.class,
		AccessMode: profile.volume.accessMode,
		Size: TopologyQuantityV1{
			Value: profile.volume.size.value,
			Unit:  profile.volume.size.unit,
		},
	}
}

// ResourcesPerNode returns the approved resources for each topology node.
func (profile TopologyProfileV1) ResourcesPerNode() TopologyResourcesPerNodeV1 {
	return TopologyResourcesPerNodeV1{
		CPU: TopologyQuantityV1{
			Value: profile.resourcesPerNode.cpu.value,
			Unit:  profile.resourcesPerNode.cpu.unit,
		},
		Memory: TopologyQuantityV1{
			Value: profile.resourcesPerNode.memory.value,
			Unit:  profile.resourcesPerNode.memory.unit,
		},
		Storage: TopologyQuantityV1{
			Value: profile.resourcesPerNode.storage.value,
			Unit:  profile.resourcesPerNode.storage.unit,
		},
		Network: TopologyQuantityV1{
			Value: profile.resourcesPerNode.network.value,
			Unit:  profile.resourcesPerNode.network.unit,
		},
	}
}

// Bundle is one complete immutable schema-1.0 acceptance aggregate.
type Bundle struct {
	acceptance       AcceptanceContractV1
	workload         WorkloadProfileV1
	topology         TopologyProfileV1
	acceptanceDigest Digest
	workloadDigest   Digest
	topologyDigest   Digest
}

// Acceptance returns the immutable acceptance root.
func (bundle Bundle) Acceptance() AcceptanceContractV1 {
	return bundle.acceptance
}

// Workload returns the immutable workload profile.
func (bundle Bundle) Workload() WorkloadProfileV1 {
	return bundle.workload
}

// Topology returns the immutable topology profile.
func (bundle Bundle) Topology() TopologyProfileV1 {
	return bundle.topology
}

// AcceptanceDigest returns the digest of the exact acceptance-root bytes.
func (bundle Bundle) AcceptanceDigest() Digest {
	return bundle.acceptanceDigest
}

// WorkloadDigest returns the digest of the exact workload-profile bytes.
func (bundle Bundle) WorkloadDigest() Digest {
	return bundle.workloadDigest
}

// TopologyDigest returns the digest of the exact topology-profile bytes.
func (bundle Bundle) TopologyDigest() Digest {
	return bundle.topologyDigest
}
