package contractbuild

import "github.com/CefasDb/cefasdb/internal/core/rf3acceptance"

const (
	rf3PublicationDocumentType = "rf3-acceptance-publication"
	rf3DecisionPolicyID        = "rf3.decision-policy.rf3-p1.v1"
	rf3AuthorityRecordPointer  = "/authorityRecord"
	rf3AttemptRootField        = "defaultRule.minimumAttempts"
	rf3AttemptEncodedUnit      = "requests"
	rf3AttemptDenominator      = "logical-item-operations"
	rf3AttemptWrite            = "one logical item submitted in a batch write"
	rf3AttemptPointRead        = "one logical point read"
	rf3AttemptFailedBatch      = "charge every item in a failed batch unless per-item evidence proves otherwise"
)

// RF3ProtectedEvidence identifies one exact protected authorization record.
type RF3ProtectedEvidence struct {
	Kind             string
	Role             string
	Locator          string
	Digest           string
	AuthorityPointer string
	AuthorityDigest  string
	Expectation      RF3ProtectedEvidenceExpectation
}

// RF3ProtectedEvidenceExpectation binds protected semantics to published bytes.
type RF3ProtectedEvidenceExpectation struct {
	Decision              string
	PolicyID              string
	Approver              string
	ApprovedAt            string
	ContractID            string
	WorkloadProfileID     string
	WorkloadDigest        string
	TopologyProfileID     string
	TopologyDigest        string
	ApplicableDecisionIDs []string
	FailureDomains        []string
	StorageClass          string
	ReviewManifestDigest  string
}

// RF3PublicationArtifactV1 binds one repository artifact to exact bytes.
type RF3PublicationArtifactV1 struct {
	Path      string
	ProfileID string
	Digest    string
}

// RF3EvidenceBindingV1 binds one evidence locator to exact bytes.
type RF3EvidenceBindingV1 struct {
	Evidence string
	Digest   string
}

// RF3DecisionPolicyBindingV1 binds the applicable decision policy.
type RF3DecisionPolicyBindingV1 struct {
	PolicyID string
	Evidence string
	Digest   string
}

// RF3PublicationReviewV1 binds the review manifest and decision policy.
type RF3PublicationReviewV1 struct {
	Manifest       RF3EvidenceBindingV1
	DecisionPolicy RF3DecisionPolicyBindingV1
}

// RF3PlatformInputsV1 carries the approved infrastructure literals.
type RF3PlatformInputsV1 struct {
	Evidence       string
	Digest         string
	FailureDomains []string
	StorageClass   string
}

// RF3AttemptSemanticsV1 defines the schema-1.0 attempt denominator.
type RF3AttemptSemanticsV1 struct {
	RootField        string
	EncodedUnit      string
	Denominator      string
	WriteAttempt     string
	PointReadAttempt string
	FailedBatch      string
}

// RF3PublicationApprovalV1 binds one role approval and authority proof.
type RF3PublicationApprovalV1 struct {
	Role              string
	Approver          string
	ApprovedAt        string
	Evidence          string
	Digest            string
	AuthorityPointer  string
	AuthorityDigest   string
	ContractID        string
	RootDigest        string
	WorkloadProfileID string
	WorkloadDigest    string
	TopologyProfileID string
	TopologyDigest    string
}

// RF3PublicationV1 is the checked-in authorization record.
type RF3PublicationV1 struct {
	DocumentType  string
	SchemaVersion string
	ContractID    string
	Bundle        struct {
		Root     RF3PublicationArtifactV1
		Workload RF3PublicationArtifactV1
		Topology RF3PublicationArtifactV1
	}
	Review           RF3PublicationReviewV1
	PlatformInputs   RF3PlatformInputsV1
	AttemptSemantics RF3AttemptSemanticsV1
	Approvals        []RF3PublicationApprovalV1
	Digest           string `json:"-"`
}

// RF3ProvenanceV1 records the accountable origin of one value.
type RF3ProvenanceV1 struct {
	Kind      string
	Owner     string
	Evidence  string
	Rationale string
}

// RF3QuantityV1 is one numeric value with unit and provenance.
type RF3QuantityV1 struct {
	Value      uint64
	Unit       string
	Provenance RF3ProvenanceV1
}

// RF3ProfileReferenceV1 identifies one exact profile.
type RF3ProfileReferenceV1 struct {
	Path      string
	ProfileID string
	Digest    string
}

// RF3ReferencesV1 binds the workload and topology profiles.
type RF3ReferencesV1 struct {
	Workload RF3ProfileReferenceV1
	Topology RF3ProfileReferenceV1
}

// RF3ApprovalV1 records one role approval embedded in the root.
type RF3ApprovalV1 struct {
	Role              string
	Approver          string
	ApprovedAt        string
	ContractID        string
	WorkloadProfileID string
	WorkloadDigest    string
	TopologyProfileID string
	TopologyDigest    string
}

// RF3AcceptanceRuleV1 exposes every default serving limit.
type RF3AcceptanceRuleV1 struct {
	ObservationWindow RF3QuantityV1
	MinimumAttempts   RF3QuantityV1
	MaxErrorRate      RF3QuantityV1
	MaxP99Latency     RF3QuantityV1
	MinimumThroughput RF3QuantityV1
	MaxRecovery       RF3QuantityV1
}

// RF3ScenarioV1 exposes one resilience scenario rule.
type RF3ScenarioV1 struct {
	Name                                string
	AllowedGRPCCodes                    []string
	RequiredDoctorClasses               []string
	RequireBaselineAfterRecovery        bool
	RequireSafeRejection                bool
	MaxAcknowledgedWritesDuringNoQuorum *RF3QuantityV1
	MinimumRejectedWritesDuringNoQuorum *RF3QuantityV1
}

// RF3VarianceV1 exposes the approved variance method.
type RF3VarianceV1 struct {
	Method         string
	Population     string
	Aggregation    string
	Metrics        []string `json:"-"`
	Formula        string   `json:"-"`
	MinimumRuns    RF3QuantityV1
	Tolerance      RF3QuantityV1
	IncompleteWhen []string `json:"-"`
}

// RF3CapacityMethodV1 exposes the approved capacity-search method.
type RF3CapacityMethodV1 struct {
	Status                       string
	Method                       string
	PeakUnit                     string
	AcceptedPeak                 uint64 `json:"-"`
	FirstProbe                   uint64 `json:"-"`
	MaximumFloorRoundedMidpoints uint64 `json:"-"`
	SaturationSignal             string
	Rounding                     string
	ObservationWindow            RF3QuantityV1
	Headroom                     RF3QuantityV1
	HeadroomFormula              string `json:"-"`
	RequiredRoundedSafeCapacity  uint64 `json:"-"`
	PassRule                     string `json:"-"`
	IncompleteWhen               string `json:"-"`
}

// RF3AcceptanceDocumentV1 is the complete typed acceptance-root projection.
type RF3AcceptanceDocumentV1 struct {
	DocumentType   string
	SchemaVersion  string
	ContractID     string
	ApprovedAt     string
	References     RF3ReferencesV1
	Approvals      []RF3ApprovalV1
	DefaultRule    RF3AcceptanceRuleV1
	Scenarios      []RF3ScenarioV1
	Variance       RF3VarianceV1
	CapacityMethod RF3CapacityMethodV1
}

// RF3PayloadV1 defines the synthetic payload.
type RF3PayloadV1 struct {
	Mode  string
	Bytes RF3QuantityV1
}

// RF3DatasetV1 exposes every synthetic dataset value.
type RF3DatasetV1 struct {
	TableNameTemplate string
	ItemCount         RF3QuantityV1
	Keyspace          RF3QuantityV1
	Users             RF3QuantityV1
	Payload           RF3PayloadV1
}

// RF3WorkloadPhaseV1 exposes one workload phase.
type RF3WorkloadPhaseV1 struct {
	Name               string
	PhaseOrder         RF3QuantityV1
	Operations         *RF3QuantityV1
	Duration           RF3QuantityV1
	Rate               *RF3QuantityV1
	Workers            *RF3QuantityV1
	BatchSize          *RF3QuantityV1
	Limit              *RF3QuantityV1
	WriteRate          *RF3QuantityV1
	ReadRate           *RF3QuantityV1
	WriteWorkers       *RF3QuantityV1
	ReadWorkers        *RF3QuantityV1
	Consistency        string
	RPCTimeout         RF3QuantityV1
	LatencySampleEvery RF3QuantityV1
}

// RF3WorkloadDocumentV1 is the complete typed workload projection.
type RF3WorkloadDocumentV1 struct {
	DocumentType  string
	SchemaVersion string
	ProfileID     string
	PublishedAt   string
	DataClass     string
	Dataset       RF3DatasetV1
	Profiles      []RF3WorkloadPhaseV1
}

// RF3PublishedAcceptanceV1 is one authorized bundle and its consumer contract.
type RF3PublishedAcceptanceV1 struct {
	Bundle        rf3acceptance.Bundle
	Publication   RF3PublicationV1
	Acceptance    RF3AcceptanceDocumentV1
	Workload      RF3WorkloadDocumentV1
	Authorization RF3AuthorizationV1
}
