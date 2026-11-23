package contractbuild

const (
	rf3ReviewManifestEvidenceKind = "review-manifest"
	rf3DecisionPolicyEvidenceKind = "decision-policy"
	rf3PlatformInputsEvidenceKind = "platform-inputs"
	rf3RoleApprovalEvidenceKind   = "role-approval"
	rf3DecisionPolicyDocumentType = "rf3-decision-policy"
	rf3ConfigurationApprovalKind  = "configuration-input-approval"
	rf3ProfileApprovalKind        = "rf3-profile-approval"
	rf3ApprovalDecision           = "approve"
	rf3ConfigurationInputScope    = "configuration-inputs"
	rf3ApprovalTaskID             = "TASK-RF3-P1-APPROVAL"
	rf3ProtectedEvidenceCount     = 6
)

// RF3ReviewManifestDocumentV1 identifies one artifact in the protected review set.
type RF3ReviewManifestDocumentV1 struct {
	Path   string `json:"path"`
	Name   string `json:"name"`
	SHA256 string `json:"sha256"`
}

// RF3ReviewManifestV1 is the exact review manifest accepted by the owners.
type RF3ReviewManifestV1 struct {
	Status         string                        `json:"status"`
	ProposalSHA256 string                        `json:"proposalSha256"`
	Documents      []RF3ReviewManifestDocumentV1 `json:"documents"`
	Decisions      []string                      `json:"decisions"`
}

// RF3DocumentBindingV1 binds a protected document path to exact bytes.
type RF3DocumentBindingV1 struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

// RF3DecisionPolicyProposalV1 binds the approved proposal revision.
type RF3DecisionPolicyProposalV1 struct {
	Path            string `json:"path"`
	SHA256          string `json:"sha256"`
	SelectionSHA256 string `json:"selectionSha256"`
	Revision        uint64 `json:"revision"`
}

// RF3ConfigurationInputBindingV1 binds the platform configuration decision.
type RF3ConfigurationInputBindingV1 struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
	Scope  string `json:"scope"`
}

// RF3DecisionPolicyProfileV1 identifies one approved profile.
type RF3DecisionPolicyProfileV1 struct {
	ProfileID string `json:"profileId"`
	Path      string `json:"path"`
	SHA256    string `json:"sha256"`
}

// RF3DecisionPolicyProfilesV1 contains both exact profile identities.
type RF3DecisionPolicyProfilesV1 struct {
	Workload RF3DecisionPolicyProfileV1 `json:"workload"`
	Topology RF3DecisionPolicyProfileV1 `json:"topology"`
}

// RF3DecisionPolicyVarianceV1 defines the protected variance procedure.
type RF3DecisionPolicyVarianceV1 struct {
	Method         string   `json:"method"`
	Population     string   `json:"population"`
	Aggregation    string   `json:"aggregation"`
	Metrics        []string `json:"metrics"`
	Formula        string   `json:"formula"`
	MinimumRuns    uint64   `json:"minimumRuns"`
	TolerancePPM   uint64   `json:"tolerancePpm"`
	IncompleteWhen []string `json:"incompleteWhen"`
}

// RF3DecisionPolicyCapacityV1 defines the protected bounded capacity search.
type RF3DecisionPolicyCapacityV1 struct {
	Method                       string `json:"method"`
	PeakUnit                     string `json:"peakUnit"`
	AcceptedPeak                 uint64 `json:"acceptedPeak"`
	FirstProbe                   uint64 `json:"firstProbe"`
	MaximumFloorRoundedMidpoints uint64 `json:"maximumFloorRoundedMidpoints"`
	ObservationWindowMSPerPoint  uint64 `json:"observationWindowMsPerPoint"`
	SaturationSignal             string `json:"saturationSignal"`
	Rounding                     string `json:"rounding"`
	HeadroomPPM                  uint64 `json:"headroomPpm"`
	HeadroomFormula              string `json:"headroomFormula"`
	RequiredRoundedSafeCapacity  uint64 `json:"requiredRoundedSafeCapacity"`
	PassRule                     string `json:"passRule"`
	IncompleteWhen               string `json:"incompleteWhen"`
}

// RF3DecisionPolicyDecisionsV1 contains every owner-approved decision family.
type RF3DecisionPolicyDecisionsV1 struct {
	WorkloadAndServingTargets string                      `json:"workloadAndServingTargets"`
	SchemaV1Applicability     string                      `json:"schemaV1Applicability"`
	Variance                  RF3DecisionPolicyVarianceV1 `json:"variance"`
	Capacity                  RF3DecisionPolicyCapacityV1 `json:"capacity"`
}

// RF3DecisionPolicyTimingV1 constrains when frozen-profile approval can occur.
type RF3DecisionPolicyTimingV1 struct {
	ExternalMinimum    string `json:"externalMinimum"`
	ProfilesFrozenAt   string `json:"profilesFrozenAt"`
	EffectiveNotBefore string `json:"effectiveNotBefore"`
}

// RF3DecisionPolicyV1 is the protected RF3-P1 decision policy.
type RF3DecisionPolicyV1 struct {
	DocumentType               string                         `json:"documentType"`
	SchemaVersion              string                         `json:"schemaVersion"`
	PolicyID                   string                         `json:"policyId"`
	TaskID                     string                         `json:"taskId"`
	ContractID                 string                         `json:"contractId"`
	SourceRevision             string                         `json:"sourceRevision"`
	Proposal                   RF3DecisionPolicyProposalV1    `json:"proposal"`
	SelectedPreparation        RF3DocumentBindingV1           `json:"selectedPreparation"`
	ConfigurationInputApproval RF3ConfigurationInputBindingV1 `json:"configurationInputApproval"`
	Profiles                   RF3DecisionPolicyProfilesV1    `json:"profiles"`
	Decisions                  RF3DecisionPolicyDecisionsV1   `json:"decisions"`
	RequiredRoles              []string                       `json:"requiredRoles"`
	ApprovalTiming             RF3DecisionPolicyTimingV1      `json:"approvalTiming"`
	Evidence                   string                         `json:"-"`
	Digest                     string                         `json:"-"`
}

// RF3PlatformInputValuesV1 contains the approved topology literals.
type RF3PlatformInputValuesV1 struct {
	FailureDomains []string `json:"failureDomains"`
	StorageClass   string   `json:"storageClass"`
}

// RF3OwnerRegistrationV1 records the authority used for the platform input.
type RF3OwnerRegistrationV1 struct {
	Scope                  string `json:"scope"`
	Team                   string `json:"team"`
	Assignee               string `json:"assignee"`
	RegisteredBy           string `json:"registeredBy"`
	OrganizationMembership string `json:"organizationMembership"`
	RegistrySHA256         string `json:"registrySha256"`
}

// RF3PlatformInputDecisionV1 is the protected configuration-input approval.
type RF3PlatformInputDecisionV1 struct {
	Kind                string                   `json:"kind"`
	RunID               string                   `json:"runId"`
	TaskID              string                   `json:"taskId"`
	InitiativeID        string                   `json:"initiativeId"`
	StepID              string                   `json:"stepId"`
	ProposalSHA256      string                   `json:"proposalSha256"`
	ManifestPath        string                   `json:"manifestPath"`
	ManifestSHA256      string                   `json:"manifestSha256"`
	CandidatePath       string                   `json:"candidatePath"`
	CandidateSHA256     string                   `json:"candidateSha256"`
	ConfigurationPath   string                   `json:"configurationPath"`
	ConfigurationSHA256 string                   `json:"configurationSha256"`
	InputSetSHA256      string                   `json:"inputSetSha256"`
	TaskScopeSHA256     string                   `json:"taskScopeSha256"`
	ProposalRevision    uint64                   `json:"proposalRevision"`
	Values              RF3PlatformInputValuesV1 `json:"values"`
	Scope               string                   `json:"scope"`
	IssuerRole          string                   `json:"issuerRole"`
	Issuer              string                   `json:"issuer"`
	Organization        string                   `json:"organization"`
	OrganizationID      uint64                   `json:"organizationId"`
	AuthoritySource     string                   `json:"authoritySource"`
	OwnerRegistration   RF3OwnerRegistrationV1   `json:"ownerRegistration"`
	Method              string                   `json:"method"`
	At                  string                   `json:"at"`
}

// RF3AuthorityRecordV1 is the authority subtree hashed independently per role.
type RF3AuthorityRecordV1 struct {
	IssuerID               uint64 `json:"issuerId"`
	OrganizationID         uint64 `json:"organizationId"`
	Repository             string `json:"repository"`
	RepositoryID           uint64 `json:"repositoryId"`
	InitiativeID           string `json:"initiativeId"`
	TaskID                 string `json:"taskId"`
	RegistrySHA256         string `json:"registrySha256"`
	DistinctPrincipals     bool   `json:"distinctPrincipals"`
	ManualOnly             bool   `json:"manualOnly"`
	Role                   string `json:"role"`
	Team                   string `json:"team"`
	Registration           string `json:"registration"`
	OrganizationMembership string `json:"organizationMembership"`
	OrganizationTeams      string `json:"organizationTeams"`
	TeamInventorySHA256    string `json:"teamInventorySha256"`
	Method                 string `json:"method"`
	Organization           string `json:"organization"`
	Scope                  string `json:"scope"`
	RegisteredBy           string `json:"registeredBy"`
	ManifestSHA256         string `json:"manifestSha256"`
}

// RF3ProtectedApprovalV1 is one protected role decision over frozen profiles.
type RF3ProtectedApprovalV1 struct {
	Kind                   string               `json:"kind"`
	SchemaVersion          string               `json:"schemaVersion"`
	Decision               string               `json:"decision"`
	Approver               string               `json:"approver"`
	ApprovedAt             string               `json:"approvedAt"`
	ContractID             string               `json:"contractId"`
	WorkloadProfileID      string               `json:"workloadProfileId"`
	WorkloadDigest         string               `json:"workloadDigest"`
	TopologyProfileID      string               `json:"topologyProfileId"`
	TopologyDigest         string               `json:"topologyDigest"`
	DecisionPolicyID       string               `json:"decisionPolicyId"`
	DecisionPolicyPath     string               `json:"decisionPolicyPath"`
	DecisionPolicyDigest   string               `json:"decisionPolicyDigest"`
	DecisionPolicyRecordID string               `json:"decisionPolicyRecordId"`
	Role                   string               `json:"role"`
	ApplicableDecisionIDs  []string             `json:"applicableDecisionIds"`
	RunID                  string               `json:"runId"`
	TaskID                 string               `json:"taskId"`
	InitiativeID           string               `json:"initiativeId"`
	StepID                 string               `json:"stepId"`
	TaskScopeSHA256        string               `json:"taskScopeSha256"`
	ManifestSHA256         string               `json:"manifestSha256"`
	ProposalSHA256         string               `json:"proposalSha256"`
	AuthoritySource        string               `json:"authoritySource"`
	AuthorityRecord        RF3AuthorityRecordV1 `json:"authorityRecord"`
}

// RF3AuthorizationV1 is the typed protected authorization returned to consumers.
type RF3AuthorizationV1 struct {
	ReviewManifest RF3ReviewManifestV1
	DecisionPolicy RF3DecisionPolicyV1
	PlatformInputs RF3PlatformInputDecisionV1
	Approvals      []RF3ProtectedApprovalV1
}
