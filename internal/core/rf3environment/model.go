// Package rf3environment validates immutable RF3 qualification environment contracts.
package rf3environment

import (
	"fmt"
	"time"

	"github.com/CefasDb/cefasdb/internal/core/rf3acceptance"
)

const schemaVersionV1 = "1.0"

// P1BindingV1 binds an environment to the exact approved RF3-P1 bytes.
type P1BindingV1 struct {
	AcceptanceContractID string
	AcceptanceDigest     rf3acceptance.Digest
	WorkloadProfileID    string
	WorkloadDigest       rf3acceptance.Digest
	TopologyProfileID    string
	TopologyDigest       rf3acceptance.Digest
}

// EnvironmentIdentityV1 identifies immutable environment-manifest bytes.
type EnvironmentIdentityV1 struct {
	ID          string
	PublishedAt time.Time
	Digest      rf3acceptance.Digest
}

// ProviderContractV1 pins the non-production provider and cluster identity.
type ProviderContractV1 struct {
	Name              string
	AccountID         string
	ClusterID         string
	KubernetesContext string
	ProviderVersion   string
	KubernetesVersion string
	NonProduction     bool
}

// NodeContractV1 pins one approved node to one failure domain.
type NodeContractV1 struct {
	NodeID        string
	ProviderID    string
	InstanceType  string
	ImageID       string
	FailureDomain string
}

// TopologyContractV1 mirrors the validated RF3-P1 topology.
type TopologyContractV1 struct {
	FailureDomainLabel string
	FailureDomains     []string
	NodeCount          uint64
	Replicas           uint64
	ReplicationFactor  uint64
	Shards             uint64
	AntiColocation     bool
	Nodes              []NodeContractV1
	ResourcesPerNode   rf3acceptance.TopologyResourcesPerNodeV1
}

// DeploymentContractV1 pins chart and image identities.
type DeploymentContractV1 struct {
	NamespacePrefix string
	ReleasePrefix   string
	ChartName       string
	ChartVersion    string
	ChartDigest     rf3acceptance.Digest
	ImageReference  string
	ImageDigest     rf3acceptance.Digest
}

// StorageContractV1 pins durable-volume provisioning and reuse policy.
type StorageContractV1 struct {
	Class             string
	Provisioner       string
	BindingMode       string
	ReclaimPolicy     string
	AccessMode        string
	VolumeMode        string
	CapacityGiB       uint64
	VolumeReusePolicy string
}

// EnvironmentSpecificationV1 contains provider, topology, deployment, and storage contracts.
type EnvironmentSpecificationV1 struct {
	Provider   ProviderContractV1
	Topology   TopologyContractV1
	Deployment DeploymentContractV1
	Storage    StorageContractV1
}

// AdmissionContractV1 pins Lease, RBAC, and short-lived credential metadata.
type AdmissionContractV1 struct {
	ControlNamespace             string
	LeaseName                    string
	ServiceAccount               string
	Role                         string
	RBACVerbs                    []string
	RBACResources                []string
	CredentialIssuer             string
	CredentialAudience           string
	MaxCredentialLifetimeSeconds uint64
	RevocationOwner              string
}

// RecoveryContractV1 identifies the reviewed IaC and recovery authority.
type RecoveryContractV1 struct {
	IaCDigest   rf3acceptance.Digest
	OperationID string
	Owner       string
}

// EnvironmentManifestV1 is one validated schema-1.0 environment data plug-in.
type EnvironmentManifestV1 struct {
	identity      EnvironmentIdentityV1
	p1            P1BindingV1
	specification EnvironmentSpecificationV1
	admission     AdmissionContractV1
	recovery      RecoveryContractV1
	acceptance    rf3acceptance.Bundle
}

// Identity returns immutable manifest identity.
func (manifest EnvironmentManifestV1) Identity() EnvironmentIdentityV1 {
	return manifest.identity
}

// P1Binding returns the exact RF3-P1 byte binding.
func (manifest EnvironmentManifestV1) P1Binding() P1BindingV1 {
	return manifest.p1
}

// Specification returns a defensive environment specification.
func (manifest EnvironmentManifestV1) Specification() EnvironmentSpecificationV1 {
	specification := manifest.specification
	specification.Topology.FailureDomains = cloneStrings(specification.Topology.FailureDomains)
	specification.Topology.Nodes = cloneNodeContracts(specification.Topology.Nodes)
	return specification
}

// Admission returns defensive Lease, RBAC, and credential metadata.
func (manifest EnvironmentManifestV1) Admission() AdmissionContractV1 {
	admission := manifest.admission
	admission.RBACVerbs = cloneStrings(admission.RBACVerbs)
	admission.RBACResources = cloneStrings(admission.RBACResources)
	return admission
}

// Recovery returns the immutable recovery contract.
func (manifest EnvironmentManifestV1) Recovery() RecoveryContractV1 {
	return manifest.recovery
}

// Contract returns the complete cross-bound environment contract.
func (manifest EnvironmentManifestV1) Contract() EnvironmentContractV1 {
	return EnvironmentContractV1{manifest: manifest, acceptance: manifest.acceptance}
}

// EnvironmentContractV1 binds one manifest to one decoded RF3-P1 aggregate.
type EnvironmentContractV1 struct {
	manifest   EnvironmentManifestV1
	acceptance rf3acceptance.Bundle
}

// Manifest returns the immutable environment manifest.
func (contract EnvironmentContractV1) Manifest() EnvironmentManifestV1 {
	return contract.manifest
}

// Acceptance returns the exact decoded RF3-P1 aggregate.
func (contract EnvironmentContractV1) Acceptance() rf3acceptance.Bundle {
	return contract.acceptance
}

// Digest returns the digest of the exact manifest bytes.
func (contract EnvironmentContractV1) Digest() rf3acceptance.Digest {
	return contract.manifest.identity.Digest
}

// ArtifactRefV1 identifies one immutable, non-secret evidence artifact.
type ArtifactRefV1 struct {
	Path   string
	Digest rf3acceptance.Digest
	Bytes  uint64
}

// ReceiptIdentityV1 identifies one immutable create or recreate observation.
type ReceiptIdentityV1 struct {
	ID         string
	Cycle      string
	CapturedAt time.Time
	Digest     rf3acceptance.Digest
}

// ReceiptContractBindingV1 binds a receipt to its manifest and RF3-P1 bytes.
type ReceiptContractBindingV1 struct {
	ManifestDigest rf3acceptance.Digest
	P1             P1BindingV1
}

// PinnedEnvironmentV1 records the observed pinned provider and deployment.
type PinnedEnvironmentV1 struct {
	Provider   ProviderContractV1
	Deployment DeploymentContractV1
	Namespace  string
	Release    string
}

// ReceiptAdmissionV1 records observed Lease and credential identities.
type ReceiptAdmissionV1 struct {
	ControlNamespace    string
	LeaseName           string
	HolderIdentity      string
	ResourceVersion     string
	ServiceAccount      string
	Role                string
	CredentialIssuer    string
	CredentialAudience  string
	CredentialIssuedAt  time.Time
	CredentialExpiresAt time.Time
}

// NodeObservationV1 records one observed node identity.
type NodeObservationV1 struct {
	NodeID        string
	NodeUID       string
	ProviderID    string
	InstanceType  string
	ImageID       string
	FailureDomain string
}

// ReplicaObservationV1 records one Pod-to-node and Pod-to-volume placement.
type ReplicaObservationV1 struct {
	Ordinal       uint64
	PodName       string
	PodUID        string
	NodeID        string
	FailureDomain string
	PVCName       string
	PVCUID        string
	PVName        string
	PVUID         string
}

// VolumeObservationV1 records one observed PVC/PV policy and identity.
type VolumeObservationV1 struct {
	PVCName       string
	PVCUID        string
	PVName        string
	PVUID         string
	StorageClass  string
	Provisioner   string
	BindingMode   string
	ReclaimPolicy string
	AccessMode    string
	VolumeMode    string
	CapacityGiB   uint64
	FailureDomain string
}

// EnvironmentObservationV1 contains defensive observed topology and storage records.
type EnvironmentObservationV1 struct {
	Nodes    []NodeObservationV1
	Replicas []ReplicaObservationV1
	Volumes  []VolumeObservationV1
}

// DoctorEvidenceV1 binds a complete healthy doctor report.
type DoctorEvidenceV1 struct {
	Classification string
	Complete       bool
	Artifact       ArtifactRefV1
}

// BaselineEvidenceV1 binds the approved functional healthy baseline.
type BaselineEvidenceV1 struct {
	Passed   bool
	Artifact ArtifactRefV1
}

// ReceiptEvidenceV1 contains the doctor, baseline, and artifact inventory.
type ReceiptEvidenceV1 struct {
	Doctor    DoctorEvidenceV1
	Baseline  BaselineEvidenceV1
	Artifacts []ArtifactRefV1
}

// CleanupReceiptV1 records the lifecycle state after observation.
type CleanupReceiptV1 struct {
	State                string
	CredentialsRevoked   bool
	EnvironmentDestroyed bool
	ResidualVolumes      uint64
}

// ObservedEnvironmentReceiptV1 is one validated immutable environment observation.
type ObservedEnvironmentReceiptV1 struct {
	identity    ReceiptIdentityV1
	binding     ReceiptContractBindingV1
	pinned      PinnedEnvironmentV1
	admission   ReceiptAdmissionV1
	observation EnvironmentObservationV1
	evidence    ReceiptEvidenceV1
	cleanup     CleanupReceiptV1
}

// Identity returns immutable receipt identity.
func (receipt ObservedEnvironmentReceiptV1) Identity() ReceiptIdentityV1 {
	return receipt.identity
}

// ContractBinding returns the manifest and RF3-P1 binding.
func (receipt ObservedEnvironmentReceiptV1) ContractBinding() ReceiptContractBindingV1 {
	return receipt.binding
}

// PinnedEnvironment returns the observed pinned environment identity.
func (receipt ObservedEnvironmentReceiptV1) PinnedEnvironment() PinnedEnvironmentV1 {
	return receipt.pinned
}

// Admission returns the observed Lease and credential identities.
func (receipt ObservedEnvironmentReceiptV1) Admission() ReceiptAdmissionV1 {
	return receipt.admission
}

// Observation returns defensive node, replica, and volume observations.
func (receipt ObservedEnvironmentReceiptV1) Observation() EnvironmentObservationV1 {
	return cloneObservation(receipt.observation)
}

// Evidence returns a defensive evidence inventory.
func (receipt ObservedEnvironmentReceiptV1) Evidence() ReceiptEvidenceV1 {
	evidence := receipt.evidence
	evidence.Artifacts = append([]ArtifactRefV1(nil), evidence.Artifacts...)
	return evidence
}

// Cleanup returns the observed cleanup state.
func (receipt ObservedEnvironmentReceiptV1) Cleanup() CleanupReceiptV1 {
	return receipt.cleanup
}

// ReceiptCycleIdentitiesV1 keeps create and recreate identities separate.
type ReceiptCycleIdentitiesV1 struct {
	CreateReceiptID   string
	RecreateReceiptID string
	CreateNodeUIDs    []string
	RecreateNodeUIDs  []string
	CreatePVCUIDs     []string
	RecreatePVCUIDs   []string
	CreatePVUIDs      []string
	RecreatePVUIDs    []string
}

// ReceiptComparisonV1 records pinned equality and forbidden volume reuse.
type ReceiptComparisonV1 struct {
	contractDigest rf3acceptance.Digest
	passed         bool
	cycles         ReceiptCycleIdentitiesV1
	reusedPVCUIDs  []string
	reusedPVUIDs   []string
}

// ContractDigest returns the shared immutable manifest digest.
func (comparison ReceiptComparisonV1) ContractDigest() rf3acceptance.Digest {
	return comparison.contractDigest
}

// Passed reports whether both cycles match and reuse no PVC or PV.
func (comparison ReceiptComparisonV1) Passed() bool {
	return comparison.passed
}

// Cycles returns defensive create and recreate identities.
func (comparison ReceiptComparisonV1) Cycles() ReceiptCycleIdentitiesV1 {
	cycles := comparison.cycles
	cycles.CreateNodeUIDs = cloneStrings(cycles.CreateNodeUIDs)
	cycles.RecreateNodeUIDs = cloneStrings(cycles.RecreateNodeUIDs)
	cycles.CreatePVCUIDs = cloneStrings(cycles.CreatePVCUIDs)
	cycles.RecreatePVCUIDs = cloneStrings(cycles.RecreatePVCUIDs)
	cycles.CreatePVUIDs = cloneStrings(cycles.CreatePVUIDs)
	cycles.RecreatePVUIDs = cloneStrings(cycles.RecreatePVUIDs)
	return cycles
}

// ReusedPVCUIDs returns PVC identities observed in both cycles.
func (comparison ReceiptComparisonV1) ReusedPVCUIDs() []string {
	return cloneStrings(comparison.reusedPVCUIDs)
}

// ReusedPVUIDs returns PV identities observed in both cycles.
func (comparison ReceiptComparisonV1) ReusedPVUIDs() []string {
	return cloneStrings(comparison.reusedPVUIDs)
}

// QuarantineReason identifies why an environment cannot admit another run.
type QuarantineReason string

const (
	QuarantineLeaseLost QuarantineReason = "lease-lost"
	// #nosec G101 -- this public enum label contains no credential material.
	QuarantineCredentialStateUnknown QuarantineReason = "credential-state-unknown"
	QuarantineCleanupFailed          QuarantineReason = "cleanup-failed"
	QuarantineEnvironmentMismatch    QuarantineReason = "environment-mismatch"
	QuarantineVolumeReuse            QuarantineReason = "volume-reuse"
)

func (reason QuarantineReason) valid() bool {
	switch reason {
	case QuarantineLeaseLost,
		QuarantineCredentialStateUnknown,
		QuarantineCleanupFailed,
		QuarantineEnvironmentMismatch,
		QuarantineVolumeReuse:
		return true
	default:
		return false
	}
}

// RecoveryReceiptInputV1 contains evidence used to clear one quarantine.
type RecoveryReceiptInputV1 struct {
	ID                   string
	EnvironmentID        string
	Reason               QuarantineReason
	RecoveredAt          time.Time
	OldRunTerminated     bool
	CredentialsRevoked   bool
	ProviderQuiescent    bool
	BaselineRestored     bool
	EnvironmentDestroyed bool
	Evidence             []ArtifactRefV1
}

// RecoveryProofV1 contains the immutable quarantine-clearance facts.
type RecoveryProofV1 struct {
	OldRunTerminated     bool
	CredentialsRevoked   bool
	ProviderQuiescent    bool
	BaselineRestored     bool
	EnvironmentDestroyed bool
	Evidence             []ArtifactRefV1
}

// RecoveryReceiptV1 proves that a quarantined environment may be reviewed for release.
type RecoveryReceiptV1 struct {
	id            string
	environmentID string
	reason        QuarantineReason
	recoveredAt   time.Time
	proof         RecoveryProofV1
}

// NewRecoveryReceiptV1 validates and constructs a recovery receipt.
func NewRecoveryReceiptV1(input RecoveryReceiptInputV1) (RecoveryReceiptV1, error) {
	if err := validateIdentity("recovery receipt id", input.ID); err != nil {
		return RecoveryReceiptV1{}, err
	}
	if err := validateIdentity("environment id", input.EnvironmentID); err != nil {
		return RecoveryReceiptV1{}, err
	}
	if !input.Reason.valid() {
		return RecoveryReceiptV1{}, fmt.Errorf("quarantine reason %q is unsupported", input.Reason)
	}
	if input.RecoveredAt.IsZero() || input.RecoveredAt.Location() != time.UTC {
		return RecoveryReceiptV1{}, fmt.Errorf("recoveredAt must be a nonzero UTC timestamp")
	}
	if !input.OldRunTerminated || !input.CredentialsRevoked || !input.ProviderQuiescent {
		return RecoveryReceiptV1{}, fmt.Errorf("recovery must prove old-run termination, credential revocation, and provider quiescence")
	}
	if input.BaselineRestored == input.EnvironmentDestroyed {
		return RecoveryReceiptV1{}, fmt.Errorf("recovery must prove exactly one of baseline restoration or environment destruction")
	}
	if len(input.Evidence) == 0 {
		return RecoveryReceiptV1{}, fmt.Errorf("recovery evidence is required")
	}
	for index := range input.Evidence {
		if err := validateArtifact(input.Evidence[index]); err != nil {
			return RecoveryReceiptV1{}, fmt.Errorf("recovery evidence[%d]: %w", index, err)
		}
	}
	return RecoveryReceiptV1{
		id:            input.ID,
		environmentID: input.EnvironmentID,
		reason:        input.Reason,
		recoveredAt:   input.RecoveredAt,
		proof: RecoveryProofV1{
			OldRunTerminated:     input.OldRunTerminated,
			CredentialsRevoked:   input.CredentialsRevoked,
			ProviderQuiescent:    input.ProviderQuiescent,
			BaselineRestored:     input.BaselineRestored,
			EnvironmentDestroyed: input.EnvironmentDestroyed,
			Evidence:             append([]ArtifactRefV1(nil), input.Evidence...),
		},
	}, nil
}

// Identity returns the recovery receipt identity and environment identity.
func (receipt RecoveryReceiptV1) Identity() (string, string, time.Time) {
	return receipt.id, receipt.environmentID, receipt.recoveredAt
}

// Reason returns the quarantine reason cleared by this receipt.
func (receipt RecoveryReceiptV1) Reason() QuarantineReason {
	return receipt.reason
}

// Proof returns a defensive recovery proof.
func (receipt RecoveryReceiptV1) Proof() RecoveryProofV1 {
	proof := receipt.proof
	proof.Evidence = append([]ArtifactRefV1(nil), proof.Evidence...)
	return proof
}

func cloneStrings(values []string) []string {
	return append([]string(nil), values...)
}

func cloneNodeContracts(values []NodeContractV1) []NodeContractV1 {
	return append([]NodeContractV1(nil), values...)
}

func cloneObservation(observation EnvironmentObservationV1) EnvironmentObservationV1 {
	observation.Nodes = append([]NodeObservationV1(nil), observation.Nodes...)
	observation.Replicas = append([]ReplicaObservationV1(nil), observation.Replicas...)
	observation.Volumes = append([]VolumeObservationV1(nil), observation.Volumes...)
	return observation
}
