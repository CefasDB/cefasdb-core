package rf3environment

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/CefasDb/cefasdb/internal/core/rf3acceptance"
)

const (
	maxDocumentBytes = 1 << 20
	maxJSONDepth     = 20
	maxJSONMembers   = 256
	maxJSONItems     = 256
	maxStringBytes   = 4096
)

var (
	identityPattern       = regexp.MustCompile("^[a-z][a-z0-9-]*(?:[.][a-z][a-z0-9-]*)+$")
	tokenPattern          = regexp.MustCompile("^[A-Za-z0-9][A-Za-z0-9._:/@+-]*$")
	versionPattern        = regexp.MustCompile("^v?[0-9]+[.][0-9]+[.][0-9]+(?:[-+][0-9A-Za-z.-]+)?$")
	kubernetesNamePattern = regexp.MustCompile("^[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?$")
	productionPattern     = regexp.MustCompile("(?i)(^|[-_.:/])prod(?:uction)?($|[-_.:/])")
	secretKeyPattern      = regexp.MustCompile("(?i)(password|secret|token|private.?key|api.?key|authorization|kubeconfig)")
	longSecretPattern     = regexp.MustCompile("^[A-Za-z0-9+/=_-]{128,}$")
)

var mutableAliases = map[string]struct{}{
	"latest": {}, "current": {}, "stable": {}, "head": {}, "main": {},
	"master": {}, "nightly": {}, "snapshot": {}, "edge": {},
}

type p1BindingJSON struct {
	AcceptanceContractID string
	AcceptanceDigest     string
	WorkloadProfileID    string
	WorkloadDigest       string
	TopologyProfileID    string
	TopologyDigest       string
}

type providerJSON struct {
	Name              string
	AccountID         string
	ClusterID         string
	KubernetesContext string
	ProviderVersion   string
	KubernetesVersion string
	NonProduction     *bool
}

type nodeContractJSON struct {
	NodeID        string
	ProviderID    string
	InstanceType  string
	ImageID       string
	FailureDomain string
}

type topologyContractJSON struct {
	FailureDomainLabel string
	FailureDomains     []string
	NodeCount          *uint64
	Replicas           *uint64
	ReplicationFactor  *uint64
	Shards             *uint64
	AntiColocation     *bool
	Nodes              []nodeContractJSON
	ResourcesPerNode   *resourcesJSON
}

type resourcesJSON struct {
	CPUCores        *uint64
	MemoryGiB       *uint64
	StorageGiB      *uint64
	NetworkMegabits *uint64
}

type deploymentContractJSON struct {
	NamespacePrefix string
	ReleasePrefix   string
	ChartName       string
	ChartVersion    string
	ChartDigest     string
	ImageReference  string
	ImageDigest     string
}

type storageJSON struct {
	Class             string
	Provisioner       string
	BindingMode       string
	ReclaimPolicy     string
	AccessMode        string
	VolumeMode        string
	CapacityGiB       *uint64
	VolumeReusePolicy string
}

type admissionContractJSON struct {
	ControlNamespace             string
	LeaseName                    string
	ServiceAccount               string
	Role                         string
	RBACVerbs                    []string
	RBACResources                []string
	CredentialIssuer             string
	CredentialAudience           string
	MaxCredentialLifetimeSeconds *uint64
	RevocationOwner              string
}

type recoveryContractJSON struct {
	IaCDigest   string
	OperationID string
	Owner       string
}

type manifestJSON struct {
	DocumentType  string
	SchemaVersion string
	EnvironmentID string
	PublishedAt   string
	NonProduction *bool
	P1            *p1BindingJSON
	Provider      *providerJSON
	Topology      *topologyContractJSON
	Deployment    *deploymentContractJSON
	Storage       *storageJSON
	Admission     *admissionContractJSON
	Recovery      *recoveryContractJSON
}

type artifactJSON struct {
	Path   string
	Digest string
	Bytes  *uint64
}

type deploymentReceiptJSON struct {
	Namespace      string
	Release        string
	ChartName      string
	ChartVersion   string
	ChartDigest    string
	ImageReference string
	ImageDigest    string
}

type admissionReceiptJSON struct {
	ControlNamespace    string
	LeaseName           string
	HolderIdentity      string
	ResourceVersion     string
	ServiceAccount      string
	Role                string
	CredentialIssuer    string
	CredentialAudience  string
	CredentialIssuedAt  string
	CredentialExpiresAt string
}

type nodeObservationJSON struct {
	NodeID        string
	NodeUID       string
	ProviderID    string
	InstanceType  string
	ImageID       string
	FailureDomain string
}

type replicaObservationJSON struct {
	Ordinal       *uint64
	PodName       string
	PodUID        string
	NodeID        string
	FailureDomain string
	PVCName       string
	PVCUID        string
	PVName        string
	PVUID         string
}

type volumeObservationJSON struct {
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
	CapacityGiB   *uint64
	FailureDomain string
}

type doctorJSON struct {
	Classification string
	Complete       *bool
	Artifact       *artifactJSON
}

type baselineJSON struct {
	Passed   *bool
	Artifact *artifactJSON
}

type cleanupJSON struct {
	State                string
	CredentialsRevoked   *bool
	EnvironmentDestroyed *bool
	ResidualVolumes      *uint64
}

type receiptJSON struct {
	DocumentType   string
	SchemaVersion  string
	ReceiptID      string
	Cycle          string
	CapturedAt     string
	EnvironmentID  string
	ManifestDigest string
	P1             *p1BindingJSON
	Provider       *providerJSON
	Deployment     *deploymentReceiptJSON
	Admission      *admissionReceiptJSON
	Nodes          []nodeObservationJSON
	Replicas       []replicaObservationJSON
	Volumes        []volumeObservationJSON
	Doctor         *doctorJSON
	Baseline       *baselineJSON
	Cleanup        *cleanupJSON
	Artifacts      []artifactJSON
}

// DecodeManifestV1 strictly decodes one schema-1.0 environment manifest.
func DecodeManifestV1(
	source rf3acceptance.Source,
	acceptance rf3acceptance.Bundle,
) (EnvironmentManifestV1, error) {
	var document manifestJSON
	if err := decodeSource(source, "environment manifest", &document); err != nil {
		return EnvironmentManifestV1{}, err
	}
	manifest, err := buildManifest(document, source, acceptance)
	if err != nil {
		return EnvironmentManifestV1{}, fmt.Errorf("environment manifest %q: %w", source.Path(), err)
	}
	return manifest, nil
}

// DecodeReceiptV1 strictly decodes one schema-1.0 observed environment receipt.
func DecodeReceiptV1(
	source rf3acceptance.Source,
	contract EnvironmentContractV1,
) (ObservedEnvironmentReceiptV1, error) {
	var document receiptJSON
	if err := decodeSource(source, "environment receipt", &document); err != nil {
		return ObservedEnvironmentReceiptV1{}, err
	}
	receipt, err := buildReceipt(document, source, contract)
	if err != nil {
		return ObservedEnvironmentReceiptV1{}, fmt.Errorf("environment receipt %q: %w", source.Path(), err)
	}
	return receipt, nil
}

// CompareReceiptsV1 compares create and recreate observations without merging them.
func CompareReceiptsV1(
	create ObservedEnvironmentReceiptV1,
	recreate ObservedEnvironmentReceiptV1,
) (ReceiptComparisonV1, error) {
	if create.identity.ID == "" || recreate.identity.ID == "" {
		return ReceiptComparisonV1{}, fmt.Errorf("both receipts are required")
	}
	if create.identity.Cycle != "create" || recreate.identity.Cycle != "recreate" {
		return ReceiptComparisonV1{}, fmt.Errorf("receipt cycles must be create then recreate")
	}
	if create.binding != recreate.binding {
		return ReceiptComparisonV1{}, fmt.Errorf("receipts do not bind the same manifest and RF3-P1 bytes")
	}
	if create.pinned.Provider != recreate.pinned.Provider ||
		create.pinned.Deployment != recreate.pinned.Deployment {
		return ReceiptComparisonV1{}, fmt.Errorf("receipts do not share pinned provider, Kubernetes, chart, and image fields")
	}

	createNodes := nodeUIDs(create.observation.Nodes)
	recreateNodes := nodeUIDs(recreate.observation.Nodes)
	createPVCs, createPVs := volumeUIDs(create.observation.Volumes)
	recreatePVCs, recreatePVs := volumeUIDs(recreate.observation.Volumes)
	reusedPVCs := intersect(createPVCs, recreatePVCs)
	reusedPVs := intersect(createPVs, recreatePVs)
	return ReceiptComparisonV1{
		contractDigest: create.binding.ManifestDigest,
		passed:         len(reusedPVCs) == 0 && len(reusedPVs) == 0,
		cycles: ReceiptCycleIdentitiesV1{
			CreateReceiptID:   create.identity.ID,
			RecreateReceiptID: recreate.identity.ID,
			CreateNodeUIDs:    createNodes,
			RecreateNodeUIDs:  recreateNodes,
			CreatePVCUIDs:     createPVCs,
			RecreatePVCUIDs:   recreatePVCs,
			CreatePVUIDs:      createPVs,
			RecreatePVUIDs:    recreatePVs,
		},
		reusedPVCUIDs: reusedPVCs,
		reusedPVUIDs:  reusedPVs,
	}, nil
}

func decodeSource[T any](source rf3acceptance.Source, role string, target *T) error {
	data := source.Bytes()
	if source.Path() == "" || len(data) == 0 {
		return fmt.Errorf("%s source is required", role)
	}
	if len(data) > maxDocumentBytes {
		return fmt.Errorf("%s %q exceeds %d bytes", role, source.Path(), maxDocumentBytes)
	}
	if _, err := rf3acceptance.NewSource(source.Path(), data); err != nil {
		return fmt.Errorf("%s source: %w", role, err)
	}
	if err := validateJSON(data); err != nil {
		return fmt.Errorf("%s %q: %w", role, source.Path(), err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("%s %q: decode JSON: %w", role, source.Path(), err)
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err != nil {
			return fmt.Errorf("%s %q: decode trailing JSON: %w", role, source.Path(), err)
		}
		return fmt.Errorf("%s %q: trailing JSON document", role, source.Path())
	}
	return nil
}

func validateJSON(data []byte) error {
	if !utf8.Valid(data) {
		return fmt.Errorf("JSON document is not valid UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("decode JSON token: %w", err)
	}
	if token != json.Delim('{') {
		return fmt.Errorf("top-level JSON value must be an object")
	}
	if err := scanObject(decoder, 1); err != nil {
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

func scanObject(decoder *json.Decoder, depth int) error {
	if depth > maxJSONDepth {
		return fmt.Errorf("JSON nesting exceeds depth %d", maxJSONDepth)
	}
	seen := make(map[string]struct{})
	members := 0
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("decode JSON key: %w", err)
		}
		key, ok := token.(string)
		if !ok || key == "" || len(key) > maxStringBytes {
			return fmt.Errorf("JSON object key is invalid")
		}
		if secretKeyPattern.MatchString(key) {
			return fmt.Errorf("secret-shaped JSON key %q is forbidden", key)
		}
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("duplicate JSON key %q", key)
		}
		seen[key] = struct{}{}
		members++
		if members > maxJSONMembers {
			return fmt.Errorf("JSON object exceeds %d members", maxJSONMembers)
		}
		if err := scanValue(decoder, depth+1); err != nil {
			return err
		}
	}
	return expectDelimiter(decoder, '}')
}

func scanValue(decoder *json.Decoder, depth int) error {
	if depth > maxJSONDepth {
		return fmt.Errorf("JSON nesting exceeds depth %d", maxJSONDepth)
	}
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("decode JSON value: %w", err)
	}
	switch value := token.(type) {
	case json.Delim:
		switch value {
		case '{':
			return scanObject(decoder, depth)
		case '[':
			items := 0
			for decoder.More() {
				items++
				if items > maxJSONItems {
					return fmt.Errorf("JSON array exceeds %d items", maxJSONItems)
				}
				if err := scanValue(decoder, depth+1); err != nil {
					return err
				}
			}
			return expectDelimiter(decoder, ']')
		default:
			return fmt.Errorf("unexpected JSON delimiter %q", value)
		}
	case string:
		if len(value) > maxStringBytes {
			return fmt.Errorf("JSON string exceeds %d bytes", maxStringBytes)
		}
		if looksSecret(value) {
			return fmt.Errorf("secret-shaped JSON string is forbidden")
		}
	case json.Number:
		if len(value.String()) > 32 {
			return fmt.Errorf("JSON number exceeds 32 bytes")
		}
	}
	return nil
}

func expectDelimiter(decoder *json.Decoder, expected json.Delim) error {
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("decode JSON closing delimiter: %w", err)
	}
	if token != expected {
		return fmt.Errorf("JSON closing delimiter %v does not match %q", token, expected)
	}
	return nil
}

func buildManifest(
	document manifestJSON,
	source rf3acceptance.Source,
	acceptance rf3acceptance.Bundle,
) (EnvironmentManifestV1, error) {
	if err := validateHeader(document.DocumentType, "rf3-environment-manifest", document.SchemaVersion); err != nil {
		return EnvironmentManifestV1{}, err
	}
	if err := validateIdentity("environment id", document.EnvironmentID); err != nil {
		return EnvironmentManifestV1{}, err
	}
	if err := rejectProduction("environment id", document.EnvironmentID); err != nil {
		return EnvironmentManifestV1{}, err
	}
	publishedAt, err := parseTimestamp("publishedAt", document.PublishedAt)
	if err != nil {
		return EnvironmentManifestV1{}, err
	}
	if document.NonProduction == nil || !*document.NonProduction {
		return EnvironmentManifestV1{}, fmt.Errorf("nonProduction must be true")
	}
	p1, err := decodeP1Binding(document.P1, acceptance)
	if err != nil {
		return EnvironmentManifestV1{}, err
	}
	provider, err := decodeProvider(document.Provider)
	if err != nil {
		return EnvironmentManifestV1{}, err
	}
	topology, err := decodeTopology(document.Topology, acceptance.Topology())
	if err != nil {
		return EnvironmentManifestV1{}, err
	}
	deployment, err := decodeDeployment(document.Deployment)
	if err != nil {
		return EnvironmentManifestV1{}, err
	}
	storage, err := decodeStorage(document.Storage, acceptance.Topology().Volume())
	if err != nil {
		return EnvironmentManifestV1{}, err
	}
	admission, err := decodeAdmission(document.Admission)
	if err != nil {
		return EnvironmentManifestV1{}, err
	}
	recovery, err := decodeRecovery(document.Recovery)
	if err != nil {
		return EnvironmentManifestV1{}, err
	}
	return EnvironmentManifestV1{
		identity: EnvironmentIdentityV1{
			ID: document.EnvironmentID, PublishedAt: publishedAt, Digest: source.Digest(),
		},
		p1: p1,
		specification: EnvironmentSpecificationV1{
			Provider: provider, Topology: topology, Deployment: deployment, Storage: storage,
		},
		admission:  admission,
		recovery:   recovery,
		acceptance: acceptance,
	}, nil
}

func decodeP1Binding(document *p1BindingJSON, acceptance rf3acceptance.Bundle) (P1BindingV1, error) {
	if document == nil {
		return P1BindingV1{}, fmt.Errorf("p1 is required")
	}
	acceptanceDigest, err := parseDigest("p1 acceptanceDigest", document.AcceptanceDigest)
	if err != nil {
		return P1BindingV1{}, err
	}
	workloadDigest, err := parseDigest("p1 workloadDigest", document.WorkloadDigest)
	if err != nil {
		return P1BindingV1{}, err
	}
	topologyDigest, err := parseDigest("p1 topologyDigest", document.TopologyDigest)
	if err != nil {
		return P1BindingV1{}, err
	}
	binding := P1BindingV1{
		AcceptanceContractID: document.AcceptanceContractID,
		AcceptanceDigest:     acceptanceDigest,
		WorkloadProfileID:    document.WorkloadProfileID,
		WorkloadDigest:       workloadDigest,
		TopologyProfileID:    document.TopologyProfileID,
		TopologyDigest:       topologyDigest,
	}
	if binding.AcceptanceContractID != acceptance.Acceptance().ID().String() ||
		!binding.AcceptanceDigest.Equal(acceptance.AcceptanceDigest()) ||
		binding.WorkloadProfileID != acceptance.Workload().ID().String() ||
		!binding.WorkloadDigest.Equal(acceptance.WorkloadDigest()) ||
		binding.TopologyProfileID != acceptance.Topology().ID().String() ||
		!binding.TopologyDigest.Equal(acceptance.TopologyDigest()) {
		return P1BindingV1{}, fmt.Errorf("p1 does not bind the decoded acceptance IDs and exact bytes")
	}
	return binding, nil
}

func decodeProvider(document *providerJSON) (ProviderContractV1, error) {
	if document == nil {
		return ProviderContractV1{}, fmt.Errorf("provider is required")
	}
	fields := []struct{ label, value string }{
		{"provider name", document.Name},
		{"provider accountId", document.AccountID},
		{"provider clusterId", document.ClusterID},
		{"provider kubernetesContext", document.KubernetesContext},
	}
	for _, field := range fields {
		if err := validateToken(field.label, field.value, 256); err != nil {
			return ProviderContractV1{}, err
		}
		if err := rejectProduction(field.label, field.value); err != nil {
			return ProviderContractV1{}, err
		}
	}
	if document.KubernetesContext == "default" {
		return ProviderContractV1{}, fmt.Errorf("provider kubernetesContext cannot be default")
	}
	if !versionPattern.MatchString(document.ProviderVersion) ||
		!versionPattern.MatchString(document.KubernetesVersion) {
		return ProviderContractV1{}, fmt.Errorf("provider and Kubernetes versions must be immutable semantic versions")
	}
	if document.NonProduction == nil || !*document.NonProduction {
		return ProviderContractV1{}, fmt.Errorf("provider nonProduction must be true")
	}
	return ProviderContractV1{
		Name: document.Name, AccountID: document.AccountID, ClusterID: document.ClusterID,
		KubernetesContext: document.KubernetesContext, ProviderVersion: document.ProviderVersion,
		KubernetesVersion: document.KubernetesVersion, NonProduction: true,
	}, nil
}

func decodeTopology(
	document *topologyContractJSON,
	approved rf3acceptance.TopologyProfileV1,
) (TopologyContractV1, error) {
	if document == nil || document.NodeCount == nil || document.Replicas == nil ||
		document.ReplicationFactor == nil || document.Shards == nil ||
		document.AntiColocation == nil || document.ResourcesPerNode == nil {
		return TopologyContractV1{}, fmt.Errorf("complete topology fields are required")
	}
	if err := validateToken("failureDomainLabel", document.FailureDomainLabel, 253); err != nil {
		return TopologyContractV1{}, err
	}
	if !equalStrings(document.FailureDomains, approved.FailureDomains()) {
		return TopologyContractV1{}, fmt.Errorf("failureDomains do not match RF3-P1")
	}
	if *document.NodeCount != approved.NodeCount() ||
		*document.Replicas != approved.Replicas() ||
		*document.ReplicationFactor != approved.ReplicationFactor() ||
		*document.Shards != approved.Shards() ||
		*document.AntiColocation != approved.AntiColocation() {
		return TopologyContractV1{}, fmt.Errorf("topology counts do not match RF3-P1")
	}
	resources := approved.ResourcesPerNode()
	if document.ResourcesPerNode.CPUCores == nil ||
		document.ResourcesPerNode.MemoryGiB == nil ||
		document.ResourcesPerNode.StorageGiB == nil ||
		document.ResourcesPerNode.NetworkMegabits == nil ||
		*document.ResourcesPerNode.CPUCores != resources.CPU.Value ||
		*document.ResourcesPerNode.MemoryGiB != resources.Memory.Value ||
		*document.ResourcesPerNode.StorageGiB != resources.Storage.Value ||
		*document.ResourcesPerNode.NetworkMegabits != resources.Network.Value {
		return TopologyContractV1{}, fmt.Errorf("resourcesPerNode does not match RF3-P1")
	}
	nodes, err := decodeNodeContracts(document.Nodes, *document.NodeCount, document.FailureDomains)
	if err != nil {
		return TopologyContractV1{}, err
	}
	return TopologyContractV1{
		FailureDomainLabel: document.FailureDomainLabel,
		FailureDomains:     cloneStrings(document.FailureDomains),
		NodeCount:          *document.NodeCount, Replicas: *document.Replicas,
		ReplicationFactor: *document.ReplicationFactor, Shards: *document.Shards,
		AntiColocation: *document.AntiColocation, Nodes: nodes, ResourcesPerNode: resources,
	}, nil
}

func decodeNodeContracts(
	documents []nodeContractJSON,
	nodeCount uint64,
	domains []string,
) ([]NodeContractV1, error) {
	if uint64(len(documents)) != nodeCount {
		return nil, fmt.Errorf("nodes must contain exactly %d entries", nodeCount)
	}
	seenNodes := make(map[string]struct{})
	seenProviders := make(map[string]struct{})
	seenDomains := make(map[string]struct{})
	result := make([]NodeContractV1, len(documents))
	for index, document := range documents {
		fields := []struct{ label, value string }{
			{"nodeId", document.NodeID}, {"providerId", document.ProviderID},
			{"instanceType", document.InstanceType}, {"imageId", document.ImageID},
		}
		for _, field := range fields {
			if err := validateToken(field.label, field.value, 512); err != nil {
				return nil, fmt.Errorf("nodes[%d]: %w", index, err)
			}
			if err := rejectProduction(field.label, field.value); err != nil {
				return nil, fmt.Errorf("nodes[%d]: %w", index, err)
			}
		}
		if !contains(domains, document.FailureDomain) {
			return nil, fmt.Errorf("nodes[%d] failureDomain %q is not approved", index, document.FailureDomain)
		}
		if _, duplicate := seenNodes[document.NodeID]; duplicate {
			return nil, fmt.Errorf("duplicate nodeId %q", document.NodeID)
		}
		if _, duplicate := seenProviders[document.ProviderID]; duplicate {
			return nil, fmt.Errorf("duplicate providerId %q", document.ProviderID)
		}
		seenNodes[document.NodeID] = struct{}{}
		seenProviders[document.ProviderID] = struct{}{}
		seenDomains[document.FailureDomain] = struct{}{}
		result[index] = NodeContractV1{
			NodeID: document.NodeID, ProviderID: document.ProviderID,
			InstanceType: document.InstanceType, ImageID: document.ImageID,
			FailureDomain: document.FailureDomain,
		}
	}
	if len(seenDomains) != len(domains) {
		return nil, fmt.Errorf("nodes must cover all three approved failure domains")
	}
	return result, nil
}

func decodeDeployment(document *deploymentContractJSON) (DeploymentContractV1, error) {
	if document == nil {
		return DeploymentContractV1{}, fmt.Errorf("deployment is required")
	}
	if err := validateNamePrefix("namespacePrefix", document.NamespacePrefix); err != nil {
		return DeploymentContractV1{}, err
	}
	if err := validateNamePrefix("releasePrefix", document.ReleasePrefix); err != nil {
		return DeploymentContractV1{}, err
	}
	if err := validateToken("chartName", document.ChartName, 128); err != nil {
		return DeploymentContractV1{}, err
	}
	if !versionPattern.MatchString(document.ChartVersion) {
		return DeploymentContractV1{}, fmt.Errorf("chartVersion must be an immutable semantic version")
	}
	chartDigest, err := parseDigest("chartDigest", document.ChartDigest)
	if err != nil {
		return DeploymentContractV1{}, err
	}
	imageDigest, err := parseDigest("imageDigest", document.ImageDigest)
	if err != nil {
		return DeploymentContractV1{}, err
	}
	if err := validateImageReference(document.ImageReference, imageDigest); err != nil {
		return DeploymentContractV1{}, err
	}
	return DeploymentContractV1{
		NamespacePrefix: document.NamespacePrefix, ReleasePrefix: document.ReleasePrefix,
		ChartName: document.ChartName, ChartVersion: document.ChartVersion,
		ChartDigest: chartDigest, ImageReference: document.ImageReference, ImageDigest: imageDigest,
	}, nil
}

func decodeStorage(
	document *storageJSON,
	approved rf3acceptance.TopologyVolumeV1,
) (StorageContractV1, error) {
	if document == nil || document.CapacityGiB == nil {
		return StorageContractV1{}, fmt.Errorf("complete storage fields are required")
	}
	for _, field := range []struct{ label, value string }{
		{"storage class", document.Class}, {"storage provisioner", document.Provisioner},
	} {
		if err := validateToken(field.label, field.value, 256); err != nil {
			return StorageContractV1{}, err
		}
	}
	if document.Class != approved.Class || document.AccessMode != approved.AccessMode ||
		*document.CapacityGiB != approved.Size.Value {
		return StorageContractV1{}, fmt.Errorf("storage class, access mode, and capacity must match RF3-P1")
	}
	if !approved.Durable || document.BindingMode != "wait-for-first-consumer" ||
		(document.ReclaimPolicy != "delete" && document.ReclaimPolicy != "retain") ||
		document.VolumeMode != "filesystem" ||
		document.VolumeReusePolicy != "forbid-prior-cycle" {
		return StorageContractV1{}, fmt.Errorf("storage policy is unsupported or permits volume reuse")
	}
	return StorageContractV1{
		Class: document.Class, Provisioner: document.Provisioner,
		BindingMode: document.BindingMode, ReclaimPolicy: document.ReclaimPolicy,
		AccessMode: document.AccessMode, VolumeMode: document.VolumeMode,
		CapacityGiB: *document.CapacityGiB, VolumeReusePolicy: document.VolumeReusePolicy,
	}, nil
}

func decodeAdmission(document *admissionContractJSON) (AdmissionContractV1, error) {
	if document == nil || document.MaxCredentialLifetimeSeconds == nil {
		return AdmissionContractV1{}, fmt.Errorf("complete admission fields are required")
	}
	for _, field := range []struct{ label, value string }{
		{"controlNamespace", document.ControlNamespace}, {"leaseName", document.LeaseName},
		{"serviceAccount", document.ServiceAccount}, {"role", document.Role},
	} {
		if err := validateKubernetesName(field.label, field.value); err != nil {
			return AdmissionContractV1{}, err
		}
	}
	if !sameStringSet(document.RBACVerbs, []string{"get", "create", "update", "patch"}) ||
		!sameStringSet(document.RBACResources, []string{"leases.coordination.k8s.io"}) {
		return AdmissionContractV1{}, fmt.Errorf("RBAC metadata must grant only the reviewed Lease operations")
	}
	if err := validateToken("credentialIssuer", document.CredentialIssuer, 256); err != nil {
		return AdmissionContractV1{}, err
	}
	if err := validateToken("credentialAudience", document.CredentialAudience, 256); err != nil {
		return AdmissionContractV1{}, err
	}
	if *document.MaxCredentialLifetimeSeconds < 60 || *document.MaxCredentialLifetimeSeconds > 3600 {
		return AdmissionContractV1{}, fmt.Errorf("maxCredentialLifetimeSeconds must be between 60 and 3600")
	}
	if document.RevocationOwner != "platform-owner" {
		return AdmissionContractV1{}, fmt.Errorf("revocationOwner must be platform-owner")
	}
	return AdmissionContractV1{
		ControlNamespace: document.ControlNamespace, LeaseName: document.LeaseName,
		ServiceAccount: document.ServiceAccount, Role: document.Role,
		RBACVerbs: cloneStrings(document.RBACVerbs), RBACResources: cloneStrings(document.RBACResources),
		CredentialIssuer: document.CredentialIssuer, CredentialAudience: document.CredentialAudience,
		MaxCredentialLifetimeSeconds: *document.MaxCredentialLifetimeSeconds,
		RevocationOwner:              document.RevocationOwner,
	}, nil
}

func decodeRecovery(document *recoveryContractJSON) (RecoveryContractV1, error) {
	if document == nil {
		return RecoveryContractV1{}, fmt.Errorf("recovery is required")
	}
	digest, err := parseDigest("recovery iacDigest", document.IaCDigest)
	if err != nil {
		return RecoveryContractV1{}, err
	}
	if err := validateIdentity("recovery operationId", document.OperationID); err != nil {
		return RecoveryContractV1{}, err
	}
	if document.Owner != "platform-owner" {
		return RecoveryContractV1{}, fmt.Errorf("recovery owner must be platform-owner")
	}
	return RecoveryContractV1{IaCDigest: digest, OperationID: document.OperationID, Owner: document.Owner}, nil
}

func buildReceipt(
	document receiptJSON,
	source rf3acceptance.Source,
	contract EnvironmentContractV1,
) (ObservedEnvironmentReceiptV1, error) {
	if contract.manifest.identity.ID == "" {
		return ObservedEnvironmentReceiptV1{}, fmt.Errorf("environment contract is required")
	}
	if err := validateHeader(document.DocumentType, "rf3-environment-receipt", document.SchemaVersion); err != nil {
		return ObservedEnvironmentReceiptV1{}, err
	}
	if err := validateIdentity("receipt id", document.ReceiptID); err != nil {
		return ObservedEnvironmentReceiptV1{}, err
	}
	if document.Cycle != "create" && document.Cycle != "recreate" {
		return ObservedEnvironmentReceiptV1{}, fmt.Errorf("cycle must be create or recreate")
	}
	capturedAt, err := parseTimestamp("capturedAt", document.CapturedAt)
	if err != nil {
		return ObservedEnvironmentReceiptV1{}, err
	}
	if capturedAt.Before(contract.manifest.identity.PublishedAt) {
		return ObservedEnvironmentReceiptV1{}, fmt.Errorf("capturedAt predates the environment manifest")
	}
	if document.EnvironmentID != contract.manifest.identity.ID {
		return ObservedEnvironmentReceiptV1{}, fmt.Errorf("environmentId does not match the manifest")
	}
	manifestDigest, err := parseDigest("manifestDigest", document.ManifestDigest)
	if err != nil || !manifestDigest.Equal(contract.Digest()) {
		return ObservedEnvironmentReceiptV1{}, fmt.Errorf("manifestDigest does not match exact manifest bytes")
	}
	p1, err := decodeP1Binding(document.P1, contract.acceptance)
	if err != nil || p1 != contract.manifest.p1 {
		return ObservedEnvironmentReceiptV1{}, fmt.Errorf("receipt p1 binding does not match the environment contract")
	}
	provider, err := decodeProvider(document.Provider)
	if err != nil || provider != contract.manifest.specification.Provider {
		return ObservedEnvironmentReceiptV1{}, fmt.Errorf("observed provider does not match the environment contract")
	}
	pinned, err := decodeReceiptDeployment(document.Deployment, contract.manifest.specification.Deployment)
	if err != nil {
		return ObservedEnvironmentReceiptV1{}, err
	}
	admission, err := decodeReceiptAdmission(document.Admission, contract.manifest.admission, capturedAt)
	if err != nil {
		return ObservedEnvironmentReceiptV1{}, err
	}
	nodes, err := decodeObservedNodes(document.Nodes, contract.manifest.specification.Topology)
	if err != nil {
		return ObservedEnvironmentReceiptV1{}, err
	}
	replicas, err := decodeObservedReplicas(document.Replicas, contract.manifest.specification.Topology, nodes)
	if err != nil {
		return ObservedEnvironmentReceiptV1{}, err
	}
	volumes, err := decodeObservedVolumes(document.Volumes, contract.manifest.specification.Storage, replicas)
	if err != nil {
		return ObservedEnvironmentReceiptV1{}, err
	}
	evidence, err := decodeEvidence(document.Doctor, document.Baseline, document.Artifacts)
	if err != nil {
		return ObservedEnvironmentReceiptV1{}, err
	}
	cleanup, err := decodeCleanup(document.Cleanup, document.Cycle)
	if err != nil {
		return ObservedEnvironmentReceiptV1{}, err
	}
	return ObservedEnvironmentReceiptV1{
		identity: ReceiptIdentityV1{
			ID: document.ReceiptID, Cycle: document.Cycle, CapturedAt: capturedAt, Digest: source.Digest(),
		},
		binding: ReceiptContractBindingV1{ManifestDigest: manifestDigest, P1: p1},
		pinned: PinnedEnvironmentV1{
			Provider: provider, Deployment: pinned.Deployment,
			Namespace: pinned.Namespace, Release: pinned.Release,
		},
		admission:   admission,
		observation: EnvironmentObservationV1{Nodes: nodes, Replicas: replicas, Volumes: volumes},
		evidence:    evidence,
		cleanup:     cleanup,
	}, nil
}

func decodeReceiptDeployment(
	document *deploymentReceiptJSON,
	approved DeploymentContractV1,
) (PinnedEnvironmentV1, error) {
	if document == nil {
		return PinnedEnvironmentV1{}, fmt.Errorf("deployment is required")
	}
	if err := validateKubernetesName("deployment namespace", document.Namespace); err != nil {
		return PinnedEnvironmentV1{}, err
	}
	if err := validateKubernetesName("deployment release", document.Release); err != nil {
		return PinnedEnvironmentV1{}, err
	}
	if !strings.HasPrefix(document.Namespace, approved.NamespacePrefix) ||
		!strings.HasPrefix(document.Release, approved.ReleasePrefix) {
		return PinnedEnvironmentV1{}, fmt.Errorf("namespace or release is outside the approved prefix")
	}
	chartDigest, err := parseDigest("deployment chartDigest", document.ChartDigest)
	if err != nil {
		return PinnedEnvironmentV1{}, err
	}
	imageDigest, err := parseDigest("deployment imageDigest", document.ImageDigest)
	if err != nil {
		return PinnedEnvironmentV1{}, err
	}
	observed := DeploymentContractV1{
		NamespacePrefix: approved.NamespacePrefix, ReleasePrefix: approved.ReleasePrefix,
		ChartName: document.ChartName, ChartVersion: document.ChartVersion,
		ChartDigest: chartDigest, ImageReference: document.ImageReference, ImageDigest: imageDigest,
	}
	if observed != approved {
		return PinnedEnvironmentV1{}, fmt.Errorf("chart or image identity does not match the environment contract")
	}
	return PinnedEnvironmentV1{Deployment: observed, Namespace: document.Namespace, Release: document.Release}, nil
}

func decodeReceiptAdmission(
	document *admissionReceiptJSON,
	approved AdmissionContractV1,
	capturedAt time.Time,
) (ReceiptAdmissionV1, error) {
	if document == nil {
		return ReceiptAdmissionV1{}, fmt.Errorf("admission is required")
	}
	if document.ControlNamespace != approved.ControlNamespace ||
		document.LeaseName != approved.LeaseName ||
		document.ServiceAccount != approved.ServiceAccount ||
		document.Role != approved.Role ||
		document.CredentialIssuer != approved.CredentialIssuer ||
		document.CredentialAudience != approved.CredentialAudience {
		return ReceiptAdmissionV1{}, fmt.Errorf("Lease, RBAC, or credential metadata does not match the manifest")
	}
	if err := validateIdentity("holderIdentity", document.HolderIdentity); err != nil {
		return ReceiptAdmissionV1{}, err
	}
	if err := validateToken("resourceVersion", document.ResourceVersion, 128); err != nil {
		return ReceiptAdmissionV1{}, err
	}
	issuedAt, err := parseTimestamp("credentialIssuedAt", document.CredentialIssuedAt)
	if err != nil {
		return ReceiptAdmissionV1{}, err
	}
	expiresAt, err := parseTimestamp("credentialExpiresAt", document.CredentialExpiresAt)
	if err != nil {
		return ReceiptAdmissionV1{}, err
	}
	maxLifetime, err := time.ParseDuration(strconv.FormatUint(approved.MaxCredentialLifetimeSeconds, 10) + "s")
	if err != nil {
		return ReceiptAdmissionV1{}, fmt.Errorf("approved credential lifetime is invalid: %w", err)
	}
	lifetime := expiresAt.Sub(issuedAt)
	if lifetime <= 0 || lifetime > maxLifetime ||
		capturedAt.Before(issuedAt) || !capturedAt.Before(expiresAt) {
		return ReceiptAdmissionV1{}, fmt.Errorf("credential lifetime or capture window is invalid")
	}
	return ReceiptAdmissionV1{
		ControlNamespace: document.ControlNamespace, LeaseName: document.LeaseName,
		HolderIdentity: document.HolderIdentity, ResourceVersion: document.ResourceVersion,
		ServiceAccount: document.ServiceAccount, Role: document.Role,
		CredentialIssuer: document.CredentialIssuer, CredentialAudience: document.CredentialAudience,
		CredentialIssuedAt: issuedAt, CredentialExpiresAt: expiresAt,
	}, nil
}

func decodeObservedNodes(
	documents []nodeObservationJSON,
	approved TopologyContractV1,
) ([]NodeObservationV1, error) {
	if len(documents) != len(approved.Nodes) {
		return nil, fmt.Errorf("nodes must contain exactly %d observations", len(approved.Nodes))
	}
	contracts := make(map[string]NodeContractV1, len(approved.Nodes))
	for _, node := range approved.Nodes {
		contracts[node.NodeID] = node
	}
	seenIDs := make(map[string]struct{})
	seenUIDs := make(map[string]struct{})
	result := make([]NodeObservationV1, len(documents))
	for index, document := range documents {
		expected, exists := contracts[document.NodeID]
		if !exists || document.ProviderID != expected.ProviderID ||
			document.InstanceType != expected.InstanceType ||
			document.ImageID != expected.ImageID ||
			document.FailureDomain != expected.FailureDomain {
			return nil, fmt.Errorf("nodes[%d] does not match an approved node", index)
		}
		if err := validateToken("nodeUid", document.NodeUID, 256); err != nil {
			return nil, fmt.Errorf("nodes[%d]: %w", index, err)
		}
		if _, duplicate := seenIDs[document.NodeID]; duplicate {
			return nil, fmt.Errorf("duplicate observed nodeId %q", document.NodeID)
		}
		if _, duplicate := seenUIDs[document.NodeUID]; duplicate {
			return nil, fmt.Errorf("duplicate observed nodeUid %q", document.NodeUID)
		}
		seenIDs[document.NodeID] = struct{}{}
		seenUIDs[document.NodeUID] = struct{}{}
		result[index] = NodeObservationV1{
			NodeID: document.NodeID, NodeUID: document.NodeUID, ProviderID: document.ProviderID,
			InstanceType: document.InstanceType, ImageID: document.ImageID,
			FailureDomain: document.FailureDomain,
		}
	}
	return result, nil
}

func decodeObservedReplicas(
	documents []replicaObservationJSON,
	approved TopologyContractV1,
	nodes []NodeObservationV1,
) ([]ReplicaObservationV1, error) {
	if uint64(len(documents)) != approved.Replicas {
		return nil, fmt.Errorf("replicas must contain exactly %d observations", approved.Replicas)
	}
	nodeDomains := make(map[string]string, len(nodes))
	for _, node := range nodes {
		nodeDomains[node.NodeID] = node.FailureDomain
	}
	ordinals := make(map[uint64]struct{})
	usedNodes := make(map[string]struct{})
	usedDomains := make(map[string]struct{})
	usedPodUIDs := make(map[string]struct{})
	usedPVCs := make(map[string]struct{})
	usedPVs := make(map[string]struct{})
	result := make([]ReplicaObservationV1, len(documents))
	for index, document := range documents {
		if document.Ordinal == nil || *document.Ordinal >= approved.Replicas {
			return nil, fmt.Errorf("replicas[%d] ordinal is invalid", index)
		}
		if nodeDomains[document.NodeID] != document.FailureDomain ||
			!contains(approved.FailureDomains, document.FailureDomain) {
			return nil, fmt.Errorf("replicas[%d] node and failure domain do not match", index)
		}
		for _, field := range []struct{ label, value string }{
			{"podName", document.PodName}, {"podUid", document.PodUID},
			{"pvcName", document.PVCName}, {"pvcUid", document.PVCUID},
			{"pvName", document.PVName}, {"pvUid", document.PVUID},
		} {
			if err := validateToken(field.label, field.value, 256); err != nil {
				return nil, fmt.Errorf("replicas[%d]: %w", index, err)
			}
		}
		if _, duplicate := ordinals[*document.Ordinal]; duplicate {
			return nil, fmt.Errorf("duplicate replica ordinal %d", *document.Ordinal)
		}
		if _, duplicate := usedNodes[document.NodeID]; duplicate {
			return nil, fmt.Errorf("replicas are colocated on node %q", document.NodeID)
		}
		if _, duplicate := usedDomains[document.FailureDomain]; duplicate {
			return nil, fmt.Errorf("replicas share failure domain %q", document.FailureDomain)
		}
		if _, duplicate := usedPodUIDs[document.PodUID]; duplicate {
			return nil, fmt.Errorf("duplicate podUid %q", document.PodUID)
		}
		if _, duplicate := usedPVCs[document.PVCUID]; duplicate {
			return nil, fmt.Errorf("duplicate pvcUid %q", document.PVCUID)
		}
		if _, duplicate := usedPVs[document.PVUID]; duplicate {
			return nil, fmt.Errorf("duplicate pvUid %q", document.PVUID)
		}
		ordinals[*document.Ordinal] = struct{}{}
		usedNodes[document.NodeID] = struct{}{}
		usedDomains[document.FailureDomain] = struct{}{}
		usedPodUIDs[document.PodUID] = struct{}{}
		usedPVCs[document.PVCUID] = struct{}{}
		usedPVs[document.PVUID] = struct{}{}
		result[index] = ReplicaObservationV1{
			Ordinal: *document.Ordinal, PodName: document.PodName, PodUID: document.PodUID,
			NodeID: document.NodeID, FailureDomain: document.FailureDomain,
			PVCName: document.PVCName, PVCUID: document.PVCUID,
			PVName: document.PVName, PVUID: document.PVUID,
		}
	}
	return result, nil
}

func decodeObservedVolumes(
	documents []volumeObservationJSON,
	approved StorageContractV1,
	replicas []ReplicaObservationV1,
) ([]VolumeObservationV1, error) {
	if len(documents) != len(replicas) {
		return nil, fmt.Errorf("volumes must contain exactly %d observations", len(replicas))
	}
	placements := make(map[string]ReplicaObservationV1, len(replicas))
	for _, replica := range replicas {
		placements[replica.PVCUID] = replica
	}
	seen := make(map[string]struct{})
	result := make([]VolumeObservationV1, len(documents))
	for index, document := range documents {
		replica, exists := placements[document.PVCUID]
		if !exists || document.PVCName != replica.PVCName ||
			document.PVName != replica.PVName || document.PVUID != replica.PVUID ||
			document.FailureDomain != replica.FailureDomain {
			return nil, fmt.Errorf("volumes[%d] does not match replica placement", index)
		}
		if document.CapacityGiB == nil ||
			document.StorageClass != approved.Class ||
			document.Provisioner != approved.Provisioner ||
			document.BindingMode != approved.BindingMode ||
			document.ReclaimPolicy != approved.ReclaimPolicy ||
			document.AccessMode != approved.AccessMode ||
			document.VolumeMode != approved.VolumeMode ||
			*document.CapacityGiB != approved.CapacityGiB {
			return nil, fmt.Errorf("volumes[%d] policy does not match the manifest", index)
		}
		if _, duplicate := seen[document.PVCUID]; duplicate {
			return nil, fmt.Errorf("duplicate volume pvcUid %q", document.PVCUID)
		}
		seen[document.PVCUID] = struct{}{}
		result[index] = VolumeObservationV1{
			PVCName: document.PVCName, PVCUID: document.PVCUID,
			PVName: document.PVName, PVUID: document.PVUID,
			StorageClass: document.StorageClass, Provisioner: document.Provisioner,
			BindingMode: document.BindingMode, ReclaimPolicy: document.ReclaimPolicy,
			AccessMode: document.AccessMode, VolumeMode: document.VolumeMode,
			CapacityGiB: *document.CapacityGiB, FailureDomain: document.FailureDomain,
		}
	}
	return result, nil
}

func decodeEvidence(
	doctorDocument *doctorJSON,
	baselineDocument *baselineJSON,
	artifactDocuments []artifactJSON,
) (ReceiptEvidenceV1, error) {
	if doctorDocument == nil || doctorDocument.Complete == nil || !*doctorDocument.Complete ||
		doctorDocument.Classification != "healthy" || doctorDocument.Artifact == nil {
		return ReceiptEvidenceV1{}, fmt.Errorf("doctor must be complete and healthy")
	}
	if baselineDocument == nil || baselineDocument.Passed == nil || !*baselineDocument.Passed ||
		baselineDocument.Artifact == nil {
		return ReceiptEvidenceV1{}, fmt.Errorf("functional baseline must pass")
	}
	doctorArtifact, err := decodeArtifact(doctorDocument.Artifact)
	if err != nil {
		return ReceiptEvidenceV1{}, fmt.Errorf("doctor artifact: %w", err)
	}
	baselineArtifact, err := decodeArtifact(baselineDocument.Artifact)
	if err != nil {
		return ReceiptEvidenceV1{}, fmt.Errorf("baseline artifact: %w", err)
	}
	if len(artifactDocuments) < 2 {
		return ReceiptEvidenceV1{}, fmt.Errorf("artifact inventory must contain doctor and baseline evidence")
	}
	artifacts := make([]ArtifactRefV1, len(artifactDocuments))
	seen := make(map[string]struct{}, len(artifactDocuments))
	for index := range artifactDocuments {
		artifact, err := decodeArtifact(&artifactDocuments[index])
		if err != nil {
			return ReceiptEvidenceV1{}, fmt.Errorf("artifacts[%d]: %w", index, err)
		}
		if _, duplicate := seen[artifact.Path]; duplicate {
			return ReceiptEvidenceV1{}, fmt.Errorf("duplicate artifact path %q", artifact.Path)
		}
		seen[artifact.Path] = struct{}{}
		artifacts[index] = artifact
	}
	if !containsArtifact(artifacts, doctorArtifact) || !containsArtifact(artifacts, baselineArtifact) {
		return ReceiptEvidenceV1{}, fmt.Errorf("referenced doctor and baseline artifacts must be in the inventory")
	}
	return ReceiptEvidenceV1{
		Doctor: DoctorEvidenceV1{
			Classification: doctorDocument.Classification, Complete: true, Artifact: doctorArtifact,
		},
		Baseline:  BaselineEvidenceV1{Passed: true, Artifact: baselineArtifact},
		Artifacts: artifacts,
	}, nil
}

func decodeCleanup(document *cleanupJSON, cycle string) (CleanupReceiptV1, error) {
	if document == nil || document.CredentialsRevoked == nil ||
		document.EnvironmentDestroyed == nil || document.ResidualVolumes == nil {
		return CleanupReceiptV1{}, fmt.Errorf("complete cleanup fields are required")
	}
	switch cycle {
	case "create":
		if document.State != "destroyed" || !*document.CredentialsRevoked ||
			!*document.EnvironmentDestroyed || *document.ResidualVolumes != 0 {
			return CleanupReceiptV1{}, fmt.Errorf("create cleanup must prove destruction, revocation, and zero residual volumes")
		}
	case "recreate":
		if document.State != "ready" || *document.CredentialsRevoked ||
			*document.EnvironmentDestroyed || *document.ResidualVolumes != 0 {
			return CleanupReceiptV1{}, fmt.Errorf("recreate cleanup must preserve one ready environment with active credentials")
		}
	}
	return CleanupReceiptV1{
		State: document.State, CredentialsRevoked: *document.CredentialsRevoked,
		EnvironmentDestroyed: *document.EnvironmentDestroyed,
		ResidualVolumes:      *document.ResidualVolumes,
	}, nil
}

func decodeArtifact(document *artifactJSON) (ArtifactRefV1, error) {
	if document == nil || document.Bytes == nil || *document.Bytes == 0 {
		return ArtifactRefV1{}, fmt.Errorf("artifact path, digest, and positive byte count are required")
	}
	if err := validateArtifactPath(document.Path); err != nil {
		return ArtifactRefV1{}, err
	}
	digest, err := parseDigest("artifact digest", document.Digest)
	if err != nil {
		return ArtifactRefV1{}, err
	}
	return ArtifactRefV1{Path: document.Path, Digest: digest, Bytes: *document.Bytes}, nil
}

func validateArtifact(artifact ArtifactRefV1) error {
	if artifact.Bytes == 0 {
		return fmt.Errorf("artifact byte count must be positive")
	}
	if err := validateArtifactPath(artifact.Path); err != nil {
		return err
	}
	if artifact.Digest.String() == "sha256:"+strings.Repeat("0", 64) {
		return fmt.Errorf("artifact digest cannot be zero")
	}
	return nil
}

func validateArtifactPath(value string) error {
	if value == "" || len(value) > 512 || path.IsAbs(value) ||
		path.Clean(value) != value || value == "." || strings.HasPrefix(value, "../") ||
		strings.ContainsAny(value, "\\\x00\r\n\t ") {
		return fmt.Errorf("artifact path %q is not a canonical relative path", value)
	}
	for _, segment := range strings.Split(value, "/") {
		if segment == "." || segment == ".." || isMutable(segment) {
			return fmt.Errorf("artifact path %q contains an invalid or mutable segment", value)
		}
	}
	return nil
}

func validateHeader(actualType, expectedType, version string) error {
	if actualType != expectedType {
		return fmt.Errorf("documentType must be %q, got %q", expectedType, actualType)
	}
	if version != schemaVersionV1 {
		return fmt.Errorf("schemaVersion must be %q, got %q", schemaVersionV1, version)
	}
	return nil
}

func validateIdentity(label, value string) error {
	if len(value) == 0 || len(value) > 192 || !identityPattern.MatchString(value) {
		return fmt.Errorf("%s %q is not canonical", label, value)
	}
	for _, segment := range strings.Split(value, ".") {
		if isMutable(segment) {
			return fmt.Errorf("%s %q contains mutable alias %q", label, value, segment)
		}
	}
	if looksSecret(value) {
		return fmt.Errorf("%s is secret-shaped", label)
	}
	return nil
}

func validateToken(label, value string, maximum int) error {
	if value == "" || len(value) > maximum || !utf8.ValidString(value) ||
		strings.TrimSpace(value) != value || strings.ContainsAny(value, "\x00\r\n\t ") ||
		!tokenPattern.MatchString(value) || isMutable(value) || looksSecret(value) {
		return fmt.Errorf("%s %q is not canonical or immutable", label, value)
	}
	switch strings.ToLower(value) {
	case "unknown", "unresolved", "tbd", "todo", "n/a":
		return fmt.Errorf("%s %q is unresolved", label, value)
	}
	return nil
}

func validateKubernetesName(label, value string) error {
	if len(value) > 63 || !kubernetesNamePattern.MatchString(value) || isMutable(value) || looksSecret(value) {
		return fmt.Errorf("%s %q is not a canonical Kubernetes name", label, value)
	}
	return nil
}

func validateNamePrefix(label, value string) error {
	if !strings.HasSuffix(value, "-") || len(value) < 2 || len(value) > 48 {
		return fmt.Errorf("%s %q must be a bounded Kubernetes prefix ending in '-'", label, value)
	}
	return validateKubernetesName(label, strings.TrimSuffix(value, "-"))
}

func validateImageReference(reference string, digest rf3acceptance.Digest) error {
	if err := validateToken("imageReference", reference, 512); err != nil {
		return err
	}
	suffix := "@" + digest.String()
	if !strings.HasSuffix(reference, suffix) || strings.Count(reference, "@") != 1 {
		return fmt.Errorf("imageReference must pin the exact imageDigest")
	}
	return nil
}

func parseDigest(label, value string) (rf3acceptance.Digest, error) {
	digest, err := rf3acceptance.NewDigest(value)
	if err != nil {
		return rf3acceptance.Digest{}, fmt.Errorf("%s: %w", label, err)
	}
	if value == "sha256:"+strings.Repeat("0", 64) {
		return rf3acceptance.Digest{}, fmt.Errorf("%s cannot be zero", label)
	}
	return digest, nil
}

func parseTimestamp(label, value string) (time.Time, error) {
	if value == "" || !strings.HasSuffix(value, "Z") || strings.TrimSpace(value) != value {
		return time.Time{}, fmt.Errorf("%s must be a canonical UTC timestamp", label)
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil || parsed.Format(time.RFC3339Nano) != value {
		return time.Time{}, fmt.Errorf("%s %q is not canonical RFC3339Nano UTC", label, value)
	}
	return parsed.UTC(), nil
}

func rejectProduction(label, value string) error {
	if productionPattern.MatchString(value) {
		return fmt.Errorf("%s %q identifies production", label, value)
	}
	return nil
}

func looksSecret(value string) bool {
	lower := strings.ToLower(value)
	for _, prefix := range []string{
		"bearer ", "-----begin ", "ghp_", "github_pat_", "akia", "asia",
		"sk-", "xoxb-", "xoxp-", "ya29.", "eyj",
	} {
		if strings.HasPrefix(lower, prefix) {
			return true
		}
	}
	return longSecretPattern.MatchString(value)
}

func isMutable(value string) bool {
	lower := strings.ToLower(value)
	if isMutableValue(lower) {
		return true
	}
	stem := strings.TrimSuffix(lower, path.Ext(lower))
	return stem != lower && isMutableValue(stem)
}

func isMutableValue(value string) bool {
	if _, exists := mutableAliases[value]; exists {
		return true
	}
	for alias := range mutableAliases {
		for _, separator := range []string{":", "/", "@", ".", "-", "_"} {
			if strings.HasSuffix(value, separator+alias) {
				return true
			}
		}
	}
	return false
}

func sameStringSet(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	seen := make(map[string]struct{}, len(left))
	for _, value := range left {
		if _, duplicate := seen[value]; duplicate || !contains(right, value) {
			return false
		}
		seen[value] = struct{}{}
	}
	return true
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func contains(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

func containsArtifact(values []ArtifactRefV1, expected ArtifactRefV1) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

func nodeUIDs(nodes []NodeObservationV1) []string {
	values := make([]string, len(nodes))
	for index, node := range nodes {
		values[index] = node.NodeUID
	}
	sort.Strings(values)
	return values
}

func volumeUIDs(volumes []VolumeObservationV1) ([]string, []string) {
	pvcUIDs := make([]string, len(volumes))
	pvUIDs := make([]string, len(volumes))
	for index, volume := range volumes {
		pvcUIDs[index] = volume.PVCUID
		pvUIDs[index] = volume.PVUID
	}
	sort.Strings(pvcUIDs)
	sort.Strings(pvUIDs)
	return pvcUIDs, pvUIDs
}

func intersect(left, right []string) []string {
	rightSet := make(map[string]struct{}, len(right))
	for _, value := range right {
		rightSet[value] = struct{}{}
	}
	var result []string
	for _, value := range left {
		if _, exists := rightSet[value]; exists {
			result = append(result, value)
		}
	}
	sort.Strings(result)
	return result
}
