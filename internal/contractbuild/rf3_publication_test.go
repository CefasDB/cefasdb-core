package contractbuild

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/CefasDb/cefasdb/internal/core/rf3acceptance"
)

type rf3PublicationArtifact struct {
	Path      string
	ProfileID string
	Digest    string
}

type rf3PublicationApproval struct {
	Role              string
	Approver          string
	ApprovedAt        string
	Evidence          string
	ContractID        string
	RootDigest        string
	WorkloadProfileID string
	WorkloadDigest    string
	TopologyProfileID string
	TopologyDigest    string
}

type rf3PublicationDocument struct {
	DocumentType  string
	SchemaVersion string
	ContractID    string
	Bundle        struct {
		Root     rf3PublicationArtifact
		Workload rf3PublicationArtifact
		Topology rf3PublicationArtifact
	}
	Review struct {
		Manifest struct {
			Evidence string
			Digest   string
		}
		DecisionPolicy struct {
			Evidence string
		}
	}
	PlatformInputs struct {
		Evidence       string
		FailureDomains []string
		StorageClass   string
	}
	AttemptSemantics struct {
		RootField        string
		EncodedUnit      string
		Denominator      string
		WriteAttempt     string
		PointReadAttempt string
		FailedBatch      string
	}
	Approvals []rf3PublicationApproval
}

func TestRF3PublicationBindsProtectedDecisionsAndFinalBundle(t *testing.T) {
	t.Parallel()

	directory := filepath.Join("..", "..", "contracts", "rf3-acceptance")
	publication, publicationDigest := readRF3Publication(
		t,
		filepath.Join(directory, "publication-v1.json"),
	)
	const expectedPublicationDigest = "sha256:e2b1c429f2cd2031de2bed134cf962c3e3d2b3d3502c281fa1b623cc6fde2fea"
	if publicationDigest != expectedPublicationDigest {
		t.Fatalf("publication digest = %q, want %q", publicationDigest, expectedPublicationDigest)
	}

	bundle, err := LoadRF3AcceptanceFiles(
		context.Background(),
		filepath.Join(directory, "v1.json"),
		filepath.Join(directory, "workload-v1.json"),
		filepath.Join(directory, "topology-v1.json"),
	)
	if err != nil {
		t.Fatalf("LoadRF3AcceptanceFiles() unexpected error: %v", err)
	}

	assertRF3PublicationIdentity(t, publication, bundle)
	assertRF3PublicationReview(t, publication)
	assertRF3PublicationAttemptSemantics(t, publication, bundle)
	assertRF3PublicationApprovals(t, publication)
}

func readRF3Publication(t *testing.T, path string) (rf3PublicationDocument, string) {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read publication: %v", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var publication rf3PublicationDocument
	if err := decoder.Decode(&publication); err != nil {
		t.Fatalf("decode publication: %v", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		t.Fatalf("decode publication trailing data: %v", err)
	}
	sum := sha256.Sum256(data)
	return publication, "sha256:" + hex.EncodeToString(sum[:])
}

func assertRF3PublicationIdentity(
	t *testing.T,
	publication rf3PublicationDocument,
	bundle rf3acceptance.Bundle,
) {
	t.Helper()

	if publication.DocumentType != "rf3-acceptance-publication" ||
		publication.SchemaVersion != "1.0" ||
		publication.ContractID != bundle.Acceptance().ID().String() {
		t.Fatalf("publication identity is invalid: %#v", publication)
	}
	if publication.Bundle.Root.Path != "v1.json" ||
		publication.Bundle.Root.ProfileID != "" ||
		publication.Bundle.Root.Digest != bundle.AcceptanceDigest().String() {
		t.Fatalf("root binding is invalid: %#v", publication.Bundle.Root)
	}
	if publication.Bundle.Workload.Path != "workload-v1.json" ||
		publication.Bundle.Workload.ProfileID != bundle.Workload().ID().String() ||
		publication.Bundle.Workload.Digest != bundle.WorkloadDigest().String() {
		t.Fatalf("workload binding is invalid: %#v", publication.Bundle.Workload)
	}
	if publication.Bundle.Topology.Path != "topology-v1.json" ||
		publication.Bundle.Topology.ProfileID != bundle.Topology().ID().String() ||
		publication.Bundle.Topology.Digest != bundle.TopologyDigest().String() {
		t.Fatalf("topology binding is invalid: %#v", publication.Bundle.Topology)
	}
}

func assertRF3PublicationReview(t *testing.T, publication rf3PublicationDocument) {
	t.Helper()

	const runPrefix = "codetask/automations/runs/RUN-e345cdf3-13d3-432e-8912-1f37f742825d/"
	const manifestPath = runPrefix +
		"drafts/approval-review-plan-implementation--1--publish-material/" +
		"4775c160088eb62e695f3da2dbe1cdc28855aeecb058427b59297f795ded07f6/manifest.json"
	if publication.Review.Manifest.Evidence != manifestPath ||
		publication.Review.Manifest.Digest !=
			"sha256:706b59e4729641c03de05ac8b1d4db96e78630cccb560ecb123d2eced87c6740" {
		t.Fatalf("review manifest binding is invalid: %#v", publication.Review.Manifest)
	}
	if publication.Review.DecisionPolicy.Evidence !=
		runPrefix+"decisions/rf3-p1-decision-policy-v1.json" {
		t.Fatalf("decision policy evidence = %q", publication.Review.DecisionPolicy.Evidence)
	}
	const platformEvidence = runPrefix +
		"decisions/input-approval-plan-implementation--1--platform-" +
		"d69b6f54ee47816284499670211f92456ef5e58c0a118cdf10a89313f89ccc14.json"
	wantDomains := []string{"rf3q-zone-a", "rf3q-zone-b", "rf3q-zone-c"}
	if publication.PlatformInputs.Evidence != platformEvidence ||
		!reflect.DeepEqual(publication.PlatformInputs.FailureDomains, wantDomains) ||
		publication.PlatformInputs.StorageClass != "cefasdb-rf3-durable-rwo-v1" {
		t.Fatalf("platform inputs are invalid: %#v", publication.PlatformInputs)
	}
}

func assertRF3PublicationAttemptSemantics(
	t *testing.T,
	publication rf3PublicationDocument,
	bundle rf3acceptance.Bundle,
) {
	t.Helper()

	semantics := publication.AttemptSemantics
	if bundle.Acceptance().DefaultRule().MinimumAttempts() != 300000 ||
		semantics.RootField != "defaultRule.minimumAttempts" ||
		semantics.EncodedUnit != "requests" ||
		semantics.Denominator != "logical-item-operations" ||
		semantics.WriteAttempt != "one logical item submitted in a batch write" ||
		semantics.PointReadAttempt != "one logical point read" ||
		semantics.FailedBatch !=
			"charge every item in a failed batch unless per-item evidence proves otherwise" {
		t.Fatalf("attempt semantics are invalid: %#v", semantics)
	}
}

func assertRF3PublicationApprovals(t *testing.T, publication rf3PublicationDocument) {
	t.Helper()
	const runPrefix = "codetask/automations/runs/RUN-e345cdf3-13d3-432e-8912-1f37f742825d/"
	const rootDigest = "sha256:10004b1885b3cbbe5d75cd9431d48449d5d6305d72c925448aba13ed9ab09853"
	const workloadDigest = "sha256:d5bba0f8c03acc85b7190a15386acaba42167d6f42825b46f0847b198a9e3d66"
	const topologyDigest = "sha256:33628e501c43019dce38209a37ca42c8cde9e56c90e23af955e2878f49e33b78"
	roles := []string{"product-owner", "database-operator", "platform-owner"}
	if len(publication.Approvals) != len(roles) {
		t.Fatalf("approval count = %d, want %d", len(publication.Approvals), len(roles))
	}
	for index, role := range roles {
		approval := publication.Approvals[index]
		if approval.Role != role ||
			approval.Approver != "github:432802 (@osvaldoandrade)" ||
			approval.ApprovedAt != "2026-11-22T16:09:32.817Z" ||
			approval.Evidence != runPrefix+"decisions/rf3-p1-"+role+"-approval-v1.json" ||
			approval.ContractID != publication.ContractID ||
			approval.RootDigest != rootDigest ||
			approval.WorkloadProfileID != "rf3.workload.synthetic-mixed-one-kops.v1" ||
			approval.WorkloadDigest != workloadDigest ||
			approval.TopologyProfileID != "rf3.topology.three-node-standard.v1" ||
			approval.TopologyDigest != topologyDigest {
			t.Fatalf("approval[%d] is invalid: %#v", index, approval)
		}
	}
}
