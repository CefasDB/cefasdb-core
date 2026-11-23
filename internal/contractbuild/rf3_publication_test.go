package contractbuild

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/CefasDb/cefasdb/internal/core/rf3acceptance"
	"github.com/CefasDb/cefasdb/internal/core/rf3environment"
)

type rf3EvidenceVerifier struct {
	remaining map[string][]byte
}

func (verifier *rf3EvidenceVerifier) VerifyRF3ProtectedEvidence(
	_ context.Context,
	evidence RF3ProtectedEvidence,
) ([]byte, error) {
	document, ok := verifier.remaining[evidence.Locator]
	if !ok {
		return nil, fmt.Errorf("unexpected protected evidence %q", evidence.Locator)
	}
	delete(verifier.remaining, evidence.Locator)
	return append([]byte(nil), document...), nil
}

func (verifier *rf3EvidenceVerifier) assertComplete(t testing.TB) {
	t.Helper()
	if len(verifier.remaining) != 0 {
		t.Fatalf("unverified protected evidence = %v", verifier.remaining)
	}
}

type rf3EvidenceVerifierFunc func(
	context.Context,
	RF3ProtectedEvidence,
) ([]byte, error)

func (verify rf3EvidenceVerifierFunc) VerifyRF3ProtectedEvidence(
	ctx context.Context,
	evidence RF3ProtectedEvidence,
) ([]byte, error) {
	return verify(ctx, evidence)
}

func TestRF3PublicationBindsProtectedDecisionsAndFinalBundle(t *testing.T) {
	t.Parallel()

	files := publishedRF3Files()
	verifier := publishedRF3EvidenceVerifier()
	got, err := LoadRF3PublishedAcceptanceFiles(context.Background(), files, verifier)
	if err != nil {
		t.Fatalf("LoadRF3PublishedAcceptanceFiles() unexpected error: %v", err)
	}
	verifier.assertComplete(t)
	assertPublishedRF3Contract(t, got)
}

func TestLoadRF3PublishedAcceptanceFilesRequiresVerifiedEvidence(t *testing.T) {
	t.Parallel()

	files := publishedRF3Files()
	tests := []struct {
		name     string
		verifier RF3ProtectedEvidenceVerifier
		wantErr  error
	}{
		{
			name: "missing verifier",
		},
		{
			name: "failed protected lookup",
			verifier: rf3EvidenceVerifierFunc(func(
				context.Context,
				RF3ProtectedEvidence,
			) ([]byte, error) {
				return nil, errors.New("synthetic protected lookup failure")
			}),
			wantErr: errors.New("synthetic protected lookup failure"),
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			got, err := LoadRF3PublishedAcceptanceFiles(
				context.Background(),
				files,
				test.verifier,
			)
			if err == nil || !reflect.DeepEqual(got, RF3PublishedAcceptanceV1{}) {
				t.Fatalf("published acceptance = %#v, %v; want zero plus error", got, err)
			}
			if test.wantErr != nil && !strings.Contains(err.Error(), test.wantErr.Error()) {
				t.Fatalf("error = %v, want %v", err, test.wantErr)
			}
		})
	}
}

func TestLoadRF3PublishedAcceptanceFilesRejectsPublicationDrift(t *testing.T) {
	t.Parallel()

	path := filepath.Join("..", "..", "contracts", "rf3-acceptance", "publication-v1.json")
	publication, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read publication: %v", err)
	}
	tests := []struct {
		name string
		old  string
		new  string
	}{
		{
			name: "decision digest",
			old:  "sha256:57c2e3335faa142886f36e66d11422dfae7606bd47db6b661ae4843976ba3049",
			new:  "sha256:07c2e3335faa142886f36e66d11422dfae7606bd47db6b661ae4843976ba3049",
		},
		{
			name: "authority pointer",
			old:  `"authorityPointer": "/authorityRecord"`,
			new:  `"authorityPointer": "/wrongAuthorityRecord"`,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			tampered := strings.Replace(string(publication), test.old, test.new, 1)
			files := copyPublishedRF3Files(t, []byte(tampered))
			got, err := LoadRF3PublishedAcceptanceFiles(
				context.Background(),
				files,
				publishedRF3EvidenceVerifier(),
			)
			if err == nil || !reflect.DeepEqual(got, RF3PublishedAcceptanceV1{}) {
				t.Fatalf("published acceptance = %#v, %v; want zero plus error", got, err)
			}
		})
	}
}

func TestLoadAuthorizedRF3EnvironmentFiles(t *testing.T) {
	t.Parallel()

	corpus := readRF3Corpus(t)
	manifest := readRF3EnvironmentFixture(t, "valid", "v1", "manifest.json")
	directory := t.TempDir()
	rootPath := writeRF3File(t, directory, rf3RootName, corpus.root)
	workloadPath := writeRF3File(t, directory, rf3WorkloadName, corpus.workload)
	topologyPath := writeRF3File(t, directory, rf3TopologyName, corpus.topology)
	manifestPath := writeRF3File(t, directory, rf3EnvironmentManifestName, manifest)
	publicationPath, verifier := writeSyntheticRF3Publication(
		t,
		directory,
		rootPath,
		workloadPath,
		topologyPath,
	)

	got, err := LoadAuthorizedRF3EnvironmentFiles(
		context.Background(),
		RF3AuthorizedEnvironmentFiles{
			Published: RF3PublishedFiles{
				RootPath:        rootPath,
				WorkloadPath:    workloadPath,
				TopologyPath:    topologyPath,
				PublicationPath: publicationPath,
			},
			ManifestPath: manifestPath,
		},
		verifier,
	)
	if err != nil {
		t.Fatalf("LoadAuthorizedRF3EnvironmentFiles() unexpected error: %v", err)
	}
	verifier.assertComplete(t)
	if reflect.DeepEqual(got, RF3AuthorizedEnvironmentV1{}) ||
		reflect.DeepEqual(got.Environment, rf3environment.EnvironmentContractV1{}) ||
		reflect.DeepEqual(got.Published.Bundle, rf3acceptance.Bundle{}) {
		t.Fatalf("authorized environment is incomplete: %#v", got)
	}
}

func assertPublishedRF3Contract(t testing.TB, got RF3PublishedAcceptanceV1) {
	t.Helper()
	const publicationDigest = "sha256:c0e288f375f710e09125a85544f0461aa4069da37ae598a7c5feb6b8c3439498"
	if got.Publication.Digest != publicationDigest {
		t.Fatalf("publication digest = %q, want %q", got.Publication.Digest, publicationDigest)
	}
	attempts := got.Publication.AttemptSemantics
	if attempts.Denominator != rf3AttemptDenominator ||
		attempts.WriteAttempt != rf3AttemptWrite ||
		attempts.PointReadAttempt != rf3AttemptPointRead ||
		attempts.FailedBatch != rf3AttemptFailedBatch {
		t.Fatalf("attempt semantics = %#v", attempts)
	}
	if got.Workload.Dataset.ItemCount.Value != 1_000_000 ||
		got.Workload.Dataset.Payload.Bytes.Value != 256 ||
		len(got.Workload.Profiles) != 4 {
		t.Fatalf("workload projection is incomplete: %#v", got.Workload)
	}
	mixed := got.Workload.Profiles[3]
	if mixed.WriteRate == nil || mixed.WriteRate.Value != 250 ||
		mixed.ReadRate == nil || mixed.ReadRate.Value != 750 ||
		mixed.BatchSize == nil || mixed.BatchSize.Value != 100 {
		t.Fatalf("mixed workload projection is incomplete: %#v", mixed)
	}
	variance := got.Acceptance.Variance
	if variance.MinimumRuns.Value != 3 ||
		variance.Tolerance.Value != 100_000 ||
		!reflect.DeepEqual(variance.Metrics, []string{"throughput", "p99", "recovery"}) ||
		variance.Formula != "ceil((max-min)/median*1000000)" ||
		!reflect.DeepEqual(
			variance.IncompleteWhen,
			[]string{"missing-or-incomparable-set", "zero-median"},
		) {
		t.Fatalf("variance projection is incomplete: %#v", variance)
	}
	capacity := got.Acceptance.CapacityMethod
	if capacity.Headroom.Value != 300_000 ||
		capacity.AcceptedPeak != 1_000 ||
		capacity.FirstProbe != 3_000 ||
		capacity.MaximumFloorRoundedMidpoints != 7 ||
		capacity.HeadroomFormula != "1-(acceptedPeak/safeCapacity)" ||
		capacity.RequiredRoundedSafeCapacity != 1_429 ||
		capacity.PassRule != "tested-passing-low-gte-1429" ||
		capacity.IncompleteWhen == "" {
		t.Fatalf("capacity projection is incomplete: %#v", capacity)
	}
	if got.Authorization.DecisionPolicy.PolicyID != rf3DecisionPolicyID ||
		len(got.Authorization.Approvals) != 3 ||
		len(got.Acceptance.Approvals) != 3 {
		t.Fatalf("authorization projection is incomplete: %#v", got.Authorization)
	}
	if got.Acceptance.References.Workload.Digest != got.Bundle.WorkloadDigest().String() ||
		got.Acceptance.References.Topology.Digest != got.Bundle.TopologyDigest().String() {
		t.Fatalf("profile references do not match decoded bundle")
	}
}

func publishedRF3Files() RF3PublishedFiles {
	directory := filepath.Join("..", "..", "contracts", "rf3-acceptance")
	return RF3PublishedFiles{
		RootPath:        filepath.Join(directory, "v1.json"),
		WorkloadPath:    filepath.Join(directory, "workload-v1.json"),
		TopologyPath:    filepath.Join(directory, "topology-v1.json"),
		PublicationPath: filepath.Join(directory, "publication-v1.json"),
	}
}

func publishedRF3EvidenceVerifier() *rf3EvidenceVerifier {
	return &rf3EvidenceVerifier{
		remaining: publishedRF3ProtectedDocuments(),
	}
}
func copyPublishedRF3Files(t testing.TB, publication []byte) RF3PublishedFiles {
	t.Helper()
	source := publishedRF3Files()
	directory := t.TempDir()
	files := RF3PublishedFiles{
		RootPath:        filepath.Join(directory, "v1.json"),
		WorkloadPath:    filepath.Join(directory, "workload-v1.json"),
		TopologyPath:    filepath.Join(directory, "topology-v1.json"),
		PublicationPath: filepath.Join(directory, "publication-v1.json"),
	}
	for _, pair := range [][2]string{
		{source.RootPath, files.RootPath},
		{source.WorkloadPath, files.WorkloadPath},
		{source.TopologyPath, files.TopologyPath},
	} {
		data, err := os.ReadFile(pair[0])
		if err != nil {
			t.Fatalf("read %s: %v", pair[0], err)
		}
		if err := os.WriteFile(pair[1], data, 0o600); err != nil {
			t.Fatalf("write %s: %v", pair[1], err)
		}
	}
	if err := os.WriteFile(files.PublicationPath, publication, 0o600); err != nil {
		t.Fatalf("write publication: %v", err)
	}
	return files
}
