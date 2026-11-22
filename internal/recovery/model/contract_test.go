package model

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func testDigest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func testCommit(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:20])
}

func sealContract(tb testing.TB, contract RecoveryContract) RecoveryContract {
	tb.Helper()
	digest, err := canonicalContractDigest(contract)
	if err != nil {
		tb.Fatal(err)
	}
	contract.SHA256 = digest
	return contract
}

func validContract(tb testing.TB) RecoveryContract {
	tb.Helper()
	oldestCommit := testCommit("oldest-release")
	candidateCommit := testCommit("candidate-release")
	contract := RecoveryContract{
		ContractID: "recovery-v1", Revision: 1,
		ApprovedAt: time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC), ApprovedBy: []string{"data-owner", "operations"},
		ScopeKind: ScopeKindEnvironment, ScopeID: "rf3-qualification", CanonicalPath: CanonicalPathClusterBackupSetV1,
		Objectives:       RecoveryObjectives{RPOSeconds: 3600, RTOSeconds: 3600, PreflightSeconds: 300},
		Measurement:      MeasurementBoundaries{DeclarationEvent: "recovery-declared-at", RecoveryPointEvent: "complete-cross-shard-recovery-point-at", ReadyEvent: "ready-and-reconciled-at", ClockSource: "utc"},
		Workload:         Workload{LogicalBytes: 107374182400, Shards: 12, ReplicationFactor: 3, WritesPerSecond: 1000, ReadsPerSecond: 2000},
		RequiredChecks:   []string{"REC-01", "REC-02", "REC-03", "REC-04", "REC-05"},
		Retention:        RetentionPolicy{MinimumAgeSeconds: 604800, MinimumQualifiedSets: 3, EvidenceSeconds: 7776000},
		ManifestVersions: []uint32{1},
		SupportedVersions: SupportedVersions{
			Oldest:         ReleaseIdentity{Version: "1.4.0", ImageDigest: testDigest("oldest-image"), SourceCommit: oldestCommit, ProvenanceURI: "https://github.com/CefasDB/cefasdb-core/commit/" + oldestCommit},
			Candidate:      ReleaseIdentity{Version: "1.5.0", ImageDigest: testDigest("candidate-image"), SourceCommit: candidateCommit, ProvenanceURI: "https://github.com/CefasDB/cefasdb-core/commit/" + candidateCommit},
			RecoveryBranch: RecoveryBranchForward,
		},
		Lease:              LeasePolicy{DurationSeconds: 1800, RenewBeforeSeconds: 300},
		RepositoryPolicyID: "repository-policy-v1", Runbook: RunbookIdentity{Revision: "runbook-v1", SHA256: testDigest("runbook-v1")},
	}
	return sealContract(tb, contract)
}

func contractJSON(tb testing.TB, contract RecoveryContract) []byte {
	contract.SHA256 = ""
	raw, _ := json.Marshal(contract)
	_, digest, _ := canonicalizeContractJSON(raw)
	contract.SHA256 = digest
	raw, _ = json.Marshal(contract)
	return raw
}

func rehashContractJSON(tb testing.TB, raw []byte) []byte {
	tb.Helper()
	_, digest, err := canonicalizeContractJSON(raw)
	if err != nil {
		tb.Fatal(err)
	}
	var document map[string]json.RawMessage
	if err := json.Unmarshal(raw, &document); err != nil {
		tb.Fatal(err)
	}
	document["sha256"], err = json.Marshal(digest)
	if err != nil {
		tb.Fatal(err)
	}
	sealed, err := json.Marshal(document)
	if err != nil {
		tb.Fatal(err)
	}
	return sealed
}

func writeBytes(tb testing.TB, raw []byte) string {
	path := filepath.Join(tb.TempDir(), "contract.json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		tb.Fatal(err)
	}
	return path
}

func assertFailure(tb testing.TB, err error, code, reason, field string) {
	var failure *ValidationError
	if !errors.As(err, &failure) {
		tb.Fatalf("error = %v, want ValidationError", err)
	}
	_ = failure.Error()
	if failure.Code != code || failure.Reason != reason || failure.Field != field {
		tb.Fatalf("error = %#v, want %s/%s/%s", failure, code, reason, field)
	}
}

func assertLoadFailure(tb testing.TB, raw []byte, code, reason, field string) {
	contract, ref, err := LoadContract(writeBytes(tb, raw))
	assertFailure(tb, err, code, reason, field)
	if !reflect.DeepEqual(contract, RecoveryContract{}) || ref != (ContractRef{}) {
		tb.Fatalf("partial output: contract=%#v ref=%#v", contract, ref)
	}
}

func TestContractLoadApproved(t *testing.T) {
	raw := contractJSON(t, validContract(t))
	path := writeBytes(t, raw)
	first, firstRef, firstErr := LoadContract(path)
	second, secondRef, secondErr := LoadContract(path)
	if firstErr != nil || secondErr != nil {
		t.Fatalf("load errors: %v, %v", firstErr, secondErr)
	}
	if !reflect.DeepEqual(first, second) || firstRef != secondRef || firstRef.SHA256 != first.SHA256 {
		t.Fatalf("loads differ: %#v/%#v and %#v/%#v", first, firstRef, second, secondRef)
	}
}

func TestContractRFC3339OffsetApproval(t *testing.T) {
	contract := validContract(t)
	contract.ApprovedAt = time.Date(2026, 11, 21, 9, 0, 0, 0, time.FixedZone("-03:00", -3*60*60))
	loaded, _, err := LoadContract(writeBytes(t, contractJSON(t, contract)))
	if err != nil {
		t.Fatal(err)
	}
	_, offset := loaded.ApprovedAt.Zone()
	if !loaded.ApprovedAt.Equal(contract.ApprovedAt) || offset != -3*60*60 {
		t.Fatalf("approvedAt = %s with offset %d, want %s with offset %d", loaded.ApprovedAt, offset, contract.ApprovedAt, -3*60*60)
	}
}

func TestContractRFC3339WireSpellings(t *testing.T) {
	const canonical = `"approvedAt":"2026-11-21T12:00:00Z"`
	cases := []struct {
		name, approvedAt string
	}{
		{"fractional-trailing-zeros", "2026-11-21T12:00:00.000Z"},
		{"explicit-zero-offset", "2026-11-21T12:00:00+00:00"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			raw := contractJSON(t, validContract(t))
			raw = bytes.Replace(raw, []byte(canonical), []byte(`"approvedAt":"`+test.approvedAt+`"`), 1)
			raw = rehashContractJSON(t, raw)
			loaded, _, err := LoadContract(writeBytes(t, raw))
			if err != nil {
				t.Fatal(err)
			}
			if err := loaded.Validate(); err != nil {
				t.Fatalf("Validate after LoadContract: %v", err)
			}
			encoded, err := json.Marshal(loaded)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Contains(encoded, []byte(`"approvedAt":"`+test.approvedAt+`"`)) {
				t.Fatalf("approvedAt spelling was not preserved: %s", encoded)
			}
		})
	}
}

func TestContractLoadedApprovalMutationBreaksHash(t *testing.T) {
	raw := contractJSON(t, validContract(t))
	raw = bytes.Replace(
		raw,
		[]byte(`"approvedAt":"2026-11-21T12:00:00Z"`),
		[]byte(`"approvedAt":"2026-11-21T12:00:00.000Z"`),
		1,
	)
	loaded, _, err := LoadContract(writeBytes(t, rehashContractJSON(t, raw)))
	if err != nil {
		t.Fatal(err)
	}
	loaded.ApprovedAt = loaded.ApprovedAt.Add(time.Second)
	assertFailure(t, loaded.Validate(), "INVALID_CONTRACT", "HASH_MISMATCH", "sha256")
}

func TestContractManifestSupportMatrix(t *testing.T) {
	cases := []struct {
		name     string
		versions []uint32
	}{
		{"legacy", []uint32{0}},
		{"current", []uint32{1}},
		{"legacy-and-current", []uint32{0, 1}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			contract := validContract(t)
			contract.ManifestVersions = test.versions
			contract = sealContract(t, contract)
			if err := contract.Validate(); err != nil {
				t.Fatal(err)
			}
			if _, _, err := LoadContract(writeBytes(t, contractJSON(t, contract))); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestContractDistinctReleaseIdentities(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*RecoveryContract)
	}{
		{"shared-image-digest", func(contract *RecoveryContract) {
			contract.SupportedVersions.Candidate.ImageDigest = contract.SupportedVersions.Oldest.ImageDigest
		}},
		{"shared-source-commit", func(contract *RecoveryContract) {
			contract.SupportedVersions.Candidate.SourceCommit = contract.SupportedVersions.Oldest.SourceCommit
		}},
		{"shared-digest-and-commit", func(contract *RecoveryContract) {
			contract.SupportedVersions.Candidate.ImageDigest = contract.SupportedVersions.Oldest.ImageDigest
			contract.SupportedVersions.Candidate.SourceCommit = contract.SupportedVersions.Oldest.SourceCommit
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			contract := validContract(t)
			test.mutate(&contract)
			contract = sealContract(t, contract)
			if err := contract.Validate(); err != nil {
				t.Fatal(err)
			}
			if _, _, err := LoadContract(writeBytes(t, contractJSON(t, contract))); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestContractValidateHashBinding(t *testing.T) {
	contract := validContract(t)
	contract.Objectives.RPOSeconds++
	assertFailure(t, contract.Validate(), "INVALID_CONTRACT", "HASH_MISMATCH", "sha256")
}

func TestContractRefWireShape(t *testing.T) {
	ref := ContractRef{ID: "recovery-v1", Revision: 1, SHA256: testDigest("contract-ref")}
	raw, err := json.Marshal(ref)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"id":"recovery-v1","revision":1,"sha256":"` + ref.SHA256 + `"}`
	if string(raw) != want {
		t.Fatalf("ContractRef JSON = %s, want %s", raw, want)
	}
}

func TestCanonicalKeyOrderAndHashExclusion(t *testing.T) {
	first := []byte(`{"runbook":{"sha256":"nested","revision":"runbook-v1"},"revision":1,"contractId":"recovery-v1","sha256":"first"}`)
	second := []byte(`{"sha256":"second","contractId":"recovery-v1","runbook":{"revision":"runbook-v1","sha256":"nested"},"revision":1}`)
	firstBytes, firstHash, firstErr := canonicalizeContractJSON(first)
	secondBytes, secondHash, secondErr := canonicalizeContractJSON(second)
	if firstErr != nil || secondErr != nil {
		t.Fatalf("canonical errors: %v, %v", firstErr, secondErr)
	}
	if !bytes.Equal(firstBytes, secondBytes) || firstHash != secondHash || string(firstBytes) != "{\"contractId\":\"recovery-v1\",\"revision\":1,\"runbook\":{\"revision\":\"runbook-v1\",\"sha256\":\"nested\"}}\n" {
		t.Fatalf("canonical mismatch: %q/%s and %q/%s", firstBytes, firstHash, secondBytes, secondHash)
	}
	_, _, err := canonicalizeContractJSON(bytes.Repeat([]byte(" "), maxContractBytes+1))
	assertFailure(t, err, "INVALID_CONTRACT", "INPUT_TOO_LARGE", "$")
	deep := []byte(`{"revision":` + strings.Repeat("[", maxJSONDepth+1) + "0" + strings.Repeat("]", maxJSONDepth+1) + "}")
	_, _, err = canonicalizeContractJSON(deep)
	assertFailure(t, err, "INVALID_CONTRACT", "JSON_TOO_DEEP", "$.revision"+strings.Repeat("[]", maxJSONDepth-1))
}

type contractCase struct {
	name, code, reason, field string
	mutate                    func(*RecoveryContract)
}

func TestContractInvariantPartitions(t *testing.T) {
	cases := []contractCase{
		{"missing approval time", "UNAPPROVED_CONTRACT", "MISSING_APPROVED_AT", "approvedAt", func(c *RecoveryContract) { c.ApprovedAt = time.Time{} }},
		{"invalid contract hash", "INVALID_CONTRACT", "INVALID_HASH", "sha256", func(c *RecoveryContract) { c.SHA256 = "bad" }},
		{"placeholder contract hash", "INVALID_CONTRACT", "PLACEHOLDER_VALUE", "sha256", func(c *RecoveryContract) { c.SHA256 = "sha256:" + strings.Repeat("0", 64) }},
		{"repeated contract hash", "INVALID_CONTRACT", "PLACEHOLDER_VALUE", "sha256", func(c *RecoveryContract) { c.SHA256 = "sha256:" + strings.Repeat("d", 64) }},
		{"empty approvers", "UNAPPROVED_CONTRACT", "EMPTY_APPROVED_BY", "approvedBy", func(c *RecoveryContract) { c.ApprovedBy = nil }},
		{"unsorted approvers", "INVALID_CONTRACT", "INVALID_APPROVER_SET", "approvedBy", func(c *RecoveryContract) { c.ApprovedBy = []string{"operations", "data-owner"} }},
		{"duplicate approver", "INVALID_CONTRACT", "INVALID_APPROVER_SET", "approvedBy", func(c *RecoveryContract) { c.ApprovedBy = []string{"data-owner", "data-owner"} }},
		{"invalid contract id", "INVALID_CONTRACT", "INVALID_IDENTIFIER", "contractId", func(c *RecoveryContract) { c.ContractID = "Bad" }},
		{"zero revision", "INVALID_CONTRACT", "INVALID_REVISION", "revision", func(c *RecoveryContract) { c.Revision = 0 }},
		{"placeholder scope", "INVALID_CONTRACT", "PLACEHOLDER_VALUE", "scopeId", func(c *RecoveryContract) { c.ScopeID = "proposed" }},
		{"placeholder repository", "INVALID_CONTRACT", "PLACEHOLDER_VALUE", "repositoryPolicyId", func(c *RecoveryContract) { c.RepositoryPolicyID = "repository-proposed" }},
		{"draft repository", "INVALID_CONTRACT", "PLACEHOLDER_VALUE", "repositoryPolicyId", func(c *RecoveryContract) { c.RepositoryPolicyID = "repository-draft" }},
		{"invalid runbook revision", "INVALID_CONTRACT", "INVALID_IDENTIFIER", "runbook.revision", func(c *RecoveryContract) { c.Runbook.Revision = "" }},
		{"pending runbook revision", "INVALID_CONTRACT", "PLACEHOLDER_VALUE", "runbook.revision", func(c *RecoveryContract) { c.Runbook.Revision = "runbook-pending" }},
		{"invalid scope enum", "INVALID_CONTRACT", "INVALID_ENUM", "scopeKind", func(c *RecoveryContract) { c.ScopeKind = "cluster" }},
		{"invalid path enum", "INVALID_CONTRACT", "INVALID_ENUM", "canonicalPath", func(c *RecoveryContract) { c.CanonicalPath = "other" }},
		{"invalid branch enum", "INVALID_CONTRACT", "INVALID_ENUM", "supportedVersions.recoveryBranch", func(c *RecoveryContract) { c.SupportedVersions.RecoveryBranch = "other" }},
		{"zero rpo", "INVALID_CONTRACT", "NON_POSITIVE_OBJECTIVE", "objectives.rpoSeconds", func(c *RecoveryContract) { c.Objectives.RPOSeconds = 0 }},
		{"zero rto", "INVALID_CONTRACT", "NON_POSITIVE_OBJECTIVE", "objectives.rtoSeconds", func(c *RecoveryContract) { c.Objectives.RTOSeconds = 0 }},
		{"zero preflight", "INVALID_CONTRACT", "NON_POSITIVE_OBJECTIVE", "objectives.preflightSeconds", func(c *RecoveryContract) { c.Objectives.PreflightSeconds = 0 }},
		{"wrong declaration event", "INVALID_CONTRACT", "INVALID_MEASUREMENT", "measurement.declarationEvent", func(c *RecoveryContract) { c.Measurement.DeclarationEvent = "wrong" }},
		{"wrong recovery event", "INVALID_CONTRACT", "INVALID_MEASUREMENT", "measurement.recoveryPointEvent", func(c *RecoveryContract) { c.Measurement.RecoveryPointEvent = "wrong" }},
		{"wrong ready event", "INVALID_CONTRACT", "INVALID_MEASUREMENT", "measurement.readyEvent", func(c *RecoveryContract) { c.Measurement.ReadyEvent = "wrong" }},
		{"wrong clock", "INVALID_CONTRACT", "INVALID_MEASUREMENT", "measurement.clockSource", func(c *RecoveryContract) { c.Measurement.ClockSource = "local" }},
		{"zero bytes", "INVALID_CONTRACT", "NON_POSITIVE_WORKLOAD", "workload.logicalBytes", func(c *RecoveryContract) { c.Workload.LogicalBytes = 0 }},
		{"zero shards", "INVALID_CONTRACT", "NON_POSITIVE_WORKLOAD", "workload.shards", func(c *RecoveryContract) { c.Workload.Shards = 0 }},
		{"zero replication", "INVALID_CONTRACT", "NON_POSITIVE_WORKLOAD", "workload.replicationFactor", func(c *RecoveryContract) { c.Workload.ReplicationFactor = 0 }},
		{"zero writes", "INVALID_CONTRACT", "NON_POSITIVE_WORKLOAD", "workload.writesPerSecond", func(c *RecoveryContract) { c.Workload.WritesPerSecond = 0 }},
		{"zero reads", "INVALID_CONTRACT", "NON_POSITIVE_WORKLOAD", "workload.readsPerSecond", func(c *RecoveryContract) { c.Workload.ReadsPerSecond = 0 }},
		{"zero minimum age", "INVALID_CONTRACT", "NON_POSITIVE_RETENTION", "retention.minimumAgeSeconds", func(c *RecoveryContract) { c.Retention.MinimumAgeSeconds = 0 }},
		{"zero qualified sets", "INVALID_CONTRACT", "NON_POSITIVE_RETENTION", "retention.minimumQualifiedSets", func(c *RecoveryContract) { c.Retention.MinimumQualifiedSets = 0 }},
		{"zero evidence", "INVALID_CONTRACT", "NON_POSITIVE_RETENTION", "retention.evidenceSeconds", func(c *RecoveryContract) { c.Retention.EvidenceSeconds = 0 }},
		{"short evidence", "INVALID_CONTRACT", "INVALID_RETENTION", "retention.evidenceSeconds", func(c *RecoveryContract) { c.Retention.EvidenceSeconds = 1 }},
		{"missing check", "INVALID_CONTRACT", "INVALID_CHECK_SET", "requiredChecks", func(c *RecoveryContract) { c.RequiredChecks = c.RequiredChecks[:4] }},
		{"duplicate check", "INVALID_CONTRACT", "INVALID_CHECK_SET", "requiredChecks", func(c *RecoveryContract) { c.RequiredChecks[4] = "REC-04" }},
		{"extra check", "INVALID_CONTRACT", "INVALID_CHECK_SET", "requiredChecks", func(c *RecoveryContract) { c.RequiredChecks = append(c.RequiredChecks, "REC-06") }},
		{"empty manifests", "INVALID_CONTRACT", "INVALID_MANIFEST_VERSION", "manifestVersions", func(c *RecoveryContract) { c.ManifestVersions = nil }},
		{"unsupported manifest", "INVALID_CONTRACT", "INVALID_MANIFEST_VERSION", "manifestVersions", func(c *RecoveryContract) { c.ManifestVersions = []uint32{2} }},
		{"duplicate manifests", "INVALID_CONTRACT", "INVALID_MANIFEST_VERSION", "manifestVersions", func(c *RecoveryContract) { c.ManifestVersions = []uint32{0, 0} }},
		{"unordered manifests", "INVALID_CONTRACT", "INVALID_MANIFEST_VERSION", "manifestVersions", func(c *RecoveryContract) { c.ManifestVersions = []uint32{1, 0} }},
		{"placeholder release", "INVALID_RELEASE_IDENTITY", "PLACEHOLDER_VALUE", "supportedVersions.oldest.imageDigest", func(c *RecoveryContract) {
			c.SupportedVersions.Oldest.ImageDigest = "sha256:" + strings.Repeat("0", 64)
		}},
		{"invalid candidate", "INVALID_RELEASE_IDENTITY", "MALFORMED_IMAGE_DIGEST", "supportedVersions.candidate.imageDigest", func(c *RecoveryContract) { c.SupportedVersions.Candidate.ImageDigest = "latest" }},
		{"identical releases", "INVALID_CONTRACT", "IDENTICAL_RELEASES", "supportedVersions", func(c *RecoveryContract) { c.SupportedVersions.Candidate = c.SupportedVersions.Oldest }},
		{"missing runbook hash", "INVALID_CONTRACT", "INVALID_RUNBOOK_HASH", "runbook.sha256", func(c *RecoveryContract) { c.Runbook.SHA256 = "" }},
		{"invalid runbook hash", "INVALID_CONTRACT", "INVALID_RUNBOOK_HASH", "runbook.sha256", func(c *RecoveryContract) { c.Runbook.SHA256 = "latest" }},
		{"placeholder runbook hash", "INVALID_CONTRACT", "INVALID_RUNBOOK_HASH", "runbook.sha256", func(c *RecoveryContract) { c.Runbook.SHA256 = "sha256:" + strings.Repeat("c", 64) }},
		{"zero lease duration", "INVALID_CONTRACT", "INVALID_LEASE", "lease", func(c *RecoveryContract) { c.Lease.DurationSeconds = 0 }},
		{"zero lease renewal", "INVALID_CONTRACT", "INVALID_LEASE", "lease", func(c *RecoveryContract) { c.Lease.RenewBeforeSeconds = 0 }},
		{"late lease renewal", "INVALID_CONTRACT", "INVALID_LEASE", "lease", func(c *RecoveryContract) { c.Lease.RenewBeforeSeconds = c.Lease.DurationSeconds }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			contract := validContract(t)
			test.mutate(&contract)
			assertFailure(t, contract.Validate(), test.code, test.reason, test.field)
			if test.field == "sha256" {
				return
			}
			assertLoadFailure(t, contractJSON(t, contract), test.code, test.reason, test.field)
		})
	}
}

func TestReleaseIdentity(t *testing.T) {
	valid := validContract(t).SupportedVersions.Oldest
	cases := []struct {
		name, code, reason, field string
		mutate                    func(*ReleaseIdentity)
	}{
		{"valid", "", "", "", func(*ReleaseIdentity) {}},
		{"missing digest", "MUTABLE_RELEASE_IDENTITY", "MISSING_IMAGE_DIGEST", "imageDigest", func(r *ReleaseIdentity) { r.ImageDigest = "" }},
		{"malformed digest", "INVALID_RELEASE_IDENTITY", "MALFORMED_IMAGE_DIGEST", "imageDigest", func(r *ReleaseIdentity) { r.ImageDigest = "latest" }},
		{"zero digest", "INVALID_RELEASE_IDENTITY", "PLACEHOLDER_VALUE", "imageDigest", func(r *ReleaseIdentity) { r.ImageDigest = "sha256:" + strings.Repeat("0", 64) }},
		{"repeated digest", "INVALID_RELEASE_IDENTITY", "PLACEHOLDER_VALUE", "imageDigest", func(r *ReleaseIdentity) { r.ImageDigest = "sha256:" + strings.Repeat("a", 64) }},
		{"mutable version", "INVALID_RELEASE_IDENTITY", "MUTABLE_VERSION", "version", func(r *ReleaseIdentity) { r.Version = "latest" }},
		{"malformed commit", "INVALID_RELEASE_IDENTITY", "MALFORMED_SOURCE_COMMIT", "sourceCommit", func(r *ReleaseIdentity) { r.SourceCommit = "ABC" }},
		{"zero commit", "INVALID_RELEASE_IDENTITY", "PLACEHOLDER_VALUE", "sourceCommit", func(r *ReleaseIdentity) { r.SourceCommit = strings.Repeat("0", 40) }},
		{"repeated commit", "INVALID_RELEASE_IDENTITY", "PLACEHOLDER_VALUE", "sourceCommit", func(r *ReleaseIdentity) { r.SourceCommit = strings.Repeat("b", 40) }},
		{"http provenance", "INVALID_RELEASE_IDENTITY", "INVALID_PROVENANCE_URI", "provenanceURI", func(r *ReleaseIdentity) { r.ProvenanceURI = "http://releases.example.com/x" }},
		{"userinfo provenance", "INVALID_RELEASE_IDENTITY", "INVALID_PROVENANCE_URI", "provenanceURI", func(r *ReleaseIdentity) { r.ProvenanceURI = "https://user@releases.example.com/x" }},
		{"placeholder provenance", "INVALID_RELEASE_IDENTITY", "INVALID_PROVENANCE_URI", "provenanceURI", func(r *ReleaseIdentity) { r.ProvenanceURI = "https://example.invalid/x" }},
		{"reserved provenance", "INVALID_RELEASE_IDENTITY", "INVALID_PROVENANCE_URI", "provenanceURI", func(r *ReleaseIdentity) { r.ProvenanceURI = "https://releases.example.com/x" }},
		{"query provenance", "INVALID_RELEASE_IDENTITY", "INVALID_PROVENANCE_URI", "provenanceURI", func(r *ReleaseIdentity) { r.ProvenanceURI = "https://releases.cefasdb.com/x?token=secret" }},
		{"fragment provenance", "INVALID_RELEASE_IDENTITY", "INVALID_PROVENANCE_URI", "provenanceURI", func(r *ReleaseIdentity) { r.ProvenanceURI = "https://releases.cefasdb.com/x#token" }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			release := valid
			test.mutate(&release)
			err := release.Validate()
			if test.code == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			assertFailure(t, err, test.code, test.reason, test.field)
		})
	}
}

func TestContractHostileInput(t *testing.T) {
	valid := contractJSON(t, validContract(t))
	cases := []struct {
		name, reason, field string
		data                []byte
	}{
		{"stale hash", "HASH_MISMATCH", "sha256", bytes.Replace(valid, []byte(`"rpoSeconds":3600`), []byte(`"rpoSeconds":3601`), 1)},
		{"missing hash", "MISSING_FIELD", "sha256", []byte(`{"contractId":"recovery-v1"}`)},
		{"malformed hash", "INVALID_HASH", "sha256", bytes.Replace(valid, []byte(`"sha256":"sha256:`), []byte(`"sha256":"bad`), 1)},
		{"wrong field type", "INVALID_JSON", "$", bytes.Replace(valid, []byte(`"revision":1`), []byte(`"revision":"one"`), 1)},
		{"malformed value", "INVALID_JSON", "$.revision", []byte(`{"revision":}`)},
		{"duplicate key", "DUPLICATE_KEY", "$.contractId", bytes.Replace(valid, []byte(`{"contractId":`), []byte(`{"contractId":"duplicate","contractId":`), 1)},
		{"unknown key", "UNKNOWN_FIELD", "$.unexpected", bytes.Replace(valid, []byte("{"), []byte(`{"unexpected":0,`), 1)},
		{"trailing json", "TRAILING_JSON", "$", append(append([]byte(nil), valid...), []byte("{}")...)},
		{"fractional number", "NON_INTEGER_NUMBER", "$.revision", bytes.Replace(valid, []byte(`"revision":1`), []byte(`"revision":1.5`), 1)},
		{"exponent number", "NON_INTEGER_NUMBER", "$.revision", bytes.Replace(valid, []byte(`"revision":1`), []byte(`"revision":1e0`), 1)},
		{"invalid utf8", "INVALID_UTF8", "$", append(append([]byte(nil), valid...), 0xff)},
		{"carriage return", "INVALID_LINE_ENDING", "$", bytes.Replace(valid, []byte("{"), []byte("{\r"), 1)},
		{"top-level array", "TOP_LEVEL_NOT_OBJECT", "$", []byte("[]")},
		{"malformed json", "INVALID_JSON", "$", []byte("{")},
		{"oversized", "INPUT_TOO_LARGE", "$", bytes.Repeat([]byte(" "), maxContractBytes+1)},
		{"warning on approved", "INVALID_WARNING", "warning", bytes.Replace(valid, []byte("{"), []byte(`{"warning":"not allowed",`), 1)},
		{"null warning on approved", "INVALID_WARNING", "warning", bytes.Replace(valid, []byte("{"), []byte(`{"warning":null,`), 1)},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			assertLoadFailure(t, test.data, "INVALID_CONTRACT", test.reason, test.field)
		})
	}
}

func TestExampleContractRejected(t *testing.T) {
	contract, ref, err := LoadContract(filepath.Join("..", "..", "..", "deploy", "recovery-contract.example.json"))
	assertFailure(t, err, "UNAPPROVED_CONTRACT", "MISSING_APPROVED_AT", "approvedAt")
	if !reflect.DeepEqual(contract, RecoveryContract{}) || ref != (ContractRef{}) {
		t.Fatal("example returned partial output")
	}
}

func FuzzLoadContract(f *testing.F) {
	valid := contractJSON(f, validContract(f))
	f.Add(valid)
	f.Add([]byte{})
	f.Add([]byte("{"))
	f.Add([]byte{0xff})
	f.Add(bytes.Replace(valid, []byte(`{"contractId":`), []byte(`{"contractId":"x","contractId":`), 1))
	f.Fuzz(func(t *testing.T, raw []byte) {
		contract, ref, err := LoadContract(writeBytes(t, raw))
		if err != nil {
			if !reflect.DeepEqual(contract, RecoveryContract{}) || ref != (ContractRef{}) {
				t.Fatal("failure returned partial output")
			}
			return
		}
		if reflect.DeepEqual(contract, RecoveryContract{}) || ref == (ContractRef{}) {
			t.Fatal("success returned zero output")
		}
	})
}
