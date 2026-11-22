// Package storetest provides a reusable EvidenceStore conformance suite.
package storetest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/CefasDb/cefasdb/internal/recovery/evidence"
	"github.com/CefasDb/cefasdb/internal/recovery/model"
)

type Driver struct {
	Open      func(testing.TB, string) (evidence.EvidenceStore, func() evidence.EvidenceStore)
	Seal      func(testing.TB, evidence.QualificationRecord) evidence.QualificationRecord
	FailNext  func(evidence.EvidenceStore)
	Token     func(scope, after string, version byte) string
	IsDurable bool
}

func digest(s string) string {
	sum := sha256.Sum256([]byte(s))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func release(version, seed string) model.ReleaseIdentity {
	sum := sha256.Sum256([]byte("commit-" + seed))
	return model.ReleaseIdentity{Version: version, ImageDigest: digest("image-" + seed), SourceCommit: hex.EncodeToString(sum[:])[:40], ProvenanceURI: "https://releases.cefasdb.com/" + seed}
}

func record(id string, at time.Time) evidence.QualificationRecord {
	return evidence.QualificationRecord{
		SchemaVersion: evidence.SchemaVersion, ScopeID: "scope-a", RunID: id, RunRevision: 5,
		ContractRef:  model.ContractRef{ID: "recovery-v1", Revision: 1, SHA256: digest("contract")},
		BackupSetRef: "backup-" + id, SourceRelease: release("1.0.0", "source"), CandidateRelease: release("2.0.0", "candidate"),
		TargetEnvironmentID: "target-rf3", TargetProofRef: "proof-" + id, RestoreRunRef: "restore-" + id,
		ReconciliationReportRef: "reconcile-" + id, UpgradeRunRef: "upgrade-" + id, Status: evidence.StatusPass,
		TargetMutated: true, MeasuredRPOSeconds: 10, MeasuredRTOSeconds: 20, OperatorID: "operator-1",
		RunbookRevision: "runbook-v1", RunbookSHA256: digest("runbook"), DiagnosticsRef: "diagnostics-" + id,
		Findings:  []evidence.Finding{{CheckID: "REC-01", Owner: "team-cefasdb", Reason: "counts and hashes matched", EvidenceRef: "finding-" + id}},
		StartedAt: at, CompletedAt: at.Add(time.Minute),
	}
}

// SampleRecord returns a valid unsealed record for adapter-specific tests.
func SampleRecord(id string, at time.Time) evidence.QualificationRecord { return record(id, at) }

func want(t *testing.T, err, target error) {
	t.Helper()
	if !errors.Is(err, target) {
		t.Fatalf("error = %v, want %v", err, target)
	}
}

func empty(t *testing.T, store evidence.EvidenceStore, runID string) {
	t.Helper()
	_, err := store.Get(context.Background(), "scope-a", runID)
	want(t, err, evidence.ErrEvidenceNotFound)
	page, err := store.List(context.Background(), "scope-a", 100, "")
	if err != nil || len(page.Items) != 0 {
		t.Fatalf("rejection left evidence: %#v, %v", page, err)
	}
}

func rejected(t *testing.T, driver Driver, input evidence.QualificationRecord, target error) {
	t.Helper()
	store, _ := driver.Open(t, "scope-a")
	ref, err := store.PutTerminal(context.Background(), input)
	if ref != (evidence.EvidenceRef{}) {
		t.Fatalf("failure returned ref %#v", ref)
	}
	want(t, err, target)
	if errors.Is(target, evidence.ErrEvidenceRedactionFailed) {
		var failure *evidence.Error
		if !errors.As(err, &failure) || failure.Field == "" {
			t.Fatalf("redaction error = %#v, want safe field path", err)
		}
	}
	empty(t, store, input.RunID)
}

// Run executes the complete adapter-neutral contract.
func Run(t *testing.T, driver Driver) {
	at := time.Date(2026, 11, 22, 3, 0, 0, 0, time.UTC)
	store, reopen := driver.Open(t, "scope-a")
	if store.Durable() != driver.IsDurable {
		t.Fatalf("Durable() = %t, want %t", store.Durable(), driver.IsDurable)
	}
	first := driver.Seal(t, record("terminal", at))
	ref, err := store.PutTerminal(context.Background(), first)
	if err != nil {
		t.Fatal(err)
	}
	again, err := store.PutTerminal(context.Background(), first)
	if err != nil || ref != again {
		t.Fatalf("identical retry = %#v, %v; want %#v", again, err, ref)
	}
	changed := first
	changed.Status = evidence.StatusNoGo
	changed = driver.Seal(t, changed)
	_, err = store.PutTerminal(context.Background(), changed)
	want(t, err, evidence.ErrEvidenceConflict)
	stored, _ := store.Get(context.Background(), "scope-a", first.RunID)
	if !reflect.DeepEqual(stored, first) {
		t.Fatal("conflict changed first record")
	}
	reloaded, err := reopen().Get(context.Background(), "scope-a", first.RunID)
	if err != nil || !reflect.DeepEqual(reloaded, first) {
		t.Fatalf("reopen = %#v, %v", reloaded, err)
	}

	first.Findings[0].Reason = "mutated input"
	read, _ := store.Get(context.Background(), "scope-a", "terminal")
	read.Findings[0].Reason = "mutated output"
	read, _ = store.Get(context.Background(), "scope-a", "terminal")
	if read.Findings[0].Reason != "counts and hashes matched" {
		t.Fatal("caller mutation changed committed evidence")
	}

	failed := driver.Seal(t, record("durability", at))
	failedStore, _ := driver.Open(t, "scope-a")
	driver.FailNext(failedStore)
	ref, err = failedStore.PutTerminal(context.Background(), failed)
	if ref != (evidence.EvidenceRef{}) {
		t.Fatal("durability failure returned a ref")
	}
	want(t, err, evidence.ErrEvidenceNotDurable)
	empty(t, failedStore, failed.RunID)

	hashMutation := driver.Seal(t, record("hash", at))
	hashMutation.Findings[0].Reason = "changed after sealing"
	rejected(t, driver, hashMutation, evidence.ErrEvidenceHashMismatch)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cancelled := driver.Seal(t, record("cancelled", at))
	cancelStore, _ := driver.Open(t, "scope-a")
	ref, err = cancelStore.PutTerminal(ctx, cancelled)
	if ref != (evidence.EvidenceRef{}) {
		t.Fatal("cancelled write returned a ref")
	}
	want(t, err, context.Canceled)
	empty(t, cancelStore, cancelled.RunID)

	fixtures := []func(*evidence.QualificationRecord){
		func(r *evidence.QualificationRecord) { r.Findings[0].Reason = "Authorization header was present" },
		func(r *evidence.QualificationRecord) { r.Findings[0].Reason = "password field was present" },
		func(r *evidence.QualificationRecord) { r.Findings[0].Reason = "Bearer abc.def.ghi" },
		func(r *evidence.QualificationRecord) {
			r.DiagnosticsRef = "https://objects.cefasdb.com/x?X-Amz-Signature=abc"
		},
		func(r *evidence.QualificationRecord) { r.Findings[0].Reason = `{"row":{"id":1}}` },
		func(r *evidence.QualificationRecord) { r.Findings[0].Reason = `{"item":{"id":1}}` },
		func(r *evidence.QualificationRecord) { r.Findings[0].Reason = `{"payload":{"id":1}}` },
	}
	for i, mutate := range fixtures {
		input := record("redaction-"+string(rune('a'+i)), at)
		mutate(&input)
		input = driver.Seal(t, input)
		rejected(t, driver, input, evidence.ErrEvidenceRedactionFailed)
	}

	denied := driver.Seal(t, record("denied", at))
	denied.ScopeID = "scope-b"
	_, err = store.PutTerminal(context.Background(), denied)
	want(t, err, evidence.ErrScopeDenied)
	_, err = store.Get(context.Background(), "scope-b", "terminal")
	want(t, err, evidence.ErrScopeDenied)
	_, err = store.List(context.Background(), "scope-b", 0, "secret")
	want(t, err, evidence.ErrScopeDenied)

	for _, id := range []string{"page-a", "page-b"} {
		input := driver.Seal(t, record(id, at))
		if _, err = store.PutTerminal(context.Background(), input); err != nil {
			t.Fatal(err)
		}
	}
	for _, size := range []int{-1, 0, 101} {
		_, err = store.List(context.Background(), "scope-a", size, "")
		want(t, err, evidence.ErrInvalidPageSize)
	}
	one, err := store.List(context.Background(), "scope-a", 1, "")
	if err != nil || len(one.Items) != 1 || one.NextPageToken == "" {
		t.Fatalf("size 1 = %#v, %v", one, err)
	}
	if page, e := store.List(context.Background(), "scope-a", 100, ""); e != nil || len(page.Items) != 3 {
		t.Fatalf("size 100 = %#v, %v", page, e)
	}
	for _, token := range []string{"!", driver.Token("scope-a", "page-a", 2), driver.Token("scope-b", "page-a", 1)} {
		_, err = store.List(context.Background(), "scope-a", 1, token)
		want(t, err, evidence.ErrInvalidPageToken)
	}
	altered := one.NextPageToken[:len(one.NextPageToken)-1] + "A"
	if altered == one.NextPageToken {
		altered = one.NextPageToken[:len(one.NextPageToken)-1] + "B"
	}
	_, err = store.List(context.Background(), "scope-a", 1, altered)
	want(t, err, evidence.ErrInvalidPageToken)
}
