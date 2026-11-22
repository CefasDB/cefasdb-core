// Package storetest provides a reusable EvidenceStore conformance suite.
package storetest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/CefasDb/cefasdb/internal/recovery/evidence"
	"github.com/CefasDb/cefasdb/internal/recovery/model"
)

// ScopeOpener opens scope-authorized handles over one shared backing store.
type ScopeOpener func(string) (evidence.EvidenceStore, func() evidence.EvidenceStore)

// Driver supplies an EvidenceStore implementation to the conformance suite.
type Driver struct {
	New       func(testing.TB) ScopeOpener
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
		ReconciliationReportRef: "reconcile-" + id, UpgradeRunRef: "upgrade-" + id, Status: evidence.StatusNoGo,
		TargetMutated: true, MeasuredRPOSeconds: 10, MeasuredRTOSeconds: 20, OperatorID: "operator-1",
		RunbookRevision: "runbook-v1", RunbookSHA256: digest("runbook"), DiagnosticsRef: "diagnostics-" + id,
		Findings:  []evidence.Finding{{CheckID: "REC-01", Owner: "team-cefasdb", Reason: evidence.FindingReasonCheckFailed, EvidenceRef: "finding-" + id}},
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
	store, _ := driver.New(t)("scope-a")
	ref, err := store.PutTerminal(context.Background(), input)
	if ref != (evidence.EvidenceRef{}) {
		t.Fatalf("failure returned ref %#v", ref)
	}
	want(t, err, target)
	rejectedValues := []string{
		input.RunID,
		input.SourceRelease.Version,
		input.CandidateRelease.Version,
		input.TargetEnvironmentID,
		input.OperatorID,
		input.DiagnosticsRef,
		input.Findings[0].CheckID,
		input.Findings[0].Owner,
		string(input.Findings[0].Reason),
		input.Findings[0].EvidenceRef,
	}
	for _, rejectedValue := range rejectedValues {
		if len(rejectedValue) > 16 && strings.Contains(err.Error(), rejectedValue) {
			t.Fatalf("error exposed rejected value from %s", input.RunID)
		}
	}
	if errors.Is(target, evidence.ErrEvidenceRedactionFailed) {
		var failure *evidence.Error
		if !errors.As(err, &failure) || failure.Field == "" {
			t.Fatalf("redaction error = %#v, want safe field path", err)
		}
	}
	empty(t, store, input.RunID)
}

type terminalResult struct {
	input evidence.QualificationRecord
	ref   evidence.EvidenceRef
	err   error
}

func concurrentPut(store evidence.EvidenceStore, inputs ...evidence.QualificationRecord) <-chan terminalResult {
	start := make(chan struct{})
	results := make(chan terminalResult, len(inputs))
	var group sync.WaitGroup
	for _, input := range inputs {
		group.Add(1)
		// Each writer exits after one PutTerminal call; the WaitGroup closes results after every writer exits.
		go func(candidate evidence.QualificationRecord) {
			defer group.Done()
			<-start
			ref, err := store.PutTerminal(context.Background(), candidate)
			results <- terminalResult{input: candidate, ref: ref, err: err}
		}(input)
	}
	close(start)
	group.Wait()
	close(results)
	return results
}

func verifyConcurrentPut(t *testing.T, driver Driver, at time.Time) {
	t.Helper()
	store, _ := driver.New(t)("scope-a")
	left := driver.Seal(t, record("terminal-race", at))
	right := evidence.CopyRecord(left)
	right.DiagnosticsRef = "diagnostics-terminal-race-other"
	right = driver.Seal(t, right)

	wins, conflicts := 0, 0
	var winner terminalResult
	for outcome := range concurrentPut(store, left, right) {
		switch {
		case outcome.err == nil:
			wins++
			winner = outcome
			if outcome.ref == (evidence.EvidenceRef{}) {
				t.Fatal("concurrent winner returned an empty ref")
			}
		case errors.Is(outcome.err, evidence.ErrEvidenceConflict):
			conflicts++
			if outcome.ref != (evidence.EvidenceRef{}) {
				t.Fatal("concurrent conflict returned a ref")
			}
		default:
			t.Fatal(outcome.err)
		}
	}
	committed, err := store.Get(context.Background(), "scope-a", left.RunID)
	if err != nil || wins != 1 || conflicts != 1 || !reflect.DeepEqual(committed, winner.input) {
		t.Fatalf("concurrent PutTerminal wins=%d conflicts=%d committed=%#v err=%v", wins, conflicts, committed, err)
	}
}

func verifyScopeIsolation(t *testing.T, driver Driver, at time.Time) {
	t.Helper()
	open := driver.New(t)
	scopeA, _ := open("scope-a")
	scopeB, _ := open("scope-b")
	left := driver.Seal(t, record("shared-run", at))
	right := evidence.CopyRecord(left)
	right.ScopeID = "scope-b"
	right = driver.Seal(t, right)

	leftRef, err := scopeA.PutTerminal(context.Background(), left)
	if err != nil {
		t.Fatal(err)
	}
	rightRef, err := scopeB.PutTerminal(context.Background(), right)
	if err != nil {
		t.Fatal(err)
	}
	if leftRef.Key == rightRef.Key {
		t.Fatalf("scope-qualified evidence keys collided: %q", leftRef.Key)
	}
	leftStored, leftErr := scopeA.Get(context.Background(), "scope-a", left.RunID)
	rightStored, rightErr := scopeB.Get(context.Background(), "scope-b", right.RunID)
	if leftErr != nil || rightErr != nil || !reflect.DeepEqual(leftStored, left) || !reflect.DeepEqual(rightStored, right) {
		t.Fatalf("scope-qualified records = %#v, %v; %#v, %v", leftStored, leftErr, rightStored, rightErr)
	}
	leftPage, leftErr := scopeA.List(context.Background(), "scope-a", 100, "")
	rightPage, rightErr := scopeB.List(context.Background(), "scope-b", 100, "")
	if leftErr != nil || rightErr != nil || len(leftPage.Items) != 1 || len(rightPage.Items) != 1 {
		t.Fatalf("scope-qualified lists = %#v, %v; %#v, %v", leftPage, leftErr, rightPage, rightErr)
	}
}

// Run executes the complete adapter-neutral contract.
func Run(t *testing.T, driver Driver) {
	at := time.Date(2026, 11, 22, 3, 0, 0, 0, time.UTC)
	open := driver.New(t)
	store, reopen := open("scope-a")
	if store.Durable() != driver.IsDurable {
		t.Fatalf("Durable() = %t, want %t", store.Durable(), driver.IsDurable)
	}
	slashStore, _ := open("scope/a")
	slashScoped := record("slash-scope", at)
	slashScoped.ScopeID = "scope/a"
	slashScoped = driver.Seal(t, slashScoped)
	if _, err := slashStore.PutTerminal(context.Background(), slashScoped); err != nil {
		t.Fatalf("canonical slash scope was rejected: %v", err)
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
	changed.DiagnosticsRef = "diagnostics-terminal-other"
	changed = driver.Seal(t, changed)
	_, err = store.PutTerminal(context.Background(), changed)
	want(t, err, evidence.ErrEvidenceConflict)
	stored, _ := store.Get(context.Background(), "scope-a", first.RunID)
	if !reflect.DeepEqual(stored, first) {
		t.Fatal("conflict changed first record")
	}
	verifyConcurrentPut(t, driver, at)
	reopened := reopen()
	reloaded, err := reopened.Get(context.Background(), "scope-a", first.RunID)
	if err != nil || !reflect.DeepEqual(reloaded, first) {
		t.Fatalf("reopen = %#v, %v", reloaded, err)
	}
	retriedRef, err := reopened.PutTerminal(context.Background(), first)
	if err != nil || retriedRef != ref {
		t.Fatalf("retry after reopen = %#v, %v; want %#v", retriedRef, err, ref)
	}

	first.Findings[0].Reason = "mutated input"
	read, _ := store.Get(context.Background(), "scope-a", "terminal")
	read.Findings[0].Reason = "mutated output"
	read, _ = store.Get(context.Background(), "scope-a", "terminal")
	if read.Findings[0].Reason != evidence.FindingReasonCheckFailed {
		t.Fatal("caller mutation changed committed evidence")
	}

	failed := driver.Seal(t, record("durability", at))
	failedStore, _ := driver.New(t)("scope-a")
	driver.FailNext(failedStore)
	ref, err = failedStore.PutTerminal(context.Background(), failed)
	if ref != (evidence.EvidenceRef{}) {
		t.Fatal("durability failure returned a ref")
	}
	want(t, err, evidence.ErrEvidenceNotDurable)
	empty(t, failedStore, failed.RunID)

	hashMutation := driver.Seal(t, record("hash", at))
	hashMutation.Findings[0].Reason = evidence.FindingReasonCheckMissing
	rejected(t, driver, hashMutation, evidence.ErrEvidenceHashMismatch)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cancelled := driver.Seal(t, record("cancelled", at))
	cancelStore, _ := driver.New(t)("scope-a")
	ref, err = cancelStore.PutTerminal(ctx, cancelled)
	if ref != (evidence.EvidenceRef{}) {
		t.Fatal("cancelled write returned a ref")
	}
	want(t, err, context.Canceled)
	empty(t, cancelStore, cancelled.RunID)

	fixtures := []func(*evidence.QualificationRecord){
		func(r *evidence.QualificationRecord) { r.Findings[0].Reason = "Authorization header was present" },
		func(r *evidence.QualificationRecord) { r.Findings[0].Reason = "clientSecret field was present" },
		func(r *evidence.QualificationRecord) { r.Findings[0].Reason = "Bearer abc.def.ghi" },
		func(r *evidence.QualificationRecord) { r.Findings[0].Reason = "token: eyJhbGciOiJIUzI1NiJ9.abc.sig" },
		func(r *evidence.QualificationRecord) { r.DiagnosticsRef = "eyJhbGciOiJIUzI1NiJ9.abc.sig" },
		func(r *evidence.QualificationRecord) { r.Findings[0].EvidenceRef = "eyJhbGciOiJIUzI1NiJ9.abc.sig" },
		func(r *evidence.QualificationRecord) { r.RunID = "eyJhbGciOiJIUzI1NiJ9.abc.sig" },
		func(r *evidence.QualificationRecord) { r.TargetEnvironmentID = "eyJhbGciOiJIUzI1NiJ9.abc.sig" },
		func(r *evidence.QualificationRecord) { r.OperatorID = "eyJhbGciOiJIUzI1NiJ9.abc.sig" },
		func(r *evidence.QualificationRecord) { r.Findings[0].CheckID = "eyJhbGciOiJIUzI1NiJ9.abc.sig" },
		func(r *evidence.QualificationRecord) { r.Findings[0].Owner = "eyJhbGciOiJIUzI1NiJ9.abc.sig" },
		func(r *evidence.QualificationRecord) { r.SourceRelease.Version = "eyJhbGciOiJIUzI1NiJ9.abc.sig" },
		func(r *evidence.QualificationRecord) { r.CandidateRelease.Version = "eyJhbGciOiJIUzI1NiJ9.abc.sig" },
		func(r *evidence.QualificationRecord) { r.Findings[0].Reason = "customer row alice@example.com" },
		func(r *evidence.QualificationRecord) {
			r.DiagnosticsRef = "https://objects.cefasdb.com/x?X-Amz-Signature=abc"
		},
		func(r *evidence.QualificationRecord) {
			r.DiagnosticsRef = "https%3A%2F%2Fobjects.cefasdb.com%2Fx%3FX-Amz-Signature%3Dabc"
		},
		func(r *evidence.QualificationRecord) {
			r.Findings[0].EvidenceRef = "https://evidence.cefasdb.com/finding?part=1"
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
	for _, runID := range []string{"terminal", "missing"} {
		_, err = store.Get(context.Background(), "scope-b", runID)
		want(t, err, evidence.ErrScopeDenied)
	}
	_, err = store.List(context.Background(), "scope-b", 0, "secret")
	want(t, err, evidence.ErrScopeDenied)
	verifyScopeIsolation(t, driver, at)

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
	firstPageRunID := one.Items[0].RunID
	one.Items[0].Findings[0].Reason = "retained page mutation"
	unchanged, err := store.Get(context.Background(), "scope-a", firstPageRunID)
	if err != nil {
		t.Fatal(err)
	}
	if unchanged.Findings[0].Reason == "retained page mutation" {
		t.Fatal("page mutation changed committed evidence")
	}
	two, err := store.List(context.Background(), "scope-a", 1, one.NextPageToken)
	if err != nil || len(two.Items) != 1 || two.Items[0].RunID <= firstPageRunID {
		t.Fatalf("second page = %#v, %v", two, err)
	}
	if page, e := store.List(context.Background(), "scope-a", 100, ""); e != nil || len(page.Items) != 3 {
		t.Fatalf("size 100 = %#v, %v", page, e)
	}
	for _, token := range []string{"!", driver.Token("scope-a", "page-a", 2), driver.Token("scope-b", "page-a", 1)} {
		page, listErr := store.List(context.Background(), "scope-a", 1, token)
		want(t, listErr, evidence.ErrInvalidPageToken)
		if !reflect.DeepEqual(page, evidence.EvidencePage{}) {
			t.Fatalf("invalid token returned partial page: %#v", page)
		}
		if len(token) > 16 && strings.Contains(listErr.Error(), token) {
			t.Fatal("invalid token error exposed the page token")
		}
	}
	altered := one.NextPageToken[:len(one.NextPageToken)-1] + "A"
	if altered == one.NextPageToken {
		altered = one.NextPageToken[:len(one.NextPageToken)-1] + "B"
	}
	page, err := store.List(context.Background(), "scope-a", 1, altered)
	want(t, err, evidence.ErrInvalidPageToken)
	if !reflect.DeepEqual(page, evidence.EvidencePage{}) {
		t.Fatalf("altered token returned partial page: %#v", page)
	}
	if got, callErr := store.Get(ctx, "scope-a", "terminal"); !reflect.DeepEqual(got, evidence.QualificationRecord{}) {
		t.Fatalf("cancelled Get returned partial record: %#v", got)
	} else {
		want(t, callErr, context.Canceled)
	}
	if got, callErr := store.List(ctx, "scope-a", 1, ""); !reflect.DeepEqual(got, evidence.EvidencePage{}) {
		t.Fatalf("cancelled List returned partial page: %#v", got)
	} else {
		want(t, callErr, context.Canceled)
	}
}
