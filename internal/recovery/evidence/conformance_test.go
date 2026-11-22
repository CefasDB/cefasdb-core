package evidence_test

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/CefasDb/cefasdb/internal/recovery/evidence"
	"github.com/CefasDb/cefasdb/internal/recovery/evidence/storetest"
	"github.com/CefasDb/cefasdb/internal/recovery/model"
)

func seal(tb testing.TB, record evidence.QualificationRecord) evidence.QualificationRecord {
	tb.Helper()
	_, digest, err := evidence.CanonicalRecord(record)
	if err != nil {
		tb.Fatal(err)
	}
	record.SHA256 = digest
	return record
}

func driver() storetest.Driver {
	key := []byte("evidence-test-cursor-key")
	return storetest.Driver{
		Open: func(_ testing.TB, scope string) (evidence.EvidenceStore, func() evidence.EvidenceStore) {
			store, reopen := newMemory(scope)
			return store, reopen
		},
		Seal:     seal,
		FailNext: func(store evidence.EvidenceStore) { store.(*memoryStore).failPersistence() },
		Token: func(scope, after string, version byte) string {
			return evidence.EncodePageToken(scope, after, key, version)
		},
	}
}

func TestConformance(t *testing.T) { storetest.Run(t, driver()) }

func TestConformance_Redaction(t *testing.T) { storetest.Run(t, driver()) }

func TestCanonicalAndLimits(t *testing.T) {
	at := time.Date(2026, 11, 22, 3, 0, 0, 0, time.UTC)
	record := storetest.SampleRecord("canonical", at)
	record.SHA256 = "sha256:" + strings.Repeat("1", 64)
	first, firstDigest, err := evidence.CanonicalRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	record.SHA256 = "sha256:" + strings.Repeat("2", 64)
	second, secondDigest, err := evidence.CanonicalRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) || firstDigest != secondDigest || len(first) == 0 || first[len(first)-1] != '\n' {
		t.Fatal("canonical hash exclusion or LF contract failed")
	}

	record = storetest.SampleRecord("oversized", at)
	record.Findings = make([]evidence.Finding, evidence.MaxFindings)
	for i := range record.Findings {
		record.Findings[i] = evidence.Finding{CheckID: "REC-01", Owner: "team-cefasdb", Reason: strings.Repeat("x", evidence.MaxFreeTextBytes), EvidenceRef: "finding"}
	}
	record.SHA256 = firstDigest
	store, _ := newMemory("scope-a")
	_, err = store.PutTerminal(t.Context(), record)
	var failure *evidence.Error
	if !errors.As(err, &failure) || failure.Code != evidence.CodeInvalidEvidence {
		t.Fatalf("oversized error = %#v", err)
	}
	if strings.Contains(failure.Error(), strings.Repeat("x", 32)) {
		t.Fatal("error exposed rejected value")
	}
}

func TestValidationAndErrors(t *testing.T) {
	at := time.Date(2026, 11, 22, 3, 0, 0, 0, time.UTC)
	base := seal(t, storetest.SampleRecord("valid", at))
	cases := []func(*evidence.QualificationRecord){
		func(r *evidence.QualificationRecord) { r.SchemaVersion = 0 },
		func(r *evidence.QualificationRecord) { r.ScopeID = "" },
		func(r *evidence.QualificationRecord) { r.RunID = "" },
		func(r *evidence.QualificationRecord) { r.RunRevision = 0 },
		func(r *evidence.QualificationRecord) { r.ContractRef.ID = "" },
		func(r *evidence.QualificationRecord) { r.BackupSetRef = "" },
		func(r *evidence.QualificationRecord) { r.TargetEnvironmentID = "" },
		func(r *evidence.QualificationRecord) { r.TargetProofRef = "" },
		func(r *evidence.QualificationRecord) { r.RestoreRunRef = "" },
		func(r *evidence.QualificationRecord) { r.ReconciliationReportRef = "" },
		func(r *evidence.QualificationRecord) { r.UpgradeRunRef = "" },
		func(r *evidence.QualificationRecord) { r.Status = "" },
		func(r *evidence.QualificationRecord) { r.OperatorID = "" },
		func(r *evidence.QualificationRecord) { r.RunbookRevision = "" },
		func(r *evidence.QualificationRecord) { r.RunbookSHA256 = "" },
		func(r *evidence.QualificationRecord) { r.DiagnosticsRef = "" },
		func(r *evidence.QualificationRecord) { r.Findings = make([]evidence.Finding, evidence.MaxFindings+1) },
		func(r *evidence.QualificationRecord) { r.StartedAt = time.Time{} },
		func(r *evidence.QualificationRecord) { r.CompletedAt = at.Add(-time.Second) },
		func(r *evidence.QualificationRecord) { r.Findings[0].CheckID = "" },
		func(r *evidence.QualificationRecord) { r.Findings[0].Owner = "" },
		func(r *evidence.QualificationRecord) { r.Findings[0].Reason = "" },
		func(r *evidence.QualificationRecord) { r.Findings[0].EvidenceRef = "" },
	}
	for i, mutate := range cases {
		input := evidence.CopyRecord(base)
		mutate(&input)
		var failure *evidence.Error
		_, _, _, err := evidence.PrepareRecord(input)
		if !errors.As(err, &failure) || failure.Code != evidence.CodeInvalidEvidence {
			t.Fatalf("case %d: %#v", i, err)
		}
	}
	for _, candidate := range []bool{false, true} {
		input := evidence.CopyRecord(base)
		if candidate {
			input.CandidateRelease.ImageDigest = ""
		} else {
			input.SourceRelease.ImageDigest = ""
		}
		_, _, _, err := evidence.PrepareRecord(input)
		var failure *model.ValidationError
		if !errors.As(err, &failure) {
			t.Fatalf("release error = %#v", err)
		}
	}
	input := evidence.CopyRecord(base)
	input.Findings[0].Reason = string([]byte{0xff})
	if _, _, err := evidence.CanonicalRecord(input); !errors.Is(err, evidence.ErrInvalidEvidence) {
		t.Fatal("invalid UTF-8 was accepted")
	}
	input = evidence.CopyRecord(base)
	input.StartedAt, input.CompletedAt = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(10000, 1, 2, 0, 0, 0, 0, time.UTC)
	_, _, err := evidence.CanonicalRecord(input)
	var encodingFailure *evidence.Error
	if !errors.As(err, &encodingFailure) || errors.Unwrap(encodingFailure) == nil {
		t.Fatalf("encoding error = %#v", err)
	}
	wrapped := &evidence.Error{Code: evidence.CodeInvalidEvidence, Operation: "test", Field: "field", Cause: errors.New("cause")}
	if !evidence.IsCode(wrapped, evidence.CodeInvalidEvidence) || errors.Unwrap(wrapped) == nil || wrapped.Error() == "" {
		t.Fatal("typed error contract failed")
	}
}

func FuzzEvidencePageToken(f *testing.F) {
	f.Add("")
	f.Add("!")
	f.Add(evidence.EncodePageToken("scope-a", "after", []byte("evidence-test-cursor-key"), 1))
	f.Fuzz(func(t *testing.T, token string) {
		store, _ := newMemory("scope-a")
		page, err := store.List(t.Context(), "scope-a", 1, token)
		if err != nil && !reflect.DeepEqual(page, evidence.EvidencePage{}) {
			t.Fatal("failure returned partial page")
		}
	})
}

func FuzzRedaction(f *testing.F) {
	f.Add([]byte("safe summary"))
	f.Add([]byte("Authorization: Bearer abc"))
	f.Add([]byte(`{"payload":{"id":1}}`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		record := storetest.SampleRecord("fuzz", time.Date(2026, 11, 22, 3, 0, 0, 0, time.UTC))
		record.Findings[0].Reason = string(raw)
		if _, digest, err := evidence.CanonicalRecord(record); err == nil {
			record.SHA256 = digest
		}
		store, _ := newMemory("scope-a")
		ref, err := store.PutTerminal(t.Context(), record)
		if err != nil {
			if ref != (evidence.EvidenceRef{}) {
				t.Fatal("failure returned evidence ref")
			}
			page, listErr := store.List(t.Context(), "scope-a", 100, "")
			if listErr != nil || len(page.Items) != 0 {
				t.Fatal("failure produced visible evidence")
			}
		}
	})
}
