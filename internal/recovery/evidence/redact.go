package evidence

import (
	"net/url"
	"regexp"
	"strings"
)

var (
	credentialPattern = regexp.MustCompile(`(?i)(^|[^a-z0-9])(authorization|password|passwd|secret|credential|api[-_ ]?key|access[-_ ]?key|private[-_ ]?key)([^a-z0-9]|$)`)
	bearerPattern     = regexp.MustCompile(`(?i)\bbearer[ \t]+[a-z0-9._~+/=-]+`)
	signedPattern     = regexp.MustCompile(`(?i)(x-amz-signature|x-goog-signature|signature|access_token|token|sig)\s*=`)
	payloadPattern    = regexp.MustCompile(`(?i)[{[,]\s*"(row|item|payload)"\s*:`)
)

func redactionFailure(path string) error {
	return fail(CodeEvidenceRedactionFailed, "PutTerminal", path)
}

func unsafeText(value string) bool {
	return credentialPattern.MatchString(value) || bearerPattern.MatchString(value) ||
		signedPattern.MatchString(value) || payloadPattern.MatchString(value)
}

func unsafeReference(value string) bool {
	if unsafeText(value) || strings.Contains(value, "?") {
		return true
	}
	parsed, err := url.Parse(value)
	return err != nil || parsed.RawQuery != "" || parsed.ForceQuery || parsed.User != nil
}

func validateRedaction(record QualificationRecord) error {
	references := []struct{ path, value string }{
		{"backupSetRef", record.BackupSetRef},
		{"sourceRelease.provenanceURI", record.SourceRelease.ProvenanceURI},
		{"candidateRelease.provenanceURI", record.CandidateRelease.ProvenanceURI},
		{"targetProofRef", record.TargetProofRef},
		{"restoreRunRef", record.RestoreRunRef},
		{"reconciliationReportRef", record.ReconciliationReportRef},
		{"upgradeRunRef", record.UpgradeRunRef},
		{"diagnosticsRef", record.DiagnosticsRef},
	}
	for _, ref := range references {
		if unsafeReference(ref.value) {
			return redactionFailure(ref.path)
		}
	}
	for i, finding := range record.Findings {
		path := "findings[" + index(i) + "]"
		if unsafeText(finding.Reason) {
			return redactionFailure(path + ".reason")
		}
		if unsafeReference(finding.EvidenceRef) {
			return redactionFailure(path + ".evidenceRef")
		}
	}
	return nil
}
