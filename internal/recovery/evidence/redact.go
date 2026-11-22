package evidence

import (
	"net/url"
	"regexp"
	"strings"
)

var (
	credentialPattern = regexp.MustCompile(
		`(?i)(^|[^a-z0-9])(?:authorization|password|passwd|secret|credentials?|api[-_ ]?key|` +
			`access[-_ ]?key|secret[-_ ]?access[-_ ]?key|private[-_ ]?key|client[-_ ]?secret|` +
			`session[-_ ]?token|refresh[-_ ]?token|cookies?|set[-_ ]?cookie)([^a-z0-9]|$)`,
	)
	bearerPattern = regexp.MustCompile(`(?i)\bbearer[ \t]+[a-z0-9._~+/=-]+`)
	jwtPattern    = regexp.MustCompile(
		`(?:^|[^A-Za-z0-9_-])eyJ[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+(?:$|[^A-Za-z0-9_-])`,
	)
	signedPattern = regexp.MustCompile(
		`(?i)(?:x-amz-(?:credential|security-token|signature)|x-goog-(?:credential|signature)|` +
			`awsaccesskeyid|signature|access_token|token|sig)\s*=`,
	)
	payloadPattern = regexp.MustCompile(`(?i)[{[,]\s*"(?:row|rows|item|items|payload)"\s*:`)
)

func redactionFailure(path string) error {
	return fail(CodeEvidenceRedactionFailed, "PutTerminal", path)
}

func unsafeText(value string) bool {
	return credentialPattern.MatchString(value) || bearerPattern.MatchString(value) || jwtPattern.MatchString(value) ||
		signedPattern.MatchString(value) || payloadPattern.MatchString(value)
}

func unescapeReference(value string) (string, bool) {
	for range 3 {
		if !strings.Contains(value, "%") {
			return value, true
		}
		decoded, err := url.PathUnescape(value)
		if err != nil || decoded == value {
			return "", false
		}
		value = decoded
	}
	return value, !strings.Contains(value, "%")
}

func unsafeReference(value string) bool {
	decoded, ok := unescapeReference(value)
	if !ok || unsafeText(decoded) || strings.Contains(decoded, "?") {
		return true
	}
	parsed, err := url.Parse(decoded)
	return err != nil || parsed.RawQuery != "" || parsed.ForceQuery || parsed.User != nil
}

type redactionField struct {
	path  string
	value string
}

func validatePlainText(fields []redactionField) error {
	for _, field := range fields {
		if unsafeText(field.value) {
			return redactionFailure(field.path)
		}
	}
	return nil
}

func recordTextFields(record QualificationRecord) []redactionField {
	return []redactionField{
		{"scopeId", record.ScopeID},
		{"runId", record.RunID},
		{"contractRef.id", record.ContractRef.ID},
		{"contractRef.sha256", record.ContractRef.SHA256},
		{"sourceRelease.version", record.SourceRelease.Version},
		{"sourceRelease.imageDigest", record.SourceRelease.ImageDigest},
		{"sourceRelease.sourceCommit", record.SourceRelease.SourceCommit},
		{"candidateRelease.version", record.CandidateRelease.Version},
		{"candidateRelease.imageDigest", record.CandidateRelease.ImageDigest},
		{"candidateRelease.sourceCommit", record.CandidateRelease.SourceCommit},
		{"targetEnvironmentId", record.TargetEnvironmentID},
		{"status", string(record.Status)},
		{"operatorId", record.OperatorID},
		{"runbookRevision", record.RunbookRevision},
		{"runbookSha256", record.RunbookSHA256},
		{"sha256", record.SHA256},
	}
}

func validateRedaction(record QualificationRecord) error {
	if err := validatePlainText(recordTextFields(record)); err != nil {
		return err
	}
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
		if !validFindingReason(finding.Reason) {
			return redactionFailure(path + ".reason")
		}
		if err := validatePlainText([]redactionField{
			{path + ".checkId", finding.CheckID},
			{path + ".owner", finding.Owner},
			{path + ".reason", string(finding.Reason)},
		}); err != nil {
			return err
		}
		if unsafeReference(finding.EvidenceRef) {
			return redactionFailure(path + ".evidenceRef")
		}
	}
	return nil
}
