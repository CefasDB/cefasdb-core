package model

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"unicode/utf8"
)

const (
	maxContractBytes = 1 << 20
	maxJSONDepth     = 64
)

var allowedJSONKeys = map[string]string{
	"$":                             "|contractId|revision|sha256|approvedAt|approvedBy|scopeKind|scopeId|canonicalPath|objectives|measurement|workload|requiredChecks|retention|manifestVersions|supportedVersions|lease|repositoryPolicyId|runbook|warning|",
	"$.objectives":                  "|rpoSeconds|rtoSeconds|preflightSeconds|",
	"$.measurement":                 "|declarationEvent|recoveryPointEvent|readyEvent|clockSource|",
	"$.workload":                    "|logicalBytes|shards|replicationFactor|writesPerSecond|readsPerSecond|",
	"$.retention":                   "|minimumAgeSeconds|minimumQualifiedSets|evidenceSeconds|",
	"$.supportedVersions":           "|oldest|candidate|recoveryBranch|",
	"$.supportedVersions.oldest":    "|version|imageDigest|sourceCommit|provenanceURI|",
	"$.supportedVersions.candidate": "|version|imageDigest|sourceCommit|provenanceURI|",
	"$.lease":                       "|durationSeconds|renewBeforeSeconds|",
	"$.runbook":                     "|revision|sha256|",
}

func canonicalizeContractJSON(raw []byte) ([]byte, string, error) {
	switch {
	case len(raw) > maxContractBytes:
		return nil, "", validationError("INVALID_CONTRACT", "INPUT_TOO_LARGE", "$")
	case !utf8.Valid(raw):
		return nil, "", validationError("INVALID_CONTRACT", "INVALID_UTF8", "$")
	case bytes.IndexByte(raw, '\r') >= 0:
		return nil, "", validationError("INVALID_CONTRACT", "INVALID_LINE_ENDING", "$")
	}
	if err := scanJSONDocument(raw); err != nil {
		return nil, "", err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var root map[string]any
	_ = decoder.Decode(&root)
	delete(root, "sha256")
	canonical, _ := json.Marshal(root)
	canonical = append(canonical, '\n')
	sum := sha256.Sum256(canonical)
	return canonical, "sha256:" + hex.EncodeToString(sum[:]), nil
}
func scanJSONDocument(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	token, err := decoder.Token()
	if err != nil {
		return validationError("INVALID_CONTRACT", "INVALID_JSON", "$")
	}
	delimiter, ok := token.(json.Delim)
	if !ok || delimiter != '{' {
		return validationError("INVALID_CONTRACT", "TOP_LEVEL_NOT_OBJECT", "$")
	}
	if err := scanJSONObject(decoder, "$", 1); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return validationError("INVALID_CONTRACT", "TRAILING_JSON", "$")
	}
	return nil
}
func scanJSONValue(decoder *json.Decoder, path string, depth int) error {
	if depth > maxJSONDepth {
		return validationError("INVALID_CONTRACT", "JSON_TOO_DEEP", path)
	}
	token, err := decoder.Token()
	if err != nil {
		return validationError("INVALID_CONTRACT", "INVALID_JSON", path)
	}
	switch value := token.(type) {
	case json.Delim:
		switch value {
		case '{':
			return scanJSONObject(decoder, path, depth)
		case '[':
			return scanJSONArray(decoder, path, depth)
		}
	case json.Number:
		if !isIntegerJSON(value.String()) {
			return validationError("INVALID_CONTRACT", "NON_INTEGER_NUMBER", path)
		}
	}
	return nil
}
func scanJSONObject(decoder *json.Decoder, path string, depth int) error {
	seen := make(map[string]struct{})
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return validationError("INVALID_CONTRACT", "INVALID_JSON", path)
		}
		key := token.(string)
		field := path + "." + key
		if _, exists := seen[key]; exists {
			return validationError("INVALID_CONTRACT", "DUPLICATE_KEY", field)
		}
		keys, knownObject := allowedJSONKeys[path]
		if !knownObject || !strings.Contains(keys, "|"+key+"|") {
			return validationError("INVALID_CONTRACT", "UNKNOWN_FIELD", field)
		}
		seen[key] = struct{}{}
		if err := scanJSONValue(decoder, field, depth+1); err != nil {
			return err
		}
	}
	token, err := decoder.Token()
	if err != nil || token != json.Delim('}') {
		return validationError("INVALID_CONTRACT", "INVALID_JSON", path)
	}
	return nil
}
func scanJSONArray(decoder *json.Decoder, path string, depth int) error {
	for decoder.More() {
		if err := scanJSONValue(decoder, path+"[]", depth+1); err != nil {
			return err
		}
	}
	token, err := decoder.Token()
	if err != nil || token != json.Delim(']') {
		return validationError("INVALID_CONTRACT", "INVALID_JSON", path)
	}
	return nil
}
func isIntegerJSON(value string) bool {
	value = strings.TrimPrefix(value, "-")
	if value == "" {
		return false
	}
	for _, char := range value {
		if char < '0' || char > '9' {
			return false
		}
	}
	return true
}
