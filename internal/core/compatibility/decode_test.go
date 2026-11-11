package compatibility_test

import (
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/CefasDb/cefasdb/internal/core/compatibility"
)

func TestDecodeRegistryValidNormalized(t *testing.T) {
	t.Parallel()

	first, err := compatibility.DecodeRegistry(
		namedReader("authoring/registry.yaml", validRegistryYAML),
		namedReader("authoring/policy.yaml", validPolicyYAML),
	)
	if err != nil {
		t.Fatalf("DecodeRegistry() unexpected error: %v", err)
	}
	second, err := compatibility.DecodeRegistry(
		namedReader("authoring/registry.yaml", validRegistryYAML),
		namedReader("authoring/policy.yaml", validPolicyYAML),
	)
	if err != nil {
		t.Fatalf("DecodeRegistry() repeat unexpected error: %v", err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("repeat decode differs:\nfirst:  %#v\nsecond: %#v", first, second)
	}
	if err := first.Validate(); err != nil {
		t.Fatalf("decoded registry does not validate: %v", err)
	}
	if len(first.Entries) != 1 || first.Entries[0].ID.String() != "item.put" {
		t.Fatalf("decoded entries = %#v", first.Entries)
	}
	entry := first.Entries[0]
	if entry.FirstSupportedRelease == nil || *entry.FirstSupportedRelease != "v1.0.0" {
		t.Fatalf("firstSupportedRelease = %#v", entry.FirstSupportedRelease)
	}
	if !reflect.DeepEqual(entry.EvidenceCaseIDs, []string{"case.item.put.error", "case.item.put.success"}) {
		t.Fatalf("normalized evidenceCaseIds = %#v", entry.EvidenceCaseIDs)
	}
	if len(entry.Bindings) != 2 {
		t.Fatalf("bindings = %#v", entry.Bindings)
	}
	if got := entry.Bindings[0].BindingID.String(); got != "grpc:cefas.v1.Cefas/PutItem" {
		t.Fatalf("first normalized binding = %q", got)
	}
	sdk := entry.Bindings[1]
	if got := sdk.BindingID.String(); got != "sdk:github.com/CefasDb/cefasdb/pkg/client.(*Client).PutItem" {
		t.Fatalf("second normalized binding = %q", got)
	}
	if sdk.BehaviorOverride == nil || sdk.BehaviorOverride.Result != "Returns the typed SDK response." {
		t.Fatalf("behavior override lost: %#v", sdk.BehaviorOverride)
	}
	if len(sdk.Deviations) != 1 || sdk.Deviations[0].Summary != "Typed request and response." {
		t.Fatalf("deviations lost: %#v", sdk.Deviations)
	}
	if !reflect.DeepEqual(
		[]string{first.Policy.ComparisonTargets[0].ID, first.Policy.ComparisonTargets[1].ID},
		[]string{"dynamodb", "partiql"},
	) {
		t.Fatalf("normalized comparison targets = %#v", first.Policy.ComparisonTargets)
	}
}

func TestDecodeRegistryRejectsMalformed(t *testing.T) {
	t.Parallel()

	unknownField := strings.Replace(
		validRegistryYAML,
		"firstSupportedRelease:",
		"firstSupportRelease:",
		1,
	)
	unknownLine, unknownColumn := coordinateOf(unknownField, "firstSupportRelease")
	unknownBindingField := strings.Replace(
		validRegistryYAML,
		"        surface: sdk\n",
		"        surface: sdk\n        responseEncoding: typed\n",
		1,
	)
	unknownBindingLine, unknownBindingColumn := coordinateOf(unknownBindingField, "responseEncoding")

	missingBehavior := replaceBetween(
		validRegistryYAML,
		"    behavior:\n",
		"    comparisonTargetId:",
		"    behavior: {}\n",
	)
	duplicateBinding := strings.Replace(
		validRegistryYAML,
		"    bindings:\n",
		"    bindings:\n      - bindingId: \"grpc:cefas.v1.Cefas/PutItem\"\n        surface: grpc\n",
		1,
	)
	duplicateEntry := registryHeaderYAML + validEntryYAML + validEntryYAML + "inventoryExclusions: []\n"
	crossEntryDuplicate := registryHeaderYAML + validEntryYAML + strings.Replace(
		validEntryYAML,
		"  - id: item.put",
		"  - id: item.get",
		1,
	) + "inventoryExclusions: []\n"
	crossEntryLine, crossEntryColumn := coordinateOfOccurrence(
		crossEntryDuplicate,
		"\"sdk:github.com/CefasDb/cefasdb/pkg/client.(*Client).PutItem\"",
		2,
	)
	missingInventoryExclusions := strings.Replace(validRegistryYAML, "inventoryExclusions: []\n", "", 1)
	invalidBehaviorOverride := strings.Replace(
		validRegistryYAML,
		"          result: Returns the typed SDK response.\n",
		"          result: \"\"\n",
		1,
	)
	overrideLine, overrideColumn := coordinateOf(invalidBehaviorOverride, "\"\"")
	invalidDeviation := strings.Replace(
		validRegistryYAML,
		"          - summary: Typed request and response.\n            description: The SDK exposes Go values instead of protobuf messages.\n",
		"          - summary: \"\"\n            description: \"\"\n",
		1,
	)
	deviationLine, deviationColumn := coordinateOf(invalidDeviation, "\"\"")
	whitespaceBehaviorError := strings.Replace(
		validRegistryYAML,
		"              condition: The request is invalid.\n",
		"              condition: The request is invalid.\n              description: \" \"\n",
		1,
	)
	whitespaceDeviation := strings.Replace(
		validRegistryYAML,
		"            description: The SDK exposes Go values instead of protobuf messages.\n",
		"            description: \" \"\n",
		1,
	)
	danglingReplacement := strings.Replace(
		strings.Replace(validRegistryYAML, "state: supported", "state: deprecated", 1),
		"inventoryExclusions: []\n",
		"    deprecation:\n      replacementEntryId: item.missing\n      noticeRelease: v2.0.0\n      removalEarliestRelease: v4.0.0\n      migration: Use the replacement entry.\ninventoryExclusions: []\n",
		1,
	)
	replacementLine, replacementColumn := coordinateOf(danglingReplacement, "item.missing")

	tests := []struct {
		name            string
		registry        string
		policy          string
		code            string
		source          string
		line            int
		column          int
		messageContains string
		entryID         string
		bindingID       string
	}{
		{
			name:            "unknown entry field preserves typo coordinate",
			registry:        unknownField,
			policy:          validPolicyYAML,
			code:            compatibility.DiagnosticSchema,
			source:          "custom-registry.yaml",
			line:            unknownLine,
			column:          unknownColumn,
			messageContains: "firstSupportRelease",
			entryID:         "item.put",
		},
		{
			name:            "unknown binding field preserves entry and binding identity",
			registry:        unknownBindingField,
			policy:          validPolicyYAML,
			code:            compatibility.DiagnosticSchema,
			source:          "custom-registry.yaml",
			line:            unknownBindingLine,
			column:          unknownBindingColumn,
			messageContains: "responseEncoding",
			entryID:         "item.put",
			bindingID:       "sdk:github.com/CefasDb/cefasdb/pkg/client.(*Client).PutItem",
		},
		{
			name:            "wrong registry schema version",
			registry:        strings.Replace(validRegistryYAML, "schemaVersion: 1", "schemaVersion: 2", 1),
			policy:          validPolicyYAML,
			code:            compatibility.DiagnosticSchema,
			source:          "custom-registry.yaml",
			messageContains: "schemaVersion",
		},
		{
			name:            "wrong policy schema version",
			registry:        validRegistryYAML,
			policy:          strings.Replace(validPolicyYAML, "schemaVersion: 1", "schemaVersion: 2", 1),
			code:            compatibility.DiagnosticSchema,
			source:          "custom-policy.yaml",
			messageContains: "schemaVersion",
		},
		{
			name:            "latest comparison alias",
			registry:        validRegistryYAML,
			policy:          strings.Replace(validPolicyYAML, "exactVersion: \"2026-10-01\"", "exactVersion: latest", 1),
			code:            compatibility.DiagnosticRequired,
			source:          "custom-policy.yaml",
			messageContains: "latest",
		},
		{
			name:            "mutable current comparison alias",
			registry:        validRegistryYAML,
			policy:          strings.Replace(validPolicyYAML, "exactVersion: \"2026-10-01\"", "exactVersion: current", 1),
			code:            compatibility.DiagnosticRequired,
			source:          "custom-policy.yaml",
			messageContains: "current",
		},
		{
			name:            "comparison version range",
			registry:        validRegistryYAML,
			policy:          strings.Replace(validPolicyYAML, "exactVersion: \"1.1.0\"", "exactVersion: 1.x", 1),
			code:            compatibility.DiagnosticRequired,
			source:          "custom-policy.yaml",
			messageContains: "1.x",
		},
		{
			name:            "zero notice policy",
			registry:        validRegistryYAML,
			policy:          strings.Replace(validPolicyYAML, "minimumNoticeReleases: 2", "minimumNoticeReleases: 0", 1),
			code:            compatibility.DiagnosticRequired,
			source:          "custom-policy.yaml",
			messageContains: "minimumNoticeReleases",
		},
		{
			name:            "missing emergency removal policy",
			registry:        validRegistryYAML,
			policy:          strings.Replace(validPolicyYAML, "emergencyRemovalAllowed: false\n", "", 1),
			code:            compatibility.DiagnosticRequired,
			source:          "custom-policy.yaml",
			messageContains: "emergencyRemovalAllowed",
		},
		{
			name:            "missing inventory exclusions",
			registry:        missingInventoryExclusions,
			policy:          validPolicyYAML,
			code:            compatibility.DiagnosticRequired,
			source:          "custom-registry.yaml",
			messageContains: "inventoryExclusions",
		},
		{
			name:            "invalid binding identity",
			registry:        strings.Replace(validRegistryYAML, "grpc:cefas.v1.Cefas/PutItem", "grpc:Cefas.PutItem", 1),
			policy:          validPolicyYAML,
			code:            compatibility.DiagnosticIdentity,
			source:          "custom-registry.yaml",
			messageContains: "grpc:Cefas.PutItem",
		},
		{
			name:            "grpc binding rejects empty service segment",
			registry:        strings.Replace(validRegistryYAML, "grpc:cefas.v1.Cefas/PutItem", "grpc:cefas..v1.Cefas/PutItem", 1),
			policy:          validPolicyYAML,
			code:            compatibility.DiagnosticIdentity,
			source:          "custom-registry.yaml",
			messageContains: "grpc:cefas..v1.Cefas/PutItem",
			entryID:         "item.put",
			bindingID:       "grpc:cefas..v1.Cefas/PutItem",
		},
		{
			name:            "sdk binding requires canonical Go symbol",
			registry:        strings.Replace(validRegistryYAML, "sdk:github.com/CefasDb/cefasdb/pkg/client.(*Client).PutItem", "sdk:not-a-go-symbol", 1),
			policy:          validPolicyYAML,
			code:            compatibility.DiagnosticIdentity,
			source:          "custom-registry.yaml",
			messageContains: "sdk:not-a-go-symbol",
			entryID:         "item.put",
			bindingID:       "sdk:not-a-go-symbol",
		},
		{
			name:            "duplicate entry identity",
			registry:        duplicateEntry,
			policy:          validPolicyYAML,
			code:            compatibility.DiagnosticIdentity,
			source:          "custom-registry.yaml",
			messageContains: "duplicate entry id",
		},
		{
			name:            "duplicate binding identity",
			registry:        duplicateBinding,
			policy:          validPolicyYAML,
			code:            compatibility.DiagnosticIdentity,
			source:          "custom-registry.yaml",
			messageContains: "duplicate binding id",
		},
		{
			name:            "duplicate binding identity across entries identifies second entry",
			registry:        crossEntryDuplicate,
			policy:          validPolicyYAML,
			code:            compatibility.DiagnosticIdentity,
			source:          "custom-registry.yaml",
			line:            crossEntryLine,
			column:          crossEntryColumn,
			messageContains: "duplicate binding id",
			entryID:         "item.get",
			bindingID:       "sdk:github.com/CefasDb/cefasdb/pkg/client.(*Client).PutItem",
		},
		{
			name:            "invalid binding behavior override identifies binding coordinate",
			registry:        invalidBehaviorOverride,
			policy:          validPolicyYAML,
			code:            compatibility.DiagnosticRequired,
			source:          "custom-registry.yaml",
			line:            overrideLine,
			column:          overrideColumn,
			messageContains: "behaviorOverride",
			entryID:         "item.put",
			bindingID:       "sdk:github.com/CefasDb/cefasdb/pkg/client.(*Client).PutItem",
		},
		{
			name:            "invalid binding deviation identifies binding coordinate",
			registry:        invalidDeviation,
			policy:          validPolicyYAML,
			code:            compatibility.DiagnosticRequired,
			source:          "custom-registry.yaml",
			line:            deviationLine,
			column:          deviationColumn,
			messageContains: "deviation[0]",
			entryID:         "item.put",
			bindingID:       "sdk:github.com/CefasDb/cefasdb/pkg/client.(*Client).PutItem",
		},
		{
			name:            "whitespace behavior error description",
			registry:        whitespaceBehaviorError,
			policy:          validPolicyYAML,
			code:            compatibility.DiagnosticRequired,
			source:          "custom-registry.yaml",
			messageContains: "behavior error description",
			entryID:         "item.put",
			bindingID:       "sdk:github.com/CefasDb/cefasdb/pkg/client.(*Client).PutItem",
		},
		{
			name:            "whitespace deviation description",
			registry:        whitespaceDeviation,
			policy:          validPolicyYAML,
			code:            compatibility.DiagnosticRequired,
			source:          "custom-registry.yaml",
			messageContains: "deviation description",
			entryID:         "item.put",
			bindingID:       "sdk:github.com/CefasDb/cefasdb/pkg/client.(*Client).PutItem",
		},
		{
			name:            "dangling replacement identifies entry and coordinate",
			registry:        danglingReplacement,
			policy:          validPolicyYAML,
			code:            compatibility.DiagnosticRequired,
			source:          "custom-registry.yaml",
			line:            replacementLine,
			column:          replacementColumn,
			messageContains: "unknown replacement entry",
			entryID:         "item.put",
		},
		{
			name:            "missing behavior",
			registry:        missingBehavior,
			policy:          validPolicyYAML,
			code:            compatibility.DiagnosticRequired,
			source:          "custom-registry.yaml",
			messageContains: "behavior result",
		},
		{
			name:            "trailing registry document",
			registry:        validRegistryYAML + "---\nschemaVersion: 1\n",
			policy:          validPolicyYAML,
			code:            compatibility.DiagnosticSchema,
			source:          "custom-registry.yaml",
			messageContains: "trailing document",
		},
		{
			name:            "trailing policy document",
			registry:        validRegistryYAML,
			policy:          validPolicyYAML + "---\nschemaVersion: 1\n",
			code:            compatibility.DiagnosticSchema,
			source:          "custom-policy.yaml",
			messageContains: "trailing document",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, err := compatibility.DecodeRegistry(
				namedReader("custom-registry.yaml", test.registry),
				namedReader("custom-policy.yaml", test.policy),
			)
			if err == nil {
				t.Fatal("DecodeRegistry() unexpectedly succeeded")
			}
			if !reflect.DeepEqual(got, compatibility.WorkingRegistry{}) {
				t.Fatalf("DecodeRegistry() returned a partial value: %#v", got)
			}
			var diagnostic compatibility.Diagnostic
			if !errors.As(err, &diagnostic) {
				t.Fatalf("error %T is not compatibility.Diagnostic: %v", err, err)
			}
			if diagnostic.Code != test.code {
				t.Fatalf("diagnostic code = %q, want %q: %v", diagnostic.Code, test.code, err)
			}
			if diagnostic.Location.Source != test.source {
				t.Fatalf("diagnostic source = %q, want %q: %v", diagnostic.Location.Source, test.source, err)
			}
			if diagnostic.Location.Line <= 0 || diagnostic.Location.Column <= 0 {
				t.Fatalf("diagnostic is not located: %#v", diagnostic.Location)
			}
			if test.line > 0 && diagnostic.Location.Line != test.line {
				t.Fatalf("diagnostic line = %d, want %d: %v", diagnostic.Location.Line, test.line, err)
			}
			if test.column > 0 && diagnostic.Location.Column != test.column {
				t.Fatalf("diagnostic column = %d, want %d: %v", diagnostic.Location.Column, test.column, err)
			}
			if !strings.Contains(err.Error(), test.messageContains) {
				t.Fatalf("diagnostic %q does not contain %q", err, test.messageContains)
			}
			if test.entryID != "" && diagnostic.EntryID != test.entryID {
				t.Fatalf("diagnostic entry id = %q, want %q: %v", diagnostic.EntryID, test.entryID, err)
			}
			if test.bindingID != "" && diagnostic.BindingID != test.bindingID {
				t.Fatalf("diagnostic binding id = %q, want %q: %v", diagnostic.BindingID, test.bindingID, err)
			}
		})
	}
}

func TestDecodeRegistryUsesFallbackReaderNames(t *testing.T) {
	t.Parallel()

	registry := strings.Replace(validRegistryYAML, "firstSupportedRelease:", "firstSupportRelease:", 1)
	_, err := compatibility.DecodeRegistry(strings.NewReader(registry), strings.NewReader(validPolicyYAML))
	if err == nil {
		t.Fatal("DecodeRegistry() unexpectedly succeeded")
	}
	var diagnostic compatibility.Diagnostic
	if !errors.As(err, &diagnostic) {
		t.Fatalf("error %T is not compatibility.Diagnostic: %v", err, err)
	}
	if diagnostic.Location.Source != "registry.yaml" {
		t.Fatalf("fallback source = %q, want registry.yaml", diagnostic.Location.Source)
	}
}

type readerWithName struct {
	io.Reader
	name string
}

func (r readerWithName) Name() string {
	return r.name
}

func namedReader(name string, content string) io.Reader {
	return readerWithName{Reader: strings.NewReader(content), name: name}
}

func replaceBetween(input string, start string, end string, replacement string) string {
	startIndex := strings.Index(input, start)
	if startIndex < 0 {
		panic("start marker not found")
	}
	endRelative := strings.Index(input[startIndex:], end)
	if endRelative < 0 {
		panic("end marker not found")
	}
	return input[:startIndex] + replacement + input[startIndex+endRelative:]
}

func coordinateOf(input string, token string) (int, int) {
	index := strings.Index(input, token)
	if index < 0 {
		panic("token not found")
	}
	line := strings.Count(input[:index], "\n") + 1
	lastNewline := strings.LastIndex(input[:index], "\n")
	column := index - lastNewline
	return line, column
}

func coordinateOfOccurrence(input string, token string, occurrence int) (int, int) {
	if occurrence <= 0 {
		panic("occurrence must be positive")
	}
	offset := 0
	for count := 0; count < occurrence; count++ {
		index := strings.Index(input[offset:], token)
		if index < 0 {
			panic("token occurrence not found")
		}
		offset += index
		if count+1 < occurrence {
			offset += len(token)
		}
	}
	line := strings.Count(input[:offset], "\n") + 1
	lastNewline := strings.LastIndex(input[:offset], "\n")
	column := offset - lastNewline
	return line, column
}

const registryHeaderYAML = `schemaVersion: 1
glossary:
  initiativeId: INIT-2026-005-domain-lifecycles
  revision: glossary-v1
  terms:
    operation: A stable public capability.
entries:
`

const validEntryYAML = `  - id: item.put
    operation: Put item
    owner: core-api
    state: supported
    firstSupportedRelease: v1.0.0
    bindings:
      - bindingId: "sdk:github.com/CefasDb/cefasdb/pkg/client.(*Client).PutItem"
        surface: sdk
        behaviorOverride:
          result: Returns the typed SDK response.
          errors:
            - code: invalid-argument
              condition: The request is invalid.
          pagination:
            mode: none
          streaming:
            mode: none
          consistency:
            - strong
          limits:
            - name: request-size
              value: 1 MiB
        deviations:
          - summary: Typed request and response.
            description: The SDK exposes Go values instead of protobuf messages.
      - bindingId: "grpc:cefas.v1.Cefas/PutItem"
        surface: grpc
        deviations: []
    behavior:
      result: Returns the stored item.
      errors:
        - code: invalid-argument
          condition: The request is invalid.
        - code: conflict
          description: The conditional write failed.
      pagination:
        mode: none
      streaming:
        mode: none
      consistency:
        - strong
      limits:
        - name: request-size
          value: 1 MiB
    comparisonTargetId: dynamodb
    evidenceCaseIds:
      - case.item.put.success
      - case.item.put.error
`

const validRegistryYAML = registryHeaderYAML + validEntryYAML + `inventoryExclusions: []
`

const validPolicyYAML = `schemaVersion: 1
comparisonTargets:
  - id: partiql
    product: PartiQL
    exactVersion: "1.1.0"
    source: "https://partiql.org"
  - id: dynamodb
    product: Amazon DynamoDB
    exactVersion: "2026-10-01"
    source: "https://docs.aws.amazon.com/dynamodb"
minimumNoticeReleases: 2
emergencyRemovalAllowed: false
`
