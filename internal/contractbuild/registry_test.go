package contractbuild_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/CefasDb/cefasdb/internal/contractbuild"
	"github.com/CefasDb/cefasdb/internal/core/compatibility"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"gopkg.in/yaml.v3"
)

func TestLoadRegistryFiles(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	registryPath := filepath.Join(directory, "registry-input.yaml")
	policyPath := filepath.Join(directory, "policy-input.yaml")
	writeFixture(t, registryPath, validRegistryYAML)
	writeFixture(t, policyPath, validPolicyYAML)

	got, err := contractbuild.LoadRegistryFiles(registryPath, policyPath)
	if err != nil {
		t.Fatalf("LoadRegistryFiles() unexpected error: %v", err)
	}
	want, err := compatibility.DecodeRegistry(
		strings.NewReader(validRegistryYAML),
		strings.NewReader(validPolicyYAML),
	)
	if err != nil {
		t.Fatalf("DecodeRegistry() fixture error: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("LoadRegistryFiles() differs from direct decoding:\ngot:  %#v\nwant: %#v", got, want)
	}

	t.Run("missing registry path is preserved", func(t *testing.T) {
		missingPath := filepath.Join(directory, "missing-registry.yaml")
		loaded, err := contractbuild.LoadRegistryFiles(missingPath, policyPath)
		assertZeroRegistry(t, loaded)
		assertPathError(t, err, missingPath)
		if !strings.Contains(err.Error(), "open registry") {
			t.Fatalf("error %q does not identify the registry input", err)
		}
	})

	t.Run("missing policy path is preserved", func(t *testing.T) {
		missingPath := filepath.Join(directory, "missing-policy.yaml")
		loaded, err := contractbuild.LoadRegistryFiles(registryPath, missingPath)
		assertZeroRegistry(t, loaded)
		assertPathError(t, err, missingPath)
		if !strings.Contains(err.Error(), "open policy") {
			t.Fatalf("error %q does not identify the policy input", err)
		}
	})

	t.Run("decode semantics and file identity come from decoder", func(t *testing.T) {
		typoPath := filepath.Join(directory, "registry-typo.yaml")
		writeFixture(t, typoPath, strings.Replace(
			validRegistryYAML,
			"firstSupportedRelease:",
			"firstSupportRelease:",
			1,
		))

		loaded, err := contractbuild.LoadRegistryFiles(typoPath, policyPath)
		assertZeroRegistry(t, loaded)
		var diagnostic compatibility.Diagnostic
		if !errors.As(err, &diagnostic) {
			t.Fatalf("error %T is not compatibility.Diagnostic: %v", err, err)
		}
		if diagnostic.Code != compatibility.DiagnosticSchema {
			t.Fatalf("diagnostic code = %q, want %q", diagnostic.Code, compatibility.DiagnosticSchema)
		}
		if diagnostic.Location.Source != typoPath {
			t.Fatalf("diagnostic source = %q, want %q", diagnostic.Location.Source, typoPath)
		}
	})
}

func TestCompatibilitySchemaV1(t *testing.T) {
	t.Parallel()

	schemaPath := filepath.Join("..", "..", "contracts", "compatibility", "schema-v1.json")
	data, err := os.ReadFile(schemaPath)
	if err != nil {
		t.Fatalf("read schema: %v", err)
	}
	var schema map[string]any
	if err := json.Unmarshal(data, &schema); err != nil {
		t.Fatalf("schema is not valid JSON: %v", err)
	}
	compiler := jsonschema.NewCompiler()
	const schemaURL = "https://cefasdb.com/contracts/compatibility/schema-v1.json"
	if err := compiler.AddResource(schemaURL, schema); err != nil {
		t.Fatalf("register schema resource: %v", err)
	}
	compiledSchema, err := compiler.Compile(schemaURL)
	if err != nil {
		t.Fatalf("compile draft 2020-12 schema: %v", err)
	}

	if got := schema["$schema"]; got != "https://json-schema.org/draft/2020-12/schema" {
		t.Fatalf("$schema = %#v", got)
	}
	description, _ := schema["description"].(string)
	if !strings.Contains(strings.ToLower(description), "non-authoritative") ||
		!strings.Contains(description, "DecodeRegistry") {
		t.Fatalf("schema description does not state its tooling role: %q", description)
	}

	defs := objectValue(t, schema["$defs"], "$defs")
	registry := definition(t, defs, "registryDocument")
	policy := definition(t, defs, "policyDocument")
	assertRequired(t, registry, "schemaVersion", "glossary", "entries", "inventoryExclusions")
	assertRequired(t, policy, "schemaVersion", "comparisonTargets", "minimumNoticeReleases", "emergencyRemovalAllowed")
	assertConstOne(t, property(t, registry, "schemaVersion"), "registry schemaVersion")
	assertConstOne(t, property(t, policy, "schemaVersion"), "policy schemaVersion")

	entry := definition(t, defs, "entry")
	binding := definition(t, defs, "binding")
	behavior := definition(t, defs, "behavior")
	deprecation := definition(t, defs, "deprecation")
	assertRequired(t, entry, "id", "operation", "owner", "state", "bindings", "behavior")
	assertRequired(t, binding, "bindingId", "surface")
	assertRequired(t, behavior, "result", "errors", "pagination", "streaming", "consistency", "limits")
	assertRequired(t, deprecation, "replacementEntryId", "noticeRelease", "removalEarliestRelease", "migration")

	assertEnum(t, property(t, entry, "state"),
		"unassessed", "experimental", "unsupported", "supported", "deprecated", "removed")
	assertEnum(t, property(t, binding, "surface"), "grpc", "http", "cli", "sdk", "sql")
	assertEnum(t, property(t, definition(t, defs, "streaming"), "mode"),
		"none", "client", "server", "bidi")

	entryID := definition(t, defs, "entryId")
	if got := entryID["pattern"]; got != "^[a-z][a-z0-9-]*(\\.[a-z][a-z0-9-]*)+$" {
		t.Fatalf("entry ID pattern = %#v", got)
	}
	bindingID := definition(t, defs, "bindingId")
	assertPatterns(t, bindingID,
		"^grpc:[A-Za-z_][A-Za-z0-9_]*(?:\\.[A-Za-z_][A-Za-z0-9_]*)*/[A-Za-z_][A-Za-z0-9_]*$",
		"^http:[A-Z]+:/[^\\t\\r\\n ]*$",
		"^cli:[^\\s]+(?: [^\\s]+)*$",
		"^sdk:[A-Za-z0-9](?:[A-Za-z0-9._~-]*[A-Za-z0-9])?(?:/[A-Za-z0-9](?:[A-Za-z0-9._~-]*[A-Za-z0-9])?)*\\.\\(\\*?[A-Za-z_][A-Za-z0-9_]*\\)\\.[A-Za-z_][A-Za-z0-9_]*$",
		"^sql:[A-Z][A-Z0-9_-]*( [A-Z][A-Z0-9_-]*)*$",
	)

	assertNullable(t, property(t, entry, "firstSupportedRelease"), "firstSupportedRelease")
	assertNullable(t, property(t, entry, "comparisonTargetId"), "comparisonTargetId")
	assertNullable(t, property(t, entry, "deprecation"), "deprecation")
	assertNullable(t, property(t, binding, "behaviorOverride"), "behaviorOverride")

	minimumNotice := property(t, policy, "minimumNoticeReleases")
	if minimumNotice["type"] != "integer" || minimumNotice["minimum"] != float64(1) {
		t.Fatalf("minimumNoticeReleases schema = %#v", minimumNotice)
	}
	assertObjectsClosed(t, schema, "$")

	var validRegistry any
	if err := yaml.Unmarshal([]byte(validRegistryYAML), &validRegistry); err != nil {
		t.Fatalf("parse valid registry fixture: %v", err)
	}
	if err := compiledSchema.Validate(validRegistry); err != nil {
		t.Fatalf("valid registry fixture diverges from schema: %v", err)
	}
	var validPolicy any
	if err := yaml.Unmarshal([]byte(validPolicyYAML), &validPolicy); err != nil {
		t.Fatalf("parse valid policy fixture: %v", err)
	}
	if err := compiledSchema.Validate(validPolicy); err != nil {
		t.Fatalf("valid policy fixture diverges from schema: %v", err)
	}

	typoYAML := strings.Replace(
		validRegistryYAML,
		"firstSupportedRelease:",
		"firstSupportRelease:",
		1,
	)
	emptyEntriesYAML := strings.Replace(
		validRegistryYAML,
		"entries:\n"+validEntryYAML,
		"entries: []\n",
		1,
	)
	invalidFixtures := []struct {
		name     string
		registry string
		policy   string
		schema   string
	}{
		{
			name:     "unknown field",
			registry: typoYAML,
			policy:   validPolicyYAML,
			schema:   typoYAML,
		},
		{
			name:     "missing required policy value",
			registry: validRegistryYAML,
			policy:   strings.Replace(validPolicyYAML, "emergencyRemovalAllowed: false\n", "", 1),
			schema:   strings.Replace(validPolicyYAML, "emergencyRemovalAllowed: false\n", "", 1),
		},
		{
			name:     "missing required inventory exclusions",
			registry: strings.Replace(validRegistryYAML, "inventoryExclusions: []\n", "", 1),
			policy:   validPolicyYAML,
			schema:   strings.Replace(validRegistryYAML, "inventoryExclusions: []\n", "", 1),
		},
		{
			name:     "wrong value type",
			registry: validRegistryYAML,
			policy:   strings.Replace(validPolicyYAML, "minimumNoticeReleases: 2", "minimumNoticeReleases: two", 1),
			schema:   strings.Replace(validPolicyYAML, "minimumNoticeReleases: 2", "minimumNoticeReleases: two", 1),
		},
		{
			name:     "invalid enum",
			registry: strings.Replace(validRegistryYAML, "state: supported", "state: active", 1),
			policy:   validPolicyYAML,
			schema:   strings.Replace(validRegistryYAML, "state: supported", "state: active", 1),
		},
		{
			name:     "invalid entry id pattern",
			registry: strings.Replace(validRegistryYAML, "id: item.put", "id: Item.put", 1),
			policy:   validPolicyYAML,
			schema:   strings.Replace(validRegistryYAML, "id: item.put", "id: Item.put", 1),
		},
		{
			name:     "invalid grpc binding segments",
			registry: strings.Replace(validRegistryYAML, "grpc:cefas.v1.Cefas/PutItem", "grpc:cefas..v1.Cefas/PutItem", 1),
			policy:   validPolicyYAML,
			schema:   strings.Replace(validRegistryYAML, "grpc:cefas.v1.Cefas/PutItem", "grpc:cefas..v1.Cefas/PutItem", 1),
		},
		{
			name:     "invalid sdk binding symbol",
			registry: strings.Replace(validRegistryYAML, "sdk:github.com/CefasDb/cefasdb/pkg/client.(*Client).PutItem", "sdk:not-a-go-symbol", 1),
			policy:   validPolicyYAML,
			schema:   strings.Replace(validRegistryYAML, "sdk:github.com/CefasDb/cefasdb/pkg/client.(*Client).PutItem", "sdk:not-a-go-symbol", 1),
		},
		{
			name: "whitespace behavior error description",
			registry: strings.Replace(
				validRegistryYAML,
				"              condition: The request is invalid.\n",
				"              condition: The request is invalid.\n              description: \" \"\n",
				1,
			),
			policy: validPolicyYAML,
			schema: strings.Replace(
				validRegistryYAML,
				"              condition: The request is invalid.\n",
				"              condition: The request is invalid.\n              description: \" \"\n",
				1,
			),
		},
		{
			name: "whitespace deviation description",
			registry: strings.Replace(
				validRegistryYAML,
				"            description: The SDK exposes Go values instead of protobuf messages.\n",
				"            description: \" \"\n",
				1,
			),
			policy: validPolicyYAML,
			schema: strings.Replace(
				validRegistryYAML,
				"            description: The SDK exposes Go values instead of protobuf messages.\n",
				"            description: \" \"\n",
				1,
			),
		},
		{
			name:     "empty entries",
			registry: emptyEntriesYAML,
			policy:   validPolicyYAML,
			schema:   emptyEntriesYAML,
		},
		{
			name: "duplicate evidence case",
			registry: strings.Replace(
				validRegistryYAML,
				"      - case.item.put.error\n",
				"      - case.item.put.success\n",
				1,
			),
			policy: validPolicyYAML,
			schema: strings.Replace(
				validRegistryYAML,
				"      - case.item.put.error\n",
				"      - case.item.put.success\n",
				1,
			),
		},
		{
			name:     "conditional supported release",
			registry: strings.Replace(validRegistryYAML, "    firstSupportedRelease: v1.0.0\n", "", 1),
			policy:   validPolicyYAML,
			schema:   strings.Replace(validRegistryYAML, "    firstSupportedRelease: v1.0.0\n", "", 1),
		},
		{
			name:     "mutable version alias",
			registry: validRegistryYAML,
			policy:   strings.Replace(validPolicyYAML, "exactVersion: \"1.1.0\"", "exactVersion: stable", 1),
			schema:   strings.Replace(validPolicyYAML, "exactVersion: \"1.1.0\"", "exactVersion: stable", 1),
		},
		{
			name:     "version range",
			registry: validRegistryYAML,
			policy:   strings.Replace(validPolicyYAML, "exactVersion: \"1.1.0\"", "exactVersion: 1.x", 1),
			schema:   strings.Replace(validPolicyYAML, "exactVersion: \"1.1.0\"", "exactVersion: 1.x", 1),
		},
	}
	for _, fixture := range invalidFixtures {
		fixture := fixture
		t.Run(fixture.name, func(t *testing.T) {
			var schemaValue any
			if err := yaml.Unmarshal([]byte(fixture.schema), &schemaValue); err != nil {
				t.Fatalf("parse invalid schema fixture: %v", err)
			}
			if err := compiledSchema.Validate(schemaValue); err == nil {
				t.Fatal("JSON Schema validator unexpectedly accepted fixture")
			}
			got, err := compatibility.DecodeRegistry(
				strings.NewReader(fixture.registry),
				strings.NewReader(fixture.policy),
			)
			if err == nil {
				t.Fatal("authoritative decoder unexpectedly accepted fixture")
			}
			assertZeroRegistry(t, got)
		})
	}
}

func TestCompatibilityImportBoundary(t *testing.T) {
	t.Parallel()

	domainDirectory := filepath.Join("..", "core", "compatibility")
	entries, err := os.ReadDir(domainDirectory)
	if err != nil {
		t.Fatalf("read domain directory: %v", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") ||
			strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		path := filepath.Join(domainDirectory, entry.Name())
		parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		for _, imported := range parsed.Imports {
			importPath, err := strconv.Unquote(imported.Path.Value)
			if err != nil {
				t.Fatalf("unquote import in %s: %v", path, err)
			}
			if forbiddenDomainImport(importPath) {
				t.Errorf("%s imports forbidden adapter/framework package %q", path, importPath)
			}
		}
	}
}

func writeFixture(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func assertZeroRegistry(t *testing.T, got compatibility.WorkingRegistry) {
	t.Helper()
	if !reflect.DeepEqual(got, compatibility.WorkingRegistry{}) {
		t.Fatalf("returned partial registry: %#v", got)
	}
}

func assertPathError(t *testing.T, err error, wantPath string) {
	t.Helper()
	if err == nil {
		t.Fatal("expected path error, got nil")
	}
	var pathError *os.PathError
	if !errors.As(err, &pathError) {
		t.Fatalf("error %T does not preserve *os.PathError: %v", err, err)
	}
	if pathError.Path != wantPath {
		t.Fatalf("path error path = %q, want %q", pathError.Path, wantPath)
	}
	if !strings.Contains(err.Error(), wantPath) {
		t.Fatalf("error %q does not name path %q", err, wantPath)
	}
}

func objectValue(t *testing.T, value any, label string) map[string]any {
	t.Helper()
	object, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("%s = %T, want object", label, value)
	}
	return object
}

func definition(t *testing.T, defs map[string]any, name string) map[string]any {
	t.Helper()
	return objectValue(t, defs[name], "$defs."+name)
}

func property(t *testing.T, schema map[string]any, name string) map[string]any {
	t.Helper()
	properties := objectValue(t, schema["properties"], "properties")
	return objectValue(t, properties[name], "property "+name)
}

func assertRequired(t *testing.T, schema map[string]any, want ...string) {
	t.Helper()
	gotValues, ok := schema["required"].([]any)
	if !ok {
		t.Fatalf("required = %T, want array", schema["required"])
	}
	got := make([]string, len(gotValues))
	for index, value := range gotValues {
		got[index], ok = value.(string)
		if !ok {
			t.Fatalf("required[%d] = %T, want string", index, value)
		}
	}
	sort.Strings(got)
	sortedWant := append([]string(nil), want...)
	sort.Strings(sortedWant)
	if !reflect.DeepEqual(got, sortedWant) {
		t.Fatalf("required = %v, want %v", got, sortedWant)
	}
}

func assertConstOne(t *testing.T, schema map[string]any, label string) {
	t.Helper()
	if schema["const"] != float64(1) {
		t.Fatalf("%s const = %#v, want 1", label, schema["const"])
	}
}

func assertEnum(t *testing.T, schema map[string]any, want ...string) {
	t.Helper()
	values, ok := schema["enum"].([]any)
	if !ok {
		t.Fatalf("enum = %T, want array", schema["enum"])
	}
	got := make([]string, len(values))
	for index, value := range values {
		got[index], ok = value.(string)
		if !ok {
			t.Fatalf("enum[%d] = %T, want string", index, value)
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("enum = %v, want %v", got, want)
	}
}

func assertPatterns(t *testing.T, schema map[string]any, want ...string) {
	t.Helper()
	branches, ok := schema["oneOf"].([]any)
	if !ok {
		t.Fatalf("oneOf = %T, want array", schema["oneOf"])
	}
	got := make([]string, len(branches))
	for index, branch := range branches {
		got[index], _ = objectValue(t, branch, "pattern branch")["pattern"].(string)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("patterns = %v, want %v", got, want)
	}
}

func assertNullable(t *testing.T, schema map[string]any, label string) {
	t.Helper()
	branches, ok := schema["anyOf"].([]any)
	if !ok {
		t.Fatalf("%s anyOf = %T, want array", label, schema["anyOf"])
	}
	for _, branch := range branches {
		object, ok := branch.(map[string]any)
		if ok && object["type"] == "null" {
			return
		}
	}
	t.Fatalf("%s does not explicitly allow null", label)
}

func assertObjectsClosed(t *testing.T, value any, path string) {
	t.Helper()
	switch value := value.(type) {
	case map[string]any:
		if value["type"] == "object" && value["additionalProperties"] != false {
			t.Errorf("%s object does not declare additionalProperties:false", path)
		}
		for key, child := range value {
			assertObjectsClosed(t, child, path+"."+key)
		}
	case []any:
		for index, child := range value {
			assertObjectsClosed(t, child, fmt.Sprintf("%s[%d]", path, index))
		}
	}
}

func fixtureKeysDeclared(root map[string]any, current map[string]any, value any, path string) error {
	current, err := schemaForValue(root, current, value)
	if err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	switch value := value.(type) {
	case map[string]any:
		properties, _ := current["properties"].(map[string]any)
		patternProperties, _ := current["patternProperties"].(map[string]any)
		for key, child := range value {
			childSchema, exists := properties[key]
			if !exists {
				if len(patternProperties) == 1 {
					for _, candidate := range patternProperties {
						childSchema = candidate
						exists = true
					}
				}
			}
			if !exists {
				return fmt.Errorf("field %q is not declared", key)
			}
			childObject, ok := childSchema.(map[string]any)
			if !ok {
				return fmt.Errorf("schema for field %q is not an object", key)
			}
			if err := fixtureKeysDeclared(root, childObject, child, path+"."+key); err != nil {
				return err
			}
		}
	case []any:
		itemSchema, ok := current["items"].(map[string]any)
		if !ok && len(value) > 0 {
			return fmt.Errorf("array has no item schema")
		}
		for index, child := range value {
			if err := fixtureKeysDeclared(root, itemSchema, child, fmt.Sprintf("%s[%d]", path, index)); err != nil {
				return err
			}
		}
	}
	return nil
}

func schemaForValue(root map[string]any, schema map[string]any, value any) (map[string]any, error) {
	for {
		if reference, ok := schema["$ref"].(string); ok {
			const prefix = "#/$defs/"
			if !strings.HasPrefix(reference, prefix) {
				return nil, fmt.Errorf("unsupported reference %q", reference)
			}
			defs, ok := root["$defs"].(map[string]any)
			if !ok {
				return nil, errors.New("missing $defs")
			}
			resolved, ok := defs[strings.TrimPrefix(reference, prefix)].(map[string]any)
			if !ok {
				return nil, fmt.Errorf("missing reference %q", reference)
			}
			schema = resolved
			continue
		}
		branches, ok := schema["anyOf"].([]any)
		if !ok || schema["type"] == "object" || schema["type"] == "array" ||
			schema["properties"] != nil || schema["items"] != nil {
			return schema, nil
		}
		selected := map[string]any(nil)
		for _, branch := range branches {
			candidate, ok := branch.(map[string]any)
			if !ok {
				continue
			}
			if value == nil && candidate["type"] == "null" {
				selected = candidate
				break
			}
			if value != nil && candidate["type"] != "null" {
				selected = candidate
				break
			}
		}
		if selected == nil {
			return nil, errors.New("no matching anyOf branch")
		}
		schema = selected
	}
}

func forbiddenDomainImport(importPath string) bool {
	switch importPath {
	case "os",
		"io/fs",
		"path/filepath",
		"net/http",
		"github.com/spf13/cobra",
		"github.com/golang/protobuf",
		"google.golang.org/protobuf":
		return true
	}
	lower := strings.ToLower(importPath)
	return strings.Contains(lower, "go-git") ||
		strings.Contains(lower, "/workflow") ||
		strings.HasSuffix(lower, "/git")
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
