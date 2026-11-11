package compatibility_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/CefasDb/cefasdb/internal/core/compatibility"
)

func TestModelValidationAndNormalization(t *testing.T) {
	t.Parallel()

	t.Run("valid aggregate", func(t *testing.T) {
		t.Parallel()
		registry := validRegistry()
		if err := registry.Validate(); err != nil {
			t.Fatalf("Validate() unexpected error: %v", err)
		}
	})

	t.Run("identity grammar and enums", func(t *testing.T) {
		t.Parallel()

		for _, value := range []string{"item.put", "table.batch-get", "sql.execute.statement"} {
			value := value
			t.Run("entry accepts "+value, func(t *testing.T) {
				t.Parallel()
				if _, err := compatibility.NewEntryID(value); err != nil {
					t.Fatalf("NewEntryID(%q): %v", value, err)
				}
			})
		}
		for _, value := range []string{"", "item", "Item.put", "item..put", "item_put", "item.put "} {
			value := value
			t.Run("entry rejects "+value, func(t *testing.T) {
				t.Parallel()
				assertCode(t, mustError(compatibility.NewEntryID(value)), compatibility.DiagnosticIdentity)
			})
		}

		bindings := map[string]compatibility.Surface{
			"grpc:cefas.v1.Cefas/PutItem": compatibility.SurfaceGRPC,
			"http:POST:/v1/PutItem":       compatibility.SurfaceHTTP,
			"cli:cefas put-item":          compatibility.SurfaceCLI,
			"sdk:github.com/CefasDb/cefasdb/pkg/client.(*Client).PutItem": compatibility.SurfaceSDK,
			"sql:INSERT": compatibility.SurfaceSQL,
		}
		for value, surface := range bindings {
			id, err := compatibility.NewBindingID(value)
			if err != nil {
				t.Fatalf("NewBindingID(%q): %v", value, err)
			}
			if id.Surface() != surface {
				t.Fatalf("NewBindingID(%q).Surface() = %q, want %q", value, id.Surface(), surface)
			}
		}
		for _, value := range []string{
			"GRPC:cefas.v1.Cefas/PutItem",
			"grpc:Cefas.PutItem",
			"grpc:cefas..v1.Cefas/PutItem",
			"grpc:cefas.v1.Cefas./PutItem",
			"http:post:/v1/PutItem",
			"cli:cefas  put-item",
			"sdk:github.com/CefasDb/cefasdb/pkg/client Client.PutItem",
			"sdk:not-a-go-symbol",
			"sdk:github.com/CefasDb/cefasdb/pkg/client.Client.PutItem",
			"sql:insert",
		} {
			if _, err := compatibility.NewBindingID(value); err == nil {
				t.Fatalf("NewBindingID(%q) unexpectedly succeeded", value)
			} else {
				assertCode(t, err, compatibility.DiagnosticIdentity)
			}
		}

		for _, surface := range []compatibility.Surface{
			compatibility.SurfaceGRPC,
			compatibility.SurfaceHTTP,
			compatibility.SurfaceCLI,
			compatibility.SurfaceSDK,
			compatibility.SurfaceSQL,
		} {
			if err := surface.Validate(); err != nil {
				t.Fatalf("Surface(%q).Validate(): %v", surface, err)
			}
		}
		if err := compatibility.Surface("queue").Validate(); err == nil {
			t.Fatal("unknown surface unexpectedly validated")
		}

		for _, state := range []compatibility.SupportState{
			compatibility.SupportUnassessed,
			compatibility.SupportExperimental,
			compatibility.SupportUnsupported,
			compatibility.SupportSupported,
			compatibility.SupportDeprecated,
			compatibility.SupportRemoved,
		} {
			if err := state.Validate(); err != nil {
				t.Fatalf("SupportState(%q).Validate(): %v", state, err)
			}
		}
		if err := compatibility.SupportState("retired").Validate(); err == nil {
			t.Fatal("unknown support state unexpectedly validated")
		}
	})

	t.Run("diagnostic invariants", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name string
			code string
			edit func(*compatibility.WorkingRegistry)
		}{
			{
				name: "registry schema",
				code: compatibility.DiagnosticSchema,
				edit: func(registry *compatibility.WorkingRegistry) {
					registry.SchemaVersion = 2
				},
			},
			{
				name: "policy schema",
				code: compatibility.DiagnosticSchema,
				edit: func(registry *compatibility.WorkingRegistry) {
					registry.Policy.SchemaVersion = 2
				},
			},
			{
				name: "glossary revision",
				code: compatibility.DiagnosticRequired,
				edit: func(registry *compatibility.WorkingRegistry) {
					registry.Glossary.Revision = ""
				},
			},
			{
				name: "glossary terms",
				code: compatibility.DiagnosticRequired,
				edit: func(registry *compatibility.WorkingRegistry) {
					registry.Glossary.Terms = nil
				},
			},
			{
				name: "latest comparison alias",
				code: compatibility.DiagnosticRequired,
				edit: func(registry *compatibility.WorkingRegistry) {
					registry.Policy.ComparisonTargets[0].ExactVersion = "latest"
				},
			},
			{
				name: "current comparison alias",
				code: compatibility.DiagnosticRequired,
				edit: func(registry *compatibility.WorkingRegistry) {
					registry.Policy.ComparisonTargets[0].ExactVersion = "current"
				},
			},
			{
				name: "stable comparison alias",
				code: compatibility.DiagnosticRequired,
				edit: func(registry *compatibility.WorkingRegistry) {
					registry.Policy.ComparisonTargets[0].ExactVersion = "stable"
				},
			},
			{
				name: "head comparison alias",
				code: compatibility.DiagnosticRequired,
				edit: func(registry *compatibility.WorkingRegistry) {
					registry.Policy.ComparisonTargets[0].ExactVersion = "head"
				},
			},
			{
				name: "comparison version range",
				code: compatibility.DiagnosticRequired,
				edit: func(registry *compatibility.WorkingRegistry) {
					registry.Policy.ComparisonTargets[0].ExactVersion = "1.x"
				},
			},
			{
				name: "zero notice window",
				code: compatibility.DiagnosticRequired,
				edit: func(registry *compatibility.WorkingRegistry) {
					registry.Policy.MinimumNoticeReleases = 0
				},
			},
			{
				name: "missing comparison targets",
				code: compatibility.DiagnosticRequired,
				edit: func(registry *compatibility.WorkingRegistry) {
					registry.Policy.ComparisonTargets = nil
				},
			},
			{
				name: "missing owner",
				code: compatibility.DiagnosticRequired,
				edit: func(registry *compatibility.WorkingRegistry) {
					registry.Entries[1].Owner = ""
				},
			},
			{
				name: "missing operation",
				code: compatibility.DiagnosticRequired,
				edit: func(registry *compatibility.WorkingRegistry) {
					registry.Entries[1].Operation = ""
				},
			},
			{
				name: "missing behavior",
				code: compatibility.DiagnosticRequired,
				edit: func(registry *compatibility.WorkingRegistry) {
					registry.Entries[1].Behavior.Result = ""
				},
			},
			{
				name: "whitespace behavior error description",
				code: compatibility.DiagnosticRequired,
				edit: func(registry *compatibility.WorkingRegistry) {
					registry.Entries[1].Behavior.Errors[0].Description = " "
				},
			},
			{
				name: "whitespace deviation description",
				code: compatibility.DiagnosticRequired,
				edit: func(registry *compatibility.WorkingRegistry) {
					registry.Entries[1].Bindings[0].Deviations[0].Description = " "
				},
			},
			{
				name: "missing supported evidence",
				code: compatibility.DiagnosticRequired,
				edit: func(registry *compatibility.WorkingRegistry) {
					registry.Entries[1].EvidenceCaseIDs = nil
				},
			},
			{
				name: "surface prefix mismatch",
				code: compatibility.DiagnosticIdentity,
				edit: func(registry *compatibility.WorkingRegistry) {
					registry.Entries[1].Bindings[0].Surface = compatibility.SurfaceHTTP
				},
			},
			{
				name: "duplicate entry",
				code: compatibility.DiagnosticIdentity,
				edit: func(registry *compatibility.WorkingRegistry) {
					registry.Entries[0].ID = registry.Entries[1].ID
				},
			},
			{
				name: "duplicate binding across entries",
				code: compatibility.DiagnosticIdentity,
				edit: func(registry *compatibility.WorkingRegistry) {
					registry.Entries[0].Bindings[0] = registry.Entries[1].Bindings[1]
				},
			},
			{
				name: "binding also excluded",
				code: compatibility.DiagnosticIdentity,
				edit: func(registry *compatibility.WorkingRegistry) {
					registry.InventoryExclusions[0].BindingID = registry.Entries[1].Bindings[0].BindingID
				},
			},
			{
				name: "unknown comparison target",
				code: compatibility.DiagnosticRequired,
				edit: func(registry *compatibility.WorkingRegistry) {
					registry.Entries[1].ComparisonTargetID = stringPointer("missing")
				},
			},
			{
				name: "deprecated without data",
				code: compatibility.DiagnosticRequired,
				edit: func(registry *compatibility.WorkingRegistry) {
					registry.Entries[1].State = compatibility.SupportDeprecated
					registry.Entries[1].Deprecation = nil
				},
			},
			{
				name: "incomplete deprecation data",
				code: compatibility.DiagnosticRequired,
				edit: func(registry *compatibility.WorkingRegistry) {
					registry.Entries[1].State = compatibility.SupportDeprecated
					registry.Entries[1].Deprecation = &compatibility.Deprecation{
						ReplacementEntryID: compatibility.MustEntryID("item.replace"),
						NoticeRelease:      "v2.0.0",
					}
				},
			},
			{
				name: "unknown replacement entry",
				code: compatibility.DiagnosticRequired,
				edit: func(registry *compatibility.WorkingRegistry) {
					registry.Entries[1].State = compatibility.SupportDeprecated
					registry.Entries[1].Deprecation = &compatibility.Deprecation{
						ReplacementEntryID:     compatibility.MustEntryID("item.missing"),
						NoticeRelease:          "v2.0.0",
						RemovalEarliestRelease: "v4.0.0",
						Migration:              "Use the replacement entry.",
					}
				},
			},
		}

		for _, test := range tests {
			test := test
			t.Run(test.name, func(t *testing.T) {
				t.Parallel()
				registry := validRegistry()
				test.edit(&registry)
				assertCode(t, registry.Validate(), test.code)
			})
		}
	})

	t.Run("deprecated entry with complete data", func(t *testing.T) {
		t.Parallel()
		registry := validRegistry()
		registry.Entries[1].State = compatibility.SupportDeprecated
		registry.Entries[1].Deprecation = &compatibility.Deprecation{
			ReplacementEntryID:     compatibility.MustEntryID("table.get"),
			NoticeRelease:          "v2.0.0",
			RemovalEarliestRelease: "v4.0.0",
			Migration:              "Use item.replace.",
		}
		if err := registry.Validate(); err != nil {
			t.Fatalf("Validate() deprecated entry: %v", err)
		}
	})

	t.Run("normalization sorts a detached copy", func(t *testing.T) {
		t.Parallel()
		registry := validRegistry()
		original := registry.Normalize()
		normalized := registry.Normalize()

		if got := normalized.Entries[0].ID.String(); got != "item.put" {
			t.Fatalf("first normalized entry = %q, want item.put", got)
		}
		if got := normalized.Entries[0].Bindings[0].BindingID.String(); got != "grpc:cefas.v1.Cefas/PutItem" {
			t.Fatalf("first normalized binding = %q", got)
		}
		if !reflect.DeepEqual(normalized.Entries[0].EvidenceCaseIDs, []string{"case.item.put.error", "case.item.put.success"}) {
			t.Fatalf("normalized evidence = %#v", normalized.Entries[0].EvidenceCaseIDs)
		}
		if !reflect.DeepEqual(
			[]string{normalized.Policy.ComparisonTargets[0].ID, normalized.Policy.ComparisonTargets[1].ID},
			[]string{"dynamodb", "partiql"},
		) {
			t.Fatalf("normalized targets = %#v", normalized.Policy.ComparisonTargets)
		}
		if got := normalized.InventoryExclusions[0].BindingID.String(); got != "cli:cefas debug" {
			t.Fatalf("first normalized exclusion = %q", got)
		}
		if !reflect.DeepEqual(normalized.Normalize(), normalized) {
			t.Fatal("Normalize() is not idempotent")
		}

		if registry.Entries[0].ID.String() != "table.get" {
			t.Fatalf("Normalize() mutated input entry order: %#v", registry.Entries)
		}
		if registry.Entries[1].Bindings[0].Surface != compatibility.SurfaceSDK {
			t.Fatalf("Normalize() mutated input binding order: %#v", registry.Entries[1].Bindings)
		}
		if registry.Policy.ComparisonTargets[0].ID != "partiql" {
			t.Fatalf("Normalize() mutated input target order: %#v", registry.Policy.ComparisonTargets)
		}
		if registry.InventoryExclusions[0].BindingID.String() != "sql:VACUUM" {
			t.Fatalf("Normalize() mutated input exclusion order: %#v", registry.InventoryExclusions)
		}

		normalized.Glossary.Terms["operation"] = "changed"
		normalized.Entries[0].EvidenceCaseIDs[0] = "changed"
		normalized.Entries[0].Behavior.Errors[0].Code = "changed"
		normalized.Entries[0].Bindings[0].Deviations = append(normalized.Entries[0].Bindings[0].Deviations, compatibility.Deviation{Summary: "changed"})
		if !reflect.DeepEqual(registry.Normalize(), original) {
			t.Fatal("mutating normalized copy changed the input aggregate")
		}
	})
}

func validRegistry() compatibility.WorkingRegistry {
	itemRelease := "v1.0.0"
	target := "dynamodb"
	return compatibility.WorkingRegistry{
		SchemaVersion: 1,
		Glossary: compatibility.GlossaryRef{
			InitiativeID: "INIT-2026-005-domain-lifecycles",
			Revision:     "glossary-v1",
			Terms: map[string]string{
				"operation": "A stable public capability.",
			},
		},
		Policy: compatibility.CompatibilityPolicy{
			SchemaVersion: 1,
			ComparisonTargets: []compatibility.ComparisonTarget{
				{ID: "partiql", Product: "PartiQL", ExactVersion: "1.1.0", Source: "partiql.org"},
				{ID: "dynamodb", Product: "Amazon DynamoDB", ExactVersion: "2026-10-01", Source: "docs.aws.amazon.com"},
			},
			MinimumNoticeReleases: 2,
		},
		Entries: []compatibility.CompatibilityEntry{
			{
				ID:        compatibility.MustEntryID("table.get"),
				Operation: "Get table metadata",
				Owner:     "core-api",
				State:     compatibility.SupportUnsupported,
				Bindings: []compatibility.InterfaceBinding{
					{
						BindingID: compatibility.MustBindingID("http:GET:/v1/tables"),
						Surface:   compatibility.SurfaceHTTP,
					},
				},
				Behavior: behavior(),
			},
			{
				ID:                    compatibility.MustEntryID("item.put"),
				Operation:             "Put item",
				Owner:                 "core-api",
				State:                 compatibility.SupportSupported,
				FirstSupportedRelease: &itemRelease,
				Bindings: []compatibility.InterfaceBinding{
					{
						BindingID: compatibility.MustBindingID("sdk:github.com/CefasDb/cefasdb/pkg/client.(*Client).PutItem"),
						Surface:   compatibility.SurfaceSDK,
						Deviations: []compatibility.Deviation{
							{Summary: "SDK uses a typed request."},
						},
					},
					{
						BindingID: compatibility.MustBindingID("grpc:cefas.v1.Cefas/PutItem"),
						Surface:   compatibility.SurfaceGRPC,
					},
				},
				Behavior:           behavior(),
				ComparisonTargetID: &target,
				EvidenceCaseIDs:    []string{"case.item.put.success", "case.item.put.error"},
			},
		},
		InventoryExclusions: []compatibility.InventoryExclusion{
			{
				BindingID:  compatibility.MustBindingID("sql:VACUUM"),
				ReasonCode: "internal",
				Rationale:  "Administrative statement is not public.",
				Owner:      "core-api",
			},
			{
				BindingID:  compatibility.MustBindingID("cli:cefas debug"),
				ReasonCode: "internal",
				Rationale:  "Diagnostic command is not public.",
				Owner:      "core-api",
			},
		},
	}
}

func behavior() compatibility.BehaviorContract {
	return compatibility.BehaviorContract{
		Result: "Returns the operation result.",
		Errors: []compatibility.BehaviorError{
			{Code: "invalid-argument", Condition: "The request is invalid."},
		},
		Pagination: compatibility.PaginationRule{Mode: "none"},
		Streaming:  compatibility.StreamingRule{Mode: "none"},
		Consistency: []string{
			"strong",
		},
		Limits: []compatibility.LimitRule{
			{Name: "request-size", Value: "1 MiB"},
		},
	}
}

func assertCode(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected diagnostic %s, got nil", want)
	}
	var diagnostic compatibility.Diagnostic
	if !errors.As(err, &diagnostic) {
		t.Fatalf("error %T is not compatibility.Diagnostic: %v", err, err)
	}
	if diagnostic.Code != want {
		t.Fatalf("diagnostic code = %q, want %q: %v", diagnostic.Code, want, err)
	}
}

func mustError[T any](value T, err error) error {
	return err
}

func stringPointer(value string) *string {
	return &value
}
