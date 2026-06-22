package sql

import "testing"

// TestParseCreateMaterializedView covers each accepted REFRESH
// variant plus the projection / PK shapes a v1 caller will use. The
// parser is allowed to drop into the planner unchanged; here we only
// verify the AST.
func TestParseCreateMaterializedView(t *testing.T) {
	cases := []struct {
		name     string
		input    string
		wantName string
		wantBase string
		wantPK   string
		wantSK   string
		wantProj []string
		wantMode string
		wantSecs int64
	}{
		{
			name:     "eager default by omission",
			input:    "CREATE MATERIALIZED VIEW v AS SELECT a, b FROM base PRIMARY KEY (a, b)",
			wantName: "v",
			wantBase: "base",
			wantPK:   "a",
			wantSK:   "b",
			wantProj: []string{"a", "b"},
			wantMode: "eager",
		},
		{
			name:     "explicit eager",
			input:    "CREATE MATERIALIZED VIEW v AS SELECT * FROM base PRIMARY KEY (a) REFRESH EAGER",
			wantName: "v",
			wantBase: "base",
			wantPK:   "a",
			wantMode: "eager",
		},
		{
			name:     "scheduled every seconds",
			input:    "CREATE MATERIALIZED VIEW v AS SELECT * FROM base PRIMARY KEY (a) REFRESH EVERY 30 SECONDS",
			wantName: "v",
			wantBase: "base",
			wantPK:   "a",
			wantMode: "scheduled",
			wantSecs: 30,
		},
		{
			name:     "scheduled every hours",
			input:    "CREATE MATERIALIZED VIEW v AS SELECT * FROM base PRIMARY KEY (a) REFRESH EVERY 2 HOURS",
			wantName: "v",
			wantBase: "base",
			wantPK:   "a",
			wantMode: "scheduled",
			wantSecs: 7200,
		},
		{
			name:     "on demand",
			input:    "CREATE MATERIALIZED VIEW v AS SELECT * FROM base PRIMARY KEY (a) REFRESH ON DEMAND",
			wantName: "v",
			wantBase: "base",
			wantPK:   "a",
			wantMode: "on_demand",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := Parse(tc.input)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			mv, ok := stmt.(*CreateMaterializedViewStmt)
			if !ok {
				t.Fatalf("expected *CreateMaterializedViewStmt, got %T", stmt)
			}
			if mv.Name != tc.wantName {
				t.Errorf("Name = %q, want %q", mv.Name, tc.wantName)
			}
			if mv.BaseTable != tc.wantBase {
				t.Errorf("BaseTable = %q, want %q", mv.BaseTable, tc.wantBase)
			}
			if mv.PK != tc.wantPK {
				t.Errorf("PK = %q, want %q", mv.PK, tc.wantPK)
			}
			if mv.SK != tc.wantSK {
				t.Errorf("SK = %q, want %q", mv.SK, tc.wantSK)
			}
			if mv.Refresh.Mode != tc.wantMode {
				t.Errorf("RefreshMode = %q, want %q", mv.Refresh.Mode, tc.wantMode)
			}
			if mv.Refresh.IntervalSeconds != tc.wantSecs {
				t.Errorf("IntervalSeconds = %d, want %d", mv.Refresh.IntervalSeconds, tc.wantSecs)
			}
			if len(tc.wantProj) > 0 {
				if len(mv.Projected) != len(tc.wantProj) {
					t.Fatalf("Projected len = %d, want %d", len(mv.Projected), len(tc.wantProj))
				}
				for i, p := range tc.wantProj {
					if mv.Projected[i] != p {
						t.Errorf("Projected[%d] = %q, want %q", i, mv.Projected[i], p)
					}
				}
			}
		})
	}
}

func TestParseDropMaterializedView(t *testing.T) {
	stmt, err := Parse("DROP MATERIALIZED VIEW v")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	mv, ok := stmt.(*DropMaterializedViewStmt)
	if !ok {
		t.Fatalf("expected *DropMaterializedViewStmt, got %T", stmt)
	}
	if mv.Name != "v" {
		t.Errorf("Name = %q, want %q", mv.Name, "v")
	}
}

// TestParseCreateTableUnaffected makes sure the CREATE-disambiguation
// lookahead does not swallow regular CREATE TABLE statements.
func TestParseCreateTableUnaffected(t *testing.T) {
	stmt, err := Parse("CREATE TABLE t (PRIMARY KEY (pk))")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if _, ok := stmt.(*CreateTableStmt); !ok {
		t.Fatalf("expected *CreateTableStmt, got %T", stmt)
	}
}
