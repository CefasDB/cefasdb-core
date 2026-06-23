package sql

import "testing"

func TestParseCreateGlobalIndex_Minimal(t *testing.T) {
	stmt, err := Parse("CREATE INDEX idx_email AS GLOBAL ON Users (email)")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	cg, ok := stmt.(*CreateGlobalIndexStmt)
	if !ok {
		t.Fatalf("stmt type = %T, want *CreateGlobalIndexStmt", stmt)
	}
	if cg.Name != "idx_email" || cg.BaseTable != "Users" || cg.IndexedColumn != "email" {
		t.Errorf("descriptor mismatch: %+v", cg)
	}
	if len(cg.Projected) != 0 {
		t.Errorf("Projected = %v, want empty", cg.Projected)
	}
}

func TestParseCreateGlobalIndex_WithProjection(t *testing.T) {
	stmt, err := Parse("CREATE INDEX idx_email AS GLOBAL ON Users (email) PROJECT (id, name)")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	cg := stmt.(*CreateGlobalIndexStmt)
	if len(cg.Projected) != 2 || cg.Projected[0] != "id" || cg.Projected[1] != "name" {
		t.Errorf("Projected mismatch: %v", cg.Projected)
	}
}

func TestParseCreateGlobalIndex_WithPlacement(t *testing.T) {
	stmt, err := Parse("CREATE INDEX idx_status AS GLOBAL ON Orders (status) WITH SHARDS=4, REPLICATION_FACTOR=2")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	cg := stmt.(*CreateGlobalIndexStmt)
	if cg.Shards != 4 || cg.ReplicationFactor != 2 {
		t.Errorf("placement mismatch: %+v", cg)
	}
}

func TestParseDropGlobalIndex(t *testing.T) {
	stmt, err := Parse("DROP INDEX idx_email")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if _, ok := stmt.(*DropGlobalIndexStmt); !ok {
		t.Errorf("type = %T, want *DropGlobalIndexStmt", stmt)
	}
}
