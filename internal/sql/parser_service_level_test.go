package sql

import "testing"

func TestParseCreateServiceLevel_Minimal(t *testing.T) {
	stmt, err := Parse("CREATE SERVICE LEVEL olap")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	cs, ok := stmt.(*CreateServiceLevelStmt)
	if !ok {
		t.Fatalf("stmt type = %T, want *CreateServiceLevelStmt", stmt)
	}
	if cs.Name != "olap" {
		t.Errorf("Name = %q, want %q", cs.Name, "olap")
	}
	if cs.Spec != (ServiceLevelSpec{}) {
		t.Errorf("Spec = %+v, want zero", cs.Spec)
	}
}

func TestParseCreateServiceLevel_WithShares(t *testing.T) {
	stmt, err := Parse("CREATE SERVICE LEVEL olap WITH SHARES=20")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	cs := stmt.(*CreateServiceLevelStmt)
	if cs.Spec.Shares != 20 {
		t.Errorf("Shares = %d, want 20", cs.Spec.Shares)
	}
}

func TestParseCreateServiceLevel_WithAllCaps(t *testing.T) {
	input := "CREATE SERVICE LEVEL batch WITH SHARES=5, MAX_IN_FLIGHT=64, MAX_ROWS_PER_SEC=1000, MAX_BYTES_PER_SEC=10485760"
	stmt, err := Parse(input)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	cs := stmt.(*CreateServiceLevelStmt)
	if cs.Spec.Shares != 5 || cs.Spec.MaxInFlight != 64 ||
		cs.Spec.MaxRowsPerSec != 1000 || cs.Spec.MaxBytesPerSec != 10485760 {
		t.Errorf("Spec mismatch: %+v", cs.Spec)
	}
}

func TestParseAlterServiceLevel(t *testing.T) {
	stmt, err := Parse("ALTER SERVICE LEVEL olap WITH SHARES=30")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	as, ok := stmt.(*AlterServiceLevelStmt)
	if !ok || as.Name != "olap" || as.Spec.Shares != 30 {
		t.Errorf("Alter mismatch: ok=%v %+v", ok, as)
	}
}

func TestParseDropServiceLevel(t *testing.T) {
	stmt, err := Parse("DROP SERVICE LEVEL olap")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	ds, ok := stmt.(*DropServiceLevelStmt)
	if !ok || ds.Name != "olap" {
		t.Errorf("Drop mismatch: ok=%v name=%q", ok, ds.Name)
	}
}

func TestParseListServiceLevels(t *testing.T) {
	stmt, err := Parse("LIST SERVICE LEVEL")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if _, ok := stmt.(*ListServiceLevelsStmt); !ok {
		t.Errorf("type = %T, want *ListServiceLevelsStmt", stmt)
	}
}
