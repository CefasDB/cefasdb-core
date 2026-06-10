package api_test

import (
	"context"
	"testing"

	cefaspb "github.com/osvaldoandrade/cefas/pkg/api/proto"
)

func TestRestoreTableFromBackupRoundTrips(t *testing.T) {
	stub, cleanup := startUnsecuredFixture(t)
	defer cleanup()
	ctx := context.Background()
	createTable(t, stub, "Users")
	for _, id := range []string{"u1", "u2", "u3"} {
		putString(t, stub, "Users", id, id+"-v")
	}

	if _, err := stub.CreateBackup(ctx, &cefaspb.CreateBackupRequest{Name: "snap"}); err != nil {
		t.Fatalf("backup: %v", err)
	}

	// Mutate the live table to prove restore reads from the backup, not
	// the current state.
	putString(t, stub, "Users", "u1", "mutated")

	resp, err := stub.RestoreTableFromBackup(ctx, &cefaspb.RestoreTableFromBackupRequest{
		BackupName:      "snap",
		SourceTableName: "Users",
		TargetTableName: "Users_restored",
	})
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if resp.GetTargetTableName() != "Users_restored" {
		t.Fatalf("target = %q", resp.GetTargetTableName())
	}
	if resp.GetRowsCopied() != 3 {
		t.Fatalf("rows copied = %d, want 3", resp.GetRowsCopied())
	}

	// The restored row reflects the backup, not the post-backup mutation.
	got, err := stub.GetItem(ctx, &cefaspb.GetItemRequest{
		Table: "Users_restored",
		Key: map[string]*cefaspb.AttributeValue{
			"id": {Value: &cefaspb.AttributeValue_S{S: "u1"}},
		},
	})
	if err != nil {
		t.Fatalf("get restored: %v", err)
	}
	if got.GetItem()["v"].GetS() != "u1-v" {
		t.Fatalf("restored v = %q, want u1-v", got.GetItem()["v"].GetS())
	}
}

func TestRestoreTableFromBackupUnknownBackup(t *testing.T) {
	stub, cleanup := startUnsecuredFixture(t)
	defer cleanup()
	ctx := context.Background()
	createTable(t, stub, "T")
	_, err := stub.RestoreTableFromBackup(ctx, &cefaspb.RestoreTableFromBackupRequest{
		BackupName:      "ghost",
		SourceTableName: "T",
		TargetTableName: "T2",
	})
	if err == nil {
		t.Fatal("expected error for unknown backup")
	}
}

func TestRestoreTableFromBackupDuplicateTarget(t *testing.T) {
	stub, cleanup := startUnsecuredFixture(t)
	defer cleanup()
	ctx := context.Background()
	createTable(t, stub, "T")
	createTable(t, stub, "T2") // pre-existing target — register should reject
	if _, err := stub.CreateBackup(ctx, &cefaspb.CreateBackupRequest{Name: "snap"}); err != nil {
		t.Fatalf("backup: %v", err)
	}
	_, err := stub.RestoreTableFromBackup(ctx, &cefaspb.RestoreTableFromBackupRequest{
		BackupName:      "snap",
		SourceTableName: "T",
		TargetTableName: "T2",
	})
	if err == nil {
		t.Fatal("expected error for duplicate target")
	}
}
