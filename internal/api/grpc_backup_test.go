package api_test

import (
	"context"
	"testing"

	cefaspb "github.com/osvaldoandrade/cefas/pkg/api/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCreateAndListBackups(t *testing.T) {
	stub, cleanup := startUnsecuredFixture(t)
	defer cleanup()
	ctx := context.Background()
	createTable(t, stub, "T1")
	createTable(t, stub, "T2")

	// Empty list before any backups.
	lst, err := stub.ListBackups(ctx, &cefaspb.ListBackupsRequest{})
	if err != nil {
		t.Fatalf("list empty: %v", err)
	}
	if len(lst.GetBackups()) != 0 {
		t.Fatalf("initial backups = %d, want 0", len(lst.GetBackups()))
	}

	// Default scope: every catalog table.
	got, err := stub.CreateBackup(ctx, &cefaspb.CreateBackupRequest{Name: "all"})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if got.GetBackup().GetName() != "all" {
		t.Fatalf("name = %q", got.GetBackup().GetName())
	}
	if len(got.GetBackup().GetTables()) != 2 {
		t.Fatalf("tables = %v, want 2", got.GetBackup().GetTables())
	}
	if got.GetBackup().GetManifestVersion() != 1 || got.GetBackup().GetManifestStatus() != "ok" {
		t.Fatalf("manifest = version %d status %q", got.GetBackup().GetManifestVersion(), got.GetBackup().GetManifestStatus())
	}
	if len(got.GetBackup().GetTableStats()) != 2 {
		t.Fatalf("table stats = %v, want 2 entries", got.GetBackup().GetTableStats())
	}

	// Scoped backup.
	if _, err := stub.CreateBackup(ctx, &cefaspb.CreateBackupRequest{Name: "just-t1", Tables: []string{"T1"}}); err != nil {
		t.Fatalf("create scoped: %v", err)
	}

	lst, err = stub.ListBackups(ctx, &cefaspb.ListBackupsRequest{})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(lst.GetBackups()) != 2 {
		t.Fatalf("backups = %d, want 2", len(lst.GetBackups()))
	}
	// Sorted by name: "all", "just-t1".
	if lst.GetBackups()[0].GetName() != "all" || lst.GetBackups()[1].GetName() != "just-t1" {
		t.Fatalf("order: %v", lst.GetBackups())
	}
	if len(lst.GetBackups()[1].GetRequestedTables()) != 1 || lst.GetBackups()[1].GetRequestedTables()[0] != "T1" {
		t.Fatalf("scoped requested tables = %v", lst.GetBackups()[1].GetRequestedTables())
	}
}

func TestCreateBackupRejectsDuplicate(t *testing.T) {
	stub, cleanup := startUnsecuredFixture(t)
	defer cleanup()
	ctx := context.Background()
	if _, err := stub.CreateBackup(ctx, &cefaspb.CreateBackupRequest{Name: "x"}); err != nil {
		t.Fatalf("first: %v", err)
	}
	if _, err := stub.CreateBackup(ctx, &cefaspb.CreateBackupRequest{Name: "x"}); err == nil {
		t.Fatal("expected duplicate-name error")
	}
}

func TestDeleteBackupRemovesBackup(t *testing.T) {
	stub, cleanup := startUnsecuredFixture(t)
	defer cleanup()
	ctx := context.Background()
	createTable(t, stub, "Users")
	if _, err := stub.CreateBackup(ctx, &cefaspb.CreateBackupRequest{Name: "snap"}); err != nil {
		t.Fatalf("create: %v", err)
	}

	resp, err := stub.DeleteBackup(ctx, &cefaspb.DeleteBackupRequest{Name: "snap"})
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if !resp.GetResult().GetMetadataDeleted() || !resp.GetResult().GetCheckpointDeleted() {
		t.Fatalf("delete result = %+v", resp.GetResult())
	}
	list, err := stub.ListBackups(ctx, &cefaspb.ListBackupsRequest{})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list.GetBackups()) != 0 {
		t.Fatalf("backups after delete = %+v", list.GetBackups())
	}
}

func TestDeleteBackupMissingBackup(t *testing.T) {
	stub, cleanup := startUnsecuredFixture(t)
	defer cleanup()
	_, err := stub.DeleteBackup(context.Background(), &cefaspb.DeleteBackupRequest{Name: "ghost"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("missing backup code = %v, want NotFound: %v", status.Code(err), err)
	}
}

func TestApplyBackupRetentionDryRun(t *testing.T) {
	stub, cleanup := startUnsecuredFixture(t)
	defer cleanup()
	ctx := context.Background()
	if _, err := stub.CreateBackup(ctx, &cefaspb.CreateBackupRequest{Name: "a"}); err != nil {
		t.Fatalf("create a: %v", err)
	}
	if _, err := stub.CreateBackup(ctx, &cefaspb.CreateBackupRequest{Name: "b"}); err != nil {
		t.Fatalf("create b: %v", err)
	}

	resp, err := stub.ApplyBackupRetention(ctx, &cefaspb.ApplyBackupRetentionRequest{
		KeepLatestSet: true,
		KeepLatest:    0,
		DryRun:        true,
	})
	if err != nil {
		t.Fatalf("retention dry-run: %v", err)
	}
	if !resp.GetDryRun() || len(resp.GetWouldDelete()) != 2 || len(resp.GetDeleted()) != 0 {
		t.Fatalf("retention response = %+v", resp)
	}
	list, err := stub.ListBackups(ctx, &cefaspb.ListBackupsRequest{})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list.GetBackups()) != 2 {
		t.Fatalf("dry-run deleted backups: %+v", list.GetBackups())
	}
}
