package catalog_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/osvaldoandrade/cefas/internal/catalog"
	"github.com/osvaldoandrade/cefas/internal/storage"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

func openCat(t *testing.T) *catalog.Catalog {
	t.Helper()
	dir := t.TempDir()
	db, err := storage.Open(storage.Options{Path: dir})
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	c, err := catalog.New(db)
	if err != nil {
		t.Fatalf("new catalog: %v", err)
	}
	return c
}

func TestUpdateTableSetsTTL(t *testing.T) {
	c := openCat(t)
	td := types.TableDescriptor{
		Name:      "Sessions",
		KeySchema: types.KeySchema{PK: "pk"},
	}
	if err := c.Create(td); err != nil {
		t.Fatalf("create: %v", err)
	}

	td.TTLAttribute = "expires_at"
	if err := c.UpdateTable(td); err != nil {
		t.Fatalf("update: %v", err)
	}

	got, err := c.Describe("Sessions")
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	if got.TTLAttribute != "expires_at" {
		t.Fatalf("TTLAttribute = %q, want %q", got.TTLAttribute, "expires_at")
	}
}

func TestUpdateTableClearsTTL(t *testing.T) {
	c := openCat(t)
	td := types.TableDescriptor{
		Name:         "Sessions",
		KeySchema:    types.KeySchema{PK: "pk"},
		TTLAttribute: "expires_at",
	}
	if err := c.Create(td); err != nil {
		t.Fatalf("create: %v", err)
	}
	td.TTLAttribute = ""
	if err := c.UpdateTable(td); err != nil {
		t.Fatalf("update: %v", err)
	}
	got, _ := c.Describe("Sessions")
	if got.TTLAttribute != "" {
		t.Fatalf("TTLAttribute = %q, want empty", got.TTLAttribute)
	}
}

func TestUpdateTableUnknownTable(t *testing.T) {
	c := openCat(t)
	err := c.UpdateTable(types.TableDescriptor{Name: "ghost", KeySchema: types.KeySchema{PK: "pk"}})
	if !errors.Is(err, types.ErrTableNotFound) {
		t.Fatalf("want ErrTableNotFound, got %v", err)
	}
}

func TestUpdateTablePersistsAcrossReload(t *testing.T) {
	c := openCat(t)
	td := types.TableDescriptor{Name: "T", KeySchema: types.KeySchema{PK: "pk"}}
	_ = c.Create(td)
	td.TTLAttribute = "exp"
	_ = c.UpdateTable(td)
	if err := c.Reload(); err != nil {
		t.Fatalf("reload: %v", err)
	}
	got, _ := c.Describe("T")
	if got.TTLAttribute != "exp" {
		t.Fatalf("TTLAttribute after reload = %q", got.TTLAttribute)
	}
}

func TestCreateTableNormalizesStreamSpecification(t *testing.T) {
	c := openCat(t)
	td := types.TableDescriptor{
		Name:      "Events",
		KeySchema: types.KeySchema{PK: "pk"},
		StreamSpecification: &types.StreamSpecification{
			StreamEnabled: true,
		},
	}
	if err := c.Create(td); err != nil {
		t.Fatalf("create: %v", err)
	}
	got, err := c.Describe("Events")
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	if got.StreamSpecification == nil || !got.StreamSpecification.StreamEnabled {
		t.Fatalf("stream specification not enabled: %+v", got.StreamSpecification)
	}
	if got.StreamSpecification.StreamViewType != types.StreamViewTypeNewAndOldImages {
		t.Fatalf("stream view = %q, want %q", got.StreamSpecification.StreamViewType, types.StreamViewTypeNewAndOldImages)
	}
	if got.StreamStatus != types.StreamStatusEnabled {
		t.Fatalf("stream status = %q, want %q", got.StreamStatus, types.StreamStatusEnabled)
	}
	if got.LatestStreamLabel == "" {
		t.Fatal("LatestStreamLabel is empty")
	}
	if !strings.Contains(got.LatestStreamArn, "table/Events/stream/"+got.LatestStreamLabel) {
		t.Fatalf("LatestStreamArn = %q, label = %q", got.LatestStreamArn, got.LatestStreamLabel)
	}
}

func TestCreateTableRejectsInvalidStreamViewType(t *testing.T) {
	c := openCat(t)
	err := c.Create(types.TableDescriptor{
		Name:      "Events",
		KeySchema: types.KeySchema{PK: "pk"},
		StreamSpecification: &types.StreamSpecification{
			StreamEnabled:  true,
			StreamViewType: "FULL_IMAGE",
		},
	})
	if err == nil || !strings.Contains(err.Error(), "streamViewType") {
		t.Fatalf("want streamViewType error, got %v", err)
	}
}

func TestCreateTableClearsDisabledStreamSpecification(t *testing.T) {
	c := openCat(t)
	if err := c.Create(types.TableDescriptor{
		Name:      "Events",
		KeySchema: types.KeySchema{PK: "pk"},
		StreamSpecification: &types.StreamSpecification{
			StreamEnabled:  false,
			StreamViewType: types.StreamViewTypeNewImage,
		},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	got, err := c.Describe("Events")
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	if got.StreamSpecification != nil || got.LatestStreamArn != "" || got.LatestStreamLabel != "" || got.StreamStatus != "" {
		t.Fatalf("disabled stream metadata not cleared: %+v", got)
	}
}
