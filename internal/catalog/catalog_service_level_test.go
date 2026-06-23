package catalog

import (
	"errors"
	"testing"

	"github.com/CefasDb/cefasdb/pkg/types"
)

func TestServiceLevel_CreateAndGet(t *testing.T) {
	c := newTestCatalog(t)
	sl, err := c.CreateServiceLevel(types.ServiceLevelDescriptor{Name: "olap", Shares: 20})
	if err != nil {
		t.Fatalf("CreateServiceLevel: %v", err)
	}
	if sl.Name != "olap" || sl.Shares != 20 {
		t.Errorf("Created mismatch: %+v", sl)
	}
	got, err := c.GetServiceLevel("olap")
	if err != nil || got.Shares != 20 {
		t.Errorf("Get olap: err=%v shares=%d", err, got.Shares)
	}
}

func TestServiceLevel_DefaultIsSynthetic(t *testing.T) {
	c := newTestCatalog(t)
	sl, err := c.GetServiceLevel(types.DefaultServiceLevelName)
	if err != nil {
		t.Fatalf("Get default: %v", err)
	}
	if sl.Name != types.DefaultServiceLevelName || sl.Shares == 0 {
		t.Errorf("default should exist synthetically: %+v", sl)
	}
}

func TestServiceLevel_CreateReservedReject(t *testing.T) {
	c := newTestCatalog(t)
	_, err := c.CreateServiceLevel(types.ServiceLevelDescriptor{Name: types.DefaultServiceLevelName, Shares: 1})
	if !errors.Is(err, types.ErrServiceLevelReserved) {
		t.Fatalf("CreateServiceLevel default = %v, want ErrServiceLevelReserved", err)
	}
}

func TestServiceLevel_DropDefaultReject(t *testing.T) {
	c := newTestCatalog(t)
	if err := c.DropServiceLevel(types.DefaultServiceLevelName); !errors.Is(err, types.ErrServiceLevelReserved) {
		t.Fatalf("DropServiceLevel default = %v, want ErrServiceLevelReserved", err)
	}
}

func TestServiceLevel_AlterUpdates(t *testing.T) {
	c := newTestCatalog(t)
	if _, err := c.CreateServiceLevel(types.ServiceLevelDescriptor{Name: "batch", Shares: 5}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := c.UpdateServiceLevel(types.ServiceLevelDescriptor{Name: "batch", Shares: 50, MaxInFlight: 64}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	got, _ := c.GetServiceLevel("batch")
	if got.Shares != 50 || got.MaxInFlight != 64 {
		t.Errorf("Updated mismatch: %+v", got)
	}
}

func TestServiceLevel_AlterMissing(t *testing.T) {
	c := newTestCatalog(t)
	if _, err := c.UpdateServiceLevel(types.ServiceLevelDescriptor{Name: "nope", Shares: 1}); !errors.Is(err, types.ErrServiceLevelNotFound) {
		t.Fatalf("Update nope = %v, want ErrServiceLevelNotFound", err)
	}
}

func TestServiceLevel_DropAndDup(t *testing.T) {
	c := newTestCatalog(t)
	if _, err := c.CreateServiceLevel(types.ServiceLevelDescriptor{Name: "tmp", Shares: 1}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := c.CreateServiceLevel(types.ServiceLevelDescriptor{Name: "tmp", Shares: 1}); !errors.Is(err, types.ErrServiceLevelExists) {
		t.Fatalf("dup Create = %v, want ErrServiceLevelExists", err)
	}
	if err := c.DropServiceLevel("tmp"); err != nil {
		t.Fatalf("Drop: %v", err)
	}
	if err := c.DropServiceLevel("tmp"); !errors.Is(err, types.ErrServiceLevelNotFound) {
		t.Fatalf("Drop missing = %v, want ErrServiceLevelNotFound", err)
	}
}

func TestServiceLevel_ListIncludesDefault(t *testing.T) {
	c := newTestCatalog(t)
	if _, err := c.CreateServiceLevel(types.ServiceLevelDescriptor{Name: "oltp", Shares: 80}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	got := c.ListServiceLevels()
	var sawDefault, sawOltp bool
	for _, sl := range got {
		if sl.Name == types.DefaultServiceLevelName {
			sawDefault = true
		}
		if sl.Name == "oltp" && sl.Shares == 80 {
			sawOltp = true
		}
	}
	if !sawDefault {
		t.Error("ListServiceLevels missing default")
	}
	if !sawOltp {
		t.Error("ListServiceLevels missing oltp")
	}
}
