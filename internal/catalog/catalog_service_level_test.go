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

func TestServiceLevel_PauseResume(t *testing.T) {
	c := newTestCatalog(t)
	if _, err := c.CreateServiceLevel(types.ServiceLevelDescriptor{Name: "olap", Shares: 20}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	paused, err := c.PauseServiceLevel("olap")
	if err != nil {
		t.Fatalf("Pause: %v", err)
	}
	if !paused.Paused {
		t.Error("Paused = false, want true after PauseServiceLevel")
	}
	got, _ := c.GetServiceLevel("olap")
	if !got.Paused {
		t.Error("GetServiceLevel did not surface the persisted Paused state")
	}
	resumed, err := c.ResumeServiceLevel("olap")
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if resumed.Paused {
		t.Error("Resumed.Paused = true, want false")
	}
}

func TestServiceLevel_PauseDefaultReject(t *testing.T) {
	c := newTestCatalog(t)
	if _, err := c.PauseServiceLevel(types.DefaultServiceLevelName); !errors.Is(err, types.ErrServiceLevelReserved) {
		t.Fatalf("Pause default = %v, want ErrServiceLevelReserved", err)
	}
}

func TestServiceLevel_PauseMissing(t *testing.T) {
	c := newTestCatalog(t)
	if _, err := c.PauseServiceLevel("nope"); !errors.Is(err, types.ErrServiceLevelNotFound) {
		t.Fatalf("Pause missing = %v, want ErrServiceLevelNotFound", err)
	}
}

func TestServiceLevel_PauseTriggersChangeListener(t *testing.T) {
	c := newTestCatalog(t)
	if _, err := c.CreateServiceLevel(types.ServiceLevelDescriptor{Name: "olap", Shares: 20}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	var fired int
	c.OnServiceLevelChanged(func(name string) {
		if name == "olap" {
			fired++
		}
	})
	if _, err := c.PauseServiceLevel("olap"); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	if _, err := c.ResumeServiceLevel("olap"); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if fired != 2 {
		t.Errorf("OnServiceLevelChanged fired %d times, want 2 (pause + resume)", fired)
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
