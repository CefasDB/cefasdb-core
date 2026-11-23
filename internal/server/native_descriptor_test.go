package server

import (
	"reflect"
	"slices"
	"testing"

	cefaspb "github.com/CefasDb/cefasdb/pkg/protocol"
	"github.com/CefasDb/cefasdb/pkg/types"
)

func TestNativeDescriptorProtobufRoundTrip(t *testing.T) {
	td := types.TableDescriptor{
		Name:         "logs",
		KeySchema:    types.KeySchema{PK: "tenant", SK: "time"},
		TTLAttribute: "expiresAt",
		TextIndexes:  []types.TextIndexDescriptor{{Name: "message", Field: "message"}},
		GSIs: []types.GSIDescriptor{
			{
				Name:       "logs_by_status",
				KeySchema:  types.KeySchema{PK: "status", SK: "time"},
				Projected:  []string{"legacy_message"},
				Projection: types.IndexProjection{Mode: "INCLUDE", Include: []string{"message", "severity"}},
			},
			{
				Name:       "logs_by_owner",
				KeySchema:  types.KeySchema{PK: "owner", SK: "time"},
				Projection: types.IndexProjection{Include: []string{}},
			},
		},
		LSIs: []types.LSIDescriptor{
			{
				Name:       "users_by_email_v1",
				SK:         "email",
				Projection: types.IndexProjection{Mode: "INCLUDE", Include: nil},
			},
			{
				Name:       "events_by_expiry_v1",
				SK:         "expires_at",
				Projection: types.IndexProjection{Mode: "KEYS_ONLY", Include: []string{}},
			},
		},
	}

	pb := tableDescriptorToPB(td)
	if pb.GetGsis()[1].GetProjection() != nil {
		t.Fatalf("all-zero Go projection encoded as %+v", pb.GetGsis()[1].GetProjection())
	}
	pb.GetGsis()[0].Projected[0] = "mutated"
	pb.GetGsis()[0].Projection.Include[0] = "mutated"
	if td.GSIs[0].Projected[0] != "legacy_message" ||
		td.GSIs[0].Projection.Include[0] != "message" {
		t.Fatal("server protobuf encoding aliased descriptor slices")
	}

	pb = tableDescriptorToPB(td)
	got := pbToTableDescriptor(pb)
	if got.Name != td.Name || got.KeySchema != td.KeySchema ||
		got.TTLAttribute != td.TTLAttribute || !reflect.DeepEqual(got.TextIndexes, td.TextIndexes) {
		t.Fatalf("native descriptor fields lost across protobuf: %+v", got)
	}
	if len(got.GSIs) != 2 || got.GSIs[0].Name != td.GSIs[0].Name ||
		got.GSIs[0].KeySchema != td.GSIs[0].KeySchema ||
		!slices.Equal(got.GSIs[0].Projected, td.GSIs[0].Projected) {
		t.Fatalf("GSI fields lost across protobuf: %+v", got.GSIs)
	}
	assertServerProjection(t, got.GSIs[0].Projection, td.GSIs[0].Projection)
	assertServerProjection(t, got.GSIs[1].Projection, types.IndexProjection{})
	if len(got.LSIs) != 2 {
		t.Fatalf("LSI count = %d, want 2", len(got.LSIs))
	}
	for i := range td.LSIs {
		if got.LSIs[i].Name != td.LSIs[i].Name || got.LSIs[i].SK != td.LSIs[i].SK {
			t.Fatalf("LSI %d = %+v, want %+v", i, got.LSIs[i], td.LSIs[i])
		}
		assertServerProjection(t, got.LSIs[i].Projection, td.LSIs[i].Projection)
	}
	if got.LSIs[0].Projection.Include != nil {
		t.Fatalf("first LSI include = %v, want nil", got.LSIs[0].Projection.Include)
	}

	pb.GetGsis()[0].Projected[0] = "mutated"
	pb.GetGsis()[0].Projection.Include[0] = "mutated"
	if got.GSIs[0].Projected[0] != "legacy_message" ||
		got.GSIs[0].Projection.Include[0] != "message" {
		t.Fatal("server protobuf decoding aliased protobuf slices")
	}

	nilProjection := pbToTableDescriptor(&cefaspb.TableDescriptor{
		Gsis: []*cefaspb.GSIDescriptor{{}},
		Lsis: []*cefaspb.LSIDescriptor{{}},
	})
	assertServerProjection(t, nilProjection.GSIs[0].Projection, types.IndexProjection{})
	assertServerProjection(t, nilProjection.LSIs[0].Projection, types.IndexProjection{})
}

func assertServerProjection(t *testing.T, got, want types.IndexProjection) {
	t.Helper()
	if got.Mode != want.Mode || !slices.Equal(got.Include, want.Include) {
		t.Fatalf("projection = %+v, want %+v", got, want)
	}
}
