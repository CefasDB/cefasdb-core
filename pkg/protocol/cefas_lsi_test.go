package cefaspb

import (
	"reflect"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestLSIDescriptorReflection(t *testing.T) {
	assertTableDescriptorFieldNumbers(t)

	tests := []struct {
		name        string
		message     protoreflect.MessageDescriptor
		field       protoreflect.Name
		number      protoreflect.FieldNumber
		kind        protoreflect.Kind
		cardinality protoreflect.Cardinality
		messageType protoreflect.FullName
	}{
		{
			name:        "table lsis",
			message:     (&TableDescriptor{}).ProtoReflect().Descriptor(),
			field:       "lsis",
			number:      14,
			kind:        protoreflect.MessageKind,
			cardinality: protoreflect.Repeated,
			messageType: "cefas.v1.LSIDescriptor",
		},
		{
			name:        "gsi projection",
			message:     (&GSIDescriptor{}).ProtoReflect().Descriptor(),
			field:       "projection",
			number:      4,
			kind:        protoreflect.MessageKind,
			cardinality: protoreflect.Optional,
			messageType: "cefas.v1.IndexProjection",
		},
		{
			name:        "gsi legacy projected",
			message:     (&GSIDescriptor{}).ProtoReflect().Descriptor(),
			field:       "projected",
			number:      3,
			kind:        protoreflect.StringKind,
			cardinality: protoreflect.Repeated,
		},
		{
			name:        "lsi name",
			message:     (&LSIDescriptor{}).ProtoReflect().Descriptor(),
			field:       "name",
			number:      1,
			kind:        protoreflect.StringKind,
			cardinality: protoreflect.Optional,
		},
		{
			name:        "lsi sort key",
			message:     (&LSIDescriptor{}).ProtoReflect().Descriptor(),
			field:       "sk",
			number:      2,
			kind:        protoreflect.StringKind,
			cardinality: protoreflect.Optional,
		},
		{
			name:        "lsi projection",
			message:     (&LSIDescriptor{}).ProtoReflect().Descriptor(),
			field:       "projection",
			number:      3,
			kind:        protoreflect.MessageKind,
			cardinality: protoreflect.Optional,
			messageType: "cefas.v1.IndexProjection",
		},
		{
			name:        "projection mode",
			message:     (&IndexProjection{}).ProtoReflect().Descriptor(),
			field:       "mode",
			number:      1,
			kind:        protoreflect.StringKind,
			cardinality: protoreflect.Optional,
		},
		{
			name:        "projection include",
			message:     (&IndexProjection{}).ProtoReflect().Descriptor(),
			field:       "include",
			number:      2,
			kind:        protoreflect.StringKind,
			cardinality: protoreflect.Repeated,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			field := tc.message.Fields().ByName(tc.field)
			if field == nil {
				t.Fatalf("field %q not found", tc.field)
			}
			if field.Number() != tc.number {
				t.Fatalf("field %q number = %d, want %d", tc.field, field.Number(), tc.number)
			}
			if field.Kind() != tc.kind {
				t.Fatalf("field %q kind = %s, want %s", tc.field, field.Kind(), tc.kind)
			}
			if field.Cardinality() != tc.cardinality {
				t.Fatalf("field %q cardinality = %s, want %s", tc.field, field.Cardinality(), tc.cardinality)
			}
			if tc.messageType != "" && field.Message().FullName() != tc.messageType {
				t.Fatalf("field %q message = %s, want %s", tc.field, field.Message().FullName(), tc.messageType)
			}
		})
	}
}

func assertTableDescriptorFieldNumbers(t *testing.T) {
	t.Helper()
	want := []protoreflect.Name{
		"name",
		"key_schema",
		"gsis",
		"spatial_indexes",
		"attribute_definitions",
		"storage_class",
		"memory_footprint_bytes",
		"stream_specification",
		"latest_stream_arn",
		"latest_stream_label",
		"stream_status",
		"text_indexes",
		"ttl_attribute",
		"lsis",
	}
	fields := (&TableDescriptor{}).ProtoReflect().Descriptor().Fields()
	if fields.Len() != len(want) {
		t.Fatalf("TableDescriptor field count = %d, want %d", fields.Len(), len(want))
	}
	for i, name := range want {
		field := fields.ByName(name)
		if field == nil || field.Number() != protoreflect.FieldNumber(i+1) {
			t.Fatalf("TableDescriptor field %q number = %v, want %d", name, field, i+1)
		}
	}
}

func TestLSIDescriptorMarshalRoundTrip(t *testing.T) {
	want := &TableDescriptor{
		Name: "events",
		Gsis: []*GSIDescriptor{
			{
				Name:       "events_by_status",
				Projected:  []string{"legacy_status", "legacy_owner"},
				Projection: &IndexProjection{Mode: "INCLUDE", Include: []string{"status", "owner"}},
			},
		},
		Lsis: []*LSIDescriptor{
			{
				Name:       "users_by_email_v1",
				Sk:         "email",
				Projection: &IndexProjection{Mode: "INCLUDE", Include: nil},
			},
			{
				Name:       "events_by_expiry_v1",
				Sk:         "expires_at",
				Projection: &IndexProjection{Mode: "KEYS_ONLY", Include: []string{}},
			},
		},
	}

	data, err := proto.Marshal(want)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got := &TableDescriptor{}
	if err := proto.Unmarshal(data, got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if len(got.GetLsis()) != 2 {
		t.Fatalf("LSI count = %d, want 2", len(got.GetLsis()))
	}
	for i, name := range []string{"users_by_email_v1", "events_by_expiry_v1"} {
		if got.GetLsis()[i].GetName() != name {
			t.Fatalf("LSI %d name = %q, want %q", i, got.GetLsis()[i].GetName(), name)
		}
	}
	if got.GetLsis()[0].GetSk() != "email" || got.GetLsis()[0].GetProjection().GetMode() != "INCLUDE" {
		t.Fatalf("first LSI = %+v", got.GetLsis()[0])
	}
	if got.GetLsis()[0].GetProjection().GetInclude() != nil {
		t.Fatalf("first LSI include = %v, want nil", got.GetLsis()[0].GetProjection().GetInclude())
	}
	if got.GetLsis()[1].GetSk() != "expires_at" || got.GetLsis()[1].GetProjection().GetMode() != "KEYS_ONLY" {
		t.Fatalf("second LSI = %+v", got.GetLsis()[1])
	}
	if len(got.GetLsis()[1].GetProjection().GetInclude()) != 0 {
		t.Fatalf("second LSI include = %v, want no elements", got.GetLsis()[1].GetProjection().GetInclude())
	}

	gsi := got.GetGsis()[0]
	if !reflect.DeepEqual(gsi.GetProjected(), []string{"legacy_status", "legacy_owner"}) {
		t.Fatalf("legacy GSI projected = %v", gsi.GetProjected())
	}
	if gsi.GetProjection().GetMode() != "INCLUDE" ||
		!reflect.DeepEqual(gsi.GetProjection().GetInclude(), []string{"status", "owner"}) {
		t.Fatalf("GSI projection = %+v", gsi.GetProjection())
	}
}
