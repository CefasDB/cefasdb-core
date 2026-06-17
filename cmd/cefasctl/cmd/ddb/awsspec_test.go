package ddb

import (
	"testing"

	"github.com/CefasDb/cefasdb/pkg/types"
)

func TestParseStreamSpecificationEnabledWithViewType(t *testing.T) {
	spec, err := parseStreamSpecification("StreamEnabled=true,StreamViewType=new_image")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if spec == nil || !spec.StreamEnabled {
		t.Fatalf("spec not enabled: %+v", spec)
	}
	if spec.StreamViewType != types.StreamViewTypeNewImage {
		t.Fatalf("StreamViewType = %q", spec.StreamViewType)
	}
}

func TestParseStreamSpecificationEnabledDefaultsViewType(t *testing.T) {
	spec, err := parseStreamSpecification("StreamEnabled=true")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if spec == nil || !spec.StreamEnabled {
		t.Fatalf("spec not enabled: %+v", spec)
	}
	if spec.StreamViewType != "" {
		t.Fatalf("StreamViewType = %q, want empty for server default", spec.StreamViewType)
	}
}

func TestParseStreamSpecificationDisabledClearsViewType(t *testing.T) {
	spec, err := parseStreamSpecification("StreamEnabled=false,StreamViewType=NEW_IMAGE")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if spec != nil {
		t.Fatalf("spec = %+v, want nil", spec)
	}
}

func TestParseStreamSpecificationRejectsInvalidViewType(t *testing.T) {
	if _, err := parseStreamSpecification("StreamEnabled=true,StreamViewType=FULL_IMAGE"); err == nil {
		t.Fatal("expected invalid StreamViewType error")
	}
}
