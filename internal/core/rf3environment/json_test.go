package rf3environment

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

func TestValidateJSONStructuralBoundaries(t *testing.T) {
	t.Parallel()

	valid := map[string][]byte{
		"maximum key length":    []byte(`{"` + strings.Repeat("k", maxStringBytes) + `":0}`),
		"maximum string length": []byte(`{"value":"` + strings.Repeat("v ", maxStringBytes/2) + `"}`),
		"maximum number length": []byte(`{"value":` + "1" + strings.Repeat("0", 31) + `}`),
		"maximum members":       objectWithMembers(maxJSONMembers),
		"maximum array items":   objectWithItems(maxJSONItems),
		"maximum depth":         nestedObject(maxJSONDepth - 1),
	}
	for name, document := range valid {
		name, document := name, document
		t.Run("accepts "+name, func(t *testing.T) {
			t.Parallel()
			if err := validateJSON(document); err != nil {
				t.Fatalf("validateJSON() unexpected error: %v", err)
			}
		})
	}

	invalid := map[string][]byte{
		"non-object":           []byte(`[]`),
		"invalid utf8":         {'{', '"', 'k', '"', ':', '"', 0xff, '"', '}'},
		"key beyond limit":     []byte(`{"` + strings.Repeat("k", maxStringBytes+1) + `":0}`),
		"string beyond limit":  []byte(`{"value":"` + strings.Repeat("v ", maxStringBytes/2) + `x"}`),
		"number beyond limit":  []byte(`{"value":` + "1" + strings.Repeat("0", 32) + `}`),
		"members beyond limit": objectWithMembers(maxJSONMembers + 1),
		"array beyond limit":   objectWithItems(maxJSONItems + 1),
		"nesting beyond limit": nestedObject(maxJSONDepth),
	}
	for name, document := range invalid {
		name, document := name, document
		t.Run("rejects "+name, func(t *testing.T) {
			t.Parallel()
			if err := validateJSON(document); err == nil {
				t.Fatal("validateJSON() error = nil")
			}
		})
	}
}

func objectWithMembers(count int) []byte {
	var document bytes.Buffer
	document.WriteByte('{')
	for index := 0; index < count; index++ {
		if index > 0 {
			document.WriteByte(',')
		}
		fmt.Fprintf(&document, "%q:0", fmt.Sprintf("field-%d", index))
	}
	document.WriteByte('}')
	return document.Bytes()
}

func objectWithItems(count int) []byte {
	var document bytes.Buffer
	document.WriteString(`{"values":[`)
	for index := 0; index < count; index++ {
		if index > 0 {
			document.WriteByte(',')
		}
		document.WriteByte('0')
	}
	document.WriteString(`]}`)
	return document.Bytes()
}

func nestedObject(levels int) []byte {
	return []byte(strings.Repeat(`{"value":`, levels) + "0" + strings.Repeat("}", levels))
}
