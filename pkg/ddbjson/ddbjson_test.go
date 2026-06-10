package ddbjson_test

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/osvaldoandrade/cefas/pkg/ddbjson"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

func TestRoundTripScalars(t *testing.T) {
	in := types.Item{
		"s":  {T: types.AttrS, S: "alice"},
		"n":  {T: types.AttrN, N: "42"},
		"b":  {T: types.AttrB, B: []byte{0x01, 0x02, 0x03}},
		"bt": {T: types.AttrBOOL, BOOL: true},
		"bf": {T: types.AttrBOOL, BOOL: false},
		"nl": {T: types.AttrNull},
	}
	wire := ddbjson.EncodeItem(in)
	out, err := ddbjson.DecodeItem(wire)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("round-trip mismatch:\n in=%+v\nout=%+v", in, out)
	}
}

func TestRoundTripSetsAndCollections(t *testing.T) {
	in := types.Item{
		"ss": {T: types.AttrSS, SS: []string{"a", "b", "c"}},
		"ns": {T: types.AttrNS, NS: []string{"1", "2", "3"}},
		"bs": {T: types.AttrBS, BS: [][]byte{{0x01}, {0x02, 0x03}}},
		"l": {T: types.AttrL, L: []types.AttributeValue{
			{T: types.AttrS, S: "x"},
			{T: types.AttrN, N: "5"},
		}},
		"m": {T: types.AttrM, M: map[string]types.AttributeValue{
			"inner": {T: types.AttrS, S: "v"},
		}},
	}
	wire := ddbjson.EncodeItem(in)
	out, err := ddbjson.DecodeItem(wire)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("round-trip mismatch:\n in=%+v\nout=%+v", in, out)
	}
}

func TestParseItemFromJSON(t *testing.T) {
	raw := []byte(`{"pk":{"S":"USER#1"},"sk":{"S":"PROFILE"},"name":{"S":"Ova"}}`)
	it, err := ddbjson.ParseItem(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if it["pk"].S != "USER#1" || it["name"].S != "Ova" {
		t.Fatalf("unexpected item: %+v", it)
	}
}

func TestParseItemBase64Binary(t *testing.T) {
	// "hello" in base64
	raw := []byte(`{"k":{"B":"aGVsbG8="}}`)
	it, err := ddbjson.ParseItem(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !bytes.Equal(it["k"].B, []byte("hello")) {
		t.Fatalf("binary decode = %v, want 'hello'", it["k"].B)
	}
}

func TestParseItemInvalidBase64(t *testing.T) {
	raw := []byte(`{"k":{"B":"!!!not-base64!!!"}}`)
	if _, err := ddbjson.ParseItem(raw); err == nil {
		t.Fatalf("expected base64 error")
	}
}

func TestAttributeNoFieldErrors(t *testing.T) {
	if _, err := (ddbjson.Attribute{}).ToAttr(); err == nil {
		t.Fatalf("empty attribute should error")
	}
}

func TestParseBindsStripsColonOptional(t *testing.T) {
	raw := []byte(`{":pk":{"S":"USER#1"},":n":{"N":"42"}}`)
	binds, err := ddbjson.ParseBinds(raw)
	if err != nil {
		t.Fatalf("parse binds: %v", err)
	}
	if binds[":pk"].S != "USER#1" || binds[":n"].N != "42" {
		t.Fatalf("binds = %+v", binds)
	}
}
