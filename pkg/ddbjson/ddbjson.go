// Package ddbjson is the canonical encoder/decoder for the
// DynamoDB-flavoured AttributeValue JSON shape used by both the
// cefas HTTP API and the cefas-cli surface.
//
// On the wire each attribute is a single-letter-tagged union:
//
//	{"S":  "alice"}          string
//	{"N":  "42"}             number (kept as canonical text)
//	{"B":  "<base64>"}       binary
//	{"BOOL": true}           boolean
//	{"NULL": true}           explicit null
//	{"SS": ["a","b"]}        string set
//	{"NS": ["1","2"]}        number set
//	{"BS": ["<b64>"]}        binary set
//	{"L": [<attr>...]}       list
//	{"M": {"name": <attr>}}  map
//	{"V": [0.1,0.2],"D":2}  native numeric vector
//
// The shape matches the AWS DynamoDB JSON literal exactly so scripts
// written against `aws dynamodb` can be ported by replacing the
// command name.
package ddbjson

import (
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/osvaldoandrade/cefas/pkg/types"
)

// Attribute is the wire form of a single AttributeValue. Exactly one
// of the tagged fields is set on the wire.
type Attribute struct {
	S    *string              `json:"S,omitempty"`
	N    *string              `json:"N,omitempty"`
	B    *string              `json:"B,omitempty"` // base64
	BOOL *bool                `json:"BOOL,omitempty"`
	NULL *bool                `json:"NULL,omitempty"`
	SS   []string             `json:"SS,omitempty"`
	NS   []string             `json:"NS,omitempty"`
	BS   []string             `json:"BS,omitempty"` // base64 each
	L    []Attribute          `json:"L,omitempty"`
	M    map[string]Attribute `json:"M,omitempty"`
	V    []float64            `json:"V,omitempty"`
	D    *int                 `json:"D,omitempty"`
}

// Item is the wire form of a row — a map of attribute name to value.
type Item map[string]Attribute

// ToAttr converts a wire attribute into the storage-layer
// AttributeValue. Returns an error on malformed base64 or when no
// field is set.
func (a Attribute) ToAttr() (types.AttributeValue, error) {
	switch {
	case a.S != nil:
		return types.AttributeValue{T: types.AttrS, S: *a.S}, nil
	case a.N != nil:
		return types.AttributeValue{T: types.AttrN, N: *a.N}, nil
	case a.B != nil:
		raw, err := base64.StdEncoding.DecodeString(*a.B)
		if err != nil {
			return types.AttributeValue{}, fmt.Errorf("invalid base64 in B: %w", err)
		}
		return types.AttributeValue{T: types.AttrB, B: raw}, nil
	case a.BOOL != nil:
		return types.AttributeValue{T: types.AttrBOOL, BOOL: *a.BOOL}, nil
	case a.NULL != nil && *a.NULL:
		return types.AttributeValue{T: types.AttrNull}, nil
	case a.SS != nil:
		return types.AttributeValue{T: types.AttrSS, SS: a.SS}, nil
	case a.NS != nil:
		return types.AttributeValue{T: types.AttrNS, NS: a.NS}, nil
	case a.BS != nil:
		bs := make([][]byte, len(a.BS))
		for i, s := range a.BS {
			raw, err := base64.StdEncoding.DecodeString(s)
			if err != nil {
				return types.AttributeValue{}, fmt.Errorf("invalid base64 in BS[%d]: %w", i, err)
			}
			bs[i] = raw
		}
		return types.AttributeValue{T: types.AttrBS, BS: bs}, nil
	case a.L != nil:
		list := make([]types.AttributeValue, len(a.L))
		for i, av := range a.L {
			v, err := av.ToAttr()
			if err != nil {
				return types.AttributeValue{}, fmt.Errorf("L[%d]: %w", i, err)
			}
			list[i] = v
		}
		return types.AttributeValue{T: types.AttrL, L: list}, nil
	case a.M != nil:
		m := make(map[string]types.AttributeValue, len(a.M))
		for k, av := range a.M {
			v, err := av.ToAttr()
			if err != nil {
				return types.AttributeValue{}, fmt.Errorf("M[%q]: %w", k, err)
			}
			m[k] = v
		}
		return types.AttributeValue{T: types.AttrM, M: m}, nil
	case a.V != nil:
		if a.D != nil && *a.D != len(a.V) {
			return types.AttributeValue{}, fmt.Errorf("V dimension %d does not match D %d", len(a.V), *a.D)
		}
		return types.AttributeValue{T: types.AttrVec, Vec: append([]float64(nil), a.V...)}, nil
	}
	return types.AttributeValue{}, fmt.Errorf("attribute value has no field set")
}

// FromAttr converts a storage-layer AttributeValue into the wire
// form. Total — the AttributeValue zero value renders as an empty
// Attribute.
func FromAttr(av types.AttributeValue) Attribute {
	switch av.T {
	case types.AttrNull:
		t := true
		return Attribute{NULL: &t}
	case types.AttrS:
		s := av.S
		return Attribute{S: &s}
	case types.AttrN:
		s := av.N
		return Attribute{N: &s}
	case types.AttrB:
		s := base64.StdEncoding.EncodeToString(av.B)
		return Attribute{B: &s}
	case types.AttrBOOL:
		b := av.BOOL
		return Attribute{BOOL: &b}
	case types.AttrSS:
		return Attribute{SS: av.SS}
	case types.AttrNS:
		return Attribute{NS: av.NS}
	case types.AttrBS:
		bs := make([]string, len(av.BS))
		for i, b := range av.BS {
			bs[i] = base64.StdEncoding.EncodeToString(b)
		}
		return Attribute{BS: bs}
	case types.AttrL:
		list := make([]Attribute, len(av.L))
		for i, v := range av.L {
			list[i] = FromAttr(v)
		}
		return Attribute{L: list}
	case types.AttrM:
		m := make(map[string]Attribute, len(av.M))
		for k, v := range av.M {
			m[k] = FromAttr(v)
		}
		return Attribute{M: m}
	case types.AttrVec:
		d := len(av.Vec)
		return Attribute{V: append([]float64(nil), av.Vec...), D: &d}
	}
	return Attribute{}
}

// DecodeItem converts a parsed wire item into the storage shape.
func DecodeItem(in map[string]Attribute) (types.Item, error) {
	out := make(types.Item, len(in))
	for k, a := range in {
		v, err := a.ToAttr()
		if err != nil {
			return nil, fmt.Errorf("attribute %q: %w", k, err)
		}
		out[k] = v
	}
	return out, nil
}

// EncodeItem converts a storage item into the wire shape.
func EncodeItem(in types.Item) map[string]Attribute {
	out := make(map[string]Attribute, len(in))
	for k, v := range in {
		out[k] = FromAttr(v)
	}
	return out
}

// DecodeBinds parses an ExpressionAttributeValues-style map. Keys
// keep their `:` prefix on the wire; callers strip it when matching
// against an expression bind variable.
func DecodeBinds(in map[string]Attribute) (map[string]types.AttributeValue, error) {
	if in == nil {
		return nil, nil
	}
	out := make(map[string]types.AttributeValue, len(in))
	for k, a := range in {
		v, err := a.ToAttr()
		if err != nil {
			return nil, fmt.Errorf("bind %s: %w", k, err)
		}
		out[k] = v
	}
	return out, nil
}

// ParseItem parses raw JSON bytes into a storage Item. Convenience
// for CLI argument handling (`--item '{...}'`).
func ParseItem(raw []byte) (types.Item, error) {
	var wire map[string]Attribute
	if err := json.Unmarshal(raw, &wire); err != nil {
		return nil, fmt.Errorf("parse item: %w", err)
	}
	return DecodeItem(wire)
}

// ParseBinds parses raw JSON bytes into a bind-variable map.
func ParseBinds(raw []byte) (map[string]types.AttributeValue, error) {
	var wire map[string]Attribute
	if err := json.Unmarshal(raw, &wire); err != nil {
		return nil, fmt.Errorf("parse binds: %w", err)
	}
	return DecodeBinds(wire)
}
