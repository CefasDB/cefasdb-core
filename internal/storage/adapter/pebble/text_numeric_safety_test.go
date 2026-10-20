package pebble

import (
	"errors"
	"strings"
	"testing"

	"github.com/CefasDb/cefasdb/pkg/types"
)

// 1025 is intentionally safe to evaluate in the unfixed baseline. It exceeds
// the contract bound without allocating the dangerous exponent used later.
func TestNativeTextNumericBoundsSafeExponent(t *testing.T) {
	value := types.AttributeValue{T: types.AttrN, N: "1e1025"}
	q := types.TextQuery{IndexName: "message", PKValue: types.AttributeValue{T: types.AttrS, S: "a"}, Fields: map[string]types.AttributeValue{"fields.attempt": value}}
	if _, err := prepareTextQuery(textTable(), q); !errors.Is(err, ErrTextQueryInvalid) {
		t.Errorf("query accepted exponent beyond bounded numeric contract: %v", err)
	}
	if _, ok := scalarCompare(value, types.AttributeValue{T: types.AttrN, N: "1"}); ok {
		t.Error("stored AV.N accepted exponent beyond bounded numeric contract")
	}
}

func TestNativeTextNumericBoundsRejectDangerousShapes(t *testing.T) {
	for _, raw := range []string{"1e999999999999", "1e-999999999999", "1p999999999999", "0x1p999999999999", "1/2", "+1", "01", "1.", "NaN", "Inf", " 1", strings.Repeat("9", 257)} {
		t.Run(raw[:min(len(raw), 32)], func(t *testing.T) {
			if _, ok := textNumber(raw); ok {
				t.Fatal("unsafe or non-decimal numeric value admitted")
			}
			value := types.AttributeValue{T: types.AttrN, N: raw}
			one := types.AttributeValue{T: types.AttrN, N: "1"}
			if _, ok := scalarCompare(value, one); ok {
				t.Fatal("stored numeric comparison bypassed bounds")
			}
			if _, ok := scalarCompare(one, value); ok {
				t.Fatal("query numeric comparison bypassed bounds")
			}
		})
	}
}

func TestNativeTextNumericBoundsPreserveExactDecimals(t *testing.T) {
	for _, raw := range []string{"0", "-0", "1.25", "1.20e+3", "1e1024", "1e-1024", strings.Repeat("9", 256)} {
		if _, ok := textNumber(raw); !ok {
			t.Fatalf("bounded decimal rejected: %.32s", raw)
		}
	}
	if cmp, ok := scalarCompare(types.AttributeValue{T: types.AttrN, N: "1.20e+3"}, types.AttributeValue{T: types.AttrN, N: "1200"}); !ok || cmp != 0 {
		t.Fatal("numeric equality lost exactness")
	}
}

func FuzzNativeTextNumericParse(f *testing.F) {
	for _, raw := range []string{"1e999999999999", "0x1p999999999999", "1e1024", "1e-1024", "0", "1/2", "1.25"} {
		f.Add(raw)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		value, ok := textNumber(raw)
		if ok && (value == nil || len(raw) > 256) {
			t.Fatal("numeric bound violated")
		}
	})
}
