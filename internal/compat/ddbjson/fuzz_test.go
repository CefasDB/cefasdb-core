package ddbjson_test

import (
	"testing"

	"github.com/CefasDb/cefasdb/internal/compat/ddbjson"
)

// FuzzUnmarshal asserts that ParseItem and ParseBinds never panic on
// arbitrary input. Errors are expected for malformed JSON or invalid
// AttributeValue shapes; only panics fail the harness.
func FuzzUnmarshal(f *testing.F) {
	seeds := [][]byte{
		// Scalars.
		[]byte(`{"id":{"S":"alice"}}`),
		[]byte(`{"n":{"N":"42"}}`),
		[]byte(`{"k":{"B":"aGVsbG8="}}`),
		[]byte(`{"bt":{"BOOL":true},"bf":{"BOOL":false}}`),
		[]byte(`{"nl":{"NULL":true}}`),
		// Sets.
		[]byte(`{"ss":{"SS":["a","b","c"]}}`),
		[]byte(`{"ns":{"NS":["1","2","3"]}}`),
		[]byte(`{"bs":{"BS":["AQ==","AgM="]}}`),
		// Collections.
		[]byte(`{"l":{"L":[{"S":"x"},{"N":"5"}]}}`),
		[]byte(`{"m":{"M":{"inner":{"S":"v"}}}}`),
		// Nested map.
		[]byte(`{"outer":{"M":{"a":{"M":{"b":{"S":"deep"}}}}}}`),
		// Vector.
		[]byte(`{"emb":{"V":[0.1,0.2,0.3],"D":3}}`),
		// Bind-style keys.
		[]byte(`{":pk":{"S":"USER#1"},":n":{"N":"42"}}`),
		// Composite item.
		[]byte(`{"pk":{"S":"USER#1"},"sk":{"S":"PROFILE"},"name":{"S":"Ova"}}`),
		// Edge cases.
		[]byte(`{}`),
		[]byte(`null`),
		[]byte(``),
		[]byte(`{"k":{}}`),
		// Type mismatch / malformed.
		[]byte(`{"k":{"S":123}}`),
		[]byte(`{"k":{"B":"!!!not-base64!!!"}}`),
		[]byte(`{"k":{"V":[1,2,3],"D":7}}`),
		[]byte(`{"k":`),
	}
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		// The contract under test is "must not panic". Errors are
		// expected for arbitrary input and intentionally ignored.
		_, _ = ddbjson.ParseItem(raw)
		_, _ = ddbjson.ParseBinds(raw)
	})
}
