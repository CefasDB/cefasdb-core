package output_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/osvaldoandrade/cefas/cmd/cefasctl/internal/output"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

func sAttr(s string) types.AttributeValue { return types.AttributeValue{T: types.AttrS, S: s} }
func nAttr(n string) types.AttributeValue { return types.AttributeValue{T: types.AttrN, N: n} }

func TestValidate(t *testing.T) {
	for _, in := range []string{"", "json", "table", "text"} {
		if _, err := output.Validate(in); err != nil {
			t.Errorf("%q errored: %v", in, err)
		}
	}
	if _, err := output.Validate("yaml"); err == nil {
		t.Errorf("expected error on invalid format")
	}
}

func TestRenderItemsJSON(t *testing.T) {
	var buf bytes.Buffer
	r := output.New(&buf, output.JSON)
	err := r.Items([]types.Item{
		{"pk": sAttr("USER#1"), "name": sAttr("Ova")},
	})
	if err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, want := range []string{`"Items"`, `"Count": 1`, `"pk"`, `"USER#1"`} {
		if !strings.Contains(out, want) {
			t.Errorf("JSON missing %q in:\n%s", want, out)
		}
	}
}

func TestRenderItemsTable(t *testing.T) {
	var buf bytes.Buffer
	r := output.New(&buf, output.Table)
	err := r.Items([]types.Item{
		{"pk": sAttr("a"), "n": nAttr("1")},
		{"pk": sAttr("b"), "n": nAttr("2")},
	})
	if err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, "pk") || !strings.Contains(out, "n") {
		t.Errorf("table missing headers: %s", out)
	}
	if !strings.Contains(out, "a") || !strings.Contains(out, "b") {
		t.Errorf("table missing rows: %s", out)
	}
}

func TestRenderItemsText(t *testing.T) {
	var buf bytes.Buffer
	r := output.New(&buf, output.Text)
	_ = r.Items([]types.Item{
		{"pk": sAttr("a"), "n": nAttr("1")},
	})
	if !strings.Contains(buf.String(), "a\t1") && !strings.Contains(buf.String(), "1\ta") {
		t.Errorf("text format unexpected: %q", buf.String())
	}
}

func TestRenderItemNil(t *testing.T) {
	var buf bytes.Buffer
	r := output.New(&buf, output.JSON)
	if err := r.Item(nil); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "{}") {
		t.Errorf("nil item should render as empty object, got %q", buf.String())
	}
}
