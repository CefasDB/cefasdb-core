package server

import (
	"strings"
	"testing"

	"github.com/osvaldoandrade/cefas/pkg/types"
)

func TestSubstituteNamesAndValues(t *testing.T) {
	src := "SET #n = :name, #c = if_not_exists(#c, :zero)"
	names := map[string]string{"n": "name", "c": "counter"}
	values := map[string]types.AttributeValue{
		"name": {T: types.AttrS, S: "Ova"},
		"zero": {T: types.AttrN, N: "0"},
	}
	out, err := substituteNames(src, names)
	if err != nil {
		t.Fatalf("names: %v", err)
	}
	if !strings.Contains(out, "name = :name") || !strings.Contains(out, "counter = if_not_exists(counter, :zero)") {
		t.Fatalf("after name sub: %q", out)
	}
	out2, err := substituteValues(out, values)
	if err != nil {
		t.Fatalf("values: %v", err)
	}
	if !strings.Contains(out2, "'Ova'") || !strings.Contains(out2, "0") {
		t.Fatalf("after value sub: %q", out2)
	}
}

func TestSubstituteSkipsQuotedStrings(t *testing.T) {
	src := "SET tag = ':literal #notaname'"
	out, err := substituteNames(src, map[string]string{})
	if err != nil {
		t.Fatalf("names: %v", err)
	}
	if out != src {
		t.Fatalf("quoted text changed: %q", out)
	}
	out2, err := substituteValues(out, map[string]types.AttributeValue{})
	if err != nil {
		t.Fatalf("values: %v", err)
	}
	if out2 != src {
		t.Fatalf("quoted text changed: %q", out2)
	}
}

func TestSubstituteMissingNameErrors(t *testing.T) {
	_, err := substituteNames("SET #n = 1", map[string]string{})
	if err == nil {
		t.Fatal("expected error for missing #n")
	}
}

func TestSubstituteValuesAcceptsColonPrefixedKey(t *testing.T) {
	// aws-cli sends `:name` as the map key. Both keyed forms work.
	out, err := substituteValues("SET v = :name", map[string]types.AttributeValue{
		":name": {T: types.AttrS, S: "ok"},
	})
	if err != nil {
		t.Fatalf("values: %v", err)
	}
	if !strings.Contains(out, "'ok'") {
		t.Fatalf("out: %q", out)
	}
}

func TestTranslateUpdateItemBuildsExpectedSQL(t *testing.T) {
	sql, img, err := translateUpdateItem(
		"Users",
		types.Item{"id": {T: types.AttrS, S: "u1"}},
		types.KeySchema{PK: "id"},
		"SET #n = :name",
		"attribute_exists(id)",
		map[string]string{"n": "name"},
		map[string]types.AttributeValue{"name": {T: types.AttrS, S: "Ova"}},
		"ALL_NEW",
	)
	if err != nil {
		t.Fatalf("translate: %v", err)
	}
	want := "UPDATE Users SET name = 'Ova' WHERE id = 'u1' IF attribute_exists(id) RETURNING NEW"
	if sql != want {
		t.Fatalf("sql = %q\nwant = %q", sql, want)
	}
	if img != "NEW" {
		t.Fatalf("image = %q, want NEW", img)
	}
}
