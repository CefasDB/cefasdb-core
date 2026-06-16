// Package output renders cefasctl responses in one of three
// formats: json (default), table, and text. The output layer is the
// only place that imports pkg/ddbjson + text/tabwriter so subcommand
// handlers stay declarative.
package output

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"text/tabwriter"

	"github.com/osvaldoandrade/cefas/pkg/ddbjson"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

// Format selects the renderer.
type Format string

const (
	JSON  Format = "json"
	Table Format = "table"
	Text  Format = "text"
)

// Validate normalises the format string and rejects unknown values.
func Validate(s string) (Format, error) {
	switch s {
	case "", "json":
		return JSON, nil
	case "table":
		return Table, nil
	case "text":
		return Text, nil
	}
	return "", fmt.Errorf("invalid output format %q: want json|table|text", s)
}

// Renderer emits payloads in the selected Format. One per CLI
// invocation; safe for sequential calls only.
type Renderer struct {
	w  io.Writer
	fm Format
}

// New returns a Renderer writing to w in format fm.
func New(w io.Writer, fm Format) *Renderer { return &Renderer{w: w, fm: fm} }

// Items renders a slice of items. JSON: a {"Items":[...]} envelope
// matching aws-cli. Table: column-aligned with the union of attribute
// names as headers. Text: tab-separated rows.
func (r *Renderer) Items(items []types.Item) error {
	switch r.fm {
	case JSON:
		wire := make([]map[string]ddbjson.Attribute, 0, len(items))
		for _, it := range items {
			wire = append(wire, ddbjson.EncodeItem(it))
		}
		return r.writeJSON(struct {
			Items []map[string]ddbjson.Attribute `json:"Items"`
			Count int                            `json:"Count"`
		}{Items: wire, Count: len(items)})
	case Table:
		return r.tabular(items)
	case Text:
		return r.text(items)
	}
	return fmt.Errorf("unsupported format")
}

// Item renders a single item, or an empty body when item is nil.
// JSON wraps in {"Item":...}; table/text reuses Items with one row.
func (r *Renderer) Item(item types.Item) error {
	if r.fm == JSON {
		if item == nil {
			return r.writeJSON(struct{}{})
		}
		return r.writeJSON(struct {
			Item map[string]ddbjson.Attribute `json:"Item"`
		}{Item: ddbjson.EncodeItem(item)})
	}
	if item == nil {
		return nil
	}
	return r.Items([]types.Item{item})
}

// Object renders any value as the chosen format. Used for non-item
// responses (table descriptors, cluster status, etc.). JSON is the
// only fully supported format; table/text fall back to a compact
// "key: value" stringification.
func (r *Renderer) Object(v any) error {
	if r.fm == JSON {
		return r.writeJSON(v)
	}
	return r.writeJSON(v) // good enough for non-item payloads in v1
}

// Raw writes opaque bytes (used by SQL/PartiQL result echo).
func (r *Renderer) Raw(b []byte) error {
	_, err := r.w.Write(b)
	return err
}

func (r *Renderer) writeJSON(v any) error {
	enc := json.NewEncoder(r.w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func (r *Renderer) tabular(items []types.Item) error {
	if len(items) == 0 {
		return nil
	}
	cols := unionColumns(items)
	tw := tabwriter.NewWriter(r.w, 0, 0, 2, ' ', 0)
	for i, c := range cols {
		if i > 0 {
			fmt.Fprint(tw, "\t")
		}
		fmt.Fprint(tw, c)
	}
	fmt.Fprintln(tw)
	for _, it := range items {
		for i, c := range cols {
			if i > 0 {
				fmt.Fprint(tw, "\t")
			}
			fmt.Fprint(tw, formatScalar(it[c]))
		}
		fmt.Fprintln(tw)
	}
	return tw.Flush()
}

func (r *Renderer) text(items []types.Item) error {
	if len(items) == 0 {
		return nil
	}
	cols := unionColumns(items)
	for _, it := range items {
		for i, c := range cols {
			if i > 0 {
				fmt.Fprint(r.w, "\t")
			}
			fmt.Fprint(r.w, formatScalar(it[c]))
		}
		fmt.Fprintln(r.w)
	}
	return nil
}

func unionColumns(items []types.Item) []string {
	seen := make(map[string]struct{})
	for _, it := range items {
		for k := range it {
			seen[k] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func formatScalar(av types.AttributeValue) string {
	switch av.T {
	case types.AttrS:
		return av.S
	case types.AttrN:
		return av.N
	case types.AttrBOOL:
		if av.BOOL {
			return "true"
		}
		return "false"
	case types.AttrNull:
		return "NULL"
	case types.AttrB:
		return fmt.Sprintf("<%d bytes>", len(av.B))
	case types.AttrSS:
		return fmt.Sprintf("%v", av.SS)
	case types.AttrNS:
		return fmt.Sprintf("%v", av.NS)
	case types.AttrL:
		return fmt.Sprintf("<list %d>", len(av.L))
	case types.AttrM:
		return fmt.Sprintf("<map %d>", len(av.M))
	}
	return ""
}
