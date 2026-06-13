package ddb

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/osvaldoandrade/cefas/pkg/types"
)

// parseKV parses a comma-separated `Key=Value,Key=Value` shorthand
// used by aws-cli on flags like --attribute-definitions and
// --key-schema. Repeats of the same key are not collapsed — the
// caller decides what to do with duplicates.
func parseKV(s string) (map[string]string, error) {
	out := make(map[string]string)
	for _, pair := range strings.Split(s, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		eq := strings.IndexByte(pair, '=')
		if eq <= 0 || eq == len(pair)-1 {
			return nil, fmt.Errorf("bad key=value pair %q", pair)
		}
		out[strings.TrimSpace(pair[:eq])] = strings.TrimSpace(pair[eq+1:])
	}
	return out, nil
}

// AttrDef captures one --attribute-definitions entry.
type AttrDef struct {
	Name             string
	Type             string // "S" | "N" | "B" | "V"
	VectorDimensions int
}

func parseAttrDefs(values []string) ([]AttrDef, error) {
	out := make([]AttrDef, 0, len(values))
	for i, v := range values {
		kv, err := parseKV(v)
		if err != nil {
			return nil, fmt.Errorf("attribute-definitions[%d]: %w", i, err)
		}
		name := kv["AttributeName"]
		typ := strings.ToUpper(kv["AttributeType"])
		if name == "" || typ == "" {
			return nil, fmt.Errorf("attribute-definitions[%d]: AttributeName and AttributeType required", i)
		}
		def := AttrDef{Name: name, Type: typ}
		if strings.HasPrefix(typ, "V<") && strings.HasSuffix(typ, ">") {
			n := strings.TrimSuffix(strings.TrimPrefix(typ, "V<"), ">")
			dim, err := strconv.Atoi(n)
			if err != nil || dim <= 0 {
				return nil, fmt.Errorf("attribute-definitions[%d]: bad vector dimension %q", i, n)
			}
			def.Type = "V"
			def.VectorDimensions = dim
		}
		out = append(out, def)
	}
	return out, nil
}

// KeySchemaEntry captures one --key-schema entry.
type KeySchemaEntry struct {
	Name    string
	KeyType string // "HASH" | "RANGE"
}

func parseKeySchema(values []string) ([]KeySchemaEntry, error) {
	out := make([]KeySchemaEntry, 0, len(values))
	for i, v := range values {
		kv, err := parseKV(v)
		if err != nil {
			return nil, fmt.Errorf("key-schema[%d]: %w", i, err)
		}
		name := kv["AttributeName"]
		kt := strings.ToUpper(kv["KeyType"])
		if name == "" || kt == "" {
			return nil, fmt.Errorf("key-schema[%d]: AttributeName and KeyType required", i)
		}
		if kt != "HASH" && kt != "RANGE" {
			return nil, fmt.Errorf("key-schema[%d]: KeyType %q must be HASH or RANGE", i, kt)
		}
		out = append(out, KeySchemaEntry{Name: name, KeyType: kt})
	}
	return out, nil
}

// PartitionAndSort returns (PK, SK) attribute names from a parsed
// KeySchema slice. SK may be empty when the table has no sort key.
func PartitionAndSort(ks []KeySchemaEntry) (pk, sk string, err error) {
	for _, e := range ks {
		switch e.KeyType {
		case "HASH":
			if pk != "" {
				return "", "", fmt.Errorf("key-schema declares two HASH keys")
			}
			pk = e.Name
		case "RANGE":
			if sk != "" {
				return "", "", fmt.Errorf("key-schema declares two RANGE keys")
			}
			sk = e.Name
		}
	}
	if pk == "" {
		return "", "", fmt.Errorf("key-schema missing HASH key")
	}
	return pk, sk, nil
}

func parseStreamSpecification(raw string) (*types.StreamSpecification, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	kv, err := parseKV(raw)
	if err != nil {
		return nil, fmt.Errorf("stream-specification: %w", err)
	}
	enabledRaw, ok := kv["StreamEnabled"]
	if !ok {
		return nil, fmt.Errorf("stream-specification: StreamEnabled is required")
	}
	switch strings.ToLower(strings.TrimSpace(enabledRaw)) {
	case "false":
		return nil, nil
	case "true":
	default:
		return nil, fmt.Errorf("stream-specification: StreamEnabled must be true or false")
	}
	view := types.NormalizeStreamViewType(kv["StreamViewType"])
	if view != "" && !types.IsValidStreamViewType(view) {
		return nil, fmt.Errorf("stream-specification: StreamViewType %q must be one of %s, %s, %s, %s",
			kv["StreamViewType"],
			types.StreamViewTypeKeysOnly,
			types.StreamViewTypeNewImage,
			types.StreamViewTypeOldImage,
			types.StreamViewTypeNewAndOldImages)
	}
	return &types.StreamSpecification{
		StreamEnabled:  true,
		StreamViewType: view,
	}, nil
}
