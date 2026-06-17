// Package pkid extracts a stable string identifier from an item
// given its KeySchema. Search-index plugins reuse it so they all
// derive the same identifier from the same item.
package pkid

import (
	"fmt"
	"strings"

	"github.com/CefasDb/cefasdb/internal/core/model"
)

// Of returns the string id of `item` under ks. Combines PK with SK
// when SK is configured. Returns ("", true) when the item is missing
// a key attribute; callers usually skip such items.
func Of(item model.Item, ks model.KeySchema) (string, bool) {
	pk, ok := encode(item, ks.PK)
	if !ok {
		return "", false
	}
	if ks.SK == "" {
		return pk, true
	}
	sk, ok := encode(item, ks.SK)
	if !ok {
		return "", false
	}
	return pk + "|" + sk, true
}

// FieldString returns item[field] as a plain string when the
// attribute is S, N, or B. Returns ("", false) for missing /
// unsupported kinds; callers decide whether to skip or error.
func FieldString(item model.Item, field string) (string, bool) {
	v, ok := item[field]
	if !ok {
		return "", false
	}
	switch v.T {
	case model.AttrS:
		return v.S, true
	case model.AttrN:
		return v.N, true
	case model.AttrB:
		return string(v.B), true
	}
	return "", false
}

func encode(item model.Item, field string) (string, bool) {
	if field == "" {
		return "", false
	}
	v, ok := item[field]
	if !ok {
		return "", false
	}
	switch v.T {
	case model.AttrS:
		return v.S, true
	case model.AttrN:
		return v.N, true
	case model.AttrB:
		return fmt.Sprintf("b%x", v.B), true
	}
	return "", false
}

// SplitPK splits the (PK, SK) bundle Of returned. Useful when a
// plugin stored ids via Of and now needs to reconstruct keys.
func SplitPK(id string) (pk, sk string) {
	i := strings.IndexByte(id, '|')
	if i < 0 {
		return id, ""
	}
	return id[:i], id[i+1:]
}
