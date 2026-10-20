// Package ddbjson preserves the server/CLI compatibility import. The wire
// implementation is public so the canonical HTTP SDK uses the same codec.
package ddbjson

import (
	wire "github.com/CefasDb/cefasdb/pkg/encoding/ddbjson"
	"github.com/CefasDb/cefasdb/pkg/types"
)

type (
	Attribute = wire.Attribute
	Item      = wire.Item
)

func FromAttr(v types.AttributeValue) Attribute             { return wire.FromAttr(v) }
func DecodeItem(v map[string]Attribute) (types.Item, error) { return wire.DecodeItem(v) }
func EncodeItem(v types.Item) map[string]Attribute          { return wire.EncodeItem(v) }
func DecodeBinds(v map[string]Attribute) (map[string]types.AttributeValue, error) {
	return wire.DecodeBinds(v)
}
func ParseItem(v []byte) (types.Item, error)                       { return wire.ParseItem(v) }
func ParseBinds(v []byte) (map[string]types.AttributeValue, error) { return wire.ParseBinds(v) }
