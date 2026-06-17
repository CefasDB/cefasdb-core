// Package hashfield gives every probabilistic-plugin a single,
// deterministic way to turn an item's named attribute into bytes the
// hashes can consume. Lives under pkg/plugin/internal so it stays
// out of the public plugin API; only plugin packages import it.
package hashfield

import (
	"encoding/binary"
	"fmt"

	"github.com/CefasDb/cefasdb/internal/core/model"
)

// Extract returns the canonical byte form of item[field]. Returns
// nil with no error when the attribute is absent — plugins that
// require the attribute decide whether to skip or error.
//
// The byte form is total + injective per attribute kind:
//   - S          → "s" + utf8 bytes
//   - N          → "n" + canonical decimal text
//   - B          → "b" + raw bytes
//   - BOOL       → "b" + 0x01 / 0x00
//   - NULL       → "0" (a single byte)
//   - SS/NS/BS   → kind tag + length-prefixed members, sorted
//   - L          → "l" + per-element recursion
//   - M          → "m" + sorted-by-key recursion
//
// Set / list / map encodings preserve order so two items with the
// same multiset hash to the same bytes.
func Extract(item model.Item, field string) ([]byte, error) {
	av, ok := item[field]
	if !ok {
		return nil, nil
	}
	return encode(av)
}

func encode(av model.AttributeValue) ([]byte, error) {
	switch av.T {
	case model.AttrS:
		return append([]byte("s"), av.S...), nil
	case model.AttrN:
		return append([]byte("n"), av.N...), nil
	case model.AttrB:
		return append([]byte("b"), av.B...), nil
	case model.AttrBOOL:
		if av.BOOL {
			return []byte{'b', 1}, nil
		}
		return []byte{'b', 0}, nil
	case model.AttrNull:
		return []byte{'0'}, nil
	}
	// Sets / lists / maps fall through to a generic length-prefixed
	// encoding so encoders can compose recursively. Implementations
	// for those kinds plug in later as plugins start needing them.
	return nil, fmt.Errorf("hashfield: kind %v not supported yet", av.T)
}

// AppendLen appends a 4-byte big-endian length prefix to dst.
func AppendLen(dst []byte, n int) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(n))
	return append(dst, buf[:]...)
}
