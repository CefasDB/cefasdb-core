package pebble

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"net/url"
	"strings"
	"unicode/utf8"

	"github.com/CefasDb/cefasdb/internal/storage"
	"github.com/CefasDb/cefasdb/pkg/types"
)

const (
	maxTextBytes     = 256 << 10
	textOverflowGram = "\x00overflow"
)

func textPath(item types.Item, path string) (types.AttributeValue, bool) {
	parts := strings.Split(path, ".")
	value, ok := item[parts[0]]
	for _, part := range parts[1:] {
		if !ok || value.T != types.AttrM {
			return types.AttributeValue{}, false
		}
		value, ok = value.M[part]
	}
	return value, ok
}

// Hashes are candidate selectors, not identity proofs. A candidate is always
// resolved to the snapshot's primary row and its exact partition/text checked.
func textPrefix(table, index string, pk []byte, gram string) []byte {
	base := []byte(storage.TableBase(table) + "/text/" + url.PathEscape(index) + "/")
	h := sha256.Sum256([]byte(gram))
	base = append(base, h[:8]...)
	primary, _ := storage.PrefixPrimaryByPK(table, pk)
	return append(base, primary[len(primary)-8:]...)
}

func textGrams(value string) (map[string]struct{}, error) {
	if len(value) > maxTextBytes || !utf8.ValidString(value) {
		return nil, fmt.Errorf("native text field exceeds 256 KiB or is not UTF-8")
	}
	// Long/high-entropy documents get one persistent overflow pointer. Queries
	// merge those pointers with the selected gram and verify complete text,
	// preserving exact results without unbounded index expansion per event.
	if len(value) > 16<<10 {
		return nil, nil
	}
	runes := []rune(strings.ToLower(value))
	grams := make(map[string]struct{})
	for i := 0; i+3 <= len(runes); i++ {
		grams[string(runes[i:i+3])] = struct{}{}
		if len(grams) > 4096 {
			return nil, nil
		}
	}
	return grams, nil
}

func planText(td types.TableDescriptor, prior, next types.Item) ([]storage.IndexOp, error) {
	var ops []storage.IndexOp
	for _, idx := range td.TextIndexes {
		keys := func(item types.Item) (map[string][]byte, error) {
			out := make(map[string][]byte)
			if item == nil {
				return out, nil
			}
			value, ok := textPath(item, idx.Field)
			if !ok {
				return out, nil
			}
			if value.T != types.AttrS {
				return nil, fmt.Errorf("native text field %q must be a string", idx.Field)
			}
			grams, err := textGrams(value.S)
			if err != nil {
				return nil, err
			}
			pk, sk, err := primaryKeyBytes(item, td.KeySchema)
			if err != nil {
				return nil, err
			}
			if len(pk) > 4096 || len(sk) > 4096 {
				return nil, fmt.Errorf("native text primary key exceeds 4096 bytes")
			}
			if grams == nil || len(grams)*(len(textPrefix(td.Name, idx.Name, pk, ""))+len(sk)) > 1<<20 {
				key := append(textPrefix(td.Name, idx.Name, pk, textOverflowGram), sk...)
				out[string(key)] = key
				return out, nil
			}
			for gram := range grams {
				key := append(textPrefix(td.Name, idx.Name, pk, gram), sk...)
				out[string(key)] = key
			}
			return out, nil
		}
		oldKeys, err := keys(prior)
		if err != nil {
			return nil, err
		}
		newKeys, err := keys(next)
		if err != nil {
			return nil, err
		}
		for key, raw := range oldKeys {
			if _, ok := newKeys[key]; !ok {
				ops = append(ops, storage.IndexOp{Op: storage.IndexOpDelete, Key: raw})
			}
		}
		for key, raw := range newKeys {
			if _, ok := oldKeys[key]; !ok {
				ops = append(ops, storage.IndexOp{Op: storage.IndexOpSet, Key: raw})
			}
		}
	}
	return ops, nil
}

func prefixEnd(prefix []byte) []byte {
	end := bytes.Clone(prefix)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] != 255 {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}
