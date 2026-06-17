package runner

import (
	"fmt"
	"strconv"

	"github.com/CefasDb/cefasdb/pkg/types"
)

func makeItem(id, users int64, payload string) types.Item {
	user := id % users
	return types.Item{
		"pk":      sAttr(fmt.Sprintf("USER#%06d", user)),
		"sk":      sAttr(fmt.Sprintf("EVENT#%012d", id)),
		"name":    sAttr(fmt.Sprintf("user-%06d", user)),
		"city":    sAttr(cityFor(id)),
		"lat":     nAttr(strconv.FormatFloat(-23.5505+float64(id%1000)/100000, 'f', -1, 64)),
		"lon":     nAttr(strconv.FormatFloat(-46.6333+float64(id%1000)/100000, 'f', -1, 64)),
		"score":   nAttr(strconv.FormatInt(id%10000, 10)),
		"active":  {T: types.AttrBOOL, BOOL: id%2 == 0},
		"payload": sAttr(payload),
	}
}

func keyFor(id, users int64) types.Item {
	return types.Item{
		"pk": sAttr(fmt.Sprintf("USER#%06d", id%users)),
		"sk": sAttr(fmt.Sprintf("EVENT#%012d", id)),
	}
}

func cityFor(id int64) string {
	switch id % 4 {
	case 0:
		return "Sao Paulo"
	case 1:
		return "Rio de Janeiro"
	case 2:
		return "Belo Horizonte"
	default:
		return "Curitiba"
	}
}

func sAttr(value string) types.AttributeValue {
	return types.AttributeValue{T: types.AttrS, S: value}
}

func nAttr(value string) types.AttributeValue {
	return types.AttributeValue{T: types.AttrN, N: value}
}

// permute scrambles seq across [0, modulo) via SplitMix64-style mixing so that
// sequential producers issue reads to a uniform spread of partition keys.
func permute(seq, modulo int64) int64 {
	if modulo <= 1 {
		return 0
	}
	x := uint64(seq + 1)
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	x *= 0xc4ceb9fe1a85ec53
	x ^= x >> 33
	return int64(x % uint64(modulo))
}
