package runner

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/CefasDb/cefasdb/pkg/types"
)

const (
	PayloadModeRepeat = "repeat"
	PayloadModeRandom = "random"
)

func NormalizePayloadMode(mode string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "", PayloadModeRepeat:
		return PayloadModeRepeat, nil
	case PayloadModeRandom:
		return PayloadModeRandom, nil
	default:
		return "", fmt.Errorf("unsupported payload mode %q", mode)
	}
}

func repeatedPayload(bytes int) string {
	if bytes <= 0 {
		return ""
	}
	return strings.Repeat("x", bytes)
}

func payloadFor(id int64, bytes int, mode, repeat string) string {
	if bytes <= 0 {
		return ""
	}
	if mode != PayloadModeRandom {
		return repeat
	}
	return deterministicPayload(id, bytes)
}

func deterministicPayload(id int64, bytes int) string {
	out := make([]byte, bytes)
	x := uint64(id) + 0x9e3779b97f4a7c15
	for i := range out {
		x += 0x9e3779b97f4a7c15
		z := x
		z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
		z = (z ^ (z >> 27)) * 0x94d049bb133111eb
		z ^= z >> 31
		out[i] = byte(33 + z%94)
	}
	return string(out)
}

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
