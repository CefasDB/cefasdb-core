package pebble

import "math/big"

// Bounded decimal JSON numbers only. big.Rat also accepts fractions and
// Go-style binary/hex exponents; an unchecked exponent can allocate an
// arbitrarily large integer before the query deadline is observed.
func textNumber(raw string) (*big.Rat, bool) {
	if len(raw) == 0 || len(raw) > 256 {
		return nil, false
	}
	i := 0
	if raw[i] == '-' {
		i++
		if i == len(raw) {
			return nil, false
		}
	}
	if raw[i] == '0' {
		i++
	} else {
		if raw[i] < '1' || raw[i] > '9' {
			return nil, false
		}
		for i < len(raw) && raw[i] >= '0' && raw[i] <= '9' {
			i++
		}
	}
	if i < len(raw) && raw[i] == '.' {
		i++
		start := i
		for i < len(raw) && raw[i] >= '0' && raw[i] <= '9' {
			i++
		}
		if i == start {
			return nil, false
		}
	}
	if i < len(raw) && (raw[i] == 'e' || raw[i] == 'E') {
		i++
		if i < len(raw) && (raw[i] == '+' || raw[i] == '-') {
			i++
		}
		start := i
		exponent := 0
		for i < len(raw) && raw[i] >= '0' && raw[i] <= '9' {
			exponent = exponent*10 + int(raw[i]-'0')
			if exponent > 1024 {
				return nil, false
			}
			i++
		}
		if i == start {
			return nil, false
		}
	}
	if i != len(raw) {
		return nil, false
	}
	return new(big.Rat).SetString(raw)
}
