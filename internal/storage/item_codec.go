package storage

import (
	"encoding/binary"
	"fmt"
	"math"
	"sort"

	"github.com/osvaldoandrade/cefas/pkg/types"
)

// Item binary format (TLV, big-endian):
//
//   magic       [2] = 'C','I' (cefas item)
//   version     [1] = 1
//   attrCount   uvarint
//   for each attribute:
//     nameLen   uvarint
//     name      [nameLen]
//     value     <see EncodeAttr>
//
// EncodeAttr format:
//
//   type        [1]   AttrType
//   payload depends on type:
//     S, N    : uvarint length + raw bytes
//     B       : uvarint length + raw bytes
//     BOOL    : [1] (0 or 1)
//     NULL    : nothing
//     SS, NS  : uvarint count + N×(uvarint len + bytes)
//     BS      : same as SS
//     L       : uvarint count + N× EncodeAttr
//     M       : uvarint count + N×(uvarint nameLen + name + EncodeAttr)
//
// Custom format (not gob/protobuf) keeps Phase 1 self-contained — no
// codegen, no reflection — and is byte-for-byte deterministic, which we
// need for snapshot bisection and deterministic Raft application.

const (
	itemMagic   = uint16(0x4349) // "CI"
	itemVersion = byte(1)
)

// EncodeItem serializes an Item to its on-disk byte form.
func EncodeItem(item types.Item) ([]byte, error) {
	if item == nil {
		return nil, fmt.Errorf("encode item: nil")
	}
	// Sort attribute names so identical items produce byte-identical
	// encodings — important for snapshot integrity and for cache
	// friendliness in the LSM.
	names := make([]string, 0, len(item))
	for n := range item {
		names = append(names, n)
	}
	sort.Strings(names)

	buf := make([]byte, 0, 64+32*len(item))
	buf = binary.BigEndian.AppendUint16(buf, itemMagic)
	buf = append(buf, itemVersion)
	buf = appendUvarint(buf, uint64(len(item)))
	for _, n := range names {
		buf = appendUvarint(buf, uint64(len(n)))
		buf = append(buf, n...)
		var err error
		buf, err = appendAttr(buf, item[n])
		if err != nil {
			return nil, fmt.Errorf("encode attr %q: %w", n, err)
		}
	}
	return buf, nil
}

// DecodeItem reverses EncodeItem.
func DecodeItem(data []byte) (types.Item, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("decode item: short buffer")
	}
	if binary.BigEndian.Uint16(data[:2]) != itemMagic {
		return nil, fmt.Errorf("decode item: bad magic")
	}
	if data[2] != itemVersion {
		return nil, fmt.Errorf("decode item: unsupported version %d", data[2])
	}
	p := data[3:]

	n, p, err := readUvarint(p)
	if err != nil {
		return nil, fmt.Errorf("decode item count: %w", err)
	}
	out := make(types.Item, n)
	for i := uint64(0); i < n; i++ {
		nameLen, rest, err := readUvarint(p)
		if err != nil {
			return nil, fmt.Errorf("decode attr name len: %w", err)
		}
		p = rest
		if uint64(len(p)) < nameLen {
			return nil, fmt.Errorf("decode attr name: short")
		}
		name := string(p[:nameLen])
		p = p[nameLen:]
		var av types.AttributeValue
		av, p, err = readAttr(p)
		if err != nil {
			return nil, fmt.Errorf("decode attr %q: %w", name, err)
		}
		out[name] = av
	}
	return out, nil
}

func appendAttr(buf []byte, av types.AttributeValue) ([]byte, error) {
	buf = append(buf, byte(av.T))
	switch av.T {
	case types.AttrNull:
		return buf, nil
	case types.AttrS:
		buf = appendUvarint(buf, uint64(len(av.S)))
		buf = append(buf, av.S...)
		return buf, nil
	case types.AttrN:
		buf = appendUvarint(buf, uint64(len(av.N)))
		buf = append(buf, av.N...)
		return buf, nil
	case types.AttrB:
		buf = appendUvarint(buf, uint64(len(av.B)))
		buf = append(buf, av.B...)
		return buf, nil
	case types.AttrBOOL:
		if av.BOOL {
			buf = append(buf, 1)
		} else {
			buf = append(buf, 0)
		}
		return buf, nil
	case types.AttrSS:
		buf = appendUvarint(buf, uint64(len(av.SS)))
		for _, s := range av.SS {
			buf = appendUvarint(buf, uint64(len(s)))
			buf = append(buf, s...)
		}
		return buf, nil
	case types.AttrNS:
		buf = appendUvarint(buf, uint64(len(av.NS)))
		for _, s := range av.NS {
			buf = appendUvarint(buf, uint64(len(s)))
			buf = append(buf, s...)
		}
		return buf, nil
	case types.AttrBS:
		buf = appendUvarint(buf, uint64(len(av.BS)))
		for _, b := range av.BS {
			buf = appendUvarint(buf, uint64(len(b)))
			buf = append(buf, b...)
		}
		return buf, nil
	case types.AttrL:
		buf = appendUvarint(buf, uint64(len(av.L)))
		var err error
		for i := range av.L {
			buf, err = appendAttr(buf, av.L[i])
			if err != nil {
				return nil, err
			}
		}
		return buf, nil
	case types.AttrM:
		buf = appendUvarint(buf, uint64(len(av.M)))
		names := make([]string, 0, len(av.M))
		for k := range av.M {
			names = append(names, k)
		}
		sort.Strings(names)
		for _, k := range names {
			buf = appendUvarint(buf, uint64(len(k)))
			buf = append(buf, k...)
			var err error
			buf, err = appendAttr(buf, av.M[k])
			if err != nil {
				return nil, err
			}
		}
		return buf, nil
	case types.AttrVec:
		buf = appendUvarint(buf, uint64(len(av.Vec)))
		for _, f := range av.Vec {
			buf = binary.BigEndian.AppendUint64(buf, math.Float64bits(f))
		}
		return buf, nil
	}
	return nil, fmt.Errorf("unknown attr type %d", av.T)
}

func readAttr(p []byte) (types.AttributeValue, []byte, error) {
	if len(p) == 0 {
		return types.AttributeValue{}, nil, fmt.Errorf("attr: empty")
	}
	t := types.AttrType(p[0])
	p = p[1:]
	av := types.AttributeValue{T: t}
	switch t {
	case types.AttrNull:
		return av, p, nil
	case types.AttrS:
		s, rest, err := readLenBytes(p)
		if err != nil {
			return av, nil, err
		}
		av.S = string(s)
		return av, rest, nil
	case types.AttrN:
		s, rest, err := readLenBytes(p)
		if err != nil {
			return av, nil, err
		}
		av.N = string(s)
		return av, rest, nil
	case types.AttrB:
		b, rest, err := readLenBytes(p)
		if err != nil {
			return av, nil, err
		}
		av.B = append([]byte(nil), b...)
		return av, rest, nil
	case types.AttrBOOL:
		if len(p) < 1 {
			return av, nil, fmt.Errorf("attr BOOL: short")
		}
		av.BOOL = p[0] != 0
		return av, p[1:], nil
	case types.AttrSS, types.AttrNS:
		n, rest, err := readUvarint(p)
		if err != nil {
			return av, nil, err
		}
		p = rest
		ss := make([]string, n)
		for i := uint64(0); i < n; i++ {
			s, r, err := readLenBytes(p)
			if err != nil {
				return av, nil, err
			}
			ss[i] = string(s)
			p = r
		}
		if t == types.AttrSS {
			av.SS = ss
		} else {
			av.NS = ss
		}
		return av, p, nil
	case types.AttrBS:
		n, rest, err := readUvarint(p)
		if err != nil {
			return av, nil, err
		}
		p = rest
		bs := make([][]byte, n)
		for i := uint64(0); i < n; i++ {
			b, r, err := readLenBytes(p)
			if err != nil {
				return av, nil, err
			}
			bs[i] = append([]byte(nil), b...)
			p = r
		}
		av.BS = bs
		return av, p, nil
	case types.AttrL:
		n, rest, err := readUvarint(p)
		if err != nil {
			return av, nil, err
		}
		p = rest
		list := make([]types.AttributeValue, n)
		for i := uint64(0); i < n; i++ {
			var v types.AttributeValue
			v, p, err = readAttr(p)
			if err != nil {
				return av, nil, err
			}
			list[i] = v
		}
		av.L = list
		return av, p, nil
	case types.AttrM:
		n, rest, err := readUvarint(p)
		if err != nil {
			return av, nil, err
		}
		p = rest
		m := make(map[string]types.AttributeValue, n)
		for i := uint64(0); i < n; i++ {
			name, r, err := readLenBytes(p)
			if err != nil {
				return av, nil, err
			}
			p = r
			var v types.AttributeValue
			v, p, err = readAttr(p)
			if err != nil {
				return av, nil, err
			}
			m[string(name)] = v
		}
		av.M = m
		return av, p, nil
	case types.AttrVec:
		n, rest, err := readUvarint(p)
		if err != nil {
			return av, nil, err
		}
		p = rest
		if uint64(len(p)) < n*8 {
			return av, nil, fmt.Errorf("attr V: short")
		}
		out := make([]float64, n)
		for i := uint64(0); i < n; i++ {
			out[i] = math.Float64frombits(binary.BigEndian.Uint64(p[:8]))
			p = p[8:]
		}
		av.Vec = out
		return av, p, nil
	}
	return av, nil, fmt.Errorf("unknown attr type %d", t)
}

// AttrCanonicalBytes returns the deterministic byte form of an attribute
// used for keying: PK hash input and SK byte slice. Only S, N and B are
// permitted as key attributes — DynamoDB has the same restriction.
// Numbers are encoded as their canonical decimal text; spatial / set
// types are rejected.
func AttrCanonicalBytes(av types.AttributeValue) ([]byte, error) {
	switch av.T {
	case types.AttrS:
		return []byte(av.S), nil
	case types.AttrN:
		return []byte(av.N), nil
	case types.AttrB:
		return append([]byte(nil), av.B...), nil
	default:
		return nil, types.ErrInvalidKeyType
	}
}

// ---------- varint helpers ----------

func appendUvarint(buf []byte, v uint64) []byte {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], v)
	return append(buf, tmp[:n]...)
}

func readUvarint(p []byte) (uint64, []byte, error) {
	v, n := binary.Uvarint(p)
	if n <= 0 {
		return 0, nil, fmt.Errorf("uvarint: invalid")
	}
	return v, p[n:], nil
}

func readLenBytes(p []byte) ([]byte, []byte, error) {
	n, rest, err := readUvarint(p)
	if err != nil {
		return nil, nil, err
	}
	if uint64(len(rest)) < n {
		return nil, nil, fmt.Errorf("len bytes: short")
	}
	return rest[:n], rest[n:], nil
}
