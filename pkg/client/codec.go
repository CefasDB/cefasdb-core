package client

import (
	cefaspb "github.com/CefasDb/cefasdb/pkg/protocol"
	"github.com/CefasDb/cefasdb/pkg/types"
)

// Translation helpers between cefas/pkg/types and the generated
// protobuf surface. Kept here so the gRPC types stay confined to the
// SDK and never leak into application code.

func attrToPB(av types.AttributeValue) *cefaspb.AttributeValue {
	switch av.T {
	case types.AttrNull:
		return &cefaspb.AttributeValue{Value: &cefaspb.AttributeValue_NullVal{NullVal: true}}
	case types.AttrS:
		return &cefaspb.AttributeValue{Value: &cefaspb.AttributeValue_S{S: av.S}}
	case types.AttrN:
		return &cefaspb.AttributeValue{Value: &cefaspb.AttributeValue_N{N: av.N}}
	case types.AttrB:
		return &cefaspb.AttributeValue{Value: &cefaspb.AttributeValue_B{B: append([]byte(nil), av.B...)}}
	case types.AttrBOOL:
		return &cefaspb.AttributeValue{Value: &cefaspb.AttributeValue_BoolVal{BoolVal: av.BOOL}}
	case types.AttrSS:
		return &cefaspb.AttributeValue{Value: &cefaspb.AttributeValue_Ss{Ss: &cefaspb.StringSet{Values: append([]string(nil), av.SS...)}}}
	case types.AttrNS:
		return &cefaspb.AttributeValue{Value: &cefaspb.AttributeValue_Ns{Ns: &cefaspb.StringSet{Values: append([]string(nil), av.NS...)}}}
	case types.AttrBS:
		out := make([][]byte, len(av.BS))
		for i := range av.BS {
			out[i] = append([]byte(nil), av.BS[i]...)
		}
		return &cefaspb.AttributeValue{Value: &cefaspb.AttributeValue_Bs{Bs: &cefaspb.BinarySet{Values: out}}}
	case types.AttrL:
		list := make([]*cefaspb.AttributeValue, len(av.L))
		for i := range av.L {
			list[i] = attrToPB(av.L[i])
		}
		return &cefaspb.AttributeValue{Value: &cefaspb.AttributeValue_L{L: &cefaspb.List{Values: list}}}
	case types.AttrM:
		m := make(map[string]*cefaspb.AttributeValue, len(av.M))
		for k, v := range av.M {
			m[k] = attrToPB(v)
		}
		return &cefaspb.AttributeValue{Value: &cefaspb.AttributeValue_M{M: &cefaspb.Map{Values: m}}}
	case types.AttrVec:
		return &cefaspb.AttributeValue{Value: &cefaspb.AttributeValue_V{V: &cefaspb.Vector{Values: append([]float64(nil), av.Vec...), Dim: int32(len(av.Vec))}}}
	}
	return nil
}

func attrFromPB(av *cefaspb.AttributeValue) types.AttributeValue {
	if av == nil {
		return types.AttributeValue{}
	}
	switch v := av.GetValue().(type) {
	case *cefaspb.AttributeValue_S:
		return types.AttributeValue{T: types.AttrS, S: v.S}
	case *cefaspb.AttributeValue_N:
		return types.AttributeValue{T: types.AttrN, N: v.N}
	case *cefaspb.AttributeValue_B:
		return types.AttributeValue{T: types.AttrB, B: append([]byte(nil), v.B...)}
	case *cefaspb.AttributeValue_BoolVal:
		return types.AttributeValue{T: types.AttrBOOL, BOOL: v.BoolVal}
	case *cefaspb.AttributeValue_NullVal:
		return types.AttributeValue{T: types.AttrNull}
	case *cefaspb.AttributeValue_Ss:
		return types.AttributeValue{T: types.AttrSS, SS: append([]string(nil), v.Ss.GetValues()...)}
	case *cefaspb.AttributeValue_Ns:
		return types.AttributeValue{T: types.AttrNS, NS: append([]string(nil), v.Ns.GetValues()...)}
	case *cefaspb.AttributeValue_Bs:
		src := v.Bs.GetValues()
		out := make([][]byte, len(src))
		for i := range src {
			out[i] = append([]byte(nil), src[i]...)
		}
		return types.AttributeValue{T: types.AttrBS, BS: out}
	case *cefaspb.AttributeValue_L:
		src := v.L.GetValues()
		out := make([]types.AttributeValue, len(src))
		for i := range src {
			out[i] = attrFromPB(src[i])
		}
		return types.AttributeValue{T: types.AttrL, L: out}
	case *cefaspb.AttributeValue_M:
		src := v.M.GetValues()
		out := make(map[string]types.AttributeValue, len(src))
		for k, mv := range src {
			out[k] = attrFromPB(mv)
		}
		return types.AttributeValue{T: types.AttrM, M: out}
	case *cefaspb.AttributeValue_V:
		return types.AttributeValue{T: types.AttrVec, Vec: append([]float64(nil), v.V.GetValues()...)}
	}
	return types.AttributeValue{}
}

func itemAttrMap(it types.Item) map[string]*cefaspb.AttributeValue {
	if it == nil {
		return nil
	}
	out := make(map[string]*cefaspb.AttributeValue, len(it))
	for k, v := range it {
		out[k] = attrToPB(v)
	}
	return out
}

func itemFromPB(in map[string]*cefaspb.AttributeValue) types.Item {
	out := make(types.Item, len(in))
	for k, v := range in {
		out[k] = attrFromPB(v)
	}
	return out
}

func tdToPB(td types.TableDescriptor) *cefaspb.TableDescriptor {
	out := &cefaspb.TableDescriptor{
		Name:                 td.Name,
		KeySchema:            &cefaspb.KeySchema{Pk: td.KeySchema.PK, Sk: td.KeySchema.SK},
		StorageClass:         td.StorageClass,
		MemoryFootprintBytes: td.MemoryFootprintBytes,
		LatestStreamArn:      td.LatestStreamArn,
		LatestStreamLabel:    td.LatestStreamLabel,
		StreamStatus:         td.StreamStatus,
	}
	if td.StreamSpecification != nil {
		out.StreamSpecification = &cefaspb.StreamSpecification{
			StreamEnabled:  td.StreamSpecification.StreamEnabled,
			StreamViewType: td.StreamSpecification.StreamViewType,
		}
	}
	for _, g := range td.GSIs {
		out.Gsis = append(out.Gsis, &cefaspb.GSIDescriptor{
			Name:      g.Name,
			KeySchema: &cefaspb.KeySchema{Pk: g.KeySchema.PK, Sk: g.KeySchema.SK},
			Projected: append([]string(nil), g.Projected...),
		})
	}
	for _, s := range td.SpatialIndexes {
		pb := &cefaspb.SpatialIndexDescriptor{
			Name:       s.Name,
			Kind:       s.Kind,
			Attributes: append([]string(nil), s.Attributes...),
			Precision:  int32(s.Precision),
		}
		for _, r := range s.Ranges {
			pb.Ranges = append(pb.Ranges, &cefaspb.NumRange{Lo: r.Lo, Hi: r.Hi})
		}
		out.SpatialIndexes = append(out.SpatialIndexes, pb)
	}
	for _, a := range td.AttributeDefinitions {
		out.AttributeDefinitions = append(out.AttributeDefinitions, &cefaspb.AttributeDefinition{
			Name:             a.Name,
			Type:             a.Type,
			VectorDimensions: int32(a.VectorDimensions),
		})
	}
	return out
}

func tdFromPB(pb *cefaspb.TableDescriptor) types.TableDescriptor {
	if pb == nil {
		return types.TableDescriptor{}
	}
	out := types.TableDescriptor{
		Name:                 pb.GetName(),
		StorageClass:         pb.GetStorageClass(),
		MemoryFootprintBytes: pb.GetMemoryFootprintBytes(),
		LatestStreamArn:      pb.GetLatestStreamArn(),
		LatestStreamLabel:    pb.GetLatestStreamLabel(),
		StreamStatus:         pb.GetStreamStatus(),
	}
	if ss := pb.GetStreamSpecification(); ss != nil {
		out.StreamSpecification = &types.StreamSpecification{
			StreamEnabled:  ss.GetStreamEnabled(),
			StreamViewType: ss.GetStreamViewType(),
		}
	}
	if ks := pb.GetKeySchema(); ks != nil {
		out.KeySchema = types.KeySchema{PK: ks.GetPk(), SK: ks.GetSk()}
	}
	for _, g := range pb.GetGsis() {
		gd := types.GSIDescriptor{Name: g.GetName(), Projected: append([]string(nil), g.GetProjected()...)}
		if gk := g.GetKeySchema(); gk != nil {
			gd.KeySchema = types.KeySchema{PK: gk.GetPk(), SK: gk.GetSk()}
		}
		out.GSIs = append(out.GSIs, gd)
	}
	for _, s := range pb.GetSpatialIndexes() {
		sd := types.SpatialIndexDescriptor{
			Name:       s.GetName(),
			Kind:       s.GetKind(),
			Attributes: append([]string(nil), s.GetAttributes()...),
			Precision:  int(s.GetPrecision()),
		}
		for _, r := range s.GetRanges() {
			sd.Ranges = append(sd.Ranges, types.NumRange{Lo: r.GetLo(), Hi: r.GetHi()})
		}
		out.SpatialIndexes = append(out.SpatialIndexes, sd)
	}
	for _, a := range pb.GetAttributeDefinitions() {
		out.AttributeDefinitions = append(out.AttributeDefinitions, types.AttributeDefinition{
			Name:             a.GetName(),
			Type:             a.GetType(),
			VectorDimensions: int(a.GetVectorDimensions()),
		})
	}
	return out
}
