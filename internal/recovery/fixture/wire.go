package fixture

import (
	"fmt"

	"github.com/CefasDb/cefasdb/pkg/client"
	cefaspb "github.com/CefasDb/cefasdb/pkg/protocol"
	"github.com/CefasDb/cefasdb/pkg/types"
	"google.golang.org/protobuf/proto"
)

func batchWriteRequestSize(table string, operations []client.BatchWriteOp) (int, error) {
	request := &cefaspb.BatchWriteItemRequest{
		Table: table,
		Ops:   make([]*cefaspb.BatchWriteOp, 0, len(operations)),
	}
	for index, operation := range operations {
		wireOperation, err := batchWriteOperationToProto(operation)
		if err != nil {
			return 0, failure(
				CodeFixtureConflict,
				"batchWriteRequestSize",
				fmt.Sprintf("operations[%d]", index),
				"INVALID_OPERATION",
				err,
			)
		}
		request.Ops = append(request.Ops, wireOperation)
	}
	return proto.Size(request), nil
}

func batchWriteOperationToProto(operation client.BatchWriteOp) (*cefaspb.BatchWriteOp, error) {
	switch {
	case operation.Put != nil:
		item, err := itemToProto(operation.Put)
		if err != nil {
			return nil, err
		}
		return &cefaspb.BatchWriteOp{Kind: cefaspb.BatchWriteOp_KIND_PUT, Item: item}, nil
	case operation.Delete != nil:
		key, err := itemToProto(operation.Delete)
		if err != nil {
			return nil, err
		}
		return &cefaspb.BatchWriteOp{Kind: cefaspb.BatchWriteOp_KIND_DELETE, Key: key}, nil
	default:
		return nil, errorsNewInvalidBatchOperation
	}
}

var errorsNewInvalidBatchOperation = fmt.Errorf("batch write operation has neither put nor delete")

func itemToProto(item types.Item) (map[string]*cefaspb.AttributeValue, error) {
	out := make(map[string]*cefaspb.AttributeValue, len(item))
	for name, value := range item {
		encoded, err := attributeToProto(value)
		if err != nil {
			return nil, fmt.Errorf("attribute %q: %w", name, err)
		}
		out[name] = encoded
	}
	return out, nil
}

func attributeToProto(value types.AttributeValue) (*cefaspb.AttributeValue, error) {
	switch value.T {
	case types.AttrNull:
		return &cefaspb.AttributeValue{
			Value: &cefaspb.AttributeValue_NullVal{NullVal: true},
		}, nil
	case types.AttrS:
		return &cefaspb.AttributeValue{Value: &cefaspb.AttributeValue_S{S: value.S}}, nil
	case types.AttrN:
		return &cefaspb.AttributeValue{Value: &cefaspb.AttributeValue_N{N: value.N}}, nil
	case types.AttrB:
		return &cefaspb.AttributeValue{
			Value: &cefaspb.AttributeValue_B{B: append([]byte(nil), value.B...)},
		}, nil
	case types.AttrBOOL:
		return &cefaspb.AttributeValue{
			Value: &cefaspb.AttributeValue_BoolVal{BoolVal: value.BOOL},
		}, nil
	case types.AttrSS:
		return &cefaspb.AttributeValue{
			Value: &cefaspb.AttributeValue_Ss{
				Ss: &cefaspb.StringSet{Values: append([]string(nil), value.SS...)},
			},
		}, nil
	case types.AttrNS:
		return &cefaspb.AttributeValue{
			Value: &cefaspb.AttributeValue_Ns{
				Ns: &cefaspb.StringSet{Values: append([]string(nil), value.NS...)},
			},
		}, nil
	case types.AttrBS:
		values := make([][]byte, len(value.BS))
		for index := range value.BS {
			values[index] = append([]byte(nil), value.BS[index]...)
		}
		return &cefaspb.AttributeValue{
			Value: &cefaspb.AttributeValue_Bs{Bs: &cefaspb.BinarySet{Values: values}},
		}, nil
	case types.AttrL:
		values := make([]*cefaspb.AttributeValue, len(value.L))
		for index := range value.L {
			encoded, err := attributeToProto(value.L[index])
			if err != nil {
				return nil, err
			}
			values[index] = encoded
		}
		return &cefaspb.AttributeValue{
			Value: &cefaspb.AttributeValue_L{L: &cefaspb.List{Values: values}},
		}, nil
	case types.AttrM:
		values := make(map[string]*cefaspb.AttributeValue, len(value.M))
		for name, nested := range value.M {
			encoded, err := attributeToProto(nested)
			if err != nil {
				return nil, err
			}
			values[name] = encoded
		}
		return &cefaspb.AttributeValue{
			Value: &cefaspb.AttributeValue_M{M: &cefaspb.Map{Values: values}},
		}, nil
	case types.AttrVec:
		return &cefaspb.AttributeValue{
			Value: &cefaspb.AttributeValue_V{
				V: &cefaspb.Vector{
					Values: append([]float64(nil), value.Vec...),
					Dim:    int32(len(value.Vec)),
				},
			},
		}, nil
	default:
		return nil, fmt.Errorf("unsupported attribute type %d", value.T)
	}
}
