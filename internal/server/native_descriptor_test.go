package server

import (
	"reflect"
	"testing"

	"github.com/CefasDb/cefasdb/pkg/types"
)

func TestNativeDescriptorProtobufRoundTrip(t *testing.T) {
	td := types.TableDescriptor{Name: "logs", KeySchema: types.KeySchema{PK: "tenant", SK: "time"}, TTLAttribute: "expiresAt", TextIndexes: []types.TextIndexDescriptor{{Name: "message", Field: "message"}}}
	got := pbToTableDescriptor(tableDescriptorToPB(td))
	if !reflect.DeepEqual(got, td) {
		t.Fatal("native descriptor fields lost across protobuf")
	}
}
