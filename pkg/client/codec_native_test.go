package client

import (
	"reflect"
	"testing"

	"github.com/CefasDb/cefasdb/pkg/types"
)

func TestSDKNativeDescriptorRoundTrip(t *testing.T) {
	td := types.TableDescriptor{Name: "logs", KeySchema: types.KeySchema{PK: "tenant", SK: "time"}, TTLAttribute: "expiresAt", TextIndexes: []types.TextIndexDescriptor{{Name: "message", Field: "message"}}}
	if got := tdFromPB(tdToPB(td)); !reflect.DeepEqual(got, td) {
		t.Fatal("native descriptor fields lost across SDK protobuf")
	}
}
