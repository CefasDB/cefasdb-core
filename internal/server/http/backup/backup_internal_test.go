package backup

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	"github.com/CefasDb/cefasdb/pkg/types"
)

// A restore that failed after registering its target is a conflict whatever
// stopped it: a plain retry would collide with the partial target (FU-01).
func TestMapBackupErrPartialRestoreIsConflict(t *testing.T) {
	for _, cause := range []error{
		&pebble.ReadWaitError{Cause: context.Canceled},
		fmt.Errorf("write item 3 into target: %w", types.ErrMissingKey),
		pebble.ErrThrottled,
	} {
		err := &pebble.RestorePartialError{Target: "t", RowsCopied: 3, Cause: cause}
		if got := mapBackupErr(err); got != http.StatusConflict {
			t.Fatalf("cause %v mapped to %d, want 409", cause, got)
		}
	}
}
