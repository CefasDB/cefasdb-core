package server

import (
	"context"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
)

// A stream read whose context ended is retryable (Unavailable), like a
// draining or leaderless store, not an Internal error (FU-01).
func TestMapStorageErrReadWaitIsUnavailable(t *testing.T) {
	for _, cause := range []error{context.DeadlineExceeded, context.Canceled} {
		err := fmt.Errorf("stream read: %w", &pebble.ReadWaitError{Cause: cause})
		if got := status.Code(mapStorageErr(err)); got != codes.Unavailable {
			t.Fatalf("cause %v mapped to %v, want Unavailable", cause, got)
		}
	}
}
