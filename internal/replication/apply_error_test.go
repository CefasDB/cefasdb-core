package replication

import (
	"errors"
	"fmt"
	"testing"

	hraft "github.com/hashicorp/raft"
)

func TestMapApplyErrorTreatsRaftLeadershipChangesAsRetryable(t *testing.T) {
	tests := []error{
		hraft.ErrNotLeader,
		hraft.ErrLeadershipLost,
		hraft.ErrLeadershipTransferInProgress,
		fmt.Errorf("wrapped: %w", hraft.ErrLeadershipLost),
	}
	for _, err := range tests {
		if got := mapApplyError(err); !errors.Is(got, ErrNotLeader) {
			t.Fatalf("mapApplyError(%v) = %v, want ErrNotLeader", err, got)
		}
	}
}

func TestMapApplyErrorPreservesNonLeadershipErrors(t *testing.T) {
	want := hraft.ErrEnqueueTimeout
	if got := mapApplyError(want); !errors.Is(got, want) {
		t.Fatalf("mapApplyError(%v) = %v, want original", want, got)
	}
}
