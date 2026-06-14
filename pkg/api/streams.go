package api

import (
	"fmt"

	"github.com/osvaldoandrade/cefas/pkg/types"
)

const maxStreamAPILimit = 100

func normalizeStreamAPILimit(limit int32) int {
	if limit <= 0 || limit > maxStreamAPILimit {
		return maxStreamAPILimit
	}
	return int(limit)
}

func paginateStreamDescriptors(streams []types.StreamDescriptor, limit int, exclusiveStartStreamARN string) ([]types.StreamDescriptor, string, error) {
	start := 0
	if exclusiveStartStreamARN != "" {
		found := false
		for i, stream := range streams {
			if stream.StreamArn == exclusiveStartStreamARN {
				start = i + 1
				found = true
				break
			}
		}
		if !found {
			return nil, "", fmt.Errorf("exclusive_start_stream_arn %q not found", exclusiveStartStreamARN)
		}
	}
	if start >= len(streams) {
		return nil, "", nil
	}
	end := start + limit
	if end >= len(streams) {
		return streams[start:], "", nil
	}
	return streams[start:end], streams[end-1].StreamArn, nil
}

func paginateStreamShards(shards []types.StreamShardDescriptor, limit int, exclusiveStartShardID string) ([]types.StreamShardDescriptor, string, error) {
	start := 0
	if exclusiveStartShardID != "" {
		found := false
		for i, shard := range shards {
			if shard.ShardID == exclusiveStartShardID {
				start = i + 1
				found = true
				break
			}
		}
		if !found {
			return nil, "", fmt.Errorf("exclusive_start_shard_id %q not found", exclusiveStartShardID)
		}
	}
	if start >= len(shards) {
		return nil, "", nil
	}
	end := start + limit
	if end >= len(shards) {
		return shards[start:], "", nil
	}
	return shards[start:end], shards[end-1].ShardID, nil
}
