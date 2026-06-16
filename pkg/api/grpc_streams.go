package api

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/osvaldoandrade/cefas/internal/auth"
	cefaspb "github.com/osvaldoandrade/cefas/pkg/api/proto"
	"github.com/osvaldoandrade/cefas/pkg/core/model"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

func (s *GRPCServer) ListStreams(ctx context.Context, req *cefaspb.ListStreamsRequest) (*cefaspb.ListStreamsResponse, error) {
	if err := requireScope(ctx, auth.ScopeTableDescribe); err != nil {
		return nil, err
	}
	streams, err := s.cat.ListStreams(req.GetTableName())
	if err != nil {
		return nil, mapStorageErr(err)
	}
	page, lastEvaluated, err := paginateStreamDescriptors(
		streams,
		normalizeStreamAPILimit(req.GetLimit()),
		req.GetExclusiveStartStreamArn(),
	)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	resp := &cefaspb.ListStreamsResponse{
		LastEvaluatedStreamArn: lastEvaluated,
	}
	for _, stream := range page {
		resp.Streams = append(resp.Streams, streamSummaryToPB(stream))
	}
	return resp, nil
}

func (s *GRPCServer) DescribeStream(ctx context.Context, req *cefaspb.DescribeStreamRequest) (*cefaspb.DescribeStreamResponse, error) {
	if err := requireScope(ctx, auth.ScopeTableDescribe); err != nil {
		return nil, err
	}
	if req.GetStreamArn() == "" {
		return nil, status.Error(codes.InvalidArgument, "stream_arn required")
	}
	desc, err := s.cat.DescribeStream(req.GetStreamArn())
	if err != nil {
		return nil, mapStorageErr(err)
	}
	shards, lastEvaluated, err := paginateStreamShards(
		desc.Shards,
		normalizeStreamAPILimit(req.GetLimit()),
		req.GetExclusiveStartShardId(),
	)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	desc.Shards = shards
	out := streamDescriptionToPB(desc)
	out.LastEvaluatedShardId = lastEvaluated
	return &cefaspb.DescribeStreamResponse{StreamDescription: out}, nil
}

func (s *GRPCServer) GetShardIterator(ctx context.Context, req *cefaspb.GetShardIteratorRequest) (*cefaspb.GetShardIteratorResponse, error) {
	if err := requireScope(ctx, auth.ScopeTableDescribe); err != nil {
		return nil, err
	}
	shardID, err := model.NewStreamShardID(req.GetShardId())
	if err != nil {
		err = fmt.Errorf("%w: %v", types.ErrStreamIteratorInvalid, err)
		s.observeStreamIteratorFailure(streamTableForARN(s.cat, req.GetStreamArn()), err)
		return nil, mapStorageErr(err)
	}
	token, err := createStreamShardIterator(s.cat, s.db, createIteratorRequest{
		StreamArn:      req.GetStreamArn(),
		ShardID:        shardID,
		IteratorType:   req.GetShardIteratorType(),
		SequenceNumber: req.GetSequenceNumber(),
	}, time.Now())
	if err != nil {
		s.observeStreamIteratorFailure(streamTableForARN(s.cat, req.GetStreamArn()), err)
		return nil, mapStorageErr(err)
	}
	return &cefaspb.GetShardIteratorResponse{ShardIterator: token}, nil
}

func (s *GRPCServer) GetRecords(ctx context.Context, req *cefaspb.GetRecordsRequest) (*cefaspb.GetRecordsResponse, error) {
	if err := requireScope(ctx, auth.ScopeTableDescribe); err != nil {
		return nil, err
	}
	result, err := getStreamRecords(
		s.cat,
		s.db,
		req.GetShardIterator(),
		req.GetLimit(),
		time.Now(),
	)
	if err != nil {
		s.observeStreamGetRecords(result, err)
		return nil, mapStorageErr(err)
	}
	s.observeStreamGetRecords(result, nil)
	resp := &cefaspb.GetRecordsResponse{NextShardIterator: result.NextShardIterator}
	for _, record := range result.Records {
		resp.Records = append(resp.Records, streamRecordToPB(record))
	}
	return resp, nil
}
