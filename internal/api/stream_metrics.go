package api

import (
	"errors"

	"github.com/osvaldoandrade/cefas/internal/catalog"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

func streamTableForARN(cat *catalog.Catalog, streamArn string) string {
	if cat == nil || streamArn == "" {
		return ""
	}
	desc, err := cat.DescribeStream(streamArn)
	if err != nil {
		return ""
	}
	return desc.TableName
}

func streamErrorReason(err error) string {
	switch {
	case errors.Is(err, types.ErrStreamTrimmed):
		return "trimmed"
	case errors.Is(err, types.ErrStreamIteratorExpired):
		return "expired"
	case errors.Is(err, types.ErrStreamIteratorInvalid):
		return "invalid"
	case errors.Is(err, types.ErrStreamNotFound):
		return "stream_not_found"
	case errors.Is(err, types.ErrStreamShardNotFound):
		return "shard_not_found"
	default:
		return "err"
	}
}

func (s *GRPCServer) observeStreamIteratorFailure(table string, err error) {
	if s == nil || s.metrics == nil {
		return
	}
	reason := streamErrorReason(err)
	s.metrics.ObserveStreamIteratorFailure(table, reason)
	if errors.Is(err, types.ErrStreamTrimmed) {
		s.metrics.ObserveStreamTrimmedError(table, "GetShardIterator")
	}
	if errors.Is(err, types.ErrStreamIteratorExpired) {
		s.metrics.ObserveStreamExpiredIterator(table, "GetShardIterator")
	}
}

func (s *GRPCServer) observeStreamGetRecords(result streamRecordsResult, err error) {
	if s == nil || s.metrics == nil {
		return
	}
	table := result.TableName
	if err != nil {
		reason := streamErrorReason(err)
		s.metrics.ObserveStreamGetRecords(table, reason, false)
		if errors.Is(err, types.ErrStreamTrimmed) {
			s.metrics.ObserveStreamTrimmedError(table, "GetRecords")
		}
		if errors.Is(err, types.ErrStreamIteratorExpired) {
			s.metrics.ObserveStreamExpiredIterator(table, "GetRecords")
		}
		return
	}
	s.metrics.ObserveStreamGetRecords(table, "ok", len(result.Records) == 0)
}

func (s *Server) observeStreamIteratorFailure(table string, err error) {
	if s == nil || s.metrics == nil {
		return
	}
	reason := streamErrorReason(err)
	s.metrics.ObserveStreamIteratorFailure(table, reason)
	if errors.Is(err, types.ErrStreamTrimmed) {
		s.metrics.ObserveStreamTrimmedError(table, "GetShardIterator")
	}
	if errors.Is(err, types.ErrStreamIteratorExpired) {
		s.metrics.ObserveStreamExpiredIterator(table, "GetShardIterator")
	}
}

func (s *Server) observeStreamGetRecords(result streamRecordsResult, err error) {
	if s == nil || s.metrics == nil {
		return
	}
	table := result.TableName
	if err != nil {
		reason := streamErrorReason(err)
		s.metrics.ObserveStreamGetRecords(table, reason, false)
		if errors.Is(err, types.ErrStreamTrimmed) {
			s.metrics.ObserveStreamTrimmedError(table, "GetRecords")
		}
		if errors.Is(err, types.ErrStreamIteratorExpired) {
			s.metrics.ObserveStreamExpiredIterator(table, "GetRecords")
		}
		return
	}
	s.metrics.ObserveStreamGetRecords(table, "ok", len(result.Records) == 0)
}
