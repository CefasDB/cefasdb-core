package api

import (
	"errors"

	"github.com/osvaldoandrade/cefas/pkg/types"
)

// observeStreamIteratorFailure and observeStreamGetRecords are the
// two metric observers shared by the HTTP and gRPC stream handlers.
// streamErrorReason and StreamTableForARN are package-level aliases
// onto streamcore (see streams.go).

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

func (s *GRPCServer) observeStreamGetRecords(result StreamRecordsResult, err error) {
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

func (s *Server) observeStreamGetRecords(result StreamRecordsResult, err error) {
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
