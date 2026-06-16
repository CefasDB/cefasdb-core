package metrics_test

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/osvaldoandrade/cefas/internal/metrics"
	pebble "github.com/osvaldoandrade/cefas/internal/storage/adapter/pebble"
)

func TestMetricsHandlerExposesRegisteredSeries(t *testing.T) {
	m := metrics.New()
	m.Observe("PutItem", "events", "ok", 0.0012)
	m.Observe("GetItem", "events", "notfound", 0.0001)
	m.AuthRejected("missing_token")
	m.ObserveStreamRetention("0", pebble.StreamRetentionStats{
		Table:           "events",
		OldestSequence:  2,
		NewestSequence:  5,
		RetainedBytes:   128,
		RecordsAppended: 5,
		RecordsTrimmed:  1,
	})
	m.ObserveStreamGetRecords("events", "ok", true)
	m.ObserveStreamIteratorFailure("events", "trimmed")
	m.ObserveStreamTrimmedError("events", "GetShardIterator")
	m.ObserveStreamExpiredIterator("events", "GetRecords")

	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, req)

	body, err := io.ReadAll(rec.Body)
	if err != nil {
		t.Fatal(err)
	}
	out := string(body)
	for _, want := range []string{
		"cefas_op_duration_seconds_bucket",
		`cefas_op_total{op="PutItem",outcome="ok",table="events"} 1`,
		`cefas_op_total{op="GetItem",outcome="notfound",table="events"} 1`,
		`cefas_auth_rejected_total{reason="missing_token"} 1`,
		`cefas_stream_records_appended{shard="0",table="events"} 5`,
		`cefas_stream_records_trimmed{shard="0",table="events"} 1`,
		`cefas_stream_retained_bytes{shard="0",table="events"} 128`,
		`cefas_stream_oldest_sequence{shard="0",table="events"} 2`,
		`cefas_stream_newest_sequence{shard="0",table="events"} 5`,
		`cefas_stream_get_records_total{outcome="ok",table="events"} 1`,
		`cefas_stream_get_records_empty_polls_total{table="events"} 1`,
		`cefas_stream_iterator_creation_failures_total{reason="trimmed",table="events"} 1`,
		`cefas_stream_trimmed_errors_total{op="GetShardIterator",table="events"} 1`,
		`cefas_stream_expired_iterator_errors_total{op="GetRecords",table="events"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("metrics body missing %q\n--- got ---\n%s", want, out)
		}
	}
}
