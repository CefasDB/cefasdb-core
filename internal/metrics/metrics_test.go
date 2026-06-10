package metrics_test

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/osvaldoandrade/cefas/internal/metrics"
)

func TestMetricsHandlerExposesRegisteredSeries(t *testing.T) {
	m := metrics.New()
	m.Observe("PutItem", "events", "ok", 0.0012)
	m.Observe("GetItem", "events", "notfound", 0.0001)
	m.AuthRejected("missing_token")

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
	} {
		if !strings.Contains(out, want) {
			t.Errorf("metrics body missing %q\n--- got ---\n%s", want, out)
		}
	}
}
