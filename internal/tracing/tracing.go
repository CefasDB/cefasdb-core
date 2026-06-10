// Package tracing wires OpenTelemetry into cefas. Init returns a
// shutdown function that flushes spans before the process exits.
//
// The package is intentionally tiny — span creation happens inline at
// HTTP/gRPC entry points using the standard otel tracer obtained via
// Tracer(). Callers can pass spans through context.Context the same
// way they already pass cancellation.
package tracing

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

const tracerName = "github.com/osvaldoandrade/cefas"

// Config controls the OTLP exporter. Endpoint empty means "tracing
// disabled" — Init becomes a no-op and Tracer() returns a no-op
// tracer.
type Config struct {
	// Endpoint is the OTLP/gRPC collector address ("host:port"). When
	// empty, tracing stays off.
	Endpoint string

	// Insecure disables TLS to the collector. Default false.
	Insecure bool

	// ServiceVersion shows up as service.version on every span.
	ServiceVersion string

	// SampleRate in [0.0, 1.0]. 1.0 means every span is exported.
	SampleRate float64
}

// Init wires the global TracerProvider. The returned shutdown
// function blocks until in-flight spans flush or the supplied
// context expires.
func Init(ctx context.Context, cfg Config) (func(context.Context) error, error) {
	if strings.TrimSpace(cfg.Endpoint) == "" {
		// Nothing to do; preserve the no-op global tracer.
		return func(context.Context) error { return nil }, nil
	}
	opts := []otlptracegrpc.Option{otlptracegrpc.WithEndpoint(cfg.Endpoint)}
	if cfg.Insecure {
		opts = append(opts, otlptracegrpc.WithInsecure())
	}
	exporter, err := otlptrace.New(ctx, otlptracegrpc.NewClient(opts...))
	if err != nil {
		return nil, fmt.Errorf("otlp exporter: %w", err)
	}

	res, err := resource.Merge(resource.Default(), resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName("cefas"),
		semconv.ServiceVersion(cfg.ServiceVersion),
	))
	if err != nil {
		return nil, fmt.Errorf("otel resource: %w", err)
	}

	sampler := sdktrace.AlwaysSample()
	if cfg.SampleRate > 0 && cfg.SampleRate < 1 {
		sampler = sdktrace.TraceIDRatioBased(cfg.SampleRate)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter, sdktrace.WithBatchTimeout(5*time.Second)),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sampler),
	)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}

// Tracer returns the cefas tracer; safe to use even when Init was
// never called (returns a no-op tracer).
func Tracer() trace.Tracer { return otel.Tracer(tracerName) }

// StartHTTPSpan opens a span for an HTTP endpoint. op is the route
// suffix (PutItem, Query, ...).
func StartHTTPSpan(ctx context.Context, op string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return Tracer().Start(ctx, "cefas.http."+op, trace.WithAttributes(attrs...))
}

// StartGRPCSpan opens a span for a gRPC RPC. method is the trailing
// segment of the full method path.
func StartGRPCSpan(ctx context.Context, method string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return Tracer().Start(ctx, "cefas.grpc."+method, trace.WithAttributes(attrs...))
}
