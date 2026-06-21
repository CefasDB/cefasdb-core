package server

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"runtime"
	"strings"
	"time"

	// Side-effect import: registers /debug/pprof/* handlers on
	// http.DefaultServeMux. We mount that mux on a dedicated listener
	// so pprof never shares a port with the production HTTP API.
	_ "net/http/pprof"
)

// PprofOptions configures the optional pprof debug listener.
//
// MutexRate maps to runtime.SetMutexProfileFraction (1 = sample every event;
// 0 = off). BlockRate maps to runtime.SetBlockProfileRate in nanoseconds (1 =
// sample every event; 0 = off). Both carry measurable overhead at rate 1, so
// leave them at 0 unless a profile run is actively in progress.
type PprofOptions struct {
	Addr      string
	MutexRate int
	BlockRate int
}

// StartPprof starts a dedicated HTTP listener that exposes /debug/pprof/*
// handlers from net/http/pprof. Returns the started *http.Server so the
// caller can drive a graceful shutdown.
//
// Addr is typically "127.0.0.1:6060" — binding on a non-loopback interface
// is allowed but logs a warning because pprof leaks internals (goroutines,
// allocation sites, command-line) that should not be reachable from the
// public network.
func StartPprof(opts PprofOptions, logf func(format string, args ...any)) (*http.Server, error) {
	if opts.Addr == "" {
		return nil, nil
	}
	ln, err := net.Listen("tcp", opts.Addr)
	if err != nil {
		return nil, fmt.Errorf("pprof listen %s: %w", opts.Addr, err)
	}
	if opts.MutexRate > 0 {
		runtime.SetMutexProfileFraction(opts.MutexRate)
	}
	if opts.BlockRate > 0 {
		runtime.SetBlockProfileRate(opts.BlockRate)
	}
	srv := &http.Server{
		Addr:              ln.Addr().String(),
		Handler:           http.DefaultServeMux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	if logf != nil && !isLoopback(opts.Addr) {
		logf("pprof listener bound to non-loopback address %s — exposes process internals; restrict access", opts.Addr)
	}
	if logf != nil {
		logf("pprof listening addr=%s mutexRate=%d blockRate=%d", ln.Addr().String(), opts.MutexRate, opts.BlockRate)
	}
	go func() {
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			if logf != nil {
				logf("pprof serve error: %v", err)
			}
		}
	}()
	return srv, nil
}

// ShutdownPprof gracefully stops the pprof listener and resets profile rates.
// Safe to call with a nil server.
func ShutdownPprof(ctx context.Context, srv *http.Server) error {
	if srv == nil {
		return nil
	}
	runtime.SetMutexProfileFraction(0)
	runtime.SetBlockProfileRate(0)
	return srv.Shutdown(ctx)
}

func isLoopback(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	host = strings.TrimSpace(host)
	if host == "" || host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
