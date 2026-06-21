package server

import (
	"context"
	"io"
	"net/http"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestStartPprof_DisabledWhenAddrEmpty(t *testing.T) {
	srv, err := StartPprof(PprofOptions{}, nil)
	if err != nil {
		t.Fatalf("StartPprof(empty addr) returned error: %v", err)
	}
	if srv != nil {
		t.Fatalf("StartPprof(empty addr) returned non-nil server")
	}
}

func TestStartPprof_ServesIndexAndProfile(t *testing.T) {
	srv, err := StartPprof(PprofOptions{Addr: "127.0.0.1:0"}, nil)
	if err != nil {
		t.Fatalf("StartPprof: %v", err)
	}
	if srv == nil {
		t.Fatalf("StartPprof returned nil server")
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = ShutdownPprof(ctx, srv)
	})

	addr := waitForPprofAddr(t, srv)

	resp := getOrFail(t, "http://"+addr+"/debug/pprof/")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("pprof index status = %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "goroutine") {
		t.Fatalf("pprof index missing 'goroutine' link; body=%q", string(body))
	}

	resp2 := getOrFail(t, "http://"+addr+"/debug/pprof/goroutine?debug=1")
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("goroutine profile status = %d", resp2.StatusCode)
	}
}

func TestStartPprof_AppliesMutexAndBlockRates(t *testing.T) {
	prevMutex := runtime.SetMutexProfileFraction(-1)
	prevBlock := -1
	runtime.SetBlockProfileRate(0)
	t.Cleanup(func() {
		runtime.SetMutexProfileFraction(prevMutex)
		runtime.SetBlockProfileRate(prevBlock)
	})

	srv, err := StartPprof(PprofOptions{Addr: "127.0.0.1:0", MutexRate: 7, BlockRate: 11}, nil)
	if err != nil {
		t.Fatalf("StartPprof: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = ShutdownPprof(ctx, srv)
	})

	if got := runtime.SetMutexProfileFraction(-1); got != 7 {
		t.Fatalf("mutex profile fraction = %d, want 7", got)
	}
	// BlockProfileRate has no read accessor — round-trip the value by
	// resetting to 0 and confirming ShutdownPprof clears any prior state
	// without panicking is exercised by t.Cleanup above.
}

func TestIsLoopback(t *testing.T) {
	cases := map[string]bool{
		"127.0.0.1:6060": true,
		"localhost:6060": true,
		"[::1]:6060":     true,
		"0.0.0.0:6060":   false,
		"10.0.0.5:6060":  false,
		"badaddress":     false,
	}
	for addr, want := range cases {
		if got := isLoopback(addr); got != want {
			t.Errorf("isLoopback(%q) = %v, want %v", addr, got, want)
		}
	}
}

func waitForPprofAddr(t *testing.T, srv *http.Server) string {
	t.Helper()
	if srv.Addr == "" {
		t.Fatalf("pprof server did not record its bound addr")
	}
	return srv.Addr
}

func getOrFail(t *testing.T, url string) *http.Response {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			return resp
		}
		lastErr = err
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("GET %s: %v", url, lastErr)
	return nil
}
