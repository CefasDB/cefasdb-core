package server_test

import (
	"context"
	"net"
	"strconv"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/CefasDb/cefasdb/internal/catalog"
	"github.com/CefasDb/cefasdb/internal/server"
	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	cefaspb "github.com/CefasDb/cefasdb/pkg/protocol"
)

// startAtomicFixture spins up a server that registers both the core
// Cefas surface (so we can create tables / put items) and the new
// CefasAtomic surface this test exercises. Mirrors
// startUnsecuredFixture but with the extra registration so the file
// fixture stays untouched.
func startAtomicFixture(t *testing.T) (cefaspb.CefasClient, cefaspb.CefasAtomicClient, func()) {
	t.Helper()
	dir := t.TempDir()
	db, err := pebble.Open(pebble.Options{Path: dir})
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	cat, _ := catalog.New(db)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gsrv := grpc.NewServer()
	core := server.NewGRPCServer(db, cat, nil)
	cefaspb.RegisterCefasServer(gsrv, core)
	server.RegisterAtomic(gsrv, core)
	go func() { _ = gsrv.Serve(ln) }()

	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return cefaspb.NewCefasClient(conn), cefaspb.NewCefasAtomicClient(conn), func() {
		_ = conn.Close()
		gsrv.GracefulStop()
		_ = db.Close()
	}
}

func TestAtomicIncrReturnRoundtrip(t *testing.T) {
	cefas, atomic, cleanup := startAtomicFixture(t)
	defer cleanup()
	ctx := context.Background()
	createTbl(t, cefas, "Counters")

	// First call lazily creates the row.
	resp, err := atomic.AtomicUpdate(ctx, &cefaspb.AtomicUpdateRequest{
		Table: "Counters",
		Key: map[string]*cefaspb.AttributeValue{
			"id": {Value: &cefaspb.AttributeValue_S{S: "page_views"}},
		},
		Actions: []*cefaspb.AtomicAction{{
			Kind:      cefaspb.AtomicActionKind_ATOMIC_INCR_RETURN,
			Attribute: "count",
			Value:     &cefaspb.AttributeValue{Value: &cefaspb.AttributeValue_N{N: "1"}},
		}},
	})
	if err != nil {
		t.Fatalf("first incr: %v", err)
	}
	if !resp.GetCreated() {
		t.Fatal("expected created=true on first call")
	}
	if got := resp.GetReturnedValues()[0].GetN(); got != "1" {
		t.Fatalf("returned[0] = %q, want \"1\"", got)
	}
	if got := resp.GetItem()["count"].GetN(); got != "1" {
		t.Fatalf("post-image count = %q, want \"1\"", got)
	}

	// Second call sees the prior value.
	resp, err = atomic.AtomicUpdate(ctx, &cefaspb.AtomicUpdateRequest{
		Table: "Counters",
		Key: map[string]*cefaspb.AttributeValue{
			"id": {Value: &cefaspb.AttributeValue_S{S: "page_views"}},
		},
		Actions: []*cefaspb.AtomicAction{{
			Kind:      cefaspb.AtomicActionKind_ATOMIC_INCR_RETURN,
			Attribute: "count",
			Value:     &cefaspb.AttributeValue{Value: &cefaspb.AttributeValue_N{N: "5"}},
		}},
	})
	if err != nil {
		t.Fatalf("second incr: %v", err)
	}
	if resp.GetCreated() {
		t.Fatal("created should be false on second call")
	}
	if got := resp.GetReturnedValues()[0].GetN(); got != "6" {
		t.Fatalf("returned[0] = %q, want \"6\"", got)
	}
}

func TestCounterColumnEnforcesAtomicIncrement(t *testing.T) {
	cefas, atomic, cleanup := startAtomicFixture(t)
	defer cleanup()
	ctx := context.Background()
	if _, err := cefas.CreateTable(ctx, &cefaspb.CreateTableRequest{
		Descriptor_: &cefaspb.TableDescriptor{
			Name:      "SchemaCounters",
			KeySchema: &cefaspb.KeySchema{Pk: "id"},
			AttributeDefinitions: []*cefaspb.AttributeDefinition{{
				Name: "count",
				Type: "COUNTER",
			}},
		},
	}); err != nil {
		t.Fatalf("create counter table: %v", err)
	}

	_, err := cefas.PutItem(ctx, &cefaspb.PutItemRequest{
		Table: "SchemaCounters",
		Item: map[string]*cefaspb.AttributeValue{
			"id":    {Value: &cefaspb.AttributeValue_S{S: "views"}},
			"count": {Value: &cefaspb.AttributeValue_N{N: "0"}},
		},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("PutItem counter code = %v, want InvalidArgument (%v)", status.Code(err), err)
	}

	resp, err := atomic.AtomicUpdate(ctx, &cefaspb.AtomicUpdateRequest{
		Table: "SchemaCounters",
		Key: map[string]*cefaspb.AttributeValue{
			"id": {Value: &cefaspb.AttributeValue_S{S: "views"}},
		},
		Actions: []*cefaspb.AtomicAction{{
			Kind:      cefaspb.AtomicActionKind_ATOMIC_INCR_RETURN,
			Attribute: "count",
			Value:     &cefaspb.AttributeValue{Value: &cefaspb.AttributeValue_N{N: "1"}},
		}},
	})
	if err != nil {
		t.Fatalf("AtomicUpdate counter increment: %v", err)
	}
	if got := resp.GetItem()["count"].GetN(); got != "1" {
		t.Fatalf("count = %q, want 1", got)
	}

	_, err = atomic.AtomicUpdate(ctx, &cefaspb.AtomicUpdateRequest{
		Table: "SchemaCounters",
		Key: map[string]*cefaspb.AttributeValue{
			"id": {Value: &cefaspb.AttributeValue_S{S: "views"}},
		},
		Actions: []*cefaspb.AtomicAction{{
			Kind:      cefaspb.AtomicActionKind_ATOMIC_SET,
			Attribute: "count",
			Value:     &cefaspb.AttributeValue{Value: &cefaspb.AttributeValue_N{N: "10"}},
		}},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("AtomicUpdate SET counter code = %v, want InvalidArgument (%v)", status.Code(err), err)
	}
}

func TestAtomicRequestIDDeduplicatesRetry(t *testing.T) {
	cefas, atomic, cleanup := startAtomicFixture(t)
	defer cleanup()
	ctx := context.Background()
	createTbl(t, cefas, "Dedup")

	req := &cefaspb.AtomicUpdateRequest{
		Table:     "Dedup",
		RequestId: "retry-1",
		Key: map[string]*cefaspb.AttributeValue{
			"id": {Value: &cefaspb.AttributeValue_S{S: "views"}},
		},
		Actions: []*cefaspb.AtomicAction{{
			Kind:      cefaspb.AtomicActionKind_ATOMIC_INCR_RETURN,
			Attribute: "count",
			Value:     &cefaspb.AttributeValue{Value: &cefaspb.AttributeValue_N{N: "1"}},
		}},
	}
	first, err := atomic.AtomicUpdate(ctx, req)
	if err != nil {
		t.Fatalf("first atomic update: %v", err)
	}
	if got := first.GetReturnedValues()[0].GetN(); got != "1" {
		t.Fatalf("first returned = %q, want 1", got)
	}

	req.Actions[0].Value = &cefaspb.AttributeValue{Value: &cefaspb.AttributeValue_N{N: "99"}}
	retry, err := atomic.AtomicUpdate(ctx, req)
	if err != nil {
		t.Fatalf("retry atomic update: %v", err)
	}
	if got := retry.GetReturnedValues()[0].GetN(); got != "1" {
		t.Fatalf("retry returned = %q, want cached 1", got)
	}

	got, err := cefas.GetItem(ctx, &cefaspb.GetItemRequest{
		Table: "Dedup",
		Key: map[string]*cefaspb.AttributeValue{
			"id": {Value: &cefaspb.AttributeValue_S{S: "views"}},
		},
	})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.GetItem()["count"].GetN() != "1" {
		t.Fatalf("stored count = %q, want 1", got.GetItem()["count"].GetN())
	}
}

// TestAtomicIncrContended is the linearizability check from the
// acceptance criteria: 10+ goroutines hammer a single key and the
// final value must equal the sum of every delta the server
// acknowledged. No retry loop on the client side.
func TestAtomicIncrContended(t *testing.T) {
	cefas, atomic, cleanup := startAtomicFixture(t)
	defer cleanup()
	ctx := context.Background()
	createTbl(t, cefas, "Hot")

	const (
		workers      = 16
		incrsPerWork = 64
	)
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < incrsPerWork; i++ {
				_, err := atomic.AtomicUpdate(ctx, &cefaspb.AtomicUpdateRequest{
					Table: "Hot",
					Key: map[string]*cefaspb.AttributeValue{
						"id": {Value: &cefaspb.AttributeValue_S{S: "k"}},
					},
					Actions: []*cefaspb.AtomicAction{{
						Kind:      cefaspb.AtomicActionKind_ATOMIC_INCR_RETURN,
						Attribute: "n",
						Value:     &cefaspb.AttributeValue{Value: &cefaspb.AttributeValue_N{N: "1"}},
					}},
				})
				if err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("contended incr: %v", err)
		}
	}

	got, err := cefas.GetItem(ctx, &cefaspb.GetItemRequest{
		Table: "Hot",
		Key: map[string]*cefaspb.AttributeValue{
			"id": {Value: &cefaspb.AttributeValue_S{S: "k"}},
		},
	})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	want := strconv.Itoa(workers * incrsPerWork)
	if n := got.GetItem()["n"].GetN(); n != want {
		t.Fatalf("post-image n = %q, want %q (no lost increments)", n, want)
	}
}

func TestAtomicApplyClamp(t *testing.T) {
	cefas, atomic, cleanup := startAtomicFixture(t)
	defer cleanup()
	ctx := context.Background()
	createTbl(t, cefas, "Bandit")

	// Seed an item — APPLY needs the attribute to exist.
	if _, err := cefas.PutItem(ctx, &cefaspb.PutItemRequest{
		Table: "Bandit",
		Item: map[string]*cefaspb.AttributeValue{
			"id":    {Value: &cefaspb.AttributeValue_S{S: "arm1"}},
			"alpha": {Value: &cefaspb.AttributeValue_N{N: "3"}},
			"beta":  {Value: &cefaspb.AttributeValue_N{N: "5"}},
		},
	}); err != nil {
		t.Fatalf("put: %v", err)
	}

	// Bandit posterior update: alpha += 1; beta = clamp(beta + 1 - 0, 0, 10).
	resp, err := atomic.AtomicUpdate(ctx, &cefaspb.AtomicUpdateRequest{
		Table: "Bandit",
		Key: map[string]*cefaspb.AttributeValue{
			"id": {Value: &cefaspb.AttributeValue_S{S: "arm1"}},
		},
		Actions: []*cefaspb.AtomicAction{
			{
				Kind:      cefaspb.AtomicActionKind_ATOMIC_INCR_RETURN,
				Attribute: "alpha",
				Value:     &cefaspb.AttributeValue{Value: &cefaspb.AttributeValue_N{N: "1"}},
			},
			{
				Kind:       cefaspb.AtomicActionKind_ATOMIC_APPLY,
				Attribute:  "beta",
				Expression: "clamp(beta + 1, 0, 10)",
			},
		},
	})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if got := resp.GetReturnedValues()[0].GetN(); got != "4" {
		t.Fatalf("alpha new = %q, want \"4\"", got)
	}
	if got := resp.GetReturnedValues()[1].GetN(); got != "6" {
		t.Fatalf("beta new = %q, want \"6\"", got)
	}
	// Saturation: pushing beta past 10 should clamp.
	resp, err = atomic.AtomicUpdate(ctx, &cefaspb.AtomicUpdateRequest{
		Table: "Bandit",
		Key: map[string]*cefaspb.AttributeValue{
			"id": {Value: &cefaspb.AttributeValue_S{S: "arm1"}},
		},
		Actions: []*cefaspb.AtomicAction{{
			Kind:       cefaspb.AtomicActionKind_ATOMIC_APPLY,
			Attribute:  "beta",
			Expression: "clamp(beta + 100, 0, 10)",
		}},
	})
	if err != nil {
		t.Fatalf("clamp: %v", err)
	}
	if got := resp.GetReturnedValues()[0].GetN(); got != "10" {
		t.Fatalf("clamped beta = %q, want \"10\"", got)
	}
}

func TestAtomicConditionExpressionGatesWrite(t *testing.T) {
	cefas, atomic, cleanup := startAtomicFixture(t)
	defer cleanup()
	ctx := context.Background()
	createTbl(t, cefas, "Gated")
	if _, err := cefas.PutItem(ctx, &cefaspb.PutItemRequest{
		Table: "Gated",
		Item: map[string]*cefaspb.AttributeValue{
			"id":      {Value: &cefaspb.AttributeValue_S{S: "row"}},
			"version": {Value: &cefaspb.AttributeValue_N{N: "1"}},
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// Wrong precondition — should fail.
	_, err := atomic.AtomicUpdate(ctx, &cefaspb.AtomicUpdateRequest{
		Table: "Gated",
		Key: map[string]*cefaspb.AttributeValue{
			"id": {Value: &cefaspb.AttributeValue_S{S: "row"}},
		},
		Condition: "version = :expected",
		Binds: map[string]*cefaspb.AttributeValue{
			"expected": {Value: &cefaspb.AttributeValue_N{N: "99"}},
		},
		Actions: []*cefaspb.AtomicAction{{
			Kind:      cefaspb.AtomicActionKind_ATOMIC_INCR_RETURN,
			Attribute: "version",
			Value:     &cefaspb.AttributeValue{Value: &cefaspb.AttributeValue_N{N: "1"}},
		}},
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("want FailedPrecondition, got %v", err)
	}
	// Correct precondition — should succeed.
	resp, err := atomic.AtomicUpdate(ctx, &cefaspb.AtomicUpdateRequest{
		Table: "Gated",
		Key: map[string]*cefaspb.AttributeValue{
			"id": {Value: &cefaspb.AttributeValue_S{S: "row"}},
		},
		Condition: "version = :expected",
		Binds: map[string]*cefaspb.AttributeValue{
			"expected": {Value: &cefaspb.AttributeValue_N{N: "1"}},
		},
		Actions: []*cefaspb.AtomicAction{{
			Kind:      cefaspb.AtomicActionKind_ATOMIC_INCR_RETURN,
			Attribute: "version",
			Value:     &cefaspb.AttributeValue{Value: &cefaspb.AttributeValue_N{N: "1"}},
		}},
	})
	if err != nil {
		t.Fatalf("expected success: %v", err)
	}
	if got := resp.GetReturnedValues()[0].GetN(); got != "2" {
		t.Fatalf("version after CAS = %q, want \"2\"", got)
	}
}
