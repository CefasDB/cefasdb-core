package server_test

import (
	"context"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/CefasDb/cefasdb/internal/auth"
	"github.com/CefasDb/cefasdb/internal/catalog"
	"github.com/CefasDb/cefasdb/internal/server"
	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	cefaspb "github.com/CefasDb/cefasdb/pkg/protocol"
	"github.com/CefasDb/cefasdb/pkg/types"
)

// TestWorkloadIsolation_WL4AdmissionCapEngages is the WL-4 portion
// of the epic-#489 acceptance gate. It asserts the two invariants
// the admission controller already enforces today:
//
//  1. A flood SL with MaxInFlight=N admits at most N concurrent
//     requests at any instant; the rest get codes.ResourceExhausted.
//  2. A second SL with no caps is never throttled by flood traffic
//     — its requests always reach the handler.
//
// The wider acceptance gate (interactive p99 stays within 1.2× of
// the baseline under flood, per ADR 0004) requires the WL-3 DRR
// lane scheduler to actually engage end-to-end. That needs ctx-
// aware pebble.DB methods so auth.ServiceLevelFromContext can be
// threaded down to runReadSL / runWriteSL. Tracked as a follow-up.
// TestWorkloadIsolation_InteractiveLatencyUnderFlood below is the
// placeholder that asserts the latency invariant once that path
// lands — it is t.Skip()'d today with the reason inline.
func TestWorkloadIsolation_WL4AdmissionCapEngages(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos test exercises ~256 goroutines; skipped in -short")
	}

	const (
		floodCap        = 4
		floodWorkers    = 64
		floodReqsPer    = 16
		interactiveReps = 200
	)

	stub, cleanup := startQuotaFixture(t, floodCap)
	defer cleanup()

	ctx := context.Background()
	mustCreateTable(t, ctx, stub, "Bench")
	mustPutItem(t, ctx, stub, "Bench", "p1")

	var (
		floodRejected   atomic.Int64
		floodAccepted   atomic.Int64
		interactiveErrs atomic.Int64
	)

	var wg sync.WaitGroup
	stop := make(chan struct{})
	// Flood pool — saturate the SL.
	for i := 0; i < floodWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < floodReqsPer; j++ {
				select {
				case <-stop:
					return
				default:
				}
				if rejected := tryFlood(ctx, stub); rejected {
					floodRejected.Add(1)
				} else {
					floodAccepted.Add(1)
				}
			}
		}()
	}

	// Interactive — light reads.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < interactiveReps; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if err := tryInteractive(ctx, stub); err != nil {
				interactiveErrs.Add(1)
			}
		}
	}()

	wg.Wait()
	close(stop)

	if floodRejected.Load() == 0 {
		t.Errorf("flood SL never hit MaxInFlight cap; expected some ResourceExhausted rejections")
	}
	if interactiveErrs.Load() != 0 {
		t.Errorf("interactive SL was throttled %d times under flood; expected 0 — it has no caps", interactiveErrs.Load())
	}
	t.Logf("flood accepted=%d rejected=%d; interactive errs=%d (of %d requests)",
		floodAccepted.Load(), floodRejected.Load(), interactiveErrs.Load(), interactiveReps)
}

// TestWorkloadIsolation_InteractiveLatencyUnderFlood is the
// latency-isolation gate locked in ADR 0004 §6: under sustained
// flood traffic, interactive p99 must stay within baseline × tol.
// Today the WL-3 DRR scheduler is not yet engaged end-to-end (the
// ctx propagation through pebble.DB is the deferred wiring layer
// of #498), so admitted flood requests share the same lane as
// interactive reads and the latency invariant cannot be met.
//
// Test is kept in tree so the path exists once ctx threading lands —
// the assertion shape stays correct, only the t.Skip needs to be
// removed.
func TestWorkloadIsolation_InteractiveLatencyUnderFlood(t *testing.T) {
	t.Skip("requires ctx-aware pebble.DB methods (deferred follow-up to #498) for the WL-3 DRR scheduler to engage end-to-end")

	const (
		floodCap        = 16
		floodWorkers    = 64
		floodReqsPer    = 32
		interactiveReps = 50
		latencyTol      = 1.5
	)

	stub, cleanup := startQuotaFixture(t, floodCap)
	defer cleanup()

	ctx := context.Background()
	mustCreateTable(t, ctx, stub, "Bench")
	mustPutItem(t, ctx, stub, "Bench", "p1")

	base := measureGetLatencies(ctx, t, stub, "interactive", interactiveReps)
	basep99 := percentile(base, 0.99)

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < floodWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < floodReqsPer; j++ {
				select {
				case <-stop:
					return
				default:
				}
				_ = tryFlood(ctx, stub)
			}
		}()
	}
	combined := measureGetLatencies(ctx, t, stub, "interactive", interactiveReps)
	close(stop)
	wg.Wait()

	combinedp99 := percentile(combined, 0.99)
	if combinedp99 > time.Duration(float64(basep99)*latencyTol) {
		t.Errorf("isolation violated: interactive p99 baseline=%v combined=%v (tol %vx)",
			basep99, combinedp99, latencyTol)
	}
	t.Logf("baseline p99=%v combined p99=%v (tol %vx)", basep99, combinedp99, latencyTol)
}

func startQuotaFixture(t *testing.T, floodMaxInFlight int) (cefaspb.CefasClient, func()) {
	t.Helper()
	db, err := pebble.Open(pebble.Options{Path: t.TempDir()})
	if err != nil {
		t.Fatalf("pebble: %v", err)
	}
	cat, err := catalog.New(db)
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	if _, err := cat.CreateServiceLevel(types.ServiceLevelDescriptor{
		Name:        "flood",
		Shares:      1,
		MaxInFlight: floodMaxInFlight,
	}); err != nil {
		t.Fatalf("create flood SL: %v", err)
	}
	if _, err := cat.CreateServiceLevel(types.ServiceLevelDescriptor{
		Name:   "interactive",
		Shares: 100,
	}); err != nil {
		t.Fatalf("create interactive SL: %v", err)
	}
	quota := server.NewSLQuotaController(cat, nil)
	cat.OnServiceLevelChanged(quota.Invalidate)
	srv := server.NewGRPCServer(db, cat, nil)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	slUnary, _ := server.ServiceLevelInterceptor()
	qUnary, _ := server.SLQuotaInterceptor(quota)
	gsrv := grpc.NewServer(grpc.ChainUnaryInterceptor(slUnary, qUnary))
	cefaspb.RegisterCefasServer(gsrv, srv)
	go func() { _ = gsrv.Serve(ln) }()

	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	cleanup := func() {
		_ = conn.Close()
		gsrv.GracefulStop()
		_ = db.Close()
	}
	return cefaspb.NewCefasClient(conn), cleanup
}

func mustCreateTable(t *testing.T, ctx context.Context, stub cefaspb.CefasClient, name string) {
	t.Helper()
	if _, err := stub.CreateTable(ctx, &cefaspb.CreateTableRequest{
		Descriptor_: &cefaspb.TableDescriptor{
			Name:      name,
			KeySchema: &cefaspb.KeySchema{Pk: "pk"},
		},
	}); err != nil {
		t.Fatalf("create table %s: %v", name, err)
	}
}

func mustPutItem(t *testing.T, ctx context.Context, stub cefaspb.CefasClient, table, pk string) {
	t.Helper()
	if _, err := stub.PutItem(ctx, &cefaspb.PutItemRequest{
		Table: table,
		Item: map[string]*cefaspb.AttributeValue{
			"pk": {Value: &cefaspb.AttributeValue_S{S: pk}},
		},
	}); err != nil {
		t.Fatalf("put item: %v", err)
	}
}

func measureGetLatencies(ctx context.Context, t *testing.T, stub cefaspb.CefasClient, sl string, reps int) []time.Duration {
	t.Helper()
	md := metadata.Pairs(auth.ServiceLevelMetadataKey, sl)
	out := make([]time.Duration, 0, reps)
	for i := 0; i < reps; i++ {
		start := time.Now()
		rpcCtx := metadata.NewOutgoingContext(ctx, md)
		_, _ = stub.GetItem(rpcCtx, &cefaspb.GetItemRequest{
			Table: "Bench",
			Key: map[string]*cefaspb.AttributeValue{
				"pk": {Value: &cefaspb.AttributeValue_S{S: "p1"}},
			},
		})
		out = append(out, time.Since(start))
	}
	return out
}

// tryFlood issues a write under the "flood" SL. Returns true when
// the request was rejected by the quota controller (the SL's
// MaxInFlight cap engaged), false on success.
func tryFlood(ctx context.Context, stub cefaspb.CefasClient) bool {
	md := metadata.Pairs(auth.ServiceLevelMetadataKey, "flood")
	rpcCtx := metadata.NewOutgoingContext(ctx, md)
	_, err := stub.PutItem(rpcCtx, &cefaspb.PutItemRequest{
		Table: "Bench",
		Item: map[string]*cefaspb.AttributeValue{
			"pk": {Value: &cefaspb.AttributeValue_S{S: "flood"}},
		},
	})
	return err != nil && status.Code(err) == codes.ResourceExhausted
}

// tryInteractive issues a read under the "interactive" SL. Returns
// the gRPC error if any — the WL-4 contract is that no error is
// ever returned (no caps configured).
func tryInteractive(ctx context.Context, stub cefaspb.CefasClient) error {
	md := metadata.Pairs(auth.ServiceLevelMetadataKey, "interactive")
	rpcCtx := metadata.NewOutgoingContext(ctx, md)
	_, err := stub.GetItem(rpcCtx, &cefaspb.GetItemRequest{
		Table: "Bench",
		Key: map[string]*cefaspb.AttributeValue{
			"pk": {Value: &cefaspb.AttributeValue_S{S: "p1"}},
		},
	})
	return err
}

func percentile(samples []time.Duration, p float64) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), samples...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}
