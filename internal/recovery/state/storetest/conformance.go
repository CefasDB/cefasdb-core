// Package storetest provides a reusable RunStore conformance suite.
package storetest

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/CefasDb/cefasdb/internal/recovery/state"
)

// ScopeOpener opens scope-authorized handles over one shared backing store.
type ScopeOpener func(string) (state.RunStore, func() state.RunStore)

// Driver supplies a RunStore implementation to the conformance suite.
type Driver struct {
	New       func(testing.TB) ScopeOpener
	Token     func(scope, after string, version byte) string
	IsDurable bool
}

type createResult struct {
	input state.RecoveryRun
	got   state.RecoveryRun
	err   error
}

func payloadDigest(t testing.TB, value string) string {
	t.Helper()
	_, result, err := state.CanonicalPayload([]byte(`{"value":` + strconv.Quote(value) + `}`))
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func requestDigest(t testing.TB, value string) string {
	t.Helper()
	_, result, err := state.CanonicalRequest([]byte(`{"value":` + strconv.Quote(value) + `}`))
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func copyRun(in state.RecoveryRun) state.RecoveryRun {
	out := in
	out.StepStates = make(map[string]state.StepState, len(in.StepStates))
	for k, v := range in.StepStates {
		out.StepStates[k] = v
	}
	return out
}

func run(t testing.TB, id string, at time.Time) state.RecoveryRun {
	t.Helper()
	return state.RecoveryRun{
		RunID: id, PayloadSHA256: payloadDigest(t, "payload-"+id), ScopeID: "scope-a", Revision: 1,
		State: "created", StepStates: map[string]state.StepState{}, CreatedAt: at, UpdatedAt: at,
	}
}

// SampleRun returns a valid revision-one run for adapter-specific tests.
func SampleRun(t testing.TB, id string, at time.Time) state.RecoveryRun {
	t.Helper()
	return run(t, id, at)
}

func want(t *testing.T, err, target error) {
	t.Helper()
	if !errors.Is(err, target) {
		t.Fatalf("error = %v, want %v", err, target)
	}
}

func concurrentCreate(store state.RunStore, inputs ...state.RecoveryRun) <-chan createResult {
	start := make(chan struct{})
	results := make(chan createResult, len(inputs))
	var group sync.WaitGroup
	for _, input := range inputs {
		group.Add(1)
		// Each writer exits after one Create call; the WaitGroup closes results after every writer exits.
		go func(candidate state.RecoveryRun) {
			defer group.Done()
			<-start
			got, err := store.Create(context.Background(), candidate)
			results <- createResult{input: candidate, got: got, err: err}
		}(input)
	}
	close(start)
	group.Wait()
	close(results)
	return results
}

func verifyConcurrentCreate(t *testing.T, store state.RunStore, at time.Time) {
	t.Helper()
	left := run(t, "create-race", at)
	right := copyRun(left)
	right.PayloadSHA256 = payloadDigest(t, "different-create-race-payload")
	wins, conflicts := 0, 0
	var winner createResult
	for outcome := range concurrentCreate(store, left, right) {
		switch {
		case outcome.err == nil:
			wins++
			winner = outcome
		case errors.Is(outcome.err, state.ErrRunPayloadConflict):
			conflicts++
		default:
			t.Fatal(outcome.err)
		}
	}
	committed, err := store.Get(context.Background(), "scope-a", left.RunID)
	if err != nil || wins != 1 || conflicts != 1 || !reflect.DeepEqual(committed, winner.got) ||
		winner.input.PayloadSHA256 != winner.got.PayloadSHA256 {
		t.Fatalf("concurrent Create wins=%d conflicts=%d committed=%#v err=%v", wins, conflicts, committed, err)
	}
}

func verifyScopeIsolation(t *testing.T, driver Driver, at time.Time) {
	t.Helper()
	open := driver.New(t)
	scopeA, _ := open("scope-a")
	scopeB, _ := open("scope-b")
	left := run(t, "shared-run", at)
	right := copyRun(left)
	right.ScopeID = "scope-b"

	leftStored, leftErr := scopeA.Create(context.Background(), left)
	rightStored, rightErr := scopeB.Create(context.Background(), right)
	if leftErr != nil || rightErr != nil || !reflect.DeepEqual(leftStored, left) || !reflect.DeepEqual(rightStored, right) {
		t.Fatalf("scope-qualified creates = %#v, %v; %#v, %v", leftStored, leftErr, rightStored, rightErr)
	}
	leftStored, leftErr = scopeA.Get(context.Background(), "scope-a", left.RunID)
	rightStored, rightErr = scopeB.Get(context.Background(), "scope-b", right.RunID)
	if leftErr != nil || rightErr != nil || !reflect.DeepEqual(leftStored, left) || !reflect.DeepEqual(rightStored, right) {
		t.Fatalf("scope-qualified runs = %#v, %v; %#v, %v", leftStored, leftErr, rightStored, rightErr)
	}
	leftPage, leftErr := scopeA.List(context.Background(), "scope-a", 100, "")
	rightPage, rightErr := scopeB.List(context.Background(), "scope-b", 100, "")
	if leftErr != nil || rightErr != nil || len(leftPage.Items) != 1 || len(rightPage.Items) != 1 {
		t.Fatalf("scope-qualified lists = %#v, %v; %#v, %v", leftPage, leftErr, rightPage, rightErr)
	}
}

// Run executes the complete adapter-neutral contract.
func Run(t *testing.T, driver Driver) {
	t.Helper()
	at := time.Date(2026, 11, 22, 3, 0, 0, 0, time.UTC)
	open := driver.New(t)
	store, reopen := open("scope-a")
	if store.Durable() != driver.IsDurable {
		t.Fatalf("Durable() = %t, want %t", store.Durable(), driver.IsDurable)
	}
	slashStore, _ := open("scope/a")
	slashScoped := run(t, "slash-scope", at)
	slashScoped.ScopeID = "scope/a"
	if _, err := slashStore.Create(context.Background(), slashScoped); err != nil {
		t.Fatalf("canonical slash scope was rejected: %v", err)
	}

	first := run(t, "create", at)
	got, err := store.Create(context.Background(), first)
	if err != nil {
		t.Fatal(err)
	}
	retry := copyRun(first)
	retry.State = "ignored"
	retry.UpdatedAt = at.Add(time.Second)
	again, err := store.Create(context.Background(), retry)
	if err != nil || !reflect.DeepEqual(got, again) {
		t.Fatalf("same-payload retry = %#v, %v", again, err)
	}
	afterReopen, err := reopen().Create(context.Background(), retry)
	if err != nil || !reflect.DeepEqual(got, afterReopen) {
		t.Fatalf("Create retry after reopen = %#v, %v; want %#v", afterReopen, err, got)
	}
	other := copyRun(first)
	other.PayloadSHA256 = payloadDigest(t, "different")
	_, err = store.Create(context.Background(), other)
	want(t, err, state.ErrRunPayloadConflict)
	stored, _ := store.Get(context.Background(), "scope-a", first.RunID)
	if !reflect.DeepEqual(stored, got) {
		t.Fatal("payload conflict mutated first run")
	}
	verifyConcurrentCreate(t, store, at)

	race := run(t, "race", at)
	if _, err = store.Create(context.Background(), race); err != nil {
		t.Fatal(err)
	}
	for revision := uint64(1); revision < 4; revision++ {
		next := copyRun(race)
		next.Revision = revision + 1
		next.UpdatedAt = at.Add(time.Duration(revision) * time.Second)
		race, err = store.CompareAndSwap(context.Background(), "scope-a", race.RunID, revision, next)
		if err != nil {
			t.Fatal(err)
		}
	}
	left, right := copyRun(race), copyRun(race)
	left.Revision, right.Revision = 5, 5
	left.State, right.State = "left", "right"
	left.UpdatedAt, right.UpdatedAt = at.Add(5*time.Second), at.Add(5*time.Second)
	type result struct {
		next state.RecoveryRun
		got  state.RecoveryRun
		err  error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	var wg sync.WaitGroup
	for _, next := range []state.RecoveryRun{left, right} {
		wg.Add(1)
		// Each writer exits after one CompareAndSwap call; the WaitGroup closes results after both writers exit.
		go func(n state.RecoveryRun) {
			defer wg.Done()
			<-start
			value, callErr := store.CompareAndSwap(context.Background(), "scope-a", race.RunID, 4, n)
			results <- result{next: n, got: value, err: callErr}
		}(next)
	}
	close(start)
	wg.Wait()
	close(results)
	var winner result
	wins, conflicts := 0, 0
	for result := range results {
		if result.err == nil {
			wins++
			winner = result
		} else if errors.Is(result.err, state.ErrRevisionConflict) {
			conflicts++
		} else {
			t.Fatal(result.err)
		}
	}
	if wins != 1 || conflicts != 1 || winner.got.Revision != 5 {
		t.Fatalf("wins=%d conflicts=%d", wins, conflicts)
	}
	equivalentRetry := copyRun(winner.next)
	zeroOffset := time.FixedZone("zero-offset", 0)
	equivalentRetry.CreatedAt = equivalentRetry.CreatedAt.In(zeroOffset)
	equivalentRetry.UpdatedAt = equivalentRetry.UpdatedAt.In(zeroOffset)
	idem, err := store.CompareAndSwap(context.Background(), "scope-a", race.RunID, 4, equivalentRetry)
	if err != nil || !reflect.DeepEqual(idem, winner.got) {
		t.Fatalf("winning retry = %#v, %v", idem, err)
	}
	reopenedCAS := reopen()
	retriedCAS, err := reopenedCAS.CompareAndSwap(context.Background(), "scope-a", race.RunID, 4, equivalentRetry)
	if err != nil || !reflect.DeepEqual(retriedCAS, winner.got) {
		t.Fatalf("CompareAndSwap retry after reopen = %#v, %v; want %#v", retriedCAS, err, winner.got)
	}
	reloadedCAS, err := reopenedCAS.Get(context.Background(), "scope-a", race.RunID)
	if err != nil || !reflect.DeepEqual(reloadedCAS, winner.got) {
		t.Fatalf("acknowledged CAS after reopen = %#v, %v; want %#v", reloadedCAS, err, winner.got)
	}

	restart := run(t, "restart", at)
	restart.StepStates["restore.apply"] = state.StepState{StepID: "restore.apply", RequestSHA256: requestDigest(t, "restore"), Status: state.StepStatusStarted, Attempt: 1}
	if _, err = store.Create(context.Background(), restart); err != nil {
		t.Fatal(err)
	}
	restartNext := copyRun(restart)
	restartNext.Revision = 2
	restartNext.UpdatedAt = at.Add(time.Second)
	restartNext.StepStates["restore.apply"] = state.StepState{StepID: "restore.apply", RequestSHA256: requestDigest(t, "restore"), Status: state.StepStatusUnknown, ExternalRef: "restore-7", Attempt: 1}
	if _, err = store.CompareAndSwap(context.Background(), "scope-a", restart.RunID, 1, restartNext); err != nil {
		t.Fatal(err)
	}
	reloaded, err := reopen().Get(context.Background(), "scope-a", restart.RunID)
	if err != nil || reloaded.StepStates["restore.apply"].Status != state.StepStatusUnknown || reloaded.StepStates["restore.apply"].ExternalRef != "restore-7" {
		t.Fatalf("reopen = %#v, %v", reloaded, err)
	}

	mutable := run(t, "copy", at)
	mutable.StepStates["x"] = state.StepState{StepID: "x", RequestSHA256: requestDigest(t, "x"), Status: state.StepStatusStarted, Attempt: 1}
	if _, err = store.Create(context.Background(), mutable); err != nil {
		t.Fatal(err)
	}
	mutable.StepStates["x"] = state.StepState{}
	read, _ := store.Get(context.Background(), "scope-a", "copy")
	read.StepStates["x"] = state.StepState{}
	read, _ = store.Get(context.Background(), "scope-a", "copy")
	if read.StepStates["x"].Status != state.StepStatusStarted {
		t.Fatal("caller mutation changed committed state")
	}

	denied := run(t, "denied", at)
	denied.ScopeID = "scope-b"
	_, err = store.Create(context.Background(), denied)
	want(t, err, state.ErrScopeDenied)
	for _, runID := range []string{"create", "missing"} {
		_, err = store.Get(context.Background(), "scope-b", runID)
		want(t, err, state.ErrScopeDenied)
	}
	_, err = store.CompareAndSwap(context.Background(), "scope-b", "race", 5, winner.next)
	want(t, err, state.ErrScopeDenied)
	crossScopeNext := copyRun(winner.next)
	crossScopeNext.ScopeID = "scope-b"
	_, err = store.CompareAndSwap(context.Background(), "scope-a", "missing", 5, crossScopeNext)
	want(t, err, state.ErrScopeDenied)
	_, err = store.List(context.Background(), "scope-b", 0, "secret")
	want(t, err, state.ErrScopeDenied)
	verifyScopeIsolation(t, driver, at)

	for _, size := range []int{-1, 0, 101} {
		_, err = store.List(context.Background(), "scope-a", size, "")
		want(t, err, state.ErrInvalidPageSize)
	}
	one, err := store.List(context.Background(), "scope-a", 1, "")
	if err != nil || len(one.Items) != 1 || one.NextPageToken == "" {
		t.Fatalf("size 1 = %#v, %v", one, err)
	}
	firstPageRunID := one.Items[0].RunID
	one.Items[0].StepStates["retained-mutation"] = state.StepState{StepID: "retained-mutation"}
	unchanged, err := store.Get(context.Background(), "scope-a", firstPageRunID)
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := unchanged.StepStates["retained-mutation"]; exists {
		t.Fatal("page mutation changed committed state")
	}
	two, err := store.List(context.Background(), "scope-a", 1, one.NextPageToken)
	if err != nil || len(two.Items) != 1 || two.Items[0].RunID <= firstPageRunID {
		t.Fatalf("second page = %#v, %v", two, err)
	}
	if page, e := store.List(context.Background(), "scope-a", 100, ""); e != nil || len(page.Items) < 4 {
		t.Fatalf("size 100 = %#v, %v", page, e)
	}
	for _, token := range []string{"!", driver.Token("scope-a", "create", 2), driver.Token("scope-b", "create", 1)} {
		page, listErr := store.List(context.Background(), "scope-a", 1, token)
		want(t, listErr, state.ErrInvalidPageToken)
		if !reflect.DeepEqual(page, state.RunPage{}) {
			t.Fatalf("invalid token returned partial page: %#v", page)
		}
	}
	altered := one.NextPageToken[:len(one.NextPageToken)-1] + "A"
	if altered == one.NextPageToken {
		altered = one.NextPageToken[:len(one.NextPageToken)-1] + "B"
	}
	_, err = store.List(context.Background(), "scope-a", 1, altered)
	want(t, err, state.ErrInvalidPageToken)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cancelled := run(t, "cancelled", at)
	_, err = store.Create(ctx, cancelled)
	want(t, err, context.Canceled)
	_, err = store.Get(context.Background(), "scope-a", "cancelled")
	want(t, err, state.ErrRunNotFound)
	if got, callErr := store.Get(ctx, "scope-a", "create"); !reflect.DeepEqual(got, state.RecoveryRun{}) {
		t.Fatalf("cancelled Get returned partial run: %#v", got)
	} else {
		want(t, callErr, context.Canceled)
	}
	if got, callErr := store.CompareAndSwap(ctx, "scope-a", "race", 4, winner.next); !reflect.DeepEqual(got, state.RecoveryRun{}) {
		t.Fatalf("cancelled CompareAndSwap returned partial run: %#v", got)
	} else {
		want(t, callErr, context.Canceled)
	}
	if page, callErr := store.List(ctx, "scope-a", 1, ""); !reflect.DeepEqual(page, state.RunPage{}) {
		t.Fatalf("cancelled List returned partial page: %#v", page)
	} else {
		want(t, callErr, context.Canceled)
	}
}
