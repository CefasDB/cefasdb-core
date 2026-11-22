// Package storetest provides a reusable RunStore conformance suite.
package storetest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/CefasDb/cefasdb/internal/recovery/state"
)

type Driver struct {
	Open      func(testing.TB, string) (state.RunStore, func() state.RunStore)
	Token     func(scope, after string, version byte) string
	IsDurable bool
}

func digest(s string) string {
	sum := sha256.Sum256([]byte(s))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func copyRun(in state.RecoveryRun) state.RecoveryRun {
	out := in
	out.StepStates = make(map[string]state.StepState, len(in.StepStates))
	for k, v := range in.StepStates {
		out.StepStates[k] = v
	}
	return out
}

func run(id string, at time.Time) state.RecoveryRun {
	return state.RecoveryRun{
		RunID: id, PayloadSHA256: digest("payload-" + id), ScopeID: "scope-a", Revision: 1,
		State: "created", StepStates: map[string]state.StepState{}, CreatedAt: at, UpdatedAt: at,
	}
}

// SampleRun returns a valid revision-one run for adapter-specific tests.
func SampleRun(id string, at time.Time) state.RecoveryRun { return run(id, at) }

func want(t *testing.T, err, target error) {
	t.Helper()
	if !errors.Is(err, target) {
		t.Fatalf("error = %v, want %v", err, target)
	}
}

// Run executes the complete adapter-neutral contract.
func Run(t *testing.T, driver Driver) {
	t.Helper()
	at := time.Date(2026, 11, 22, 3, 0, 0, 0, time.UTC)
	store, reopen := driver.Open(t, "scope-a")
	if store.Durable() != driver.IsDurable {
		t.Fatalf("Durable() = %t, want %t", store.Durable(), driver.IsDurable)
	}

	first := run("create", at)
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
	other := copyRun(first)
	other.PayloadSHA256 = digest("different")
	_, err = store.Create(context.Background(), other)
	want(t, err, state.ErrRunPayloadConflict)
	stored, _ := store.Get(context.Background(), "scope-a", first.RunID)
	if !reflect.DeepEqual(stored, got) {
		t.Fatal("payload conflict mutated first run")
	}

	race := run("race", at)
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
	idem, err := store.CompareAndSwap(context.Background(), "scope-a", race.RunID, 4, winner.next)
	if err != nil || !reflect.DeepEqual(idem, winner.got) {
		t.Fatalf("winning retry = %#v, %v", idem, err)
	}

	restart := run("restart", at)
	restart.StepStates["restore.apply"] = state.StepState{StepID: "restore.apply", RequestSHA256: digest("restore"), Status: state.StepStatusUnknown, ExternalRef: "restore-7", Attempt: 1}
	if _, err = store.Create(context.Background(), restart); err != nil {
		t.Fatal(err)
	}
	reloaded, err := reopen().Get(context.Background(), "scope-a", restart.RunID)
	if err != nil || reloaded.StepStates["restore.apply"].Status != state.StepStatusUnknown || reloaded.StepStates["restore.apply"].ExternalRef != "restore-7" {
		t.Fatalf("reopen = %#v, %v", reloaded, err)
	}

	mutable := run("copy", at)
	mutable.StepStates["x"] = state.StepState{StepID: "x", RequestSHA256: digest("x"), Status: state.StepStatusStarted, Attempt: 1}
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

	denied := run("denied", at)
	denied.ScopeID = "scope-b"
	_, err = store.Create(context.Background(), denied)
	want(t, err, state.ErrScopeDenied)
	_, err = store.Get(context.Background(), "scope-b", "create")
	want(t, err, state.ErrScopeDenied)
	_, err = store.CompareAndSwap(context.Background(), "scope-b", "race", 5, winner.next)
	want(t, err, state.ErrScopeDenied)
	_, err = store.List(context.Background(), "scope-b", 0, "secret")
	want(t, err, state.ErrScopeDenied)

	for _, size := range []int{-1, 0, 101} {
		_, err = store.List(context.Background(), "scope-a", size, "")
		want(t, err, state.ErrInvalidPageSize)
	}
	one, err := store.List(context.Background(), "scope-a", 1, "")
	if err != nil || len(one.Items) != 1 || one.NextPageToken == "" {
		t.Fatalf("size 1 = %#v, %v", one, err)
	}
	if page, e := store.List(context.Background(), "scope-a", 100, ""); e != nil || len(page.Items) < 4 {
		t.Fatalf("size 100 = %#v, %v", page, e)
	}
	for _, token := range []string{"!", driver.Token("scope-a", "create", 2), driver.Token("scope-b", "create", 1)} {
		_, err = store.List(context.Background(), "scope-a", 1, token)
		want(t, err, state.ErrInvalidPageToken)
	}
	altered := one.NextPageToken[:len(one.NextPageToken)-1] + "A"
	if altered == one.NextPageToken {
		altered = one.NextPageToken[:len(one.NextPageToken)-1] + "B"
	}
	_, err = store.List(context.Background(), "scope-a", 1, altered)
	want(t, err, state.ErrInvalidPageToken)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cancelled := run("cancelled", at)
	_, err = store.Create(ctx, cancelled)
	want(t, err, context.Canceled)
	_, err = store.Get(context.Background(), "scope-a", "cancelled")
	want(t, err, state.ErrRunNotFound)
}
