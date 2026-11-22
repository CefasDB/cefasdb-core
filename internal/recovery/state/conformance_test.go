package state_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/CefasDb/cefasdb/internal/recovery/state"
	"github.com/CefasDb/cefasdb/internal/recovery/state/storetest"
)

func driver() storetest.Driver {
	key := []byte("state-test-cursor-key")
	return storetest.Driver{
		Open: func(_ testing.TB, scope string) (state.RunStore, func() state.RunStore) {
			store, reopen := newMemory(scope)
			return store, reopen
		},
		Token: func(scope, after string, version byte) string {
			return state.EncodePageToken(scope, after, key, version)
		},
	}
}

func TestConformance(t *testing.T) { storetest.Run(t, driver()) }

func TestValidationAndErrors(t *testing.T) {
	at := time.Date(2026, 11, 22, 3, 0, 0, 0, time.UTC)
	base := storetest.SampleRun("valid", at)
	cases := []func(*state.RecoveryRun){
		func(r *state.RecoveryRun) { r.ScopeID = "" },
		func(r *state.RecoveryRun) { r.RunID = "" },
		func(r *state.RecoveryRun) { r.PayloadSHA256 = "bad" },
		func(r *state.RecoveryRun) { r.Revision = 0 },
		func(r *state.RecoveryRun) { r.State = "" },
		func(r *state.RecoveryRun) { r.State = strings.Repeat("x", 4097) },
		func(r *state.RecoveryRun) { r.CreatedAt = time.Time{} },
		func(r *state.RecoveryRun) { r.UpdatedAt = at.Add(-time.Second) },
		func(r *state.RecoveryRun) {
			r.StepStates = map[string]state.StepState{"bad": {StepID: "other", RequestSHA256: base.PayloadSHA256, Status: state.StepStatusStarted, Attempt: 1}}
		},
		func(r *state.RecoveryRun) {
			r.StepStates = map[string]state.StepState{"step": {StepID: "step", RequestSHA256: "bad", Status: state.StepStatusStarted, Attempt: 1}}
		},
		func(r *state.RecoveryRun) {
			r.StepStates = map[string]state.StepState{"step": {StepID: "step", RequestSHA256: base.PayloadSHA256, Status: state.StepStatusNotStarted, Attempt: 1}}
		},
		func(r *state.RecoveryRun) {
			r.StepStates = map[string]state.StepState{"step": {StepID: "step", RequestSHA256: base.PayloadSHA256, Status: state.StepStatusStarted}}
		},
		func(r *state.RecoveryRun) {
			r.StepStates = map[string]state.StepState{"step": {StepID: "step", RequestSHA256: base.PayloadSHA256, Status: "other", Attempt: 1}}
		},
		func(r *state.RecoveryRun) {
			r.StepStates = map[string]state.StepState{"step": {StepID: "step", RequestSHA256: base.PayloadSHA256, Status: state.StepStatusStarted, Attempt: 1, ExternalRef: strings.Repeat("x", 4097)}}
		},
	}
	for i, mutate := range cases {
		input := state.CopyRun(base)
		mutate(&input)
		var failure *state.Error
		if err := state.ValidateRun(input); !errors.As(err, &failure) || failure.Code != state.CodeInvalidRun {
			t.Fatalf("case %d: %#v", i, err)
		}
	}
	current := state.CopyRun(base)
	current.Revision = 4
	current.StepStates["step"] = state.StepState{StepID: "step", RequestSHA256: base.PayloadSHA256, Status: state.StepStatusStarted, Attempt: 2}
	next := state.CopyRun(current)
	next.Revision = 5
	next.UpdatedAt = at.Add(time.Second)
	if err := state.ValidateAdvance(current, next, 4); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*state.RecoveryRun){
		func(r *state.RecoveryRun) { r.Revision = 6 },
		func(r *state.RecoveryRun) { r.ScopeID = "other" },
		func(r *state.RecoveryRun) { r.UpdatedAt = at.Add(-time.Second) },
		func(r *state.RecoveryRun) { delete(r.StepStates, "step") },
		func(r *state.RecoveryRun) {
			step := r.StepStates["step"]
			step.Attempt = 1
			r.StepStates["step"] = step
		},
	} {
		input := state.CopyRun(next)
		mutate(&input)
		if !errors.Is(state.ValidateAdvance(current, input, 4), state.ErrRevisionConflict) {
			t.Fatal("invalid advance was accepted")
		}
	}
	wrapped := &state.Error{Code: state.CodeInvalidRun, Operation: "test", Field: "field", Cause: context.Canceled}
	if !state.IsCode(wrapped, state.CodeInvalidRun) || !errors.Is(wrapped, context.Canceled) || wrapped.Error() == "" {
		t.Fatal("typed error contract failed")
	}
}

func FuzzRunPageToken(f *testing.F) {
	f.Add("")
	f.Add("!")
	f.Add(state.EncodePageToken("scope-a", "after", []byte("state-test-cursor-key"), 1))
	f.Fuzz(func(t *testing.T, token string) {
		store, _ := newMemory("scope-a")
		page, err := store.List(t.Context(), "scope-a", 1, token)
		if err != nil && !reflect.DeepEqual(page, state.RunPage{}) {
			t.Fatal("failure returned partial page")
		}
	})
}
