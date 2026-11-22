package state_test

import (
	"bytes"
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
		New: func(_ testing.TB) storetest.ScopeOpener {
			data := newMemoryBacking()
			return func(scope string) (state.RunStore, func() state.RunStore) {
				return openMemory(data, scope)
			}
		},
		Token: func(scope, after string, version byte) string {
			return state.EncodePageToken(scope, after, key, version)
		},
	}
}

func TestConformance(t *testing.T) { storetest.Run(t, driver()) }

func TestValidationAndErrors(t *testing.T) {
	at := time.Date(2026, 11, 22, 3, 0, 0, 0, time.UTC)
	base := storetest.SampleRun(t, "valid", at)
	cases := []func(*state.RecoveryRun){
		func(r *state.RecoveryRun) { r.ScopeID = "" },
		func(r *state.RecoveryRun) { r.ScopeID = "Scope-A" },
		func(r *state.RecoveryRun) { r.ScopeID = "scope//a" },
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
	slashScope := state.CopyRun(base)
	slashScope.ScopeID = "scope/a"
	if err := state.ValidateRun(slashScope); err != nil {
		t.Fatalf("canonical slash scope was rejected: %v", err)
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
	if !errors.Is(state.ValidateAdvance(current, next, 3), state.ErrRevisionConflict) {
		t.Fatal("advance accepted a stale expected revision")
	}
	overflowCurrent := state.CopyRun(current)
	overflowCurrent.Revision = ^uint64(0)
	overflowNext := state.CopyRun(overflowCurrent)
	overflowNext.Revision = 0
	if !errors.Is(state.ValidateAdvance(overflowCurrent, overflowNext, ^uint64(0)), state.ErrRevisionConflict) {
		t.Fatal("advance accepted revision overflow")
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

func TestCanonicalPayloadAndRequest(t *testing.T) {
	left := []byte(`{"step":{"attempt":1,"id":"restore.apply"},"runId":"rec-1"}`)
	right := []byte(" { \"runId\" : \"rec-1\", \"step\" : { \"id\" : \"restore.apply\", \"attempt\" : 1 } } \n")
	canonical, payloadDigest, err := state.CanonicalPayload(left)
	if err != nil {
		t.Fatal(err)
	}
	request, requestDigest, err := state.CanonicalRequest(right)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(canonical, request) || payloadDigest != requestDigest || canonical[len(canonical)-1] != '\n' {
		t.Fatalf("canonical mismatch: %q %s; %q %s", canonical, payloadDigest, request, requestDigest)
	}

	for _, raw := range [][]byte{
		{},
		[]byte(`{"runId":"a","runId":"b"}`),
		[]byte(`{"runId":"a"} {"runId":"b"}`),
		{0xff},
	} {
		if _, _, err = state.CanonicalPayload(raw); !errors.Is(err, state.ErrInvalidRun) {
			t.Fatalf("invalid canonical input %q: %v", raw, err)
		}
	}
}

func TestCopyAndPaginationScopeIsolation(t *testing.T) {
	at := time.Date(2026, 11, 22, 3, 0, 0, 0, time.UTC)
	withNilSteps := storetest.SampleRun(t, "nil-steps", at)
	withNilSteps.StepStates = nil
	if copied := state.CopyRun(withNilSteps); copied.StepStates != nil {
		t.Fatal("CopyRun changed a nil step map")
	}

	allowed := storetest.SampleRun(t, "allowed", at)
	denied := storetest.SampleRun(t, "denied", at)
	denied.ScopeID = "scope-b"
	page, err := state.Paginate(
		map[string]state.RecoveryRun{allowed.RunID: allowed, denied.RunID: denied},
		"scope-a",
		100,
		"",
		[]byte("state-test-cursor-key"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Items[0].RunID != allowed.RunID {
		t.Fatalf("scope-filtered page = %#v", page)
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
