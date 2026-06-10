package auth_test

import (
	"net/http/httptest"
	"testing"

	identitymw "github.com/codecompany/identity-middleware/pkg/identitymiddleware"

	"github.com/osvaldoandrade/cefas/internal/auth"
)

func TestHasScopeExactAndWildcard(t *testing.T) {
	c := &auth.Claims{Scopes: []string{"cefas:item:write:events", "cefas:item:read:*"}}

	if !auth.HasScope(c, "cefas:item:write:events") {
		t.Error("exact match failed")
	}
	if auth.HasScope(c, "cefas:item:write:other") {
		t.Error("specific scope leaked to other tables")
	}
	if !auth.HasScope(c, "cefas:item:read:events") {
		t.Error("wildcard read scope did not satisfy a specific table read")
	}
	if !auth.HasScope(c, "cefas:item:read:other") {
		t.Error("wildcard read scope did not satisfy another table read")
	}
	if auth.HasScope(c, "cefas:item:delete:events") {
		t.Error("read scope satisfied an unrelated verb")
	}
}

func TestHasScopeNilClaims(t *testing.T) {
	if auth.HasScope(nil, "cefas:any") {
		t.Error("nil claims should not satisfy any scope")
	}
}

func TestRequireAnyScopeWithoutClaimsDevMode(t *testing.T) {
	// No claims in context — auth disabled. Handler must let the call
	// through so single-node dev mode keeps working without tokens.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/v1/PutItem", nil)
	if !auth.RequireAnyScope(rec, req, "cefas:item:write:events") {
		t.Errorf("dev mode should pass without scopes")
	}
	if rec.Code != 200 {
		t.Errorf("unexpected status %d", rec.Code)
	}
}

func TestRequireAnyScopeForbiddenWithoutMatch(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/v1/PutItem", nil)
	claims := &auth.Claims{Scopes: []string{"unrelated:scope"}}
	req = req.WithContext(identitymw.WithClaims(req.Context(), claims))
	if auth.RequireAnyScope(rec, req, "cefas:item:write:events") {
		t.Errorf("expected denial")
	}
	if rec.Code != 403 {
		t.Errorf("status = %d, want 403", rec.Code)
	}
}

func TestRequireAnyScopeAllowedOnExact(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/v1/PutItem", nil)
	claims := &auth.Claims{Scopes: []string{"cefas:item:write:events"}}
	req = req.WithContext(identitymw.WithClaims(req.Context(), claims))
	if !auth.RequireAnyScope(rec, req,
		auth.TableScope(auth.ScopeItemWrite, "events"),
		auth.WildcardScope(auth.ScopeItemWrite)) {
		t.Errorf("exact-scope token should pass")
	}
}

func TestRequireAnyScopeAllowedOnWildcard(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/v1/PutItem", nil)
	claims := &auth.Claims{Scopes: []string{"cefas:item:write:*"}}
	req = req.WithContext(identitymw.WithClaims(req.Context(), claims))
	if !auth.RequireAnyScope(rec, req,
		auth.TableScope(auth.ScopeItemWrite, "events"),
		auth.WildcardScope(auth.ScopeItemWrite)) {
		t.Errorf("wildcard token should pass")
	}
}

func TestTableScopeBuilders(t *testing.T) {
	if got := auth.TableScope("cefas:item:write", "events"); got != "cefas:item:write:events" {
		t.Errorf("TableScope = %q", got)
	}
	if got := auth.WildcardScope("cefas:item:write"); got != "cefas:item:write:*" {
		t.Errorf("WildcardScope = %q", got)
	}
}
