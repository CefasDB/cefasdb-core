package auth_test

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/osvaldoandrade/cefas/internal/auth"
)

// mockJWKS spins up an HTTP server that serves an RSA public key as
// JWKS and returns the matching private key for signing.
type mockJWKS struct {
	server *httptest.Server
	priv   *rsa.PrivateKey
	kid    string
}

func newMockJWKS(t testing.TB) *mockJWKS {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa keygen: %v", err)
	}
	kid := "test-kid"
	pub := priv.Public().(*rsa.PublicKey)

	// JWKS body matching the format identity-middleware parses.
	body := map[string]any{
		"keys": []map[string]any{{
			"kty": "RSA",
			"alg": "RS256",
			"use": "sig",
			"kid": kid,
			"n":   base64.RawURLEncoding.EncodeToString(pub.N.Bytes()),
			"e":   base64.RawURLEncoding.EncodeToString([]byte{0x01, 0x00, 0x01}),
		}},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(body)
	}))
	t.Cleanup(srv.Close)
	return &mockJWKS{server: srv, priv: priv, kid: kid}
}

func (m *mockJWKS) signToken(t testing.TB, scopes []string, audience string, issuer string) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"sub":   "test-user",
		"iss":   issuer,
		"aud":   audience,
		"iat":   time.Now().Unix(),
		"exp":   time.Now().Add(5 * time.Minute).Unix(),
		"scope": strings.Join(scopes, " "),
	})
	tok.Header["kid"] = m.kid
	signed, err := tok.SignedString(m.priv)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return signed
}

func newValidator(t testing.TB, m *mockJWKS) *auth.Validator {
	t.Helper()
	v, err := auth.NewValidator(auth.Config{
		JwksURL:   m.server.URL,
		Issuer:    "test-issuer",
		Audience:  "test-aud",
		ClockSkew: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("validator: %v", err)
	}
	return v
}

// echoHandler asserts that claims arrive on the request context and
// echoes the subject in the body — used to confirm the middleware
// wired the identity through.
func echoHandler(w http.ResponseWriter, r *http.Request) {
	claims, ok := auth.ClaimsFromContext(r)
	if !ok {
		http.Error(w, "no claims", http.StatusInternalServerError)
		return
	}
	fmt.Fprintf(w, "ok: %s", claims.Subject)
}

func TestMiddlewareMissingTokenRejects(t *testing.T) {
	m := newMockJWKS(t)
	v := newValidator(t, m)
	h := v.Middleware(nil)(http.HandlerFunc(echoHandler))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/anything", nil)
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
}

func TestMiddlewareInvalidTokenRejects(t *testing.T) {
	m := newMockJWKS(t)
	v := newValidator(t, m)
	h := v.Middleware(nil)(http.HandlerFunc(echoHandler))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/anything", nil)
	req.Header.Set("Authorization", "Bearer not.a.real.token")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
}

func TestMiddlewareAcceptsValidToken(t *testing.T) {
	m := newMockJWKS(t)
	v := newValidator(t, m)
	tok := m.signToken(t, []string{"cefas:table:create"}, "test-aud", "test-issuer")

	h := v.Middleware(nil)(http.HandlerFunc(echoHandler))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/anything", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %q", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "test-user") {
		t.Fatalf("body = %q, want subject in it", rec.Body.String())
	}
}

func TestMiddlewareSkipsPublicPath(t *testing.T) {
	m := newMockJWKS(t)
	v := newValidator(t, m)
	skip := map[string]bool{"/v1/Health": true}
	h := v.Middleware(skip)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/v1/Health", nil)
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("public path rejected: %d", rec.Code)
	}
}

func TestMiddlewareWrongAudienceRejects(t *testing.T) {
	m := newMockJWKS(t)
	v := newValidator(t, m)
	tok := m.signToken(t, []string{"cefas:table:create"}, "wrong-aud", "test-issuer")
	h := v.Middleware(nil)(http.HandlerFunc(echoHandler))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/anything", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
}

// Full allow/deny matrix: middleware + per-handler scope enforcement
// against a representative endpoint chain.
func TestScopeMatrix(t *testing.T) {
	m := newMockJWKS(t)
	v := newValidator(t, m)

	// Handler that uses RequireAnyScope just like a real cefas
	// endpoint would.
	guard := func(scope ...string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if !auth.RequireAnyScope(w, r, scope...) {
				return
			}
			w.WriteHeader(http.StatusOK)
		}
	}

	type tc struct {
		name        string
		tokenScopes []string
		required    []string
		wantStatus  int
	}
	cases := []tc{
		{
			name:        "exact table scope allowed",
			tokenScopes: []string{"cefas:item:write:events"},
			required: []string{
				auth.TableScope(auth.ScopeItemWrite, "events"),
				auth.WildcardScope(auth.ScopeItemWrite),
			},
			wantStatus: http.StatusOK,
		},
		{
			name:        "wildcard scope allowed",
			tokenScopes: []string{"cefas:item:write:*"},
			required: []string{
				auth.TableScope(auth.ScopeItemWrite, "events"),
				auth.WildcardScope(auth.ScopeItemWrite),
			},
			wantStatus: http.StatusOK,
		},
		{
			name:        "wrong table denied",
			tokenScopes: []string{"cefas:item:write:other"},
			required: []string{
				auth.TableScope(auth.ScopeItemWrite, "events"),
				auth.WildcardScope(auth.ScopeItemWrite),
			},
			wantStatus: http.StatusForbidden,
		},
		{
			name:        "cluster admin needed for cluster ops",
			tokenScopes: []string{"cefas:item:write:*"},
			required:    []string{auth.ScopeClusterAdmin},
			wantStatus:  http.StatusForbidden,
		},
		{
			name:        "cluster admin holder allowed",
			tokenScopes: []string{auth.ScopeClusterAdmin},
			required:    []string{auth.ScopeClusterAdmin},
			wantStatus:  http.StatusOK,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tok := m.signToken(t, c.tokenScopes, "test-aud", "test-issuer")
			h := v.Middleware(nil)(guard(c.required...))
			rec := httptest.NewRecorder()
			req := httptest.NewRequest("POST", "/x", nil)
			req.Header.Set("Authorization", "Bearer "+tok)
			h.ServeHTTP(rec, req)
			if rec.Code != c.wantStatus {
				t.Fatalf("status = %d, want %d; body = %q", rec.Code, c.wantStatus, rec.Body.String())
			}
		})
	}
}
