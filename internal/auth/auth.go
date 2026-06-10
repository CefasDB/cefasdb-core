// Package auth wires the team's identity-middleware (RS256 JWT
// validation against a Tikti JWKS endpoint) into the cefas HTTP API.
//
// The package exposes three primitives:
//
//   - Validator — thin wrapper around identitymiddleware.Validator that
//     keeps configuration sanity-checking local to cefas and decouples
//     handler code from the upstream type.
//
//   - Middleware — net/http middleware that authenticates the bearer
//     token and stuffs the resulting Claims into request context. It
//     does NOT enforce per-operation scopes; that's a handler concern
//     because the required scope depends on the request body (table
//     name, etc.) which isn't visible at the middleware layer without
//     buffering the body twice.
//
//   - RequireAnyScope / RequireAllScopes — helpers handlers call after
//     parsing their request to enforce per-operation, per-table scopes.
//
// In single-node dev mode the server runs with Validator == nil and
// every handler short-circuits the auth check.
package auth

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	identitymw "github.com/codecompany/identity-middleware/pkg/identitymiddleware"
)

// Config configures the validator. JwksURL is required; everything
// else has documented defaults from the upstream config.go.
type Config struct {
	JwksURL   string
	Issuer    string
	Audience  string
	Audiences []string
	ClockSkew time.Duration
	CacheTTL  time.Duration
}

// Validator authenticates bearer tokens against the configured JWKS.
type Validator struct {
	inner *identitymw.Validator
}

// NewValidator builds a Validator. Returns an error if JwksURL is
// empty or the JWKS-backed validator construction fails.
func NewValidator(cfg Config) (*Validator, error) {
	if strings.TrimSpace(cfg.JwksURL) == "" {
		return nil, errors.New("auth: JwksURL is required")
	}
	v, err := identitymw.NewValidator(identitymw.Config{
		JwksURL:   cfg.JwksURL,
		Issuer:    cfg.Issuer,
		Audience:  cfg.Audience,
		Audiences: cfg.Audiences,
		ClockSkew: cfg.ClockSkew,
		CacheTTL:  cfg.CacheTTL,
	})
	if err != nil {
		return nil, fmt.Errorf("auth: build validator: %w", err)
	}
	return &Validator{inner: v}, nil
}

// Claims is the per-request identity surface handlers care about.
// Re-exported from identity-middleware so cefas callers don't import
// it directly.
type Claims = identitymw.Claims

// ClaimsFromContext returns the authenticated identity, if any.
func ClaimsFromContext(r *http.Request) (*Claims, bool) {
	return identitymw.ClaimsFromContext(r.Context())
}

// Middleware authenticates the bearer token on every request the
// returned http.Handler serves. The optional `skipPaths` set is for
// public endpoints like /v1/Health and /v1/cluster/status — they must
// stay reachable so load balancers can probe an unjoined node.
//
// On missing/invalid token the middleware responds 401; expired or
// unauthorised tokens return their natural status from
// identitymiddleware (401/403). Successful auth attaches Claims to
// the request context.
func (v *Validator) Middleware(skipPaths map[string]bool) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if skipPaths[r.URL.Path] {
				next.ServeHTTP(w, r)
				return
			}
			authz := strings.TrimSpace(r.Header.Get("Authorization"))
			if authz == "" {
				http.Error(w, "missing bearer token", http.StatusUnauthorized)
				return
			}
			token := extractBearer(authz)
			if token == "" {
				http.Error(w, "invalid bearer token", http.StatusUnauthorized)
				return
			}
			claims, err := v.inner.Validate(token)
			if err != nil {
				http.Error(w, err.Error(), http.StatusUnauthorized)
				return
			}
			r = r.WithContext(identitymw.WithClaims(r.Context(), claims))
			next.ServeHTTP(w, r)
		})
	}
}

func extractBearer(authz string) string {
	const prefix = "Bearer "
	if len(authz) <= len(prefix) {
		return ""
	}
	if !strings.EqualFold(authz[:len(prefix)], prefix) {
		return ""
	}
	return strings.TrimSpace(authz[len(prefix):])
}
