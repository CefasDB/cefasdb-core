package auth

import (
	"net/http"
	"strings"
)

// Scope strings issued by the Tikti tenant for cefas. Generic verbs
// here; the table-scoped form (e.g. cefas:item:write:events) is built
// at request time by the handler from these prefixes.
const (
	ScopeTableCreate   = "cefas:table:create"
	ScopeTableDrop     = "cefas:table:drop"
	ScopeTableDescribe = "cefas:table:describe"

	// Per-item / per-query operations. Caller is expected to hold
	// either the table-specific scope (cefas:item:write:events) or
	// the wildcard (cefas:item:write:*).
	ScopeItemRead   = "cefas:item:read"
	ScopeItemWrite  = "cefas:item:write"
	ScopeItemDelete = "cefas:item:delete"
	ScopeQuery      = "cefas:query"
	ScopeScan       = "cefas:scan"
	ScopeSpatial    = "cefas:spatial"

	// Cluster admin — covers AddVoter, RemoveServer, future
	// shard-management endpoints.
	ScopeClusterAdmin = "cefas:cluster:admin"
)

// TableScope returns the table-scoped variant of `base`, e.g.
// TableScope("cefas:item:write", "events") → "cefas:item:write:events".
func TableScope(base, table string) string {
	return base + ":" + table
}

// WildcardScope returns the wildcard form of `base`, e.g.
// WildcardScope("cefas:item:write") → "cefas:item:write:*".
func WildcardScope(base string) string {
	return base + ":*"
}

// HasScope reports whether `c` holds `want`. Wildcard claims match
// any sibling: a token holding "cefas:item:write:*" satisfies
// "cefas:item:write:<any table>". The reverse does NOT hold — a
// specific scope does not grant the wildcard.
func HasScope(c *Claims, want string) bool {
	if c == nil {
		return false
	}
	for _, s := range c.Scopes {
		if s == want {
			return true
		}
		// Wildcard claim form: "<prefix>:*" matches any "<prefix>:<x>".
		if strings.HasSuffix(s, ":*") {
			prefix := s[:len(s)-1] // includes the trailing ":"
			if strings.HasPrefix(want, prefix) {
				return true
			}
		}
	}
	return false
}

// HasAnyScope is true when the caller holds at least one of `want`.
// Empty `want` means "no auth required" and always returns true.
func HasAnyScope(c *Claims, want ...string) bool {
	if len(want) == 0 {
		return true
	}
	for _, w := range want {
		if HasScope(c, w) {
			return true
		}
	}
	return false
}

// RequireAnyScope is the handler-side gate: if the request context
// has no claims (auth disabled) it lets the call through. If claims
// are present but lack every scope in `want`, it writes 403 and
// returns false — the caller must return immediately.
func RequireAnyScope(w http.ResponseWriter, r *http.Request, want ...string) bool {
	claims, ok := ClaimsFromContext(r)
	if !ok {
		// No auth configured. The middleware would have rejected the
		// request earlier when auth IS configured, so reaching this
		// branch is the single-node-dev-mode contract.
		return true
	}
	if HasAnyScope(claims, want...) {
		return true
	}
	http.Error(w, "forbidden: missing required scope", http.StatusForbidden)
	return false
}
