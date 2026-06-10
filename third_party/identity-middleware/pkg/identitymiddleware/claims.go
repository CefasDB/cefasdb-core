package identitymiddleware

import "time"

// Claims represents the normalized fields extracted from a token.
type Claims struct {
	Subject    string
	Email      string
	TenantID   string
	Issuer     string
	Audience   []string
	ExpiresAt  time.Time
	IssuedAt   time.Time
	Scopes     []string
	EventTypes []string
	Raw        map[string]any
}

// HasScope returns true if the scope is present.
func (c *Claims) HasScope(scope string) bool {
	if c == nil {
		return false
	}
	for _, s := range c.Scopes {
		if s == scope {
			return true
		}
	}
	return false
}

// HasAllScopes returns true if all required scopes are present.
func (c *Claims) HasAllScopes(required []string) bool {
	if len(required) == 0 {
		return true
	}
	if c == nil {
		return false
	}
	set := make(map[string]struct{}, len(c.Scopes))
	for _, s := range c.Scopes {
		set[s] = struct{}{}
	}
	for _, r := range required {
		if _, ok := set[r]; !ok {
			return false
		}
	}
	return true
}
