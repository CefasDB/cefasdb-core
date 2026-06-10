package identitymiddleware

import "context"

type ctxKey string

const claimsKey ctxKey = "identityClaims"

// WithClaims stores claims in context.
func WithClaims(ctx context.Context, claims *Claims) context.Context {
	return context.WithValue(ctx, claimsKey, claims)
}

// ClaimsFromContext loads claims from context.
func ClaimsFromContext(ctx context.Context) (*Claims, bool) {
	val := ctx.Value(claimsKey)
	if val == nil {
		return nil, false
	}
	claims, ok := val.(*Claims)
	return claims, ok
}
