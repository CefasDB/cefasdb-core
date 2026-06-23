package auth

import (
	"context"

	identitymw "github.com/codecompany/identity-middleware/pkg/identitymiddleware"

	"github.com/CefasDb/cefasdb/pkg/types"
)

// ServiceLevelMetadataKey is the gRPC metadata / HTTP header that
// lets a caller explicitly nominate the workload service level for
// the request. Overrides any bearer-claim SL.
const ServiceLevelMetadataKey = "x-cefas-service-level"

// ServiceLevelClaimKey is the JWT claim a token may carry to
// auto-tag every request from that identity. Lower precedence than
// the explicit metadata header.
const ServiceLevelClaimKey = "service_level"

type serviceLevelKey struct{}

// WithServiceLevel attaches the resolved SL name to ctx. The
// interceptor calls this once per request after the resolution
// chain; handlers must use ServiceLevelFromContext for reads.
func WithServiceLevel(ctx context.Context, name string) context.Context {
	if name == "" {
		name = types.DefaultServiceLevelName
	}
	return context.WithValue(ctx, serviceLevelKey{}, name)
}

// ServiceLevelFromContext returns the SL name attached by the
// interceptor. Falls back to the default SL when nothing is
// attached (single-node tests, raw calls).
func ServiceLevelFromContext(ctx context.Context) string {
	if name, ok := ctx.Value(serviceLevelKey{}).(string); ok && name != "" {
		return name
	}
	return types.DefaultServiceLevelName
}

// ServiceLevelFromClaims extracts the service-level claim from the
// authenticated identity, if any. Returns "" when no claim is
// present so the caller can fall through the chain.
func ServiceLevelFromClaims(c *identitymw.Claims) string {
	if c == nil || c.Raw == nil {
		return ""
	}
	if v, ok := c.Raw[ServiceLevelClaimKey].(string); ok {
		return v
	}
	return ""
}
