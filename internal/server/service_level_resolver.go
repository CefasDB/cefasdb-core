package server

import (
	"context"

	identitymw "github.com/codecompany/identity-middleware/pkg/identitymiddleware"
	"google.golang.org/grpc/metadata"

	"github.com/CefasDb/cefasdb/internal/auth"
	"github.com/CefasDb/cefasdb/pkg/types"
)

// ResolveServiceLevel walks the per-request resolution chain:
//
//  1. x-cefas-service-level metadata header (explicit override)
//  2. service_level bearer-claim
//  3. fallback DefaultServiceLevelName
//
// Step 3 (table-attribute → SL mapping) is intentionally deferred —
// it requires per-RPC introspection of the target table and a tag
// schema we have not finalised. Phase 4 / #499 can extend the chain
// without changing this function's signature.
func ResolveServiceLevel(ctx context.Context) string {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if vals := md.Get(auth.ServiceLevelMetadataKey); len(vals) > 0 {
			if name := vals[0]; name != "" {
				return name
			}
		}
	}
	if claims, ok := identitymw.ClaimsFromContext(ctx); ok {
		if name := auth.ServiceLevelFromClaims(claims); name != "" {
			return name
		}
	}
	return types.DefaultServiceLevelName
}
