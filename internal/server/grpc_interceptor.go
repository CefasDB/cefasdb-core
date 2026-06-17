package server

import (
	"context"
	"strings"

	identitymw "github.com/codecompany/identity-middleware/pkg/identitymiddleware"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/osvaldoandrade/cefas/internal/auth"
)

// AuthInterceptor returns gRPC unary and stream interceptors that
// authenticate the bearer token on every non-public method. Public
// methods (typically the cluster status and the SDK reflection probe)
// are listed in `skipMethods` by full gRPC path, e.g.
// "/cefas.v1.Cefas/ClusterStatus".
//
// On successful authentication the request context carries the same
// *identitymw.Claims the HTTP middleware uses, so the gRPC handlers
// call the shared auth.RequireAnyScope path with no special casing.
func AuthInterceptor(v *auth.Validator, skipMethods map[string]bool) (grpc.UnaryServerInterceptor, grpc.StreamServerInterceptor) {
	if v == nil {
		// Dev mode: identity routines short-circuit. Returning nil
		// interceptors lets the gRPC server skip the middleware
		// layer entirely.
		return nil, nil
	}
	authn := func(ctx context.Context, method string) (context.Context, error) {
		if skipMethods[method] {
			return ctx, nil
		}
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "missing metadata")
		}
		token, err := extractBearerFromMD(md)
		if err != nil {
			return nil, err
		}
		claims, err := v.ValidateToken(token)
		if err != nil {
			return nil, status.Error(codes.Unauthenticated, err.Error())
		}
		return identitymw.WithClaims(ctx, claims), nil
	}

	unary := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		newCtx, err := authn(ctx, info.FullMethod)
		if err != nil {
			return nil, err
		}
		return handler(newCtx, req)
	}
	stream := func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		newCtx, err := authn(ss.Context(), info.FullMethod)
		if err != nil {
			return err
		}
		return handler(srv, &wrappedStream{ServerStream: ss, ctx: newCtx})
	}
	return unary, stream
}

// wrappedStream replaces the embedded context so downstream handlers
// see the authenticated claims when calling stream.Context().
type wrappedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedStream) Context() context.Context { return w.ctx }

func extractBearerFromMD(md metadata.MD) (string, error) {
	auths := md.Get("authorization")
	if len(auths) == 0 {
		return "", status.Error(codes.Unauthenticated, "missing bearer token")
	}
	const prefix = "bearer "
	authz := strings.TrimSpace(auths[0])
	if len(authz) <= len(prefix) || !strings.EqualFold(authz[:len(prefix)], prefix) {
		return "", status.Error(codes.Unauthenticated, "invalid Authorization header")
	}
	return strings.TrimSpace(authz[len(prefix):]), nil
}

// requireScope enforces a single required scope inside a handler. It
// short-circuits to nil when no claims are attached (dev mode).
func requireScope(ctx context.Context, scope string) error {
	return requireAnyScope(ctx, scope)
}

func requireAnyScope(ctx context.Context, scopes ...string) error {
	claims, ok := identitymw.ClaimsFromContext(ctx)
	if !ok {
		// No identity wired (dev mode). Permit.
		return nil
	}
	if auth.HasAnyScope(claims, scopes...) {
		return nil
	}
	return status.Error(codes.PermissionDenied, "missing required scope")
}
