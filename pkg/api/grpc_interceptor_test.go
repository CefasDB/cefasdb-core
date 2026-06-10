package api_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/osvaldoandrade/cefas/internal/auth"
	"github.com/osvaldoandrade/cefas/internal/catalog"
	"github.com/osvaldoandrade/cefas/internal/storage"
	"github.com/osvaldoandrade/cefas/pkg/api"
	cefaspb "github.com/osvaldoandrade/cefas/pkg/api/proto"
)

// mockJWKS exposes an RSA public key as JWKS for the gRPC interceptor
// to validate tokens against.
type mockJWKS struct {
	server *httptest.Server
	priv   *rsa.PrivateKey
	kid    string
}

func newMockJWKS(t testing.TB) *mockJWKS {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa: %v", err)
	}
	kid := "grpc-test-kid"
	pub := priv.Public().(*rsa.PublicKey)
	body := map[string]any{"keys": []map[string]any{{
		"kty": "RSA", "alg": "RS256", "use": "sig", "kid": kid,
		"n": base64.RawURLEncoding.EncodeToString(pub.N.Bytes()),
		"e": base64.RawURLEncoding.EncodeToString([]byte{0x01, 0x00, 0x01}),
	}}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(body)
	}))
	t.Cleanup(srv.Close)
	return &mockJWKS{server: srv, priv: priv, kid: kid}
}

func (m *mockJWKS) token(t testing.TB, scopes []string) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"sub":   "grpc-user",
		"iss":   "grpc-issuer",
		"aud":   "grpc-aud",
		"iat":   time.Now().Unix(),
		"exp":   time.Now().Add(5 * time.Minute).Unix(),
		"scope": strings.Join(scopes, " "),
	})
	tok.Header["kid"] = m.kid
	s, err := tok.SignedString(m.priv)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return s
}

func startSecuredFixture(t *testing.T, m *mockJWKS) (*grpc.ClientConn, func()) {
	t.Helper()
	v, err := auth.NewValidator(auth.Config{
		JwksURL:   m.server.URL,
		Issuer:    "grpc-issuer",
		Audience:  "grpc-aud",
		ClockSkew: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("validator: %v", err)
	}
	dir := t.TempDir()
	db, err := storage.Open(storage.Options{Path: dir})
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	cat, _ := catalog.New(db)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	skip := map[string]bool{"/cefas.v1.Cefas/ClusterStatus": true}
	unary, stream := api.AuthInterceptor(v, skip)
	gsrv := grpc.NewServer(grpc.UnaryInterceptor(unary), grpc.StreamInterceptor(stream))
	cefaspb.RegisterCefasServer(gsrv, api.NewGRPCServer(db, cat, nil))
	go func() { _ = gsrv.Serve(ln) }()

	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	cleanup := func() {
		_ = conn.Close()
		gsrv.GracefulStop()
		_ = db.Close()
	}
	return conn, cleanup
}

func withBearer(ctx context.Context, token string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
}

func TestGRPCAuthMissingTokenRejects(t *testing.T) {
	m := newMockJWKS(t)
	conn, cleanup := startSecuredFixture(t, m)
	defer cleanup()
	stub := cefaspb.NewCefasClient(conn)

	_, err := stub.PutItem(context.Background(), &cefaspb.PutItemRequest{Table: "x"})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("want Unauthenticated, got %v", err)
	}
}

func TestGRPCAuthInvalidTokenRejects(t *testing.T) {
	m := newMockJWKS(t)
	conn, cleanup := startSecuredFixture(t, m)
	defer cleanup()
	stub := cefaspb.NewCefasClient(conn)

	ctx := withBearer(context.Background(), "not.a.real.token")
	_, err := stub.PutItem(ctx, &cefaspb.PutItemRequest{Table: "x"})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("want Unauthenticated, got %v", err)
	}
}

func TestGRPCAuthValidTokenWrongScopeRejected(t *testing.T) {
	m := newMockJWKS(t)
	conn, cleanup := startSecuredFixture(t, m)
	defer cleanup()
	stub := cefaspb.NewCefasClient(conn)

	tok := m.token(t, []string{"unrelated:scope"})
	ctx := withBearer(context.Background(), tok)
	_, err := stub.CreateTable(ctx, &cefaspb.CreateTableRequest{
		Descriptor_: &cefaspb.TableDescriptor{
			Name:      "t",
			KeySchema: &cefaspb.KeySchema{Pk: "id"},
		},
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("want PermissionDenied, got %v", err)
	}
}

func TestGRPCAuthValidTokenCorrectScopeAllowed(t *testing.T) {
	m := newMockJWKS(t)
	conn, cleanup := startSecuredFixture(t, m)
	defer cleanup()
	stub := cefaspb.NewCefasClient(conn)

	tok := m.token(t, []string{
		auth.ScopeTableCreate,
		auth.WildcardScope(auth.ScopeItemWrite),
		auth.WildcardScope(auth.ScopeItemRead),
	})
	ctx := withBearer(context.Background(), tok)

	if _, err := stub.CreateTable(ctx, &cefaspb.CreateTableRequest{
		Descriptor_: &cefaspb.TableDescriptor{
			Name:      "events",
			KeySchema: &cefaspb.KeySchema{Pk: "id"},
		},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := stub.PutItem(ctx, &cefaspb.PutItemRequest{
		Table: "events",
		Item: map[string]*cefaspb.AttributeValue{
			"id": {Value: &cefaspb.AttributeValue_S{S: "k1"}},
			"v":  {Value: &cefaspb.AttributeValue_S{S: "hello"}},
		},
	}); err != nil {
		t.Fatalf("put: %v", err)
	}
	resp, err := stub.GetItem(ctx, &cefaspb.GetItemRequest{
		Table: "events",
		Key: map[string]*cefaspb.AttributeValue{
			"id": {Value: &cefaspb.AttributeValue_S{S: "k1"}},
		},
	})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !resp.GetFound() || resp.GetItem()["v"].GetS() != "hello" {
		t.Fatalf("unexpected item: %+v", resp)
	}
}

func TestGRPCAuthPublicMethodBypass(t *testing.T) {
	m := newMockJWKS(t)
	conn, cleanup := startSecuredFixture(t, m)
	defer cleanup()
	stub := cefaspb.NewCefasClient(conn)
	// No token attached — ClusterStatus is in the skip set, must work.
	resp, err := stub.ClusterStatus(context.Background(), &cefaspb.ClusterStatusRequest{})
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if resp.GetMode() != "single-node" {
		t.Fatalf("mode = %q", resp.GetMode())
	}
}
