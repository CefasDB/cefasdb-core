package server

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestBuildGRPCOpts_NoTLS_NoValidator verifies the happy "plaintext,
// unauthenticated" path: no TLS material, no validator. The service-
// level interceptor still gets wired (always-on for #489), so we
// expect the two interceptor-chain options.
func TestBuildGRPCOpts_NoTLS_NoValidator(t *testing.T) {
	opts, err := BuildGRPCOpts(nil, "", "", "", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(opts) != 2 {
		t.Fatalf("expected 2 options (chain unary + stream), got %d", len(opts))
	}
}

// TestBuildGRPCOpts_TLSMissingKey rejects half-configured TLS.
func TestBuildGRPCOpts_TLSMissingKey(t *testing.T) {
	if _, err := BuildGRPCOpts(nil, "cert.pem", "", "", nil); err == nil {
		t.Fatalf("expected error when key path is empty")
	}
	if _, err := BuildGRPCOpts(nil, "", "key.pem", "", nil); err == nil {
		t.Fatalf("expected error when cert path is empty")
	}
}

// TestBuildGRPCOpts_InvalidCertPath surfaces filesystem errors from the
// underlying tls loader.
func TestBuildGRPCOpts_InvalidCertPath(t *testing.T) {
	dir := t.TempDir()
	bogus := filepath.Join(dir, "missing.pem")
	if _, err := BuildGRPCOpts(nil, bogus, bogus, "", nil); err == nil {
		t.Fatalf("expected error for missing cert/key files")
	}
}

// TestBuildGRPCOpts_TLS exercises the single-TLS code path with a
// freshly-generated self-signed cert. It guards against regressions in
// the cert-loading / option-assembly section.
func TestBuildGRPCOpts_TLS(t *testing.T) {
	certPath, keyPath := writeSelfSignedCert(t)
	opts, err := BuildGRPCOpts(nil, certPath, keyPath, "", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(opts) != 3 {
		t.Fatalf("expected 3 options (Creds + 2 interceptor chains), got %d", len(opts))
	}
}

// TestBuildGRPCOpts_MTLS exercises the mTLS branch, reusing the same
// cert as the client CA bundle to keep the test hermetic.
func TestBuildGRPCOpts_MTLS(t *testing.T) {
	certPath, keyPath := writeSelfSignedCert(t)
	opts, err := BuildGRPCOpts(nil, certPath, keyPath, certPath, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(opts) != 3 {
		t.Fatalf("expected 3 options (Creds + 2 interceptor chains), got %d", len(opts))
	}
}

// TestBuildGRPCOpts_MTLS_BadCABundle rejects a non-PEM CA bundle.
func TestBuildGRPCOpts_MTLS_BadCABundle(t *testing.T) {
	certPath, keyPath := writeSelfSignedCert(t)
	dir := t.TempDir()
	bad := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(bad, []byte("not a pem block"), 0o600); err != nil {
		t.Fatalf("write bad ca: %v", err)
	}
	if _, err := BuildGRPCOpts(nil, certPath, keyPath, bad, nil); err == nil {
		t.Fatalf("expected error for non-PEM ca bundle")
	}
}

// writeSelfSignedCert produces a minimal self-signed ECDSA cert/key
// pair under t.TempDir and returns their paths.
func writeSelfSignedCert(t *testing.T) (string, string) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "cefas-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}

	dir := t.TempDir()
	certPath := filepath.Join(dir, "cert.pem")
	keyPath := filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	return certPath, keyPath
}
