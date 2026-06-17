package clicfg_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/CefasDb/cefasdb/cmd/cefasctl/internal/clicfg"
)

func TestDefaults(t *testing.T) {
	p := clicfg.Defaults()
	if p.Endpoint == "" || p.Output != "json" || p.Timeout != 30*time.Second {
		t.Fatalf("unexpected defaults: %+v", p)
	}
}

func TestLoadProfileMissingFileReturnsDefaults(t *testing.T) {
	p, err := clicfg.LoadProfile(filepath.Join(t.TempDir(), "nope.yaml"), "default")
	if err != nil {
		t.Fatalf("missing file should not error: %v", err)
	}
	if p.Endpoint == "" {
		t.Fatalf("defaults lost: %+v", p)
	}
}

func TestLoadProfileYAML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "c.yaml")
	yaml := `
profiles:
  default:
    endpoint: cefas.local:9090
    token: t-default
  prod:
    endpoint: cefas.prod:9090
    tokenFile: /etc/cefas/prod.jwt
    insecure: true
    output: table
    timeout: 5s
`
	if err := os.WriteFile(path, []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}
	p, err := clicfg.LoadProfile(path, "prod")
	if err != nil {
		t.Fatal(err)
	}
	if p.Endpoint != "cefas.prod:9090" || p.TokenFile != "/etc/cefas/prod.jwt" {
		t.Fatalf("prod profile lost: %+v", p)
	}
	if !p.Insecure || p.Output != "table" || p.Timeout != 5*time.Second {
		t.Fatalf("prod profile fields lost: %+v", p)
	}
}

func TestLoadProfileUnknown(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "c.yaml")
	os.WriteFile(path, []byte("profiles:\n  default:\n    endpoint: x\n"), 0o644)
	if _, err := clicfg.LoadProfile(path, "nope"); err == nil {
		t.Fatalf("expected unknown-profile error")
	}
}

func TestApplyEnvOverrides(t *testing.T) {
	t.Setenv("CEFAS_ENDPOINT", "envhost:1234")
	t.Setenv("CEFAS_OUTPUT", "text")
	t.Setenv("CEFAS_INSECURE", "true")

	p := clicfg.Defaults()
	clicfg.ApplyEnv(&p)
	if p.Endpoint != "envhost:1234" || p.Output != "text" || !p.Insecure {
		t.Fatalf("env overlay missed: %+v", p)
	}
}

func TestResolveTokenPrecedence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tok")
	os.WriteFile(path, []byte("file-token\n"), 0o600)

	// Explicit Token wins.
	tk, err := clicfg.ResolveToken(clicfg.Profile{Token: "explicit", TokenFile: path})
	if err != nil || tk != "explicit" {
		t.Fatalf("explicit lost: %q %v", tk, err)
	}
	// File fallback strips trailing whitespace.
	tk, err = clicfg.ResolveToken(clicfg.Profile{TokenFile: path})
	if err != nil || tk != "file-token" {
		t.Fatalf("file token = %q %v", tk, err)
	}
	// Nothing → empty string, no error.
	tk, err = clicfg.ResolveToken(clicfg.Profile{})
	if err != nil || tk != "" {
		t.Fatalf("empty case: %q %v", tk, err)
	}
}
