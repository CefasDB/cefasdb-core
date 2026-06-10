// Package clicfg loads cefas-cli configuration with the same
// precedence the server uses: flag > env > YAML > default. YAML
// supports named profiles à la aws-cli; --profile picks one.
package clicfg

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v3"
)

// Profile is a single named connection target.
type Profile struct {
	Endpoint   string        `yaml:"endpoint"`
	Token      string        `yaml:"token,omitempty"`
	TokenFile  string        `yaml:"tokenFile,omitempty"`
	TLSCAPath  string        `yaml:"tlsCaPath,omitempty"`
	Insecure   bool          `yaml:"insecure,omitempty"`
	Timeout    time.Duration `yaml:"timeout,omitempty"`
	Output     string        `yaml:"output,omitempty"`
}

// File is the on-disk YAML shape.
type File struct {
	Profiles map[string]Profile `yaml:"profiles"`
}

// Defaults returns a Profile populated with the same fallbacks every
// flag carries.
func Defaults() Profile {
	return Profile{
		Endpoint: "localhost:9090",
		Output:   "json",
		Timeout:  30 * time.Second,
	}
}

// DefaultPath returns the conventional location of the config file
// (~/.cefas/config.yaml). Returns "" when HOME is unset.
func DefaultPath() string {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return ""
	}
	return filepath.Join(home, ".cefas", "config.yaml")
}

// LoadProfile resolves the named profile from `path`. If path is
// empty or missing, returns Defaults() — same fall-back semantics
// the server uses. profileName defaults to "default" when empty.
func LoadProfile(path, profileName string) (Profile, error) {
	cfg := Defaults()
	if path == "" {
		path = DefaultPath()
	}
	if profileName == "" {
		profileName = "default"
	}
	if path == "" {
		return cfg, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return cfg, fmt.Errorf("read config %s: %w", path, err)
	}
	var f File
	if err := yaml.Unmarshal(b, &f); err != nil {
		return cfg, fmt.Errorf("parse config %s: %w", path, err)
	}
	p, ok := f.Profiles[profileName]
	if !ok {
		return cfg, fmt.Errorf("profile %q not found in %s", profileName, path)
	}
	// Overlay non-zero fields from the profile onto the defaults so
	// the user can override per-profile only what they care about.
	overlay(&cfg, p)
	return cfg, nil
}

// ApplyEnv overlays CEFAS_* environment variables onto p. Mirrors
// the server's env mapping.
func ApplyEnv(p *Profile) {
	if v := os.Getenv("CEFAS_ENDPOINT"); v != "" {
		p.Endpoint = v
	}
	if v := os.Getenv("CEFAS_TOKEN"); v != "" {
		p.Token = v
	}
	if v := os.Getenv("CEFAS_TOKEN_FILE"); v != "" {
		p.TokenFile = v
	}
	if v := os.Getenv("CEFAS_TLS_CA"); v != "" {
		p.TLSCAPath = v
	}
	if v := os.Getenv("CEFAS_INSECURE"); v == "true" || v == "1" {
		p.Insecure = true
	}
	if v := os.Getenv("CEFAS_OUTPUT"); v != "" {
		p.Output = v
	}
	if v := os.Getenv("CEFAS_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			p.Timeout = d
		}
	}
}

// ResolveToken applies the documented precedence: explicit Token
// wins; else TokenFile is read; else an empty string (anonymous).
func ResolveToken(p Profile) (string, error) {
	if p.Token != "" {
		return p.Token, nil
	}
	if p.TokenFile != "" {
		b, err := os.ReadFile(p.TokenFile)
		if err != nil {
			return "", fmt.Errorf("read token file %s: %w", p.TokenFile, err)
		}
		// Strip whitespace so accidental trailing newlines don't
		// invalidate the JWT.
		return trimWhitespace(string(b)), nil
	}
	return "", nil
}

func trimWhitespace(s string) string {
	for len(s) > 0 && (s[len(s)-1] == '\n' || s[len(s)-1] == '\r' || s[len(s)-1] == ' ' || s[len(s)-1] == '\t') {
		s = s[:len(s)-1]
	}
	for len(s) > 0 && (s[0] == '\n' || s[0] == '\r' || s[0] == ' ' || s[0] == '\t') {
		s = s[1:]
	}
	return s
}

func overlay(dst *Profile, src Profile) {
	if src.Endpoint != "" {
		dst.Endpoint = src.Endpoint
	}
	if src.Token != "" {
		dst.Token = src.Token
	}
	if src.TokenFile != "" {
		dst.TokenFile = src.TokenFile
	}
	if src.TLSCAPath != "" {
		dst.TLSCAPath = src.TLSCAPath
	}
	if src.Insecure {
		dst.Insecure = src.Insecure
	}
	if src.Timeout > 0 {
		dst.Timeout = src.Timeout
	}
	if src.Output != "" {
		dst.Output = src.Output
	}
}
