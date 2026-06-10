// Package runtime owns the cefas-cli global flag values + Dial
// helper. Both the root cmd package (which binds the flags) and the
// subcommand packages (which read them) import this — keeps the
// import graph cycle-free.
package runtime

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	"github.com/osvaldoandrade/cefas/cmd/cefas-cli/internal/clicfg"
	"github.com/osvaldoandrade/cefas/cmd/cefas-cli/internal/output"
	"github.com/osvaldoandrade/cefas/pkg/client"
)

// Flags is the live binding target for cobra persistent flags.
// root.go binds every PersistentFlag onto fields of this exported
// struct so subcommands can read them.
var Flags struct {
	ConfigPath    string
	ProfileName   string
	Endpoint      string
	Token         string
	TokenFile     string
	TLSCAPath     string
	Insecure      bool
	Output        string
	Timeout       time.Duration
	NoStream      bool
}

// Dial resolves the active profile (flag > env > yaml > defaults)
// and returns a connected client.Client. Caller owns Close.
func Dial(ctx context.Context) (*client.Client, clicfg.Profile, error) {
	p, err := clicfg.LoadProfile(Flags.ConfigPath, Flags.ProfileName)
	if err != nil {
		return nil, p, err
	}
	clicfg.ApplyEnv(&p)
	overlay(&p)

	token, err := clicfg.ResolveToken(p)
	if err != nil {
		return nil, p, err
	}
	opts := []client.Option{}
	if token != "" {
		opts = append(opts, client.WithBearer(token))
	}
	switch {
	case p.Insecure:
		opts = append(opts, client.WithPlaintext())
	case p.TLSCAPath != "":
		tlsCfg, err := buildTLSConfig(p.TLSCAPath)
		if err != nil {
			return nil, p, err
		}
		opts = append(opts, client.WithTLS(tlsCfg))
	}
	c, err := client.Dial(ctx, p.Endpoint, opts...)
	if err != nil {
		return nil, p, fmt.Errorf("dial %s: %w", p.Endpoint, err)
	}
	return c, p, nil
}

// Format returns the validated output format for the active
// profile, honouring the --output flag overlay already applied via
// Dial-time overlay (when subcommands need the format without
// dialling, they pass the Profile they received from Dial).
func Format(p clicfg.Profile) (output.Format, error) {
	return output.Validate(p.Output)
}

func overlay(p *clicfg.Profile) {
	if Flags.Endpoint != "" {
		p.Endpoint = Flags.Endpoint
	}
	if Flags.Token != "" {
		p.Token = Flags.Token
	}
	if Flags.TokenFile != "" {
		p.TokenFile = Flags.TokenFile
	}
	if Flags.TLSCAPath != "" {
		p.TLSCAPath = Flags.TLSCAPath
	}
	if Flags.Insecure {
		p.Insecure = true
	}
	if Flags.Output != "" {
		p.Output = Flags.Output
	}
	if Flags.Timeout > 0 {
		p.Timeout = Flags.Timeout
	}
}

func buildTLSConfig(caPath string) (*tls.Config, error) {
	pem, err := os.ReadFile(caPath)
	if err != nil {
		return nil, fmt.Errorf("read CA bundle %s: %w", caPath, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("CA bundle %s has no PEM certs", caPath)
	}
	return &tls.Config{RootCAs: pool}, nil
}
