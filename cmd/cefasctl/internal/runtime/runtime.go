// Package runtime owns cefasctl connection/session options plus the
// Dial helper. The root command binds flags into a Session and
// subcommands resolve the active session through context, keeping the
// import graph cycle-free.
package runtime

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	"github.com/CefasDb/cefasdb/cmd/cefasctl/internal/clicfg"
	"github.com/CefasDb/cefasdb/cmd/cefasctl/internal/output"
	"github.com/CefasDb/cefasdb/pkg/client"
	"github.com/CefasDb/cefasdb/pkg/types"
)

// Options is the complete CLI connection/runtime option set.
type Options struct {
	ConfigPath  string
	ProfileName string
	Endpoint    string
	Token       string
	TokenFile   string
	TLSCAPath   string
	Insecure    bool
	Output      string
	Timeout     time.Duration
	NoStream    bool
}

// Flags is the legacy binding target for callers that build only a
// subcommand tree in tests. New code should pass a Session in context.
var Flags Options

// Session is the mutable state for one CLI or REPL session.
type Session struct {
	options    Options
	tableCache map[string]types.TableDescriptor
}

type sessionContextKey struct{}

// NewSession returns a session initialized with opts.
func NewSession(opts Options) *Session {
	return &Session{options: opts, tableCache: map[string]types.TableDescriptor{}}
}

// Options returns a copy of the current session options.
func (s *Session) Options() Options {
	if s == nil {
		return Options{}
	}
	return s.options
}

// BindTarget returns the live options object for flag binding.
func (s *Session) BindTarget() *Options {
	if s == nil {
		return &Flags
	}
	return &s.options
}

// Update replaces the session options atomically for single-threaded
// CLI usage.
func (s *Session) Update(opts Options) {
	if s != nil {
		if connectionOptionsChanged(s.options, opts) {
			s.tableCache = map[string]types.TableDescriptor{}
		}
		s.options = opts
	}
}

// CachedTable returns a table descriptor cached for this REPL session.
func (s *Session) CachedTable(name string) (types.TableDescriptor, bool) {
	if s == nil || s.tableCache == nil {
		return types.TableDescriptor{}, false
	}
	td, ok := s.tableCache[name]
	return td, ok
}

// CacheTable stores a table descriptor for later simple REPL commands.
func (s *Session) CacheTable(td types.TableDescriptor) {
	if s == nil || td.Name == "" {
		return
	}
	if s.tableCache == nil {
		s.tableCache = map[string]types.TableDescriptor{}
	}
	s.tableCache[td.Name] = td
}

// ClearCachedTable removes one descriptor from the session cache.
func (s *Session) ClearCachedTable(name string) {
	if s == nil || s.tableCache == nil {
		return
	}
	delete(s.tableCache, name)
}

// WithSession attaches a CLI session to ctx.
func WithSession(ctx context.Context, s *Session) context.Context {
	if s == nil {
		return ctx
	}
	return context.WithValue(ctx, sessionContextKey{}, s)
}

// FromContext returns the active CLI session, if any.
func FromContext(ctx context.Context) *Session {
	if ctx == nil {
		return nil
	}
	s, _ := ctx.Value(sessionContextKey{}).(*Session)
	return s
}

// ResolveProfile resolves the active profile (flag > env > yaml >
// defaults) without opening a network connection.
func ResolveProfile(ctx context.Context) (clicfg.Profile, error) {
	opts := optionsFromContext(ctx)
	p, err := clicfg.LoadProfile(opts.ConfigPath, opts.ProfileName)
	if err != nil {
		return p, err
	}
	clicfg.ApplyEnv(&p)
	overlay(&p, opts)
	return p, nil
}

// Dial resolves the active profile (flag > env > yaml > defaults)
// and returns a connected client.Client. Caller owns Close.
func Dial(ctx context.Context) (*client.Client, clicfg.Profile, error) {
	p, err := ResolveProfile(ctx)
	if err != nil {
		return nil, p, err
	}
	return dialProfile(ctx, p)
}

// DialEndpoint is Dial with an explicit endpoint override. It keeps the
// active profile's auth and TLS settings, which lets commands fan out
// to several cluster nodes without changing the global session.
func DialEndpoint(ctx context.Context, endpoint string) (*client.Client, clicfg.Profile, error) {
	p, err := ResolveProfile(ctx)
	if err != nil {
		return nil, p, err
	}
	if endpoint != "" {
		p.Endpoint = endpoint
	}
	return dialProfile(ctx, p)
}

func dialProfile(ctx context.Context, p clicfg.Profile) (*client.Client, clicfg.Profile, error) {
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

func optionsFromContext(ctx context.Context) Options {
	if s := FromContext(ctx); s != nil {
		return s.Options()
	}
	return Flags
}

func overlay(p *clicfg.Profile, opts Options) {
	if opts.Endpoint != "" {
		p.Endpoint = opts.Endpoint
	}
	if opts.Token != "" {
		p.Token = opts.Token
	}
	if opts.TokenFile != "" {
		p.TokenFile = opts.TokenFile
	}
	if opts.TLSCAPath != "" {
		p.TLSCAPath = opts.TLSCAPath
	}
	if opts.Insecure {
		p.Insecure = true
	}
	if opts.Output != "" {
		p.Output = opts.Output
	}
	if opts.Timeout > 0 {
		p.Timeout = opts.Timeout
	}
}

func connectionOptionsChanged(a, b Options) bool {
	return a.ConfigPath != b.ConfigPath ||
		a.ProfileName != b.ProfileName ||
		a.Endpoint != b.Endpoint ||
		a.Token != b.Token ||
		a.TokenFile != b.TokenFile ||
		a.TLSCAPath != b.TLSCAPath ||
		a.Insecure != b.Insecure
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
