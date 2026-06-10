package identitymiddleware

import "time"

// Config controls validation and JWKS cache behavior.
type Config struct {
	Issuer         string
	Audience       string
	Audiences      []string
	JwksURL        string
	CacheTTL       time.Duration
	HTTPTimeout    time.Duration
	ClockSkew      time.Duration
	ScopeClaim     string
	ScopeSeparator string
	Logger         Logger
}

// Logger is an optional logger for debug output.
type Logger interface {
	Printf(format string, args ...any)
}

func (c *Config) normalize() {
	if c.CacheTTL == 0 {
		c.CacheTTL = 5 * time.Minute
	}
	if c.HTTPTimeout == 0 {
		c.HTTPTimeout = 5 * time.Second
	}
	if c.ClockSkew == 0 {
		c.ClockSkew = 30 * time.Second
	}
	if c.ScopeClaim == "" {
		c.ScopeClaim = "scope"
	}
	if c.ScopeSeparator == "" {
		c.ScopeSeparator = " "
	}
}

func (c *Config) audienceList() []string {
	if len(c.Audiences) > 0 {
		return c.Audiences
	}
	if c.Audience != "" {
		return []string{c.Audience}
	}
	return nil
}
