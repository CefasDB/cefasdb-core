package identitymiddleware

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Validator validates RS256 access tokens using JWKS.
type Validator struct {
	cfg    Config
	client *http.Client

	mu        sync.Mutex
	keys      map[string]*rsaKey
	allKeys   []*rsaKey
	fetchedAt time.Time
}

type rsaKey struct {
	kid string
	pub interface{}
}

// NewValidator creates a validator with JWKS cache.
func NewValidator(cfg Config) (*Validator, error) {
	cfg.normalize()
	if strings.TrimSpace(cfg.JwksURL) == "" {
		return nil, errors.New("jwks url is required")
	}
	return &Validator{
		cfg: cfg,
		client: &http.Client{
			Timeout: cfg.HTTPTimeout,
		},
		keys:    make(map[string]*rsaKey),
		allKeys: []*rsaKey{},
	}, nil
}

// Validate parses and validates an access token.
func (v *Validator) Validate(tokenString string) (*Claims, error) {
	if strings.TrimSpace(tokenString) == "" {
		return nil, ErrMissingToken
	}

	kid, err := tokenKid(tokenString)
	if err != nil {
		return nil, ErrInvalidToken
	}

	key, err := v.keyForKid(kid)
	if err != nil {
		return nil, err
	}

	parser := jwt.NewParser(jwt.WithValidMethods([]string{"RS256"}), jwt.WithoutClaimsValidation())
	tok, err := parser.Parse(tokenString, func(_ *jwt.Token) (interface{}, error) {
		return key, nil
	})
	if err != nil || !tok.Valid {
		return nil, ErrInvalidToken
	}

	claimsMap, ok := tok.Claims.(jwt.MapClaims)
	if !ok {
		return nil, ErrInvalidToken
	}

	if err := v.validateRegistered(claimsMap); err != nil {
		return nil, err
	}

	out := &Claims{Raw: map[string]any{}}
	for k, val := range claimsMap {
		out.Raw[k] = val
	}
	out.Subject = getStringClaim(claimsMap, "sub")
	out.Email = getStringClaim(claimsMap, "email")
	out.TenantID = getStringClaim(claimsMap, "tid")
	out.Issuer = getStringClaim(claimsMap, "iss")
	out.Audience = audiencesFromClaims(claimsMap["aud"])
	out.Scopes = parseScopes(claimsMap[v.cfg.ScopeClaim], v.cfg.ScopeSeparator)
	out.EventTypes = parseScopes(claimsMap["eventTypes"], ",")
	out.ExpiresAt = getTimeClaim(claimsMap, "exp")
	out.IssuedAt = getTimeClaim(claimsMap, "iat")
	return out, nil
}

// RequireScopes validates token and enforces required scopes.
func (v *Validator) RequireScopes(tokenString string, required []string) (*Claims, error) {
	claims, err := v.Validate(tokenString)
	if err != nil {
		return nil, err
	}
	if !claims.HasAllScopes(required) {
		return nil, ErrUnauthorizedScope
	}
	return claims, nil
}

func (v *Validator) validateRegistered(claims jwt.MapClaims) error {
	now := time.Now()
	skew := v.cfg.ClockSkew
	if exp := getTimeClaim(claims, "exp"); !exp.IsZero() {
		if now.After(exp.Add(skew)) {
			return ErrTokenExpired
		}
	}
	if nbf := getTimeClaim(claims, "nbf"); !nbf.IsZero() {
		if now.Before(nbf.Add(-skew)) {
			return ErrTokenNotYetValid
		}
	}
	if iat := getTimeClaim(claims, "iat"); !iat.IsZero() {
		if now.Before(iat.Add(-skew)) {
			return ErrTokenNotYetValid
		}
	}
	if v.cfg.Issuer != "" {
		if iss := getStringClaim(claims, "iss"); iss != v.cfg.Issuer {
			return ErrInvalidIssuer
		}
	}
	if audiences := v.cfg.audienceList(); len(audiences) > 0 {
		if !audienceAllowed(audiences, audiencesFromClaims(claims["aud"])) {
			return ErrInvalidAudience
		}
	}
	return nil
}

func (v *Validator) keyForKid(kid string) (interface{}, error) {
	keys, all, err := v.loadKeys(false)
	if err != nil {
		return nil, err
	}
	if kid != "" {
		if k, ok := keys[kid]; ok {
			return k.pub, nil
		}
	}
	keys, all, err = v.loadKeys(true)
	if err != nil {
		return nil, err
	}
	if kid != "" {
		if k, ok := keys[kid]; ok {
			return k.pub, nil
		}
	}
	if kid == "" && len(all) == 1 {
		return all[0].pub, nil
	}
	if kid != "" && len(all) == 1 {
		return all[0].pub, nil
	}
	return nil, ErrInvalidToken
}

func (v *Validator) loadKeys(force bool) (map[string]*rsaKey, []*rsaKey, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if !force && time.Since(v.fetchedAt) < v.cfg.CacheTTL && len(v.keys) > 0 {
		return v.keys, v.allKeys, nil
	}

	v.logf("identitymw: fetching jwks from %s", v.cfg.JwksURL)

	ctx, cancel := context.WithTimeout(context.Background(), v.cfg.HTTPTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, v.cfg.JwksURL, nil)
	if err != nil {
		return nil, nil, err
	}
	resp, err := v.client.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return nil, nil, fmt.Errorf("jwks fetch failed: status %d", resp.StatusCode)
	}
	data, err := ioReadAll(resp.Body)
	if err != nil {
		return nil, nil, err
	}
	parsed, all, err := parseJWKS(data)
	if err != nil {
		return nil, nil, err
	}
	keys := make(map[string]*rsaKey)
	allKeys := make([]*rsaKey, 0, len(all))
	for kid, pub := range parsed {
		keys[kid] = &rsaKey{kid: kid, pub: pub}
	}
	for _, pub := range all {
		allKeys = append(allKeys, &rsaKey{pub: pub})
	}
	v.keys = keys
	v.allKeys = allKeys
	v.fetchedAt = time.Now()
	return keys, allKeys, nil
}

func tokenKid(tokenString string) (string, error) {
	parts := strings.Split(tokenString, ".")
	if len(parts) < 2 {
		return "", errors.New("token malformed")
	}
	b, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return "", err
	}
	var header map[string]any
	if err := json.Unmarshal(b, &header); err != nil {
		return "", err
	}
	if alg, ok := header["alg"].(string); ok {
		if alg != "RS256" {
			return "", ErrInvalidToken
		}
	}
	kid, _ := header["kid"].(string)
	return kid, nil
}

func audienceAllowed(required []string, tokenAud []string) bool {
	if len(required) == 0 {
		return true
	}
	set := make(map[string]struct{}, len(tokenAud))
	for _, a := range tokenAud {
		set[a] = struct{}{}
	}
	for _, r := range required {
		if _, ok := set[r]; ok {
			return true
		}
	}
	return false
}

func audiencesFromClaims(v any) []string {
	switch t := v.(type) {
	case string:
		if t == "" {
			return nil
		}
		return []string{t}
	case []any:
		out := make([]string, 0, len(t))
		for _, item := range t {
			if s, ok := item.(string); ok {
				if s != "" {
					out = append(out, s)
				}
			}
		}
		return out
	case []string:
		out := make([]string, 0, len(t))
		for _, s := range t {
			if s != "" {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}

func getStringClaim(claims jwt.MapClaims, key string) string {
	if v, ok := claims[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func getTimeClaim(claims jwt.MapClaims, key string) time.Time {
	v, ok := claims[key]
	if !ok {
		return time.Time{}
	}
	switch t := v.(type) {
	case float64:
		return time.Unix(int64(t), 0)
	case int64:
		return time.Unix(t, 0)
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return time.Unix(i, 0)
		}
	}
	return time.Time{}
}

func ioReadAll(r io.Reader) ([]byte, error) {
	b, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (v *Validator) logf(format string, args ...any) {
	if v.cfg.Logger == nil {
		return
	}
	v.cfg.Logger.Printf(format, args...)
}
