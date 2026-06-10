package identitymiddleware

import "errors"

var (
	ErrMissingToken      = errors.New("missing token")
	ErrInvalidToken      = errors.New("invalid token")
	ErrInvalidIssuer     = errors.New("invalid issuer")
	ErrInvalidAudience   = errors.New("invalid audience")
	ErrTokenExpired      = errors.New("token expired")
	ErrTokenNotYetValid  = errors.New("token not yet valid")
	ErrUnauthorizedScope = errors.New("insufficient scope")
)
