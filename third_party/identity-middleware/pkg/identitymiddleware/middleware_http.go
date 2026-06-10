package identitymiddleware

import "net/http"

// HTTPMiddleware validates bearer tokens and attaches claims to request context.
func (v *Validator) HTTPMiddleware(requiredScopes ...string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tok := extractBearer(r.Header.Get("Authorization"))
			if tok == "" {
				http.Error(w, ErrMissingToken.Error(), http.StatusUnauthorized)
				return
			}
			claims, err := v.RequireScopes(tok, requiredScopes)
			if err != nil {
				status := http.StatusUnauthorized
				if err == ErrUnauthorizedScope {
					status = http.StatusForbidden
				}
				http.Error(w, err.Error(), status)
				return
			}
			r = r.WithContext(WithClaims(r.Context(), claims))
			next.ServeHTTP(w, r)
		})
	}
}
