package identitymiddleware

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

// GinMiddleware validates bearer tokens and attaches claims to context.
func (v *Validator) GinMiddleware(requiredScopes ...string) gin.HandlerFunc {
	return func(c *gin.Context) {
		tok := extractBearer(c.GetHeader("Authorization"))
		if tok == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": ErrMissingToken.Error()})
			return
		}
		claims, err := v.RequireScopes(tok, requiredScopes)
		if err != nil {
			status := http.StatusUnauthorized
			if err == ErrUnauthorizedScope {
				status = http.StatusForbidden
			}
			c.AbortWithStatusJSON(status, gin.H{"error": err.Error()})
			return
		}
		c.Set("identity", claims)
		c.Request = c.Request.WithContext(WithClaims(c.Request.Context(), claims))
		c.Next()
	}
}

// GinClaims returns claims stored by GinMiddleware.
func GinClaims(c *gin.Context) (*Claims, bool) {
	v, ok := c.Get("identity")
	if !ok {
		return nil, false
	}
	claims, ok := v.(*Claims)
	return claims, ok
}

func extractBearer(auth string) string {
	if auth == "" {
		return ""
	}
	parts := strings.SplitN(auth, " ", 2)
	if len(parts) != 2 || strings.ToLower(parts[0]) != "bearer" {
		return ""
	}
	return strings.TrimSpace(parts[1])
}
