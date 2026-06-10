package identitymiddleware

import "strings"

func parseScopes(v any, sep string) []string {
	if v == nil {
		return nil
	}
	switch t := v.(type) {
	case string:
		return splitScopes(t, sep)
	case []any:
		out := make([]string, 0, len(t))
		for _, item := range t {
			if s, ok := item.(string); ok {
				if s = strings.TrimSpace(s); s != "" {
					out = append(out, s)
				}
			}
		}
		return out
	case []string:
		out := make([]string, 0, len(t))
		for _, s := range t {
			if s = strings.TrimSpace(s); s != "" {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}

func splitScopes(in string, sep string) []string {
	if sep == "" {
		sep = " "
	}
	parts := strings.Split(in, sep)
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		out = append(out, p)
	}
	return out
}
