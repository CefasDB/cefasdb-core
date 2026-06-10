// Package fileloader resolves the `file://path` form aws-cli accepts
// on flags like --item or --request-items. Plain inline JSON passes
// through unchanged so callers can use the same loader regardless of
// the input shape.
package fileloader

import (
	"fmt"
	"os"
	"strings"
)

// Load returns the raw bytes for `arg`. When arg starts with the
// "file://" prefix the remainder is read from disk; otherwise the
// argument bytes are returned as-is.
func Load(arg string) ([]byte, error) {
	if strings.HasPrefix(arg, "file://") {
		path := strings.TrimPrefix(arg, "file://")
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", path, err)
		}
		return b, nil
	}
	return []byte(arg), nil
}
