// Package contractbuild contains filesystem and source adapters for compatibility tooling.
package contractbuild

import (
	"fmt"
	"os"

	"github.com/CefasDb/cefasdb/internal/core/compatibility"
)

// LoadRegistryFiles opens the registry and policy authoring files, delegates
// their semantics to compatibility.DecodeRegistry, and closes both files.
func LoadRegistryFiles(registryPath, policyPath string) (compatibility.WorkingRegistry, error) {
	registryFile, err := os.Open(registryPath)
	if err != nil {
		return compatibility.WorkingRegistry{}, fmt.Errorf("open registry %s: %w", registryPath, err)
	}

	policyFile, err := os.Open(policyPath)
	if err != nil {
		_ = registryFile.Close()
		return compatibility.WorkingRegistry{}, fmt.Errorf("open policy %s: %w", policyPath, err)
	}

	working, decodeErr := compatibility.DecodeRegistry(registryFile, policyFile)
	policyCloseErr := policyFile.Close()
	registryCloseErr := registryFile.Close()

	if decodeErr != nil {
		return compatibility.WorkingRegistry{}, decodeErr
	}
	if registryCloseErr != nil {
		return compatibility.WorkingRegistry{}, fmt.Errorf("close registry %s: %w", registryPath, registryCloseErr)
	}
	if policyCloseErr != nil {
		return compatibility.WorkingRegistry{}, fmt.Errorf("close policy %s: %w", policyPath, policyCloseErr)
	}
	return working, nil
}
