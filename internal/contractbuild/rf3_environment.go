package contractbuild

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"

	"github.com/CefasDb/cefasdb/internal/core/rf3acceptance"
	"github.com/CefasDb/cefasdb/internal/core/rf3environment"
)

const maxRF3EnvironmentFileBytes = 1 << 20

// LoadRF3EnvironmentFiles loads one complete RF3-P1 and environment contract
// from the four explicit caller-supplied files.
//
// Cancelling ctx stops a read at its next checkpoint. Every error returns
// rf3environment.EnvironmentContractV1{}, and every opened file is closed
// before return.
func LoadRF3EnvironmentFiles(
	ctx context.Context,
	rootPath, workloadPath, topologyPath, manifestPath string,
) (rf3environment.EnvironmentContractV1, error) {
	acceptance, err := LoadRF3AcceptanceFiles(ctx, rootPath, workloadPath, topologyPath)
	if err != nil {
		return rf3environment.EnvironmentContractV1{}, err
	}
	return loadRF3EnvironmentManifest(
		ctx,
		acceptance,
		rootPath,
		manifestPath,
		openRF3AcceptanceFile,
	)
}

func loadRF3EnvironmentManifest(
	ctx context.Context,
	acceptance rf3acceptance.Bundle,
	rootPath, manifestPath string,
	openFile func(string) (io.ReadCloser, error),
) (rf3environment.EnvironmentContractV1, error) {
	if ctx == nil {
		return rf3environment.EnvironmentContractV1{}, errors.New(
			"load RF3 environment files: context is required",
		)
	}
	if err := ctx.Err(); err != nil {
		return rf3environment.EnvironmentContractV1{}, fmt.Errorf(
			"load RF3 environment files: %w",
			err,
		)
	}
	if openFile == nil {
		return rf3environment.EnvironmentContractV1{}, errors.New(
			"load RF3 environment files: opener is required",
		)
	}

	sourcePath, err := rf3EnvironmentSourcePath(rootPath, manifestPath)
	if err != nil {
		return rf3environment.EnvironmentContractV1{}, err
	}
	reader, err := openFile(manifestPath)
	if err != nil {
		return rf3environment.EnvironmentContractV1{}, fmt.Errorf(
			"open environment manifest %q: %w",
			manifestPath,
			err,
		)
	}
	if reader == nil {
		return rf3environment.EnvironmentContractV1{}, fmt.Errorf(
			"open environment manifest %q: opener returned a nil reader",
			manifestPath,
		)
	}

	data, operationErr := readRF3EnvironmentManifest(ctx, reader, manifestPath)
	closeErr := reader.Close()
	if closeErr != nil {
		closeErr = fmt.Errorf("close environment manifest %q: %w", manifestPath, closeErr)
	}
	if operationErr == nil {
		if err := ctx.Err(); err != nil {
			operationErr = fmt.Errorf("load RF3 environment files: %w", err)
		}
	}
	if err := errors.Join(operationErr, closeErr); err != nil {
		return rf3environment.EnvironmentContractV1{}, err
	}

	source, err := rf3acceptance.NewSource(sourcePath, data)
	if err != nil {
		return rf3environment.EnvironmentContractV1{}, fmt.Errorf(
			"construct environment manifest source for %q as %q: %w",
			manifestPath,
			sourcePath,
			err,
		)
	}
	manifest, err := rf3environment.DecodeManifestV1(source, acceptance)
	if err != nil {
		return rf3environment.EnvironmentContractV1{}, err
	}
	return manifest.Contract(), nil
}

func rf3EnvironmentSourcePath(rootPath, manifestPath string) (string, error) {
	relative, err := filepath.Rel(filepath.Dir(rootPath), manifestPath)
	if err != nil {
		return "", fmt.Errorf(
			"derive environment manifest source path %q from root %q: %w",
			manifestPath,
			rootPath,
			err,
		)
	}
	return filepath.ToSlash(relative), nil
}

func readRF3EnvironmentManifest(
	ctx context.Context,
	reader io.Reader,
	manifestPath string,
) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(
		rf3ContextReader{ctx: ctx, reader: reader},
		maxRF3EnvironmentFileBytes+1,
	))
	if err != nil {
		return nil, fmt.Errorf("read environment manifest %q: %w", manifestPath, err)
	}
	if len(data) > maxRF3EnvironmentFileBytes {
		return nil, fmt.Errorf(
			"read environment manifest %q: file exceeds %d bytes",
			manifestPath,
			maxRF3EnvironmentFileBytes,
		)
	}
	return data, nil
}
