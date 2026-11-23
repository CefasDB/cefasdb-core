package contractbuild

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/CefasDb/cefasdb/internal/core/rf3acceptance"
	"github.com/CefasDb/cefasdb/internal/core/rf3environment"
)

// RF3PublishedFiles names the four exact files in an authorized bundle.
type RF3PublishedFiles struct {
	RootPath        string
	WorkloadPath    string
	TopologyPath    string
	PublicationPath string
}

// RF3ProtectedEvidenceVerifier resolves protected records at their authority boundary.
//
// VerifyRF3ProtectedEvidence returns the exact immutable UTF-8 bytes stored at
// evidence.Locator without normalization. The authorization adapter recomputes
// the whole-document digest and authority-subtree digest, strictly decodes the
// returned document, and compares every decision field with Expectation.
type RF3ProtectedEvidenceVerifier interface {
	VerifyRF3ProtectedEvidence(
		context.Context,
		RF3ProtectedEvidence,
	) ([]byte, error)
}

// RF3AuthorizedEnvironmentFiles names an authorized bundle and environment manifest.
type RF3AuthorizedEnvironmentFiles struct {
	Published    RF3PublishedFiles
	ManifestPath string
}

// RF3AuthorizedEnvironmentV1 combines verified authorization with the P2 contract.
type RF3AuthorizedEnvironmentV1 struct {
	Published   RF3PublishedAcceptanceV1
	Environment rf3environment.EnvironmentContractV1
}

// LoadRF3PublishedAcceptanceFiles loads the four source files and verifies every
// protected authorization record before returning a consumer-visible contract.
//
// Cancelling ctx stops file or evidence verification at its next checkpoint.
// Every error returns RF3PublishedAcceptanceV1{}.
func LoadRF3PublishedAcceptanceFiles(
	ctx context.Context,
	files RF3PublishedFiles,
	verifier RF3ProtectedEvidenceVerifier,
) (RF3PublishedAcceptanceV1, error) {
	if verifier == nil {
		return RF3PublishedAcceptanceV1{}, errors.New(
			"load RF3 published acceptance files: protected evidence verifier is required",
		)
	}
	bundle, sources, err := loadRF3PublishedAcceptanceSources(ctx, files)
	if err != nil {
		return RF3PublishedAcceptanceV1{}, err
	}
	publicationSource, err := loadRF3PublicationSource(ctx, files)
	if err != nil {
		return RF3PublishedAcceptanceV1{}, err
	}
	published, evidence, err := decodeRF3PublishedDocuments(
		sources,
		publicationSource,
		bundle,
	)
	if err != nil {
		return RF3PublishedAcceptanceV1{}, err
	}
	if err := verifyRF3ProtectedEvidence(ctx, verifier, evidence, &published); err != nil {
		return RF3PublishedAcceptanceV1{}, err
	}
	return published, nil
}

// LoadAuthorizedRF3EnvironmentFiles loads a P2 environment only after the P1
// publication and all six protected records pass verification.
//
// Every error returns RF3AuthorizedEnvironmentV1{}; the legacy
// LoadRF3EnvironmentFiles function remains a semantic fixture loader and does
// not establish authorization.
func LoadAuthorizedRF3EnvironmentFiles(
	ctx context.Context,
	files RF3AuthorizedEnvironmentFiles,
	verifier RF3ProtectedEvidenceVerifier,
) (RF3AuthorizedEnvironmentV1, error) {
	published, err := LoadRF3PublishedAcceptanceFiles(ctx, files.Published, verifier)
	if err != nil {
		return RF3AuthorizedEnvironmentV1{}, err
	}
	environment, err := loadRF3EnvironmentManifest(
		ctx,
		published.Bundle,
		files.Published.RootPath,
		files.ManifestPath,
		openRF3AcceptanceFile,
	)
	if err != nil {
		return RF3AuthorizedEnvironmentV1{}, err
	}
	return RF3AuthorizedEnvironmentV1{
		Published:   published,
		Environment: environment,
	}, nil
}

func loadRF3PublishedAcceptanceSources(
	ctx context.Context,
	files RF3PublishedFiles,
) (rf3acceptance.Bundle, [3]rf3acceptance.Source, error) {
	if ctx == nil {
		return rf3acceptance.Bundle{}, [3]rf3acceptance.Source{}, errors.New(
			"load RF3 published acceptance files: context is required",
		)
	}
	if err := ctx.Err(); err != nil {
		return rf3acceptance.Bundle{}, [3]rf3acceptance.Source{}, fmt.Errorf(
			"load RF3 published acceptance files: %w",
			err,
		)
	}
	inputs, err := newRF3AcceptanceInputs(rf3AcceptancePaths{
		root:     files.RootPath,
		workload: files.WorkloadPath,
		topology: files.TopologyPath,
	})
	if err != nil {
		return rf3acceptance.Bundle{}, [3]rf3acceptance.Source{}, err
	}
	return readAndDecodeRF3PublishedAcceptance(ctx, inputs)
}

func readAndDecodeRF3PublishedAcceptance(
	ctx context.Context,
	inputs []rf3AcceptanceInput,
) (rf3acceptance.Bundle, [3]rf3acceptance.Source, error) {
	opened, operationErr := openRF3AcceptanceInputs(ctx, inputs, openRF3AcceptanceFile)
	if operationErr == nil {
		operationErr = readRF3AcceptanceInputs(ctx, inputs)
	}
	closeErr := closeRF3AcceptanceInputs(inputs[:opened])
	if operationErr == nil {
		if err := ctx.Err(); err != nil {
			operationErr = fmt.Errorf("load RF3 published acceptance files: %w", err)
		}
	}
	if err := errors.Join(operationErr, closeErr); err != nil {
		return rf3acceptance.Bundle{}, [3]rf3acceptance.Source{}, err
	}
	sources, err := newRF3AcceptanceSources(inputs)
	if err != nil {
		return rf3acceptance.Bundle{}, [3]rf3acceptance.Source{}, err
	}
	bundle, err := rf3acceptance.DecodeBundle(sources[0], sources[1], sources[2])
	if err != nil {
		return rf3acceptance.Bundle{}, [3]rf3acceptance.Source{}, err
	}
	return bundle, sources, nil
}

func loadRF3PublicationSource(
	ctx context.Context,
	files RF3PublishedFiles,
) (rf3acceptance.Source, error) {
	if err := ctx.Err(); err != nil {
		return rf3acceptance.Source{}, fmt.Errorf(
			"load RF3 publication file: %w",
			err,
		)
	}
	sourcePath, err := filepath.Rel(filepath.Dir(files.RootPath), files.PublicationPath)
	if err != nil {
		return rf3acceptance.Source{}, fmt.Errorf(
			"derive publication source path %q from root %q: %w",
			files.PublicationPath,
			files.RootPath,
			err,
		)
	}
	input := rf3AcceptanceInput{
		role:       "publication",
		path:       files.PublicationPath,
		sourcePath: filepath.ToSlash(sourcePath),
	}
	reader, err := openRF3AcceptanceFile(input.path)
	if err != nil {
		return rf3acceptance.Source{}, fmt.Errorf(
			"open publication %q: %w",
			input.path,
			err,
		)
	}
	if reader == nil {
		return rf3acceptance.Source{}, fmt.Errorf(
			"open publication %q: opener returned a nil reader",
			input.path,
		)
	}
	input.reader = reader
	data, operationErr := readRF3AcceptanceInput(ctx, input)
	input.data = data
	closeErr := reader.Close()
	if closeErr != nil {
		closeErr = fmt.Errorf("close publication %q: %w", input.path, closeErr)
	}
	if operationErr == nil {
		if err := ctx.Err(); err != nil {
			operationErr = fmt.Errorf("load RF3 publication file: %w", err)
		}
	}
	if err := errors.Join(operationErr, closeErr); err != nil {
		return rf3acceptance.Source{}, err
	}
	source, err := rf3acceptance.NewSource(input.sourcePath, input.data)
	if err != nil {
		return rf3acceptance.Source{}, fmt.Errorf(
			"construct publication source for %q as %q: %w",
			input.path,
			input.sourcePath,
			err,
		)
	}
	return source, nil
}
