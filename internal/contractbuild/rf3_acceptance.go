package contractbuild

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/CefasDb/cefasdb/internal/core/rf3acceptance"
)

const maxRF3AcceptanceFileBytes = 1 << 20

type rf3AcceptancePaths struct {
	root     string
	workload string
	topology string
}

type rf3AcceptanceInput struct {
	role       string
	path       string
	sourcePath string
	reader     io.ReadCloser
	data       []byte
}

type rf3ContextReader struct {
	ctx    context.Context
	reader io.Reader
}

// LoadRF3AcceptanceFiles loads one immutable RF3 acceptance bundle from the
// three caller-supplied files.
//
// Cancelling ctx stops a read at its next checkpoint. Every error returns
// rf3acceptance.Bundle{}, and every opened file is closed before return.
func LoadRF3AcceptanceFiles(
	ctx context.Context,
	rootPath, workloadPath, topologyPath string,
) (rf3acceptance.Bundle, error) {
	paths := rf3AcceptancePaths{
		root:     rootPath,
		workload: workloadPath,
		topology: topologyPath,
	}
	return loadRF3AcceptanceFiles(ctx, paths, openRF3AcceptanceFile)
}

func openRF3AcceptanceFile(path string) (io.ReadCloser, error) {
	// #nosec G304 -- the adapter must open the exact path supplied by its trusted caller.
	return os.Open(path)
}

func loadRF3AcceptanceFiles(
	ctx context.Context,
	paths rf3AcceptancePaths,
	openFile func(string) (io.ReadCloser, error),
) (rf3acceptance.Bundle, error) {
	if ctx == nil {
		return rf3acceptance.Bundle{}, errors.New("load RF3 acceptance files: context is required")
	}
	if err := ctx.Err(); err != nil {
		return rf3acceptance.Bundle{}, fmt.Errorf("load RF3 acceptance files: %w", err)
	}
	if openFile == nil {
		return rf3acceptance.Bundle{}, errors.New("load RF3 acceptance files: opener is required")
	}

	inputs, err := newRF3AcceptanceInputs(paths)
	if err != nil {
		return rf3acceptance.Bundle{}, err
	}
	opened, operationErr := openRF3AcceptanceInputs(ctx, inputs, openFile)
	if operationErr == nil {
		operationErr = readRF3AcceptanceInputs(ctx, inputs)
	}
	closeErr := closeRF3AcceptanceInputs(inputs[:opened])
	if operationErr == nil {
		if err := ctx.Err(); err != nil {
			operationErr = fmt.Errorf("load RF3 acceptance files: %w", err)
		}
	}
	if err := errors.Join(operationErr, closeErr); err != nil {
		return rf3acceptance.Bundle{}, err
	}

	sources, err := newRF3AcceptanceSources(inputs)
	if err != nil {
		return rf3acceptance.Bundle{}, err
	}
	return rf3acceptance.DecodeBundle(sources[0], sources[1], sources[2])
}

func newRF3AcceptanceInputs(paths rf3AcceptancePaths) ([]rf3AcceptanceInput, error) {
	sourcePaths, err := rf3AcceptanceSourcePaths(paths)
	if err != nil {
		return nil, err
	}
	return []rf3AcceptanceInput{
		{role: "root", path: paths.root, sourcePath: sourcePaths.root},
		{role: "workload", path: paths.workload, sourcePath: sourcePaths.workload},
		{role: "topology", path: paths.topology, sourcePath: sourcePaths.topology},
	}, nil
}

func rf3AcceptanceSourcePaths(paths rf3AcceptancePaths) (rf3AcceptancePaths, error) {
	rootDirectory := filepath.Dir(paths.root)
	workload, err := filepath.Rel(rootDirectory, paths.workload)
	if err != nil {
		return rf3AcceptancePaths{}, fmt.Errorf(
			"derive workload source path %q from root %q: %w",
			paths.workload,
			paths.root,
			err,
		)
	}
	topology, err := filepath.Rel(rootDirectory, paths.topology)
	if err != nil {
		return rf3AcceptancePaths{}, fmt.Errorf(
			"derive topology source path %q from root %q: %w",
			paths.topology,
			paths.root,
			err,
		)
	}
	return rf3AcceptancePaths{
		root:     filepath.ToSlash(filepath.Base(paths.root)),
		workload: filepath.ToSlash(workload),
		topology: filepath.ToSlash(topology),
	}, nil
}

func openRF3AcceptanceInputs(
	ctx context.Context,
	inputs []rf3AcceptanceInput,
	openFile func(string) (io.ReadCloser, error),
) (int, error) {
	for index := range inputs {
		if err := ctx.Err(); err != nil {
			return index, fmt.Errorf("open %s %q: %w", inputs[index].role, inputs[index].path, err)
		}
		reader, err := openFile(inputs[index].path)
		if err != nil {
			return index, fmt.Errorf("open %s %q: %w", inputs[index].role, inputs[index].path, err)
		}
		if reader == nil {
			return index, fmt.Errorf("open %s %q: opener returned a nil reader", inputs[index].role, inputs[index].path)
		}
		inputs[index].reader = reader
	}
	return len(inputs), nil
}

func readRF3AcceptanceInputs(ctx context.Context, inputs []rf3AcceptanceInput) error {
	for index := range inputs {
		data, err := readRF3AcceptanceInput(ctx, inputs[index])
		if err != nil {
			return err
		}
		inputs[index].data = data
	}
	return nil
}

func readRF3AcceptanceInput(ctx context.Context, input rf3AcceptanceInput) ([]byte, error) {
	reader := io.LimitReader(
		rf3ContextReader{ctx: ctx, reader: input.reader},
		maxRF3AcceptanceFileBytes+1,
	)
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("read %s %q: %w", input.role, input.path, err)
	}
	if len(data) > maxRF3AcceptanceFileBytes {
		return nil, fmt.Errorf(
			"read %s %q: file exceeds %d bytes",
			input.role,
			input.path,
			maxRF3AcceptanceFileBytes,
		)
	}
	return data, nil
}

func closeRF3AcceptanceInputs(inputs []rf3AcceptanceInput) error {
	var result error
	for index := len(inputs) - 1; index >= 0; index-- {
		if err := inputs[index].reader.Close(); err != nil {
			result = errors.Join(
				result,
				fmt.Errorf("close %s %q: %w", inputs[index].role, inputs[index].path, err),
			)
		}
	}
	return result
}

func newRF3AcceptanceSources(inputs []rf3AcceptanceInput) ([3]rf3acceptance.Source, error) {
	var sources [3]rf3acceptance.Source
	for index := range inputs {
		source, err := rf3acceptance.NewSource(inputs[index].sourcePath, inputs[index].data)
		if err != nil {
			return [3]rf3acceptance.Source{}, fmt.Errorf(
				"construct %s source for %q as %q: %w",
				inputs[index].role,
				inputs[index].path,
				inputs[index].sourcePath,
				err,
			)
		}
		sources[index] = source
	}
	return sources, nil
}

func (reader rf3ContextReader) Read(data []byte) (int, error) {
	if err := reader.ctx.Err(); err != nil {
		return 0, err
	}
	count, err := reader.reader.Read(data)
	if contextErr := reader.ctx.Err(); contextErr != nil {
		return count, contextErr
	}
	return count, err
}
