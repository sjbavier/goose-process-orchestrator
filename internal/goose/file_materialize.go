package goose

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// materializeFileParameters writes validated uploaded content below a fresh
// run directory and returns a new ordinary parameter map containing only paths.
func filesystemCause(err error) error {
	var pathErr *os.PathError
	if errors.As(err, &pathErr) {
		return pathErr.Err
	}
	return err
}

func materializeFileParameters(runDir string, params, files map[string]string) (map[string]string, error) {
	merged := make(map[string]string, len(params)+len(files))
	for key, value := range params {
		merged[key] = value
	}
	if len(files) == 0 {
		return merged, nil
	}
	absRun, err := filepath.Abs(runDir)
	if err != nil {
		return nil, fmt.Errorf("materialize file parameters: resolve run directory: %w", filesystemCause(err))
	}
	inputs := filepath.Join(absRun, "inputs")
	if err := os.Mkdir(inputs, 0o700); err != nil {
		return nil, fmt.Errorf("materialize file parameters: create inputs directory: %w", filesystemCause(err))
	}
	if err := os.Chmod(inputs, 0o700); err != nil {
		return nil, fmt.Errorf("materialize file parameters: protect inputs directory: %w", filesystemCause(err))
	}
	for _, key := range sortedKeys(files) {
		path := filepath.Join(inputs, key)
		rel, err := filepath.Rel(absRun, path)
		if err != nil || rel == "." || rel == ".." || filepath.IsAbs(rel) || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return nil, errors.New("materialize file parameters: generated path escaped run directory")
		}
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return nil, fmt.Errorf("materialize file parameter %q: %w", key, err)
		}
		if err := f.Chmod(0o600); err == nil {
			_, err = f.WriteString(files[key])
		}
		closeErr := f.Close()
		if err != nil {
			return nil, fmt.Errorf("materialize file parameter %q: %w", key, err)
		}
		if closeErr != nil {
			return nil, fmt.Errorf("materialize file parameter %q: close destination: %w", key, filesystemCause(closeErr))
		}
		absolute, err := filepath.Abs(path)
		if err != nil {
			return nil, fmt.Errorf("materialize file parameter %q: %w", key, err)
		}
		merged[key] = absolute
	}
	return merged, nil
}
