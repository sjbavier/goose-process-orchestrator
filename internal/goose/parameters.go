package goose

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"unicode"
	"unicode/utf8"
)

// The approved bounded parameter contract. These limits are deliberately
// conservative: a recipe parameter becomes one argv element in a child process
// and a durable audit value, so the safe set is narrower than what either YAML
// or the CLI happens to tolerate.
const (
	// MaxParameters caps how many declared parameters one request may set.
	MaxParameters = 32
	// MaxParameterValueBytes caps a single value's UTF-8 length.
	MaxParameterValueBytes = 4 << 10
	// MaxParameterKeyBytes caps a key's length, matching parameterKeyPattern.
	MaxParameterKeyBytes        = 64
	MaxFileParameterValueBytes  = 256 << 10
	MaxFileParametersTotalBytes = 1 << 20
)

// parameterKeyPattern is an ASCII identifier: a letter, then up to 63 letters,
// digits, underscores, or hyphens. It excludes '=' by construction, so a key
// can never shift the boundary of the key=value argv element it is rendered in.
var parameterKeyPattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_-]{0,63}$`)

// ErrInvalidParameters wraps every rejection of a caller's parameter map.
var ErrInvalidParameters = errors.New("invalid recipe parameters")

// ValidateParameterShape enforces the approved count, key, and value limits
// without consulting a recipe's declarations. It is the shape half of the
// parameter contract; rejecting unknown and missing-required keys needs the
// recipe schema and is layered on top of this.
//
// It must run before a scheduler reservation and before any process start, so
// an invalid request never creates a task or launches a child.
func ValidateParameterShape(params map[string]string) error {
	if len(params) > MaxParameters {
		return fmt.Errorf("%w: %d parameters exceeds the maximum of %d", ErrInvalidParameters, len(params), MaxParameters)
	}
	// Iterate in sorted key order so the same bad map always reports the same
	// first offending key, instead of whichever one Go's map order surfaced.
	for _, key := range sortedKeys(params) {
		if !parameterKeyPattern.MatchString(key) {
			return fmt.Errorf("%w: key %q must match %s", ErrInvalidParameters, key, parameterKeyPattern)
		}
		if err := validateParameterValue(key, params[key]); err != nil {
			return err
		}
	}
	return nil
}

func validateParameterValue(key, value string) error {
	if len(value) > MaxParameterValueBytes {
		return fmt.Errorf("%w: value for key %q is %d bytes, exceeding the maximum of %d", ErrInvalidParameters, key, len(value), MaxParameterValueBytes)
	}
	// Invalid UTF-8 would not survive the canonical JSON encoding used for
	// durable identity, so accepting it would silently break idempotency.
	if !utf8.ValidString(value) {
		return fmt.Errorf("%w: value for key %q is not valid UTF-8", ErrInvalidParameters, key)
	}
	for _, r := range value {
		// PRM-01 evidence: Goose 1.53 silently normalized a newline in a
		// value to a space, so a line terminator cannot be passed through
		// faithfully. Reject rather than persist a value the child would
		// receive differently.
		if unicode.IsControl(r) || r == ' ' || r == ' ' {
			return fmt.Errorf("%w: value for key %q contains the disallowed control or line-terminator character %U", ErrInvalidParameters, key, r)
		}
	}
	return nil
}

// parameterArgs renders a validated map as deterministic argv pairs, sorted by
// key. Each value is its own argv element, so a value containing '=', spaces,
// or quotes is passed through exactly as given and is never shell-joined.
// PRM-01 evidence confirmed Goose preserves an '=' inside a value.
func parameterArgs(params map[string]string) []string {
	if len(params) == 0 {
		return nil
	}
	args := make([]string, 0, 2*len(params))
	for _, key := range sortedKeys(params) {
		args = append(args, "--params", key+"="+params[key])
	}
	return args
}

func sortedKeys(params map[string]string) []string {
	keys := make([]string, 0, len(params))
	for key := range params {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func ValidateRequestParameterShape(params, files map[string]string) error {
	if len(params)+len(files) > MaxParameters {
		return fmt.Errorf("%w: %d combined parameters exceeds the maximum of %d", ErrInvalidParameters, len(params)+len(files), MaxParameters)
	}
	if err := ValidateParameterShape(params); err != nil {
		return err
	}
	total := 0
	for _, key := range sortedKeys(files) {
		if !parameterKeyPattern.MatchString(key) {
			return fmt.Errorf("%w: file key %q must match %s", ErrInvalidParameters, key, parameterKeyPattern)
		}
		if _, exists := params[key]; exists {
			return fmt.Errorf("%w: key %q appears in both parameters and file_parameters", ErrInvalidParameters, key)
		}
		value := files[key]
		if len(value) > MaxFileParameterValueBytes {
			return fmt.Errorf("%w: file value for key %q is %d bytes, exceeding the maximum of %d", ErrInvalidParameters, key, len(value), MaxFileParameterValueBytes)
		}
		if !utf8.ValidString(value) {
			return fmt.Errorf("%w: file value for key %q is not valid UTF-8", ErrInvalidParameters, key)
		}
		for _, r := range value {
			if r == 0 || (unicode.IsControl(r) && r != 9 && r != 10 && r != 13) {
				return fmt.Errorf("%w: file value for key %q contains disallowed control character %U", ErrInvalidParameters, key, r)
			}
		}
		total += len(value)
	}
	if total > MaxFileParametersTotalBytes {
		return fmt.Errorf("%w: file parameter content totals %d bytes, exceeding the maximum of %d", ErrInvalidParameters, total, MaxFileParametersTotalBytes)
	}
	return nil
}
