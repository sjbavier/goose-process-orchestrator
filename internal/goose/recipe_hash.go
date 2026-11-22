package goose

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
)

// ErrRecipeHashMismatch means the resolved recipe's bytes do not hash to the
// SHA-256 the caller expected.
var ErrRecipeHashMismatch = errors.New("recipe_sha256_mismatch")

// NormalizeSHA256 lower-cases and validates a hex SHA-256 string. An empty
// input is returned unchanged (meaning "no expectation").
func NormalizeSHA256(s string) (string, error) {
	s = strings.ToLower(strings.TrimSpace(s))
	if s == "" {
		return "", nil
	}
	if len(s) != 64 {
		return "", fmt.Errorf("expect_recipe_sha256 must be 64 hex characters")
	}
	if _, err := hex.DecodeString(s); err != nil {
		return "", fmt.Errorf("expect_recipe_sha256 must be hex: %w", err)
	}
	return s, nil
}

// RecipeSHA256 resolves recipe through the same allowlist as launch and
// returns the hex SHA-256 of the exact file bytes.
func (c Config) RecipeSHA256(recipe string) (string, error) {
	path, err := approvedFile(c.RecipeRoot, recipe)
	if err != nil {
		return "", err
	}
	return hashFile(path)
}

func hashFile(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

// NormalizeSHA256 adapts the package function to scheduler.RecipeHasher.
func (c Config) NormalizeSHA256(s string) (string, error) { return NormalizeSHA256(s) }
