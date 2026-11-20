package goose

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	yaml "go.yaml.in/yaml/v3"
)

// MaxRecipeBytes caps how much of an allowlisted recipe the schema loader will
// read. Recipes are operator-authored files, but the loader is an admission
// control path, so it is bounded rather than trusting the file's size.
const MaxRecipeBytes = 1 << 20

// ErrRecipeSchema wraps every failure to establish a recipe's declared
// parameter contract. It is distinct from ErrInvalidParameters: this one means
// "the recipe's declarations could not be trusted", not "the caller's input was
// malformed". Both fail closed.
var ErrRecipeSchema = errors.New("recipe parameter schema is unusable")

// requirementRequired is the only requirement value that makes a declared
// parameter mandatory. Other values (notably optional) are recorded as declared
// but not demanded; an interactive requirement cannot be satisfied by this
// non-interactive launcher either way, so it is not treated as mandatory here.
const requirementRequired = "required"

// declaredInputTypeString is the only input type this feature supplies. A
// caller may not pass a value for a parameter declared as another type, because
// only string values have a proven argv and persistence contract.
const declaredInputTypeString = "string"

// Declaration is one parameter a recipe declares. Only the fields this
// admission check needs are decoded; every other recipe field is ignored, and
// the recipe is never re-serialized or executed by this package.
type Declaration struct {
	Key         string `yaml:"key"`
	InputType   string `yaml:"input_type"`
	Requirement string `yaml:"requirement"`
}

// recipeSchema is the minimal view of a recipe document.
type recipeSchema struct {
	Parameters []Declaration `yaml:"parameters"`
	Prompt     string        `yaml:"prompt"`
}

// LoadDeclarations reads an already containment-checked recipe path and returns
// its declared parameters. It fails closed: an unreadable, oversized, or
// undecodable recipe is an error, never an empty declaration set, because an
// empty set would silently accept any parameter map as "no unknown keys".
func LoadDeclarations(recipePath string) ([]Declaration, error) {
	file, err := os.Open(recipePath)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrRecipeSchema, err)
	}
	defer file.Close()
	// One byte past the cap distinguishes "exactly at the limit" from "too big".
	contents, err := io.ReadAll(io.LimitReader(file, MaxRecipeBytes+1))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrRecipeSchema, err)
	}
	if len(contents) > MaxRecipeBytes {
		return nil, fmt.Errorf("%w: recipe exceeds the %d byte limit", ErrRecipeSchema, MaxRecipeBytes)
	}
	var schema recipeSchema
	if err := yaml.Unmarshal(contents, &schema); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrRecipeSchema, err)
	}
	seen := make(map[string]bool, len(schema.Parameters))
	for _, declaration := range schema.Parameters {
		if declaration.Key == "" {
			return nil, fmt.Errorf("%w: a declared parameter has no key", ErrRecipeSchema)
		}
		if seen[declaration.Key] {
			return nil, fmt.Errorf("%w: parameter %q is declared more than once", ErrRecipeSchema, declaration.Key)
		}
		seen[declaration.Key] = true
	}
	return schema.Parameters, nil
}

// ValidateRequest resolves a recipe against this Config's allowlisted root and
// enforces the whole parameter contract against it. It is the admission-time
// entry point: callers use it to reject a request before reserving scheduler
// capacity or creating a task, and it starts no process.
func (c Config) ValidateRequest(recipe string, params map[string]string) error {
	return c.ValidateRequestWithFiles(recipe, params, nil)
}
func (c Config) ValidateRequestWithFiles(recipe string, params, files map[string]string) error {
	if err := ValidateRequestParameterShape(params, files); err != nil {
		return err
	}
	resolved, err := approvedFile(c.RecipeRoot, recipe)
	if err != nil {
		return err
	}
	declarations, err := loadRunnableRecipe(resolved)
	if err != nil {
		return err
	}
	return validateAgainstDeclarations(declarations, params, files)
}

// loadRunnableRecipe verifies the recipe can run through Goose's headless
// --recipe path and returns its parameter declarations. A blank prompt is an
// admission error because Goose cannot supply one separately in this mode.
func loadRunnableRecipe(recipePath string) ([]Declaration, error) {
	file, err := os.Open(recipePath)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrRecipeSchema, err)
	}
	defer file.Close()
	contents, err := io.ReadAll(io.LimitReader(file, MaxRecipeBytes+1))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrRecipeSchema, err)
	}
	if len(contents) > MaxRecipeBytes {
		return nil, fmt.Errorf("%w: recipe exceeds the %d byte limit", ErrRecipeSchema, MaxRecipeBytes)
	}
	var schema recipeSchema
	if err := yaml.Unmarshal(contents, &schema); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrRecipeSchema, err)
	}
	if strings.TrimSpace(schema.Prompt) == "" {
		return nil, fmt.Errorf("%w: recipe has no prompt; headless goose run requires one", ErrRecipeSchema)
	}
	if _, err := LoadDeclarations(recipePath); err != nil {
		return nil, err
	}
	return schema.Parameters, nil
}

// validateDeclaredParameters is the declaration half of the contract, for
// callers that have already validated the input's shape.
func validateDeclaredParameters(recipePath string, params map[string]string) error {
	declarations, err := LoadDeclarations(recipePath)
	if err != nil {
		return err
	}
	return validateAgainstDeclarations(declarations, params, nil)
}

// ValidateParameters enforces the whole parameter contract for one request: the
// approved value shape, then the recipe's own declarations. It reads the recipe
// but starts no process, so an invalid request is rejected before a scheduler
// reservation and before any child launch.
func ValidateParameters(recipePath string, params map[string]string) error {
	if err := ValidateParameterShape(params); err != nil {
		return err
	}
	declarations, err := LoadDeclarations(recipePath)
	if err != nil {
		return err
	}
	return validateAgainstDeclarations(declarations, params, nil)
}

func validateAgainstDeclarations(declarations []Declaration, params, files map[string]string) error {
	declared := make(map[string]Declaration, len(declarations))
	for _, declaration := range declarations {
		declared[declaration.Key] = declaration
	}
	all := make(map[string]string, len(params)+len(files))
	for key, value := range params {
		all[key] = value
	}
	for key, value := range files {
		all[key] = value
	}
	// Sorted iteration keeps the reported first offender independent of map order.
	for _, key := range sortedKeys(all) {
		declaration, ok := declared[key]
		if !ok {
			return fmt.Errorf("%w: key %q is not declared by this recipe (declared: %s)", ErrInvalidParameters, key, declaredKeyList(declarations))
		}
		if declaration.InputType != "" && declaration.InputType != declaredInputTypeString {
			return fmt.Errorf("%w: key %q is declared as input_type %q, and only %q values can be supplied", ErrInvalidParameters, key, declaration.InputType, declaredInputTypeString)
		}
	}
	for _, declaration := range declarations {
		if declaration.Requirement != requirementRequired {
			continue
		}
		if _, scalar := params[declaration.Key]; !scalar {
			if _, file := files[declaration.Key]; file {
				continue
			}
			return fmt.Errorf("%w: required key %q was not supplied", ErrInvalidParameters, declaration.Key)
		}
	}
	return nil
}

func declaredKeyList(declarations []Declaration) string {
	if len(declarations) == 0 {
		return "none"
	}
	keys := make([]string, 0, len(declarations))
	for _, declaration := range declarations {
		keys = append(keys, declaration.Key)
	}
	sort.Strings(keys)
	return strings.Join(keys, ", ")
}
