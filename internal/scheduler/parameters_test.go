package scheduler

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// recordingValidator stands in for the launcher's Config. It records what it
// was asked to validate so a test can prove admission consulted it with the
// request's own recipe, and it can be told to reject.
type recordingValidator struct {
	calls  int
	recipe string
	params map[string]string
	files  map[string]string
	err    error
}

func (v *recordingValidator) ValidateRequestWithFiles(recipe string, params, files map[string]string) error {
	v.calls++
	v.recipe, v.params, v.files = recipe, params, files
	return v.err
}

func TestSpawnValidatesParametersBeforeReservingCapacity(t *testing.T) {
	store := testStore(t)
	runsRoot := t.TempDir()
	sched := testScheduler(t, store, nil, 1, 1)
	validator := &recordingValidator{err: errors.New("declaration rejected")}
	service, err := NewService(store, sched, func() string { return "task-1" }, runsRoot, WithParameterValidator(validator))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	_, err = service.Spawn(ctx, SpawnRequest{Recipe: "allowed.yaml", IdempotencyKey: "k1", FileParameters: map[string]string{"k": "multi\nline"}})
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("Spawn = %v, want ErrInvalidRequest", err)
	}
	if !errorsContains(err, "declaration rejected") {
		t.Fatalf("Spawn error %v should surface the validator's reason", err)
	}
	if validator.calls != 1 || validator.recipe != "allowed.yaml" || validator.files["k"] != "multi\nline" {
		t.Fatalf("validator was consulted %d times with recipe %q and files %#v", validator.calls, validator.recipe, validator.files)
	}
	// No capacity was consumed and no task row exists: the single-slot
	// scheduler must still admit a subsequent valid request.
	if listed, err := service.List(ctx, "", "", 100); err != nil || len(listed) != 0 {
		t.Fatalf("rejected request left %d task rows (err %v)", len(listed), err)
	}
	validator.err = nil
	if _, err := service.Spawn(ctx, SpawnRequest{Recipe: "allowed.yaml", IdempotencyKey: "k2", Parameters: map[string]string{"k": "v"}}); err != nil {
		t.Fatalf("a valid request after a rejection = %v; capacity was leaked", err)
	}
}

func TestSpawnPersistsValidatedParameters(t *testing.T) {
	store := testStore(t)
	runsRoot := t.TempDir()
	sched := testScheduler(t, store, nil, 1, 1)
	service, err := NewService(store, sched, func() string { return "task-1" }, runsRoot, WithParameterValidator(&recordingValidator{}))
	if err != nil {
		t.Fatal(err)
	}
	params := map[string]string{"feature_slug": "x", "mode": "local"}
	task, err := service.Spawn(context.Background(), SpawnRequest{Recipe: "allowed.yaml", IdempotencyKey: "k1", Parameters: params})
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	stored, err := service.Status(context.Background(), task.ID)
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if len(stored.Parameters) != 2 || stored.Parameters["feature_slug"] != "x" {
		t.Fatalf("persisted parameters = %v", stored.Parameters)
	}
}

// TestSpawnWithoutAValidatorFailsClosed proves a service that was never given a
// validator refuses parameters instead of admitting unvalidated input.
func TestSpawnWithoutAValidatorFailsClosed(t *testing.T) {
	store := testStore(t)
	runsRoot := t.TempDir()
	sched := testScheduler(t, store, nil, 1, 1)
	service, err := NewService(store, sched, func() string { return "task-1" }, runsRoot)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := service.Spawn(ctx, SpawnRequest{Recipe: "allowed.yaml", IdempotencyKey: "k1", Parameters: map[string]string{"k": "v"}}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("Spawn with no validator = %v, want ErrInvalidRequest", err)
	}
	// A parameterless request on the same service is unaffected.
	if _, err := service.Spawn(ctx, SpawnRequest{Recipe: "allowed.yaml", IdempotencyKey: "k2"}); err != nil {
		t.Fatalf("parameterless Spawn = %v, want success", err)
	}
}

func errorsContains(err error, substring string) bool {
	return err != nil && strings.Contains(err.Error(), substring)
}

func TestSpawnValidatesParameterlessRecipeBeforeReservingCapacity(t *testing.T) {
	store := testStore(t)
	runsRoot := t.TempDir()
	sched := testScheduler(t, store, nil, 1, 1)
	validator := &recordingValidator{err: errors.New("recipe has no prompt; headless goose run requires one")}
	service, err := NewService(store, sched, func() string { return "task-1" }, runsRoot, WithParameterValidator(validator))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	_, err = service.Spawn(ctx, SpawnRequest{Recipe: "promptless.yaml", IdempotencyKey: "k1"})
	if !errors.Is(err, ErrInvalidRequest) || !errorsContains(err, "recipe has no prompt") {
		t.Fatalf("Spawn(prompt-less) = %v", err)
	}
	if validator.calls != 1 || validator.recipe != "promptless.yaml" {
		t.Fatalf("validator calls = %d with %q", validator.calls, validator.recipe)
	}
	listed, listErr := service.List(ctx, "", "", 100)
	if listErr != nil || len(listed) != 0 {
		t.Fatalf("rejected request left %d rows (err %v)", len(listed), listErr)
	}
}

type hashingValidator struct {
	recordingValidator
	sha string
}

func (v *hashingValidator) RecipeSHA256(string) (string, error) { return v.sha, nil }
func (v *hashingValidator) NormalizeSHA256(s string) (string, error) {
	return strings.ToLower(s), nil
}

func TestSpawnRecipeSHAExpectation(t *testing.T) {
	store := testStore(t)
	runsRoot := t.TempDir()
	sched := testScheduler(t, store, nil, 1, 1)
	sha := strings.Repeat("a", 64)
	ids := []string{"task-1", "task-2"}
	service, err := NewService(store, sched, func() string { id := ids[0]; ids = ids[1:]; return id }, runsRoot, WithParameterValidator(&hashingValidator{sha: sha}))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	_, err = service.Spawn(ctx, SpawnRequest{Recipe: "r.yaml", IdempotencyKey: "k1", ExpectRecipeSHA256: strings.Repeat("b", 64)})
	if !errors.Is(err, ErrRecipeHashMismatch) {
		t.Fatalf("Spawn = %v, want ErrRecipeHashMismatch", err)
	}
	if listed, _ := service.List(ctx, "", "", 100); len(listed) != 0 {
		t.Fatalf("mismatch left %d rows", len(listed))
	}
	got, err := service.Spawn(ctx, SpawnRequest{Recipe: "r.yaml", IdempotencyKey: "k2", ExpectRecipeSHA256: strings.ToUpper(sha), ExpectProvider: "p", ExpectModel: "m"})
	if err != nil {
		t.Fatalf("matching spawn = %v (capacity leaked by the rejection?)", err)
	}
	row, err := store.Get(ctx, got.ID)
	if err != nil || row.RecipeSHA256 != sha || row.ExpectRecipeSHA256 != sha || row.ExpectProvider != "p" || row.ExpectModel != "m" {
		t.Fatalf("row = %+v err %v", row, err)
	}
}
