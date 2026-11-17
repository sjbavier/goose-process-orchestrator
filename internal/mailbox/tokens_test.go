package mailbox

import "testing"

func TestTokensMintResolveRevoke(t *testing.T) {
	tokens := NewTokens()
	token, err := tokens.Mint("task-a")
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}
	if token == "" {
		t.Fatal("Mint returned an empty token")
	}
	taskID, err := tokens.Resolve(token)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if taskID != "task-a" {
		t.Fatalf("Resolve returned %q, want task-a", taskID)
	}

	tokens.Revoke("task-a")
	if _, err := tokens.Resolve(token); err != ErrUnknownToken {
		t.Fatalf("Resolve after Revoke = %v, want ErrUnknownToken", err)
	}
}

func TestTokensMintRequiresTaskID(t *testing.T) {
	tokens := NewTokens()
	if _, err := tokens.Mint(""); err == nil {
		t.Fatal("Mint(\"\") should fail")
	}
}

func TestTokensMintIsUnique(t *testing.T) {
	tokens := NewTokens()
	first, err := tokens.Mint("task-a")
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}
	second, err := tokens.Mint("task-a")
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}
	if first == second {
		t.Fatal("two mints for the same task produced the same token")
	}
	// Both remain valid until an explicit Revoke.
	for _, token := range []string{first, second} {
		if taskID, err := tokens.Resolve(token); err != nil || taskID != "task-a" {
			t.Fatalf("Resolve(%q) = %q, %v", token, taskID, err)
		}
	}
}

func TestTokensResolveUnknownOrEmpty(t *testing.T) {
	tokens := NewTokens()
	if _, err := tokens.Resolve(""); err != ErrUnknownToken {
		t.Fatalf("Resolve(\"\") = %v, want ErrUnknownToken", err)
	}
	if _, err := tokens.Resolve("never-minted"); err != ErrUnknownToken {
		t.Fatalf("Resolve(unknown) = %v, want ErrUnknownToken", err)
	}
}

func TestTokensCrossTaskIsolation(t *testing.T) {
	tokens := NewTokens()
	tokenA, err := tokens.Mint("task-a")
	if err != nil {
		t.Fatalf("Mint task-a: %v", err)
	}
	tokenB, err := tokens.Mint("task-b")
	if err != nil {
		t.Fatalf("Mint task-b: %v", err)
	}
	resolvedA, err := tokens.Resolve(tokenA)
	if err != nil || resolvedA != "task-a" {
		t.Fatalf("Resolve(tokenA) = %q, %v, want task-a", resolvedA, err)
	}
	resolvedB, err := tokens.Resolve(tokenB)
	if err != nil || resolvedB != "task-b" {
		t.Fatalf("Resolve(tokenB) = %q, %v, want task-b", resolvedB, err)
	}
	if resolvedA == resolvedB {
		t.Fatal("distinct tasks resolved to the same task id")
	}
}
