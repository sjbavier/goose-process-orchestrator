// Package mailbox is the per-task-token-authenticated transport a spawned
// agent uses to read its own inbox and send task-to-task messages, without
// ever trusting a caller-supplied task id or sender. The orchestrator alone
// mints tokens and derives the acting task id from one at call time; a
// connection never gets to say who it is.
package mailbox

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"sync"
)

// ErrUnknownToken means the token is empty, malformed, or was never minted
// by this Tokens (or has since been revoked). It never distinguishes those
// cases to a caller: all are the same "not authorized" outcome.
var ErrUnknownToken = errors.New("mailbox token is unknown or has been revoked")

// Tokens is an in-memory, concurrency-safe registry mapping a per-task
// mailbox token to the task id it identifies. It is the only path from a
// token back to a task id; nothing else in this package accepts a
// caller-supplied task id as an identity claim.
type Tokens struct {
	mu      sync.RWMutex
	byToken map[string]string
}

// NewTokens returns an empty registry.
func NewTokens() *Tokens {
	return &Tokens{byToken: make(map[string]string)}
}

// Mint generates a fresh, cryptographically random token for taskID and
// registers it. Minting again for the same taskID adds another valid token
// for it rather than invalidating the previous one; call Revoke first for
// single-token-per-task semantics.
func (t *Tokens) Mint(taskID string) (string, error) {
	if taskID == "" {
		return "", errors.New("task id is required to mint a mailbox token")
	}
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	token := base64.RawURLEncoding.EncodeToString(buf)
	t.mu.Lock()
	t.byToken[token] = taskID
	t.mu.Unlock()
	return token, nil
}

// Resolve returns the task id a token was minted for, or ErrUnknownToken.
func (t *Tokens) Resolve(token string) (string, error) {
	if token == "" {
		return "", ErrUnknownToken
	}
	t.mu.RLock()
	taskID, ok := t.byToken[token]
	t.mu.RUnlock()
	if !ok {
		return "", ErrUnknownToken
	}
	return taskID, nil
}

// Revoke invalidates every token minted for taskID. Safe to call when none
// exist.
func (t *Tokens) Revoke(taskID string) {
	t.mu.Lock()
	for token, id := range t.byToken {
		if id == taskID {
			delete(t.byToken, token)
		}
	}
	t.mu.Unlock()
}
