package mailbox

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/local/goose-process-orchestrator/internal/tasks"
)

// fakeStore is a minimal in-memory double satisfying Store, instrumented to
// prove Handle never reaches it for a rejected token.
type fakeStore struct {
	enqueueCalls int
	inboxCalls   int
	// inboxPolicy, when set, rejects an Enqueue whose sender does not match
	// allowedSender, mirroring tasks.ErrInboxClosed.
	allowedSender string
	messages      map[string][]tasks.Message
}

func (f *fakeStore) Enqueue(ctx context.Context, taskID, sender, kind, body string) (tasks.Message, error) {
	f.enqueueCalls++
	if f.allowedSender != "" && sender != f.allowedSender {
		return tasks.Message{}, tasks.ErrInboxClosed
	}
	msg := tasks.Message{ID: int64(f.enqueueCalls), TaskID: taskID, Sender: sender, Kind: kind, Body: body, CreatedAt: time.Unix(0, 0).UTC()}
	if f.messages == nil {
		f.messages = make(map[string][]tasks.Message)
	}
	f.messages[taskID] = append(f.messages[taskID], msg)
	return msg, nil
}

func (f *fakeStore) ReadInbox(ctx context.Context, taskID string) ([]tasks.Message, error) {
	f.inboxCalls++
	return f.messages[taskID], nil
}

func TestHandleRejectsUnknownTokenBeforeAnyStoreAccess(t *testing.T) {
	tokens := NewTokens()
	store := &fakeStore{}

	resp := Handle(context.Background(), tokens, store, Request{Token: "not-minted", Op: OpReadInbox})
	if resp.OK {
		t.Fatal("expected rejection for an unminted token")
	}
	if !errors.Is(ErrUnknownToken, ErrUnknownToken) {
		t.Fatal("sanity: ErrUnknownToken must be comparable")
	}
	if resp.Error != ErrUnknownToken.Error() {
		t.Fatalf("resp.Error = %q, want %q", resp.Error, ErrUnknownToken.Error())
	}
	if store.inboxCalls != 0 || store.enqueueCalls != 0 {
		t.Fatalf("store was reached for a rejected token: inboxCalls=%d enqueueCalls=%d", store.inboxCalls, store.enqueueCalls)
	}

	resp = Handle(context.Background(), tokens, store, Request{Token: "", Op: OpSendMessage, To: "task-b", Body: "hi"})
	if resp.OK || store.enqueueCalls != 0 {
		t.Fatal("empty token must be rejected before any store access")
	}
}

func TestHandleReadInboxReturnsOnlyTheTokensOwnTask(t *testing.T) {
	tokens := NewTokens()
	tokenA, _ := tokens.Mint("task-a")
	tokenB, _ := tokens.Mint("task-b")
	store := &fakeStore{messages: map[string][]tasks.Message{
		"task-a": {{ID: 1, TaskID: "task-a", Sender: "controller", Body: "for a"}},
		"task-b": {{ID: 2, TaskID: "task-b", Sender: "controller", Body: "for b"}},
	}}

	respA := Handle(context.Background(), tokens, store, Request{Token: tokenA, Op: OpReadInbox})
	if !respA.OK || len(respA.Messages) != 1 || respA.Messages[0].Body != "for a" {
		t.Fatalf("task-a's token got %+v", respA)
	}
	respB := Handle(context.Background(), tokens, store, Request{Token: tokenB, Op: OpReadInbox})
	if !respB.OK || len(respB.Messages) != 1 || respB.Messages[0].Body != "for b" {
		t.Fatalf("task-b's token got %+v", respB)
	}
	// Nothing in Request lets tokenA's caller ask for task-b's inbox: there is
	// no target field for OpReadInbox at all, so cross-task reads are
	// impossible by construction, not by a runtime check.
}

func TestHandleSendMessageStampsSenderFromTokenNeverFromRequest(t *testing.T) {
	tokens := NewTokens()
	token, _ := tokens.Mint("task-a")
	store := &fakeStore{}

	resp := Handle(context.Background(), tokens, store, Request{Token: token, Op: OpSendMessage, To: "task-b", Kind: "note", Body: "hello"})
	if !resp.OK {
		t.Fatalf("send_message rejected: %+v", resp)
	}
	got := store.messages["task-b"]
	if len(got) != 1 || got[0].Sender != "task-a" {
		t.Fatalf("stored message = %+v, want sender task-a", got)
	}
}

func TestHandleSendMessageRequiresRecipient(t *testing.T) {
	tokens := NewTokens()
	token, _ := tokens.Mint("task-a")
	store := &fakeStore{}

	resp := Handle(context.Background(), tokens, store, Request{Token: token, Op: OpSendMessage, Body: "hello"})
	if resp.OK || store.enqueueCalls != 0 {
		t.Fatalf("expected rejection for a missing recipient, got %+v", resp)
	}
}

func TestHandleSendMessageRejectedByRecipientInboxPolicyBeforeStorage(t *testing.T) {
	tokens := NewTokens()
	token, _ := tokens.Mint("task-a")
	// allowedSender simulates a controller_only recipient: only "controller"
	// may enqueue, so a task-identity sender (task-a) is rejected.
	store := &fakeStore{allowedSender: "controller"}

	resp := Handle(context.Background(), tokens, store, Request{Token: token, Op: OpSendMessage, To: "task-b", Body: "hello"})
	if resp.OK {
		t.Fatal("expected rejection for a controller_only recipient")
	}
	if resp.Error != tasks.ErrInboxClosed.Error() {
		t.Fatalf("resp.Error = %q, want %q", resp.Error, tasks.ErrInboxClosed.Error())
	}
	if len(store.messages["task-b"]) != 0 {
		t.Fatal("a rejected message must never reach storage")
	}
}

func TestHandleUnknownOp(t *testing.T) {
	tokens := NewTokens()
	token, _ := tokens.Mint("task-a")
	store := &fakeStore{}

	resp := Handle(context.Background(), tokens, store, Request{Token: token, Op: "delete_everything"})
	if resp.OK {
		t.Fatal("an unknown op must be rejected")
	}
}
