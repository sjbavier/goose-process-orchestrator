package tasks

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
)

func TestEnqueueAndReadInboxPreservesOrderAndMarksDeliveredOnce(t *testing.T) {
	ctx := context.Background()
	_, store := openSQLiteStore(t)
	if _, _, err := store.Create(ctx, Task{ID: "task-1", Recipe: "allowed.yaml", IdempotencyKey: "key-1"}); err != nil {
		t.Fatal(err)
	}

	first, err := store.Enqueue(ctx, "task-1", ControllerSender, "instruction", "do the first thing")
	if err != nil {
		t.Fatalf("Enqueue first: %v", err)
	}
	second, err := store.Enqueue(ctx, "task-1", ControllerSender, "instruction", "do the second thing")
	if err != nil {
		t.Fatalf("Enqueue second: %v", err)
	}
	if first.ID >= second.ID {
		t.Fatalf("message ids not ascending: %d, %d", first.ID, second.ID)
	}
	if first.DeliveredAt != nil {
		t.Fatalf("Enqueue must not mark delivered: %+v", first)
	}

	messages, err := store.ReadInbox(ctx, "task-1")
	if err != nil {
		t.Fatalf("ReadInbox: %v", err)
	}
	if len(messages) != 2 || messages[0].Body != "do the first thing" || messages[1].Body != "do the second thing" {
		t.Fatalf("ReadInbox order/content = %+v", messages)
	}
	for _, m := range messages {
		if m.DeliveredAt == nil {
			t.Fatalf("message %d not marked delivered: %+v", m.ID, m)
		}
	}

	// A message already marked delivered must not be redelivered.
	again, err := store.ReadInbox(ctx, "task-1")
	if err != nil {
		t.Fatalf("ReadInbox again: %v", err)
	}
	if len(again) != 0 {
		t.Fatalf("ReadInbox again = %+v, want empty", again)
	}
}

func TestEnqueueRejectsTaskSenderUnderControllerOnlyPolicy(t *testing.T) {
	ctx := context.Background()
	db, store := openSQLiteStore(t)
	if _, _, err := store.Create(ctx, Task{ID: "task-1", Recipe: "allowed.yaml", IdempotencyKey: "key-1"}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Create(ctx, Task{ID: "task-2", Recipe: "allowed.yaml", IdempotencyKey: "key-2"}); err != nil {
		t.Fatal(err)
	}

	policy, err := store.InboxPolicy(ctx, "task-1")
	if err != nil || policy != ControllerOnly {
		t.Fatalf("default InboxPolicy = (%q, %v), want (controller_only, nil)", policy, err)
	}

	if _, err := store.Enqueue(ctx, "task-1", "task-2", "note", "hello from a peer"); !errors.Is(err, ErrInboxClosed) {
		t.Fatalf("Enqueue from task sender under controller_only = %v, want ErrInboxClosed", err)
	}
	assertNoRows(t, db, "task-1")

	// The controller is always accepted under the default policy.
	if _, err := store.Enqueue(ctx, "task-1", ControllerSender, "note", "hello from the controller"); err != nil {
		t.Fatalf("Enqueue from controller under controller_only: %v", err)
	}

	// Opening the recipient's inbox allows a task sender.
	if _, err := db.Exec(`UPDATE tasks SET inbox_policy = 'open' WHERE id = ?`, "task-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Enqueue(ctx, "task-1", "task-2", "note", "hello from a peer, now allowed"); err != nil {
		t.Fatalf("Enqueue from task sender under open: %v", err)
	}
}

func TestEnqueueFailsClosedOnUnknownTaskBeforeStorage(t *testing.T) {
	ctx := context.Background()
	db, store := openSQLiteStore(t)
	if _, err := store.Enqueue(ctx, "missing-task", ControllerSender, "note", "hi"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Enqueue for unknown task = %v, want ErrNotFound", err)
	}
	assertNoRows(t, db, "missing-task")
}

func TestEnqueueRejectsOversizeAndMalformedMessagesBeforeStorage(t *testing.T) {
	ctx := context.Background()
	db, store := openSQLiteStore(t)
	if _, _, err := store.Create(ctx, Task{ID: "task-1", Recipe: "allowed.yaml", IdempotencyKey: "key-1"}); err != nil {
		t.Fatal(err)
	}

	cases := map[string]struct {
		sender, kind, body string
	}{
		"empty sender":      {"", "note", "body"},
		"empty body":        {ControllerSender, "note", ""},
		"oversize body":     {ControllerSender, "note", strings.Repeat("x", MaxMessageBodyBytes+1)},
		"oversize kind":     {ControllerSender, strings.Repeat("k", MaxMessageKindBytes+1), "body"},
		"control char body": {ControllerSender, "note", "bad\x00byte"},
		"invalid utf8 body": {ControllerSender, "note", "bad\xff\xfebody"},
	}
	for name, c := range cases {
		if _, err := store.Enqueue(ctx, "task-1", c.sender, c.kind, c.body); !errors.Is(err, ErrInvalidMessage) {
			t.Errorf("%s: Enqueue = %v, want ErrInvalidMessage", name, err)
		}
	}
	assertNoRows(t, db, "task-1")
}

func TestEnqueueEnforcesBoundedInboxQuota(t *testing.T) {
	ctx := context.Background()
	_, store := openSQLiteStore(t)
	if _, _, err := store.Create(ctx, Task{ID: "task-1", Recipe: "allowed.yaml", IdempotencyKey: "key-1"}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < MaxPendingMessages; i++ {
		if _, err := store.Enqueue(ctx, "task-1", ControllerSender, "note", "filler"); err != nil {
			t.Fatalf("Enqueue %d: %v", i, err)
		}
	}
	if _, err := store.Enqueue(ctx, "task-1", ControllerSender, "note", "one too many"); !errors.Is(err, ErrInboxFull) {
		t.Fatalf("Enqueue past quota = %v, want ErrInboxFull", err)
	}

	// Delivering the backlog frees room again.
	if _, err := store.ReadInbox(ctx, "task-1"); err != nil {
		t.Fatalf("ReadInbox: %v", err)
	}
	if _, err := store.Enqueue(ctx, "task-1", ControllerSender, "note", "room again"); err != nil {
		t.Fatalf("Enqueue after delivery freed quota: %v", err)
	}
}

func assertNoRows(t *testing.T, db *sql.DB, taskID string) {
	t.Helper()
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM task_messages WHERE task_id = ?`, taskID).Scan(&count); err != nil {
		t.Fatalf("count task_messages: %v", err)
	}
	if count != 0 {
		t.Fatalf("task_messages for %q = %d rows, want 0", taskID, count)
	}
}
