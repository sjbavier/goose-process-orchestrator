package mailbox

import (
	"context"
	"fmt"

	"github.com/local/goose-process-orchestrator/internal/tasks"
)

// Store is exactly the subset of tasks.Store one mailbox call needs.
type Store interface {
	Enqueue(ctx context.Context, taskID, sender, kind, body string) (tasks.Message, error)
	ReadInbox(ctx context.Context, taskID string) ([]tasks.Message, error)
}

// Handle resolves req.Token to a task id and performs exactly the operation
// it names, deriving the acting identity from the token alone: req never
// carries a sender or an identity-bearing task id for Handle to trust
// directly. Token resolution happens before any Store access, so an
// unknown/malformed token never reaches storage. A token scoped to task A
// can only ever read or send-as task A — OpReadInbox has no caller-supplied
// target at all, and OpSendMessage's To is a recipient, never the sender.
func Handle(ctx context.Context, tokens *Tokens, store Store, req Request) Response {
	taskID, err := tokens.Resolve(req.Token)
	if err != nil {
		return Response{Error: err.Error()}
	}
	switch req.Op {
	case OpReadInbox:
		messages, err := store.ReadInbox(ctx, taskID)
		if err != nil {
			return Response{Error: err.Error()}
		}
		return Response{OK: true, Messages: toWire(messages)}
	case OpSendMessage:
		if req.To == "" {
			return Response{Error: "to is required"}
		}
		msg, err := store.Enqueue(ctx, req.To, taskID, req.Kind, req.Body)
		if err != nil {
			return Response{Error: err.Error()}
		}
		return Response{OK: true, MessageID: msg.ID, CreatedAt: msg.CreatedAt.Format(timeLayout)}
	default:
		return Response{Error: fmt.Sprintf("unknown op %q", req.Op)}
	}
}

func toWire(messages []tasks.Message) []WireMessage {
	out := make([]WireMessage, len(messages))
	for i, m := range messages {
		out[i] = WireMessage{ID: m.ID, Sender: m.Sender, Kind: m.Kind, Body: m.Body, CreatedAt: m.CreatedAt.Format(timeLayout)}
	}
	return out
}
