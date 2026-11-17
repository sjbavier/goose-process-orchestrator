package mailbox

import "time"

// timeLayout is the one wire timestamp format this protocol uses, matching
// the rest of this project's MCP-facing timestamps (see mcpserver.output).
const timeLayout = time.RFC3339

// Op values. Request.Op must be exactly one of these.
const (
	OpReadInbox   = "read_inbox"
	OpSendMessage = "send_message"
)

// Request is the one-shot wire request a Client sends and Serve/Handle
// reads: exactly one per connection. Token is the only identity claim; To is
// a recipient task id supplied by the caller for OpSendMessage, but the
// sender is never part of this struct — Handle derives it solely by
// resolving Token.
type Request struct {
	Token string `json:"token"`
	Op    string `json:"op"`
	To    string `json:"to,omitempty"`
	Kind  string `json:"kind,omitempty"`
	Body  string `json:"body,omitempty"`
}

// WireMessage is one task_messages row as returned over the wire.
type WireMessage struct {
	ID        int64  `json:"id"`
	Sender    string `json:"sender"`
	Kind      string `json:"kind,omitempty"`
	Body      string `json:"body"`
	CreatedAt string `json:"created_at"`
}

// Response is the one-shot wire response matching a Request. Error is set
// and OK is false for any rejection, whether token resolution, shape
// validation, inbox_policy, quota, or an unknown task id — Response never
// distinguishes those with a typed error code, so a caller cannot branch on
// a rejection reason finer than "it failed."
type Response struct {
	OK        bool          `json:"ok"`
	Error     string        `json:"error,omitempty"`
	Messages  []WireMessage `json:"messages,omitempty"`
	MessageID int64         `json:"message_id,omitempty"`
	CreatedAt string        `json:"created_at,omitempty"`
}
