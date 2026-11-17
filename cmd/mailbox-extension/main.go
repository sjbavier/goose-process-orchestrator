// Command mailbox-extension is the stdio MCP extension a spawned agent uses
// to read its own inbox and send task-to-task messages. Unlike cmd/mcp-shim,
// it is a real, scoped MCP server (two tools, typed input/output), not a raw
// byte relay: every call it makes to the orchestrator's mailbox listener
// carries this process's own MAILBOX_TOKEN, and the orchestrator alone
// derives the acting task id from that token (see internal/mailbox). This
// binary never trusts a caller-supplied task id or sender, and it never
// calls os/exec, so its own argv can never be influenced by message content
// or any other caller-controlled data.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/local/goose-process-orchestrator/internal/mailbox"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func main() {
	socket := flag.String("socket", "", "Unix domain socket path the orchestrator's mailbox listener serves (required)")
	flag.Parse()

	if *socket == "" {
		fmt.Fprintln(os.Stderr, "--socket is required")
		os.Exit(1)
	}
	token := os.Getenv("MAILBOX_TOKEN")
	if token == "" {
		fmt.Fprintln(os.Stderr, "MAILBOX_TOKEN environment variable is required")
		os.Exit(1)
	}

	server, err := newServer(mailbox.Client{Socket: *socket, Token: token})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := server.Run(context.Background(), &mcp.StdioTransport{}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// ReadInboxInput takes no fields: a spawned agent can only ever read its own
// inbox, identified by this process's own token, never a caller-supplied id.
type ReadInboxInput struct{}

type InboxMessage struct {
	ID        int64  `json:"id"`
	Sender    string `json:"sender"`
	Kind      string `json:"kind,omitempty"`
	Body      string `json:"body"`
	CreatedAt string `json:"created_at"`
}
type ReadInboxOutput struct {
	Messages []InboxMessage `json:"messages,omitempty"`
}

// SendMessageInput carries a recipient but never a sender: the orchestrator
// always derives the sender from this process's own MAILBOX_TOKEN.
type SendMessageInput struct {
	To   string `json:"to"`
	Kind string `json:"kind,omitempty"`
	Body string `json:"body"`
}
type SendMessageOutput struct {
	MessageID int64  `json:"message_id"`
	CreatedAt string `json:"created_at"`
}

func newServer(client mailbox.Client) (*mcp.Server, error) {
	s := mcp.NewServer(&mcp.Implementation{Name: "goose-process-orchestrator-mailbox", Version: "0.1.0"}, nil)
	mcp.AddTool(s, &mcp.Tool{Name: "read_inbox", Description: "Read and deliver this task's own undelivered inbox messages. Each message is delivered exactly once; a later call never returns the same message again."}, func(ctx context.Context, _ *mcp.CallToolRequest, in ReadInboxInput) (*mcp.CallToolResult, ReadInboxOutput, error) {
		resp, err := client.ReadInbox()
		if err != nil {
			return nil, ReadInboxOutput{}, fmt.Errorf("read_inbox: %w", err)
		}
		if !resp.OK {
			return nil, ReadInboxOutput{}, fmt.Errorf("read_inbox: %s", resp.Error)
		}
		messages := make([]InboxMessage, len(resp.Messages))
		for i, m := range resp.Messages {
			messages[i] = InboxMessage{ID: m.ID, Sender: m.Sender, Kind: m.Kind, Body: m.Body, CreatedAt: m.CreatedAt}
		}
		return nil, ReadInboxOutput{Messages: messages}, nil
	})
	mcp.AddTool(s, &mcp.Tool{Name: "send_message", Description: "Send a message to another task's inbox. The sender is always this task's own identity, derived server-side from this process's mailbox token; there is no sender argument. The recipient's inbox_policy governs whether it accepts a task-identity sender at all."}, func(ctx context.Context, _ *mcp.CallToolRequest, in SendMessageInput) (*mcp.CallToolResult, SendMessageOutput, error) {
		resp, err := client.SendMessage(in.To, in.Kind, in.Body)
		if err != nil {
			return nil, SendMessageOutput{}, fmt.Errorf("send_message: %w", err)
		}
		if !resp.OK {
			return nil, SendMessageOutput{}, fmt.Errorf("send_message: %s", resp.Error)
		}
		return nil, SendMessageOutput{MessageID: resp.MessageID, CreatedAt: resp.CreatedAt}, nil
	})
	return s, nil
}
