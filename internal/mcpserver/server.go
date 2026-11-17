package mcpserver

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/local/goose-process-orchestrator/internal/logs"
	"github.com/local/goose-process-orchestrator/internal/scheduler"
	"github.com/local/goose-process-orchestrator/internal/tasks"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type Service interface {
	Spawn(context.Context, scheduler.SpawnRequest) (tasks.Task, error)
	Status(context.Context, string) (tasks.Task, error)
	Result(context.Context, string) (tasks.Task, error)
	Cancel(context.Context, string) (tasks.Task, error)
	List(ctx context.Context, status string, limit int) ([]tasks.Task, error)
	Logs(ctx context.Context, id, stream string, maxBytes int) (scheduler.LogsResult, error)
	Progress(ctx context.Context, id string) (logs.Progress, error)
	SendMessage(ctx context.Context, id, body, kind string) (tasks.Message, error)
}
type SpawnInput struct {
	Recipe         string `json:"recipe"`
	IdempotencyKey string `json:"idempotency_key"`
	// MaxTurns overrides the server's --max-turns default for this one task
	// when positive; omit it (or send 0) to use the server default.
	MaxTurns int `json:"max_turns,omitempty"`
	// Parameters supplies the recipe's declared string parameters. Keys must
	// be declared by the recipe and every required declaration must be
	// present. Values are durable local audit data and are also written to
	// run artifacts, so they must never contain secrets. Parameters are
	// deliberately absent from every response: see TaskOutput.
	Parameters map[string]string `json:"parameters,omitempty"`
	// InboxPolicy selects the spawned task's mailbox policy. Omit it (or
	// send "") for the default controller_only policy; "open" allows any
	// sender to message this task's inbox.
	InboxPolicy string `json:"inbox_policy,omitempty"`
}
type IDInput struct {
	ID string `json:"id"`
}

// TaskOutput deliberately omits the task's recipe parameters. Responses are
// the widest-read surface here, and parameter values are request input that a
// caller already holds, so echoing them only broadens exposure.
type TaskOutput struct {
	ID              string `json:"id"`
	Status          string `json:"status"`
	Result          string `json:"result,omitempty"`
	CancelRequested bool   `json:"cancel_requested"`
}
type ListInput struct {
	// Status, when non-empty, filters to exactly that task status.
	Status string `json:"status,omitempty"`
	// Limit bounds how many tasks are returned; non-positive or omitted
	// means the server default, and any value is still capped at the
	// server-side maximum.
	Limit int `json:"limit,omitempty"`
}
type TaskListItem struct {
	ID              string `json:"id"`
	Recipe          string `json:"recipe"`
	Status          string `json:"status"`
	CreatedAt       string `json:"created_at"`
	UpdatedAt       string `json:"updated_at"`
	CancelRequested bool   `json:"cancel_requested"`
}
type ListOutput struct {
	Tasks []TaskListItem `json:"tasks"`
}
type LogsInput struct {
	ID string `json:"id"`
	// Stream selects "stdout", "stderr", or "" / "both" (default both).
	Stream string `json:"stream,omitempty"`
	// MaxBytes bounds the tail returned per stream; non-positive or
	// omitted means the server default (logs.DefaultTailBytes), and any
	// value is still capped at the server-side maximum
	// (logs.MaxTailBytes) regardless of what is requested.
	MaxBytes int `json:"max_bytes,omitempty"`
}
type LogsOutput struct {
	Stdout string `json:"stdout,omitempty"`
	// StdoutTruncated is true when bytes before Stdout were dropped
	// because the captured stream was larger than the bound applied.
	StdoutTruncated bool   `json:"stdout_truncated"`
	Stderr          string `json:"stderr,omitempty"`
	StderrTruncated bool   `json:"stderr_truncated"`
}

// SendMessageInput never carries a sender: the controller's own connection
// is always privileged and the orchestrator always stamps
// tasks.ControllerSender server-side (see scheduler.Service.SendMessage).
type SendMessageInput struct {
	ID   string `json:"id"`
	Body string `json:"body"`
	// Kind is a short free-text label for forward-compatible typed messages;
	// omit it for a plain instruction.
	Kind string `json:"kind,omitempty"`
}
type SendMessageOutput struct {
	MessageID int64  `json:"message_id"`
	CreatedAt string `json:"created_at"`
}
type ProgressInput struct {
	ID string `json:"id"`
}
type ProgressOutput struct {
	// MessageEventCount counts stream-json "message" events seen in the
	// task's captured output. This is NOT a turn count — a single logical
	// reply can be streamed as several "message" events (observed in this
	// project's own saved sample output); treat it only as a rough
	// activity signal, never as "N turns have happened."
	MessageEventCount int `json:"message_event_count"`
	// LastAssistantText is the reassembled text of the most recent
	// assistant message seen, bounded to logs.MaxAssistantTextBytes.
	LastAssistantText string `json:"last_assistant_text,omitempty"`
	Provider          string `json:"provider,omitempty"`
	Model             string `json:"model,omitempty"`
	// Complete is true once a stream-json "complete" event has been seen;
	// the token/cost fields below are zero until then.
	Complete         bool    `json:"complete"`
	TotalTokens      int64   `json:"total_tokens,omitempty"`
	InputTokens      int64   `json:"input_tokens,omitempty"`
	OutputTokens     int64   `json:"output_tokens,omitempty"`
	CacheReadTokens  int64   `json:"cache_read_tokens,omitempty"`
	CacheWriteTokens int64   `json:"cache_write_tokens,omitempty"`
	CostUSD          float64 `json:"cost_usd,omitempty"`
}

func output(t tasks.Task) TaskOutput {
	return TaskOutput{ID: t.ID, Status: string(t.Status), Result: t.Result, CancelRequested: t.CancelRequestedAt != nil}
}
func listItem(t tasks.Task) TaskListItem {
	return TaskListItem{
		ID:              t.ID,
		Recipe:          t.Recipe,
		Status:          string(t.Status),
		CreatedAt:       t.CreatedAt.Format(time.RFC3339),
		UpdatedAt:       t.UpdatedAt.Format(time.RFC3339),
		CancelRequested: t.CancelRequestedAt != nil,
	}
}
func logsOutput(r scheduler.LogsResult) LogsOutput {
	return LogsOutput{Stdout: r.Stdout, StdoutTruncated: r.StdoutTruncated, Stderr: r.Stderr, StderrTruncated: r.StderrTruncated}
}
func progressOutput(p logs.Progress) ProgressOutput {
	return ProgressOutput{
		MessageEventCount: p.MessageEventCount,
		LastAssistantText: p.LastAssistantText,
		Provider:          p.Provider,
		Model:             p.Model,
		Complete:          p.Complete,
		TotalTokens:       p.TotalTokens,
		InputTokens:       p.InputTokens,
		OutputTokens:      p.OutputTokens,
		CacheReadTokens:   p.CacheReadTokens,
		CacheWriteTokens:  p.CacheWriteTokens,
		CostUSD:           p.CostUSD,
	}
}
func New(service Service) (*mcp.Server, error) {
	if service == nil {
		return nil, errors.New("service is required")
	}
	s := mcp.NewServer(&mcp.Implementation{Name: "goose-process-orchestrator", Version: "0.1.0"}, nil)
	mcp.AddTool(s, &mcp.Tool{Name: "spawn", Description: "Queue an approved recipe. Optional max_turns overrides the server's default turn limit for this one task. Optional parameters supplies the recipe's declared string parameters; never pass secrets, because values are persisted locally and written to run artifacts. Optional inbox_policy selects the task's mailbox policy (\"controller_only\", the default, or \"open\")."}, func(ctx context.Context, _ *mcp.CallToolRequest, in SpawnInput) (*mcp.CallToolResult, TaskOutput, error) {
		// A client transport/request lifecycle is not task ownership. Detach only
		// cancellation so a stdio disconnect cannot cancel a successfully queued task.
		t, e := service.Spawn(context.WithoutCancel(ctx), scheduler.SpawnRequest{Recipe: in.Recipe, IdempotencyKey: in.IdempotencyKey, MaxTurns: in.MaxTurns, Parameters: in.Parameters, InboxPolicy: in.InboxPolicy})
		if e != nil {
			return nil, TaskOutput{}, fmt.Errorf("spawn: %w", e)
		}
		return nil, output(t), nil
	})
	mcp.AddTool(s, &mcp.Tool{Name: "status", Description: "Read task status"}, func(ctx context.Context, _ *mcp.CallToolRequest, in IDInput) (*mcp.CallToolResult, TaskOutput, error) {
		t, e := service.Status(ctx, in.ID)
		if e != nil {
			return nil, TaskOutput{}, fmt.Errorf("status: %w", e)
		}
		return nil, output(t), nil
	})
	mcp.AddTool(s, &mcp.Tool{Name: "result", Description: "Read task result"}, func(ctx context.Context, _ *mcp.CallToolRequest, in IDInput) (*mcp.CallToolResult, TaskOutput, error) {
		t, e := service.Result(ctx, in.ID)
		if e != nil {
			return nil, TaskOutput{}, fmt.Errorf("result: %w", e)
		}
		return nil, output(t), nil
	})
	mcp.AddTool(s, &mcp.Tool{Name: "cancel", Description: "Request task cancellation"}, func(ctx context.Context, _ *mcp.CallToolRequest, in IDInput) (*mcp.CallToolResult, TaskOutput, error) {
		t, e := service.Cancel(ctx, in.ID)
		if e != nil {
			return nil, TaskOutput{}, fmt.Errorf("cancel: %w", e)
		}
		return nil, output(t), nil
	})
	mcp.AddTool(s, &mcp.Tool{Name: "list", Description: "List tasks newest first, with optional status filter and bounded limit. Use this to rediscover task IDs when they were lost (chat compaction, new session, different client)."}, func(ctx context.Context, _ *mcp.CallToolRequest, in ListInput) (*mcp.CallToolResult, ListOutput, error) {
		ts, e := service.List(ctx, in.Status, in.Limit)
		if e != nil {
			return nil, ListOutput{}, fmt.Errorf("list: %w", e)
		}
		items := make([]TaskListItem, len(ts))
		for i, t := range ts {
			items[i] = listItem(t)
		}
		return nil, ListOutput{Tasks: items}, nil
	})
	mcp.AddTool(s, &mcp.Tool{Name: "logs", Description: "Read a bounded tail of a task's captured stdout/stderr. Output may contain anything the recipe or model wrote, including secrets if the recipe was authored carelessly; this tool does not filter it."}, func(ctx context.Context, _ *mcp.CallToolRequest, in LogsInput) (*mcp.CallToolResult, LogsOutput, error) {
		r, e := service.Logs(ctx, in.ID, in.Stream, in.MaxBytes)
		if e != nil {
			return nil, LogsOutput{}, fmt.Errorf("logs: %w", e)
		}
		return nil, logsOutput(r), nil
	})
	mcp.AddTool(s, &mcp.Tool{Name: "progress", Description: "Defensively parsed progress summary for a running or finished task: a rough message-event count (not a turn count), the last assistant text seen, provider/model, and tokens/cost once available. Unknown or malformed stream lines are skipped, never fatal."}, func(ctx context.Context, _ *mcp.CallToolRequest, in ProgressInput) (*mcp.CallToolResult, ProgressOutput, error) {
		p, e := service.Progress(ctx, in.ID)
		if e != nil {
			return nil, ProgressOutput{}, fmt.Errorf("progress: %w", e)
		}
		return nil, progressOutput(p), nil
	})
	mcp.AddTool(s, &mcp.Tool{Name: "send_message", Description: "Queue an instruction or note for a running task's inbox, delivered at its next checkpoint (read_inbox call), not mid-turn. The message always comes from this controller connection; there is no sender argument. Never include secrets: the body is durable local audit data and becomes part of the agent's context once delivered."}, func(ctx context.Context, _ *mcp.CallToolRequest, in SendMessageInput) (*mcp.CallToolResult, SendMessageOutput, error) {
		m, e := service.SendMessage(ctx, in.ID, in.Body, in.Kind)
		if e != nil {
			return nil, SendMessageOutput{}, fmt.Errorf("send_message: %w", e)
		}
		return nil, SendMessageOutput{MessageID: m.ID, CreatedAt: m.CreatedAt.Format(time.RFC3339)}, nil
	})
	return s, nil
}
