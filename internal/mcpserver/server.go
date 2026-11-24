package mcpserver

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/local/goose-process-orchestrator/internal/scheduler"
	"github.com/local/goose-process-orchestrator/internal/tasks"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type Service interface {
	Spawn(context.Context, scheduler.SpawnRequest) (tasks.Task, error)
	Cancel(context.Context, string) (tasks.Task, error)
	List(ctx context.Context, status, keyPrefix string, limit int) ([]tasks.Task, error)
	Logs(ctx context.Context, id, stream string, maxBytes int) (scheduler.LogsResult, error)
	Progress(ctx context.Context, id string) (scheduler.ProgressResult, error)
	Outcome(ctx context.Context, id string) (scheduler.OutcomeResult, error)
	Wait(ctx context.Context, ids []string, mode string, timeout time.Duration) (scheduler.WaitResult, error)
	SendMessage(ctx context.Context, id, body, kind string) (tasks.Message, error)
	ServerInfo(context.Context) (scheduler.ServerInfo, error)
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
	// FileParameters supplies bounded UTF-8 content for declared string parameters.
	// Contents are persisted unencrypted and materialized under the task run directory; never pass secrets.
	FileParameters map[string]string `json:"file_parameters,omitempty"`
	// InboxPolicy selects the spawned task's mailbox policy. Omit it (or
	// send "") for the default controller_only policy; "open" allows any
	// sender to message this task's inbox.
	InboxPolicy string `json:"inbox_policy,omitempty"`
	// ExpectRecipeSHA256 (hex) must equal the SHA-256 of the resolved recipe
	// at admission and again at launch, else the spawn/task fails with
	// recipe_sha256_mismatch. ExpectProvider/ExpectModel are checked against
	// the live stream (requestedModel, not necessarily the serving model).
	ExpectRecipeSHA256 string `json:"expect_recipe_sha256,omitempty"`
	ExpectProvider     string `json:"expect_provider,omitempty"`
	ExpectModel        string `json:"expect_model,omitempty"`
	// TimeoutSeconds is an optional per-task wall-clock limit from process
	// start; on expiry the task is cancelled and ends timed_out.
	TimeoutSeconds int `json:"timeout_seconds,omitempty"`
}
type ServerInfoInput struct{}
type ServerInfoOutput struct {
	RecipeRoot            string `json:"recipe_root"`
	Workdir               string `json:"workdir,omitempty"`
	RunsDir               string `json:"runs_dir"`
	GooseBinary           string `json:"goose_binary"`
	GooseVersion          string `json:"goose_version,omitempty"`
	MaxTurns              int    `json:"max_turns"`
	MaxTurnsCeiling       int    `json:"max_turns_ceiling"`
	TimeoutCeilingSeconds int    `json:"timeout_ceiling_seconds,omitempty"`
	MaxConcurrent         int    `json:"max_concurrent"`
	Queued                int    `json:"queued"`
	Running               int    `json:"running"`
	MailboxEnabled        bool   `json:"mailbox_enabled"`
	OutputLimit           int64  `json:"output_limit"`
	RunsMaxAge            string `json:"runs_max_age"`
	RunsMaxCount          int    `json:"runs_max_count"`
	GPOVersion            string `json:"gpo_version,omitempty"`
	GPOCommit             string `json:"gpo_commit,omitempty"`
	DaemonStartedAt       string `json:"daemon_started_at"`
}

type IDInput struct {
	ID string `json:"id"`
}

type WaitInput struct {
	IDs            []string `json:"ids"`
	Mode           string   `json:"mode,omitempty"`
	TimeoutSeconds int      `json:"timeout_seconds,omitempty"`
}

type WaitOutput struct {
	Tasks    []TaskOutput `json:"tasks"`
	TimedOut bool         `json:"timed_out"`
}

// TaskOutput deliberately omits the task's recipe parameters. Responses are
// the widest-read surface here, and parameter values are request input that a
// caller already holds, so echoing them only broadens exposure.
//
// The fields below RunDir are additive and populated only by the "status"
// and "result" tools (via outcomeOutput); "spawn" and "cancel" keep their
// exact previous shape (output), so these are always absent/omitted there.
type TaskOutput struct {
	ID              string `json:"id"`
	Status          string `json:"status"`
	Result          string `json:"result,omitempty"`
	CancelRequested bool   `json:"cancel_requested"`

	RunDir             string `json:"run_dir,omitempty"`
	FinalText          string `json:"final_text,omitempty"`
	FinalTextTruncated bool   `json:"final_text_truncated,omitempty"`
	StdoutTruncated    bool   `json:"stdout_truncated,omitempty"`
	BytesDropped       int64  `json:"bytes_dropped,omitempty"`
	// Outcome is one of "completed", "stalled_max_turns",
	// "no_complete_event", "process_failed", or absent for a non-terminal
	// or cancelled task — see scheduler.OutcomeResult.
	Outcome             string `json:"outcome,omitempty"`
	ExitCode            *int   `json:"exit_code,omitempty"`
	Failure             string `json:"failure,omitempty"`
	CreatedAt           string `json:"created_at,omitempty"`
	StartedAt           string `json:"started_at,omitempty"`
	EndedAt             string `json:"ended_at,omitempty"`
	StderrTail          string `json:"stderr_tail,omitempty"`
	StderrTailTruncated bool   `json:"stderr_tail_truncated,omitempty"`
	// RecipeSHA256 is the hash of the recipe bytes recorded for the task;
	// Provider/Model are the effective values observed in the stream.
	RecipeSHA256 string `json:"recipe_sha256,omitempty"`
	// TimeoutSeconds is the per-task wall-clock limit, if one was set.
	TimeoutSeconds int    `json:"timeout_seconds,omitempty"`
	Provider       string `json:"provider,omitempty"`
	Model          string `json:"model,omitempty"`
	// GooseVersion is the goose binary version captured at launch.
	GooseVersion string `json:"goose_version,omitempty"`
	// LeftoverPIDs lists processes still alive in the task's process group
	// after its leader exited. LeftoverDetection is "ok" (scan ran; an empty
	// list means none), "unavailable" (platform/scan cannot tell; absence of
	// pids is NOT evidence of none), or absent (not scanned).
	LeftoverPIDs      []int  `json:"leftover_pids,omitempty"`
	LeftoverDetection string `json:"leftover_detection,omitempty"`
}
type ListInput struct {
	// Status, when non-empty, filters to exactly that task status.
	Status string `json:"status,omitempty"`
	// KeyPrefix filters to caller-chosen idempotency keys beginning with this value.
	KeyPrefix string `json:"key_prefix,omitempty"`
	// Limit bounds how many tasks are returned; non-positive or omitted
	// means the server default, and any value is still capped at the
	// server-side maximum.
	Limit int `json:"limit,omitempty"`
}
type TaskListItem struct {
	ID              string `json:"id"`
	IdempotencyKey  string `json:"idempotency_key"`
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
	// ToolCallCount/LastEventAt/StdoutTruncated/BytesDropped come from the
	// task row (chunks C1/C2/C5), not this on-demand re-parse; see
	// scheduler.ProgressResult. Zero/absent for a task with no live summary
	// ever recorded (reconciled, or predating migration 005).
	ToolCallCount   int64  `json:"tool_call_count,omitempty"`
	LastEventAt     string `json:"last_event_at,omitempty"`
	StdoutTruncated bool   `json:"stdout_truncated,omitempty"`
	BytesDropped    int64  `json:"bytes_dropped,omitempty"`
}

func output(t tasks.Task) TaskOutput {
	return TaskOutput{ID: t.ID, Status: string(t.Status), Result: t.Result, CancelRequested: t.CancelRequestedAt != nil}
}

// outcomeOutput builds the enriched TaskOutput "status"/"result" return,
// straight off the (possibly NULL) task row plus the outcome classification
// and best-effort run_dir/stderr_tail scheduler.Service.Outcome already
// resolved. Every field output(t) already sets stays byte-identical.
func outcomeOutput(r scheduler.OutcomeResult) TaskOutput {
	t := r.Task
	out := output(t)
	out.RunDir, out.Outcome = r.RunDir, r.Outcome
	out.StdoutTruncated, out.BytesDropped = r.StdoutTruncated, r.BytesDropped
	out.FinalText, out.ExitCode, out.Failure = t.FinalText, t.ExitCode, t.Failure
	if t.FinalTextTruncated != nil {
		out.FinalTextTruncated = *t.FinalTextTruncated
	}
	out.CreatedAt = t.CreatedAt.Format(time.RFC3339)
	if t.StartedAt != nil {
		out.StartedAt = t.StartedAt.Format(time.RFC3339)
	}
	if t.EndedAt != nil {
		out.EndedAt = t.EndedAt.Format(time.RFC3339)
	}
	out.StderrTail, out.StderrTailTruncated = r.StderrTail, r.StderrTailTruncated
	out.RecipeSHA256, out.Provider, out.Model = t.RecipeSHA256, t.Provider, t.Model
	out.TimeoutSeconds = t.TimeoutSeconds
	out.GooseVersion, out.LeftoverPIDs, out.LeftoverDetection = t.GooseVersion, t.LeftoverPIDs, t.LeftoverDetection
	return out
}
func listItem(t tasks.Task) TaskListItem {
	return TaskListItem{
		ID:              t.ID,
		IdempotencyKey:  t.IdempotencyKey,
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
func progressOutput(p scheduler.ProgressResult) ProgressOutput {
	out := ProgressOutput{
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
		ToolCallCount:     p.ToolCallCount,
		StdoutTruncated:   p.StdoutTruncated,
		BytesDropped:      p.BytesDropped,
	}
	if !p.LastEventAt.IsZero() {
		out.LastEventAt = p.LastEventAt.UTC().Format(time.RFC3339)
	}
	return out
}
func New(service Service) (*mcp.Server, error) {
	if service == nil {
		return nil, errors.New("service is required")
	}
	s := mcp.NewServer(&mcp.Implementation{Name: "goose-process-orchestrator", Version: "0.1.0"}, nil)
	mcp.AddTool(s, &mcp.Tool{Name: "server_info", Description: "Read server configuration, cached Goose version, capacity, feature flags, retention, and current queue counts."}, func(ctx context.Context, _ *mcp.CallToolRequest, _ ServerInfoInput) (*mcp.CallToolResult, ServerInfoOutput, error) {
		info, e := service.ServerInfo(ctx)
		if e != nil {
			return nil, ServerInfoOutput{}, fmt.Errorf("server_info: %w", e)
		}
		return nil, ServerInfoOutput{RecipeRoot: info.RecipeRoot, Workdir: info.Workdir, RunsDir: info.RunsDir, GooseBinary: info.GooseBinary, GooseVersion: info.GooseVersion, MaxTurns: info.MaxTurns, MaxTurnsCeiling: info.MaxTurnsCeiling, TimeoutCeilingSeconds: info.TimeoutCeilingSeconds, MaxConcurrent: info.MaxConcurrent, Queued: info.Queued, Running: info.Running, MailboxEnabled: info.MailboxEnabled, OutputLimit: info.OutputLimit, RunsMaxAge: info.RunsMaxAge.String(), RunsMaxCount: info.RunsMaxCount, GPOVersion: info.GPOVersion, GPOCommit: info.GPOCommit, DaemonStartedAt: info.DaemonStartedAt.Format(time.RFC3339)}, nil
	})
	mcp.AddTool(s, &mcp.Tool{Name: "spawn", Description: "Queue an approved recipe. Optional max_turns overrides the server's default turn limit. Optional timeout_seconds bounds wall-clock time from process start (status timed_out on expiry). Optional parameters supplies declared strings. Optional file_parameters supplies bounded UTF-8 content that is persisted unencrypted and materialized as private run files; never pass secrets. Optional inbox_policy selects \"controller_only\" (default) or \"open\"."}, func(ctx context.Context, _ *mcp.CallToolRequest, in SpawnInput) (*mcp.CallToolResult, TaskOutput, error) {
		// A client transport/request lifecycle is not task ownership. Detach only
		// cancellation so a stdio disconnect cannot cancel a successfully queued task.
		t, e := service.Spawn(context.WithoutCancel(ctx), scheduler.SpawnRequest{Recipe: in.Recipe, IdempotencyKey: in.IdempotencyKey, MaxTurns: in.MaxTurns, Parameters: in.Parameters, FileParameters: in.FileParameters, InboxPolicy: in.InboxPolicy, ExpectRecipeSHA256: in.ExpectRecipeSHA256, ExpectProvider: in.ExpectProvider, ExpectModel: in.ExpectModel, TimeoutSeconds: in.TimeoutSeconds})
		if e != nil {
			return nil, TaskOutput{}, fmt.Errorf("spawn: %w", e)
		}
		return nil, output(t), nil
	})
	mcp.AddTool(s, &mcp.Tool{Name: "status", Description: "Read task status, including run_dir, final_text, outcome, exit_code, failure, and timestamps once available. outcome is one of \"completed\", \"stalled_max_turns\", \"no_complete_event\", or \"process_failed\" once terminal; absent otherwise. stderr_tail may appear for failed or timed-out tasks when the run directory is still available."}, func(ctx context.Context, _ *mcp.CallToolRequest, in IDInput) (*mcp.CallToolResult, TaskOutput, error) {
		r, e := service.Outcome(ctx, in.ID)
		if e != nil {
			return nil, TaskOutput{}, fmt.Errorf("status: %w", e)
		}
		return nil, outcomeOutput(r), nil
	})
	mcp.AddTool(s, &mcp.Tool{Name: "result", Description: "Read task result, including run_dir, final_text, outcome, exit_code, failure, and timestamps once available. outcome is one of \"completed\", \"stalled_max_turns\", \"no_complete_event\", or \"process_failed\" once terminal; absent otherwise. stderr_tail may appear for failed or timed-out tasks when the run directory is still available."}, func(ctx context.Context, _ *mcp.CallToolRequest, in IDInput) (*mcp.CallToolResult, TaskOutput, error) {
		r, e := service.Outcome(ctx, in.ID)
		if e != nil {
			return nil, TaskOutput{}, fmt.Errorf("result: %w", e)
		}
		return nil, outcomeOutput(r), nil
	})
	mcp.AddTool(s, &mcp.Tool{Name: "wait", Description: "Wait for one or more tasks without client polling. mode defaults to all; all/any wake on terminal, interrupted, or unknown status. timeout_seconds defaults to 240 and is capped at 280. Returns every task in caller order with the same enriched fields as status/result plus timed_out."}, func(ctx context.Context, _ *mcp.CallToolRequest, in WaitInput) (*mcp.CallToolResult, WaitOutput, error) {
		if in.TimeoutSeconds < 0 || in.TimeoutSeconds > int(scheduler.MaxWaitTimeout/time.Second) {
			return nil, WaitOutput{}, fmt.Errorf("wait: %w: timeout_seconds must be between 0 and %d", scheduler.ErrInvalidWait, int(scheduler.MaxWaitTimeout/time.Second))
		}
		r, e := service.Wait(ctx, in.IDs, in.Mode, time.Duration(in.TimeoutSeconds)*time.Second)
		if e != nil {
			return nil, WaitOutput{}, fmt.Errorf("wait: %w", e)
		}
		out := make([]TaskOutput, len(r.Tasks))
		for i := range r.Tasks {
			out[i] = outcomeOutput(r.Tasks[i])
		}
		return nil, WaitOutput{Tasks: out, TimedOut: r.TimedOut}, nil
	})
	mcp.AddTool(s, &mcp.Tool{Name: "cancel", Description: "Request task cancellation"}, func(ctx context.Context, _ *mcp.CallToolRequest, in IDInput) (*mcp.CallToolResult, TaskOutput, error) {
		t, e := service.Cancel(ctx, in.ID)
		if e != nil {
			return nil, TaskOutput{}, fmt.Errorf("cancel: %w", e)
		}
		return nil, output(t), nil
	})
	mcp.AddTool(s, &mcp.Tool{Name: "list", Description: "List tasks newest first, with optional status filter and bounded limit. Use this to rediscover task IDs when they were lost (chat compaction, new session, different client)."}, func(ctx context.Context, _ *mcp.CallToolRequest, in ListInput) (*mcp.CallToolResult, ListOutput, error) {
		ts, e := service.List(ctx, in.Status, in.KeyPrefix, in.Limit)
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
