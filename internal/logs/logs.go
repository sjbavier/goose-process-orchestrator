// Package logs reads and defensively interprets a task's captured output.
// It never claims a documented schema for goose's stream-json output: the
// only confirmed shapes come from one saved real sample (see
// .goose/workflows/phase-1b-runner-activation/phase1/evidence/smoke-attempt2.stdout);
// anything else, including tool-call events, is tolerated by skipping the
// line rather than assumed.
package logs

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"os"
	"strings"
)

// DefaultTailBytes is the tail size logs returns per stream when a caller
// does not specify one.
const DefaultTailBytes int64 = 8 * 1024

// MaxTailBytes is the server-side maximum tail size per stream, regardless
// of what a caller requests. It stays below artifacts.DefaultOutputLimit
// (256 KiB) so this read path can only shrink, never grow, what the
// capture writer already retained.
const MaxTailBytes int64 = 64 * 1024

// MaxProgressReadBytes bounds how much of a captured stream ParseProgress's
// caller will ever read, independent of the capture writer's own limit, as
// a defensive ceiling in case that limit is ever changed elsewhere.
const MaxProgressReadBytes int64 = 1024 * 1024

// MaxAssistantTextBytes bounds the reassembled last-assistant-text a
// Progress ever carries.
const MaxAssistantTextBytes = 4000

// DefaultStderrTailBytes bounds the stderr tail a failed or timed-out task
// carries: generous enough to show a real failure, small enough not to
// duplicate what the dedicated logs tool is for.
const DefaultStderrTailBytes int64 = 4 * 1024

// Tail returns up to maxBytes from the end of the file at path, and
// whether bytes before the returned slice were dropped. A missing file
// returns empty data and no error: stdout.raw/stderr.raw may not exist yet
// for a task that has not started writing output.
func Tail(path string, maxBytes int64) (data []byte, truncated bool, err error) {
	if maxBytes <= 0 || maxBytes > MaxTailBytes {
		maxBytes = MaxTailBytes
	}
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, false, err
	}
	size := info.Size()
	start := int64(0)
	truncated = size > maxBytes
	if truncated {
		start = size - maxBytes
	}
	if _, err := f.Seek(start, io.SeekStart); err != nil {
		return nil, false, err
	}
	data, err = io.ReadAll(f)
	if err != nil {
		return nil, false, err
	}
	return data, truncated, nil
}

// ReadBounded reads up to maxBytes from the start of path. A missing file
// returns empty data and no error, for the same reason as Tail.
func ReadBounded(path string, maxBytes int64) ([]byte, error) {
	if maxBytes <= 0 || maxBytes > MaxProgressReadBytes {
		maxBytes = MaxProgressReadBytes
	}
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(io.LimitReader(f, maxBytes))
}

// Progress is a defensively parsed summary of one task's stream-json
// output.
type Progress struct {
	// MessageEventCount counts stream-json "message" events seen. This is
	// NOT a turn count: the saved real sample shows one logical reply
	// streamed as multiple "message" events (one per text fragment,
	// sharing one message id), so this field over-counts turns by the
	// model's streaming chunk granularity. Treat it only as a rough
	// activity signal.
	MessageEventCount int
	// LastAssistantText is the reassembled text of the most recently seen
	// assistant message id (concatenating that id's text fragments in
	// the order they were seen), bounded to MaxAssistantTextBytes.
	LastAssistantText string
	// Provider/Model come from the most recent message event's
	// metadata.inference fields seen, if any.
	Provider, Model string
	// Complete is true once a "complete" event has been seen.
	Complete bool
	// TotalTokens/InputTokens/OutputTokens/CacheReadTokens/CacheWriteTokens/CostUSD
	// come from the "complete" event's fields, if seen. All zero until then.
	TotalTokens, InputTokens, OutputTokens, CacheReadTokens, CacheWriteTokens int64
	CostUSD                                                                   float64
}

type messageEvent struct {
	Message struct {
		ID      string `json:"id"`
		Role    string `json:"role"`
		Content []struct {
			Type    string `json:"type"`
			Text    string `json:"text"`
			ID      string `json:"id"`
			Kind    string `json:"kind"`
			Message string `json:"message"`
		} `json:"content"`
		Metadata struct {
			Inference struct {
				Provider       string `json:"provider"`
				RequestedModel string `json:"requestedModel"`
			} `json:"inference"`
		} `json:"metadata"`
	} `json:"message"`
}

type completeEvent struct {
	TotalTokens           int64   `json:"total_tokens"`
	InputTokens           int64   `json:"input_tokens"`
	OutputTokens          int64   `json:"output_tokens"`
	CacheReadInputTokens  int64   `json:"cache_read_input_tokens"`
	CacheWriteInputTokens int64   `json:"cache_write_input_tokens"`
	CostUSD               float64 `json:"cost_usd"`
}

// ParseProgress defensively parses raw stream-json output, line by line.
// A line that is not valid JSON, lacks a recognized "type", or fails to
// unmarshal into that type's expected shape is skipped, never fatal — this
// never returns an error and never panics on arbitrary (including
// adversarial or model-generated) input.
func ParseProgress(raw []byte) Progress {
	var p Progress
	fragments := map[string]*strings.Builder{}
	order := make([]string, 0, 1)
	seen := map[string]bool{}

	scanner := bufio.NewScanner(bytes.NewReader(raw))
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var envelope struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(line, &envelope); err != nil {
			continue
		}
		switch envelope.Type {
		case "message":
			var msg messageEvent
			if err := json.Unmarshal(line, &msg); err != nil {
				continue
			}
			p.MessageEventCount++
			if msg.Message.Role != "assistant" {
				continue
			}
			id := msg.Message.ID
			b, ok := fragments[id]
			if !ok {
				b = &strings.Builder{}
				fragments[id] = b
			}
			if !seen[id] {
				seen[id] = true
				order = append(order, id)
			}
			for _, c := range msg.Message.Content {
				if c.Type == "text" {
					b.WriteString(c.Text)
				}
			}
			applyInference(&p, msg)
		case "complete":
			var c completeEvent
			if err := json.Unmarshal(line, &c); err != nil {
				continue
			}
			applyComplete(&p, c)
		default:
			// Unknown event type, including any undocumented tool-call
			// shape: skipped, not fatal.
		}
	}
	if n := len(order); n > 0 {
		p.LastAssistantText = boundText(fragments[order[n-1]].String(), MaxAssistantTextBytes)
	}
	return p
}

// applyInference records provider/model from an assistant message's
// metadata. Shared by ParseProgress and Incremental so they cannot diverge.
func applyInference(p *Progress, msg messageEvent) {
	inference := msg.Message.Metadata.Inference
	if inference.Provider != "" {
		p.Provider = inference.Provider
	}
	if inference.RequestedModel != "" {
		p.Model = inference.RequestedModel
	}
}

// applyComplete records a "complete" event's totals. Shared by ParseProgress
// and Incremental so they cannot diverge.
func applyComplete(p *Progress, c completeEvent) {
	p.Complete = true
	p.TotalTokens, p.InputTokens, p.OutputTokens = c.TotalTokens, c.InputTokens, c.OutputTokens
	p.CacheReadTokens, p.CacheWriteTokens = c.CacheReadInputTokens, c.CacheWriteInputTokens
	p.CostUSD = c.CostUSD
}

func boundText(s string, maxBytes int) string {
	if len(s) <= maxBytes {
		return s
	}
	return s[:maxBytes]
}
