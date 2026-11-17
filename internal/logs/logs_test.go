package logs

import (
	"os"
	"path/filepath"
	"testing"
)

func readFixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestTailMissingFileIsEmptyNotError(t *testing.T) {
	data, truncated, err := Tail(filepath.Join(t.TempDir(), "missing.raw"), 100)
	if err != nil || truncated || len(data) != 0 {
		t.Fatalf("Tail(missing) = (%q, %v, %v), want empty/false/nil", data, truncated, err)
	}
}

func TestTailUnderLimitReturnsWholeFileNotTruncated(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stdout.raw")
	if err := os.WriteFile(path, []byte("short"), 0o600); err != nil {
		t.Fatal(err)
	}
	data, truncated, err := Tail(path, 100)
	if err != nil || truncated || string(data) != "short" {
		t.Fatalf("Tail(under limit) = (%q, %v, %v)", data, truncated, err)
	}
}

func TestTailOverLimitReturnsSuffixTruncated(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stdout.raw")
	if err := os.WriteFile(path, []byte("0123456789"), 0o600); err != nil {
		t.Fatal(err)
	}
	data, truncated, err := Tail(path, 4)
	if err != nil || !truncated || string(data) != "6789" {
		t.Fatalf("Tail(over limit) = (%q, %v, %v), want 6789/true/nil", data, truncated, err)
	}
}

func TestTailClampsRequestAboveServerMaximum(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stdout.raw")
	big := make([]byte, MaxTailBytes+100)
	for i := range big {
		big[i] = 'x'
	}
	if err := os.WriteFile(path, big, 0o600); err != nil {
		t.Fatal(err)
	}
	data, truncated, err := Tail(path, MaxTailBytes*10)
	if err != nil || !truncated || int64(len(data)) != MaxTailBytes {
		t.Fatalf("Tail(over server max) = (%d bytes, %v, %v), want %d bytes/true/nil", len(data), truncated, err, MaxTailBytes)
	}
}

func TestReadBoundedMissingFileIsEmptyNotError(t *testing.T) {
	data, err := ReadBounded(filepath.Join(t.TempDir(), "missing.raw"), 100)
	if err != nil || len(data) != 0 {
		t.Fatalf("ReadBounded(missing) = (%q, %v)", data, err)
	}
}

func TestParseProgressSingleTurn(t *testing.T) {
	p := ParseProgress(readFixture(t, "single-turn.jsonl"))
	if p.MessageEventCount != 1 {
		t.Fatalf("MessageEventCount = %d, want 1", p.MessageEventCount)
	}
	if p.LastAssistantText != "Hello, world." {
		t.Fatalf("LastAssistantText = %q", p.LastAssistantText)
	}
	if p.Provider != "anthropic" || p.Model != "claude-test-1" {
		t.Fatalf("Provider/Model = %q/%q", p.Provider, p.Model)
	}
	if !p.Complete || p.TotalTokens != 120 || p.InputTokens != 100 || p.OutputTokens != 20 || p.CostUSD != 0.00042 {
		t.Fatalf("complete fields = %+v", p)
	}
}

// TestParseProgressMultiFragmentIsNotMiscountedAsTurns proves the decision
// recorded in plan.md: a real sample showed one logical reply streamed as
// several "message" events sharing one id. This fixture synthesizes two
// such logical turns (5 message events total) and asserts
// MessageEventCount reflects events, not turns, while LastAssistantText
// reassembles only the most recent turn's fragments.
func TestParseProgressMultiFragmentIsNotMiscountedAsTurns(t *testing.T) {
	p := ParseProgress(readFixture(t, "multi-fragment.jsonl"))
	if p.MessageEventCount != 5 {
		t.Fatalf("MessageEventCount = %d, want 5 (events, not 2 turns)", p.MessageEventCount)
	}
	if p.LastAssistantText != "DE" {
		t.Fatalf("LastAssistantText = %q, want DE (only the latest turn's fragments)", p.LastAssistantText)
	}
	if p.Provider != "chatgpt_codex" || p.Model != "gpt-test" {
		t.Fatalf("Provider/Model = %q/%q", p.Provider, p.Model)
	}
	if !p.Complete || p.TotalTokens != 5052 {
		t.Fatalf("complete fields = %+v", p)
	}
}

func TestParseProgressSkipsNonTextToolCallContent(t *testing.T) {
	p := ParseProgress(readFixture(t, "tool-call.jsonl"))
	// The toolRequest event (unrecognized top-level type) and the user-role
	// message are not counted toward LastAssistantText; only the assistant
	// message's text content item contributes.
	if p.MessageEventCount != 2 {
		t.Fatalf("MessageEventCount = %d, want 2 (user + assistant message events; toolRequest is a different top-level type)", p.MessageEventCount)
	}
	if p.LastAssistantText != "Running command" {
		t.Fatalf("LastAssistantText = %q, want %q (toolUse content item ignored)", p.LastAssistantText, "Running command")
	}
	if !p.Complete || p.CacheReadTokens != 10 || p.CacheWriteTokens != 5 {
		t.Fatalf("complete fields = %+v", p)
	}
}

func TestParseProgressSkipsMalformedLinesWithoutFailing(t *testing.T) {
	p := ParseProgress(readFixture(t, "malformed.jsonl"))
	if p.MessageEventCount != 1 {
		t.Fatalf("MessageEventCount = %d, want 1 (only the one well-formed message event)", p.MessageEventCount)
	}
	if p.LastAssistantText != "ok" {
		t.Fatalf("LastAssistantText = %q, want ok", p.LastAssistantText)
	}
	if !p.Complete || p.TotalTokens != 7 {
		t.Fatalf("complete fields = %+v, want the second (well-formed) complete event, not the malformed first one", p)
	}
}

func TestParseProgressNoCompleteEventYet(t *testing.T) {
	p := ParseProgress(readFixture(t, "no-complete.jsonl"))
	if p.Complete {
		t.Fatal("Complete = true, want false: no complete event in this fixture")
	}
	if p.LastAssistantText != "still working" {
		t.Fatalf("LastAssistantText = %q, want %q", p.LastAssistantText, "still working")
	}
	if p.TotalTokens != 0 || p.CostUSD != 0 {
		t.Fatalf("token/cost fields should stay zero without a complete event: %+v", p)
	}
}

func TestParseProgressEmptyInputIsZeroValueNotPanic(t *testing.T) {
	p := ParseProgress(nil)
	if p.MessageEventCount != 0 || p.Complete || p.LastAssistantText != "" {
		t.Fatalf("ParseProgress(nil) = %+v, want zero value", p)
	}
}

// FuzzParseProgressNeverPanics feeds arbitrary bytes (standing in for
// adversarial or malformed stream-json from a goose child process/model)
// into ParseProgress, asserting only that it never panics and never
// returns a LastAssistantText longer than the documented bound.
// security-bug-audit: part of the intake's explicit "fuzzing of MCP input
// handling" requirement — this stream ultimately backs the `progress` MCP
// tool's output.
func FuzzParseProgressNeverPanics(f *testing.F) {
	for _, name := range []string{"single-turn.jsonl", "multi-fragment.jsonl", "tool-call.jsonl", "malformed.jsonl", "no-complete.jsonl"} {
		b, err := os.ReadFile(filepath.Join("testdata", name))
		if err != nil {
			f.Fatal(err)
		}
		f.Add(b)
	}
	f.Add([]byte(""))
	f.Add([]byte("{"))
	f.Add([]byte(`{"type":"message","message":null}`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		p := ParseProgress(raw)
		if len(p.LastAssistantText) > MaxAssistantTextBytes {
			t.Fatalf("LastAssistantText length %d exceeds bound %d", len(p.LastAssistantText), MaxAssistantTextBytes)
		}
	})
}

func TestParseProgressBoundsLastAssistantText(t *testing.T) {
	long := make([]byte, MaxAssistantTextBytes+500)
	for i := range long {
		long[i] = 'a'
	}
	line := []byte(`{"type":"message","message":{"id":"r1","role":"assistant","content":[{"type":"text","text":"`)
	line = append(line, long...)
	line = append(line, []byte(`"}]}}`)...)
	p := ParseProgress(line)
	if len(p.LastAssistantText) != MaxAssistantTextBytes {
		t.Fatalf("LastAssistantText length = %d, want %d", len(p.LastAssistantText), MaxAssistantTextBytes)
	}
}
