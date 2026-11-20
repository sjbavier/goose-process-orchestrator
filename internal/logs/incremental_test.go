package logs

import (
	"strings"
	"testing"
	"time"
)

var parityFixtures = []string{"single-turn.jsonl", "multi-fragment.jsonl", "tool-call.jsonl", "malformed.jsonl", "no-complete.jsonl", "tool-request-content.jsonl"}

func feed(t *testing.T, raw []byte, split func([]byte) [][]byte) Snapshot {
	t.Helper()
	var inc Incremental
	for _, chunk := range split(raw) {
		n, err := inc.Write(chunk)
		if n != len(chunk) || err != nil {
			t.Fatalf("Write = (%d, %v), want (%d, nil)", n, err, len(chunk))
		}
	}
	inc.Flush()
	return inc.Snapshot()
}

// TestIncrementalMatchesOneShotAtEverySplit proves the incremental parser
// agrees with ParseProgress however the bytes are chunked, including splits
// mid-line, mid-token and mid-UTF-8-rune.
func TestIncrementalMatchesOneShotAtEverySplit(t *testing.T) {
	for _, name := range parityFixtures {
		raw := readFixture(t, name)
		want := ParseProgress(raw)
		for cut := 0; cut <= len(raw); cut++ {
			got := feed(t, raw, func(b []byte) [][]byte { return [][]byte{b[:cut], b[cut:]} })
			if got.Progress != want {
				t.Fatalf("%s split at %d:\n got  %+v\n want %+v", name, cut, got.Progress, want)
			}
		}
		got := feed(t, raw, func(b []byte) [][]byte {
			out := make([][]byte, len(b))
			for i := range b {
				out[i] = b[i : i+1]
			}
			return out
		})
		if got.Progress != want {
			t.Fatalf("%s byte-by-byte:\n got  %+v\n want %+v", name, got.Progress, want)
		}
	}
}

func TestIncrementalIgnoresUnterminatedLineUntilFlush(t *testing.T) {
	var inc Incremental
	inc.Write([]byte(`{"type":"complete","total_tokens":9}`))
	if inc.Snapshot().Complete {
		t.Fatal("an unterminated line must not be interpreted before Flush")
	}
	inc.Flush()
	if s := inc.Snapshot(); !s.Complete || s.TotalTokens != 9 {
		t.Fatalf("after Flush: %+v", s.Progress)
	}
}

func TestIncrementalCapsAssistantTextWhileAccumulating(t *testing.T) {
	var inc Incremental
	frag := `{"type":"message","message":{"id":"big","role":"assistant","content":[{"type":"text","text":"` + strings.Repeat("x", 100) + `"}]}}` + "\n"
	for n := 0; n < 1000; n++ {
		inc.Write([]byte(frag))
		if len(inc.text) > MaxAssistantTextBytes {
			t.Fatalf("builder grew to %d bytes, want <= %d", len(inc.text), MaxAssistantTextBytes)
		}
	}
	if got := len(inc.Snapshot().LastAssistantText); got != MaxAssistantTextBytes {
		t.Fatalf("LastAssistantText length = %d, want %d", got, MaxAssistantTextBytes)
	}
}

func TestIncrementalNeverFailsOnJunkAndKeepsParsing(t *testing.T) {
	var inc Incremental
	junk := []string{"not json", "[1,2,3]", `"string"`, "42", "null", `{"no_type":1}`, `{"type":42}`, `{"type":"message","message":"x"}`, "\x00\xff\xfe", "{"}
	for _, line := range junk {
		if n, err := inc.Write([]byte(line + "\n")); n != len(line)+1 || err != nil {
			t.Fatalf("Write(%q) = (%d, %v)", line, n, err)
		}
	}
	inc.Write([]byte(`{"type":"complete","total_tokens":3}` + "\n"))
	if s := inc.Snapshot(); !s.Complete || s.TotalTokens != 3 || s.MessageEventCount != 0 {
		t.Fatalf("junk disturbed later parsing: %+v", s.Progress)
	}
}

func TestIncrementalDropsOversizedLineAndResumes(t *testing.T) {
	var inc Incremental
	chunk := []byte(strings.Repeat("x", 1<<20))
	for n := 0; n < 5; n++ {
		inc.Write(chunk)
	}
	if len(inc.buf) > maxLineBytes {
		t.Fatalf("buffer grew past the line ceiling: %d", len(inc.buf))
	}
	inc.Write([]byte("\n" + `{"type":"complete","total_tokens":4}` + "\n"))
	if s := inc.Snapshot(); !s.Complete || s.TotalTokens != 4 {
		t.Fatalf("did not resume after oversized line: %+v", s.Progress)
	}
}

func TestIncrementalToolCallCount(t *testing.T) {
	cases := map[string]int{
		"tool-call.jsonl":            1, // one top-level toolRequest line
		"tool-request-content.jsonl": 3, // call_a repeated twice counts once, plus call_b and call_c
		"single-turn.jsonl":          0,
	}
	for name, want := range cases {
		raw := readFixture(t, name)
		got := feed(t, raw, func(b []byte) [][]byte {
			out := make([][]byte, len(b))
			for i := range b {
				out[i] = b[i : i+1]
			}
			return out
		})
		if got.ToolCallCount != want {
			t.Errorf("%s: ToolCallCount = %d, want %d", name, got.ToolCallCount, want)
		}
	}
	var inc Incremental
	inc.Write([]byte(`{"type":"toolRequest"}` + "\n" + `{"type":"toolRequest"}` + "\n"))
	if got := inc.Snapshot().ToolCallCount; got != 2 {
		t.Fatalf("id-less tool requests: got %d, want 2 (each counted)", got)
	}
}

func TestIncrementalLastEventAtAdvancesOnlyOnRecognizedEvents(t *testing.T) {
	tick := time.Unix(1000, 0)
	calls := 0
	inc := Incremental{Now: func() time.Time { calls++; tick = tick.Add(time.Second); return tick }}
	if !inc.Snapshot().LastEventAt.IsZero() {
		t.Fatal("LastEventAt must be zero before any event")
	}
	inc.Write(readFixture(t, "malformed.jsonl"))
	inc.Flush()
	// malformed.jsonl holds exactly two recognized events: the valid assistant
	// message and the final valid complete; every other line is skipped.
	if calls != 2 {
		t.Fatalf("clock consulted %d times, want 2", calls)
	}
	if got, want := inc.Snapshot().LastEventAt, time.Unix(1002, 0); !got.Equal(want) {
		t.Fatalf("LastEventAt = %v, want %v", got, want)
	}
}

// TestStallMatcherDisabledByDefault documents that, with no confirmed sample
// of Goose's max-turns stop text in this repository, nothing is matched on a
// guess: a zero-value parser never reports a stall.
func TestStallMatcherDisabledByDefault(t *testing.T) {
	var inc Incremental
	inc.Write([]byte(`{"type":"message","message":{"id":"a","role":"assistant","content":[{"type":"text","text":"I've reached the maximum number of actions I can do without user input"}]}}` + "\n"))
	inc.Write([]byte(`{"type":"complete","total_tokens":1}` + "\n"))
	if inc.Snapshot().StalledMaxTurns {
		t.Fatal("StalledMaxTurns must stay false when no StallMatcher is configured")
	}
}

func TestStallMatcherLatchesOnceMatched(t *testing.T) {
	inc := Incremental{StallMatcher: func(s string) bool { return strings.Contains(s, "STALL") }}
	inc.Write([]byte(`{"type":"message","message":{"id":"a","role":"assistant","content":[{"type":"text","text":"working"}]}}` + "\n"))
	if inc.Snapshot().StalledMaxTurns {
		t.Fatal("matched too early")
	}
	inc.Write([]byte(`{"type":"message","message":{"id":"b","role":"assistant","content":[{"type":"text","text":"STALL"}]}}` + "\n"))
	if !inc.Snapshot().StalledMaxTurns {
		t.Fatal("matcher hit was not reported")
	}
	inc.Write([]byte(`{"type":"message","message":{"id":"c","role":"assistant","content":[{"type":"text","text":"later text"}]}}` + "\n"))
	if !inc.Snapshot().StalledMaxTurns {
		t.Fatal("StalledMaxTurns must never un-set once true")
	}
}

func TestIncrementalSnapshotIsACopy(t *testing.T) {
	var inc Incremental
	inc.Write([]byte(`{"type":"complete","total_tokens":1}` + "\n"))
	before := inc.Snapshot()
	inc.Write([]byte(`{"type":"complete","total_tokens":2}` + "\n"))
	if before.TotalTokens != 1 {
		t.Fatalf("earlier snapshot changed: %+v", before.Progress)
	}
}

func TestIncrementalConcurrentWriteAndSnapshot(t *testing.T) {
	var inc Incremental
	done := make(chan struct{})
	go func() {
		defer close(done)
		for n := 0; n < 500; n++ {
			inc.Write([]byte(`{"type":"toolRequest","id":"` + strings.Repeat("a", n%7) + `"}` + "\n"))
		}
	}()
	for n := 0; n < 500; n++ {
		_ = inc.Snapshot()
	}
	<-done
}
