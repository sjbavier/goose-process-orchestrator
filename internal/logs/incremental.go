package logs

import (
	"bytes"
	"encoding/json"
	"sync"
	"time"
)

// maxLineBytes matches the one-shot parser's per-line ceiling. A longer line
// is discarded whole (never partially interpreted) and parsing resumes at the
// next line.
const maxLineBytes = 4 * 1024 * 1024

// maxTrackedIDs bounds the de-duplication sets so an unbounded run cannot
// grow them without limit. Past the bound a set is reset, which can at worst
// over-count a repeated tool request or treat a repeated message id as new.
const maxTrackedIDs = 4096

// Snapshot is a point-in-time copy of an Incremental parser's state. It
// shares no memory with the parser.
type Snapshot struct {
	// Progress carries the same fields, with the same meaning, that
	// ParseProgress returns for the same bytes.
	Progress
	// ToolCallCount counts distinct tool requests seen: top-level
	// {"type":"toolRequest"} lines and "toolRequest" items inside a message's
	// content, de-duplicated by their id when they have one. The content-item
	// shape comes from operator observation of real runs, not a saved sample,
	// so treat this as a best-effort activity signal, not an exact figure.
	ToolCallCount int
	// LastEventAt is when the most recent recognized event was parsed, per the
	// parser's clock. Zero until one is seen. It is not read from the stream.
	LastEventAt time.Time
	// StalledMaxTurns latches true once StallMatcher has matched the final
	// assistant text. It is false when no matcher is configured.
	StalledMaxTurns bool
}

// Incremental is a stateful, concurrency-safe counterpart to ParseProgress
// for stream-json arriving in arbitrary chunks. Like ParseProgress it is
// defensive, not a schema guarantee: it claims no documented shape, skips any
// line it does not recognize, never returns an error and never panics on
// arbitrary input. It implements io.Writer so it can sit beside the disk
// capture in an io.MultiWriter.
//
// The zero value is ready to use. Set Now and StallMatcher before the first
// Write.
type Incremental struct {
	// Now stamps LastEventAt. Nil means time.Now.
	Now func() time.Time
	// StallMatcher reports whether the final assistant text is Goose's
	// max-turns stop message. Nil (the default) never matches: no confirmed
	// real sample of that message exists in this repository, so nothing is
	// matched on a guess.
	StallMatcher func(finalAssistantText string) bool

	mu       sync.Mutex
	buf      []byte
	dropping bool
	snap     Snapshot
	seenIDs  map[string]struct{}
	haveLast bool
	lastID   string
	text     []byte
	toolIDs  map[string]struct{}
}

// Write consumes any chunk of output, line-aligned or not. It always reports
// the whole chunk as consumed and never fails, so it cannot break a
// surrounding io.MultiWriter.
func (i *Incremental) Write(p []byte) (int, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	rest := p
	for len(rest) > 0 {
		nl := bytes.IndexByte(rest, '\n')
		if nl < 0 {
			i.appendPartial(rest)
			break
		}
		i.appendPartial(rest[:nl])
		i.finishLine()
		rest = rest[nl+1:]
	}
	return len(p), nil
}

// Flush processes a trailing line that never received its newline, as the
// one-shot parser does for the last line of a file. Call it once the output
// has ended.
func (i *Incremental) Flush() {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.finishLine()
}

// Snapshot returns a copy of the current state. A line still waiting for its
// newline is not included.
func (i *Incremental) Snapshot() Snapshot {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.StallMatcher != nil && !i.snap.StalledMaxTurns && i.haveLast && i.StallMatcher(string(i.text)) {
		i.snap.StalledMaxTurns = true
	}
	out := i.snap
	out.LastAssistantText = string(i.text)
	return out
}

func (i *Incremental) appendPartial(b []byte) {
	if i.dropping {
		return
	}
	if len(i.buf)+len(b) > maxLineBytes {
		i.buf = nil
		i.dropping = true
		return
	}
	i.buf = append(i.buf, b...)
}

func (i *Incremental) finishLine() {
	line := bytes.TrimSpace(i.buf)
	skip := i.dropping
	i.buf = i.buf[:0]
	i.dropping = false
	if skip || len(line) == 0 {
		return
	}
	i.handleLine(line)
}

func (i *Incremental) stamp() {
	now := time.Now
	if i.Now != nil {
		now = i.Now
	}
	i.snap.LastEventAt = now()
}

func (i *Incremental) handleLine(line []byte) {
	var envelope struct {
		Type string `json:"type"`
		ID   string `json:"id"`
	}
	if err := json.Unmarshal(line, &envelope); err != nil {
		return
	}
	switch envelope.Type {
	case "message":
		var msg messageEvent
		if err := json.Unmarshal(line, &msg); err != nil {
			return
		}
		i.stamp()
		i.snap.MessageEventCount++
		for _, c := range msg.Message.Content {
			if c.Type == "toolRequest" {
				i.countToolRequest(c.ID)
			}
		}
		if msg.Message.Role != "assistant" {
			return
		}
		i.applyAssistantText(msg)
		applyInference(&i.snap.Progress, msg)
	case "complete":
		var c completeEvent
		if err := json.Unmarshal(line, &c); err != nil {
			return
		}
		i.stamp()
		applyComplete(&i.snap.Progress, c)
	case "toolRequest":
		i.stamp()
		i.countToolRequest(envelope.ID)
	}
}

func (i *Incremental) countToolRequest(id string) {
	if id != "" {
		if i.toolIDs == nil || len(i.toolIDs) >= maxTrackedIDs {
			i.toolIDs = map[string]struct{}{}
		}
		if _, dup := i.toolIDs[id]; dup {
			return
		}
		i.toolIDs[id] = struct{}{}
	}
	i.snap.ToolCallCount++
}

// applyAssistantText tracks the text of the most recently first-seen
// assistant message id, capped at MaxAssistantTextBytes while it accumulates,
// so memory stays bounded however long the run is. Fragments of any other id
// cannot affect that result and are ignored.
func (i *Incremental) applyAssistantText(msg messageEvent) {
	id := msg.Message.ID
	if i.seenIDs == nil || len(i.seenIDs) >= maxTrackedIDs {
		i.seenIDs = map[string]struct{}{}
		if i.haveLast {
			i.seenIDs[i.lastID] = struct{}{}
		}
	}
	if _, seen := i.seenIDs[id]; !seen {
		i.seenIDs[id] = struct{}{}
		i.haveLast, i.lastID = true, id
		i.text = i.text[:0]
	}
	if id != i.lastID {
		return
	}
	for _, c := range msg.Message.Content {
		if c.Type != "text" {
			continue
		}
		room := MaxAssistantTextBytes - len(i.text)
		if room <= 0 {
			return
		}
		if len(c.Text) > room {
			i.text = append(i.text, c.Text[:room]...)
			return
		}
		i.text = append(i.text, c.Text...)
	}
}
