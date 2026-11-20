package logs

import "strings"

// stallStopPrefix is the fixed prefix of Goose's own max-turns stop message,
// confirmed present in goose 1.43.0, 1.52.0 (Desktop), and 1.53.0 binaries.
const stallStopPrefix = "I've reached the maximum number of actions I can do without user input"

// DefaultStallMatcher is the StallMatcher used by production runners. It
// matches only Goose's own injected stop message, not a model reply that
// merely quotes the same phrase: modelGenerated must be false (no
// metadata.inference seen on any fragment of the text) as well as the text
// itself matching.
func DefaultStallMatcher(finalAssistantText string, modelGenerated bool) bool {
	return !modelGenerated && strings.HasPrefix(finalAssistantText, stallStopPrefix)
}
