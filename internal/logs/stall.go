package logs

import "strings"

// stallStopPrefix is the fixed prefix of Goose's own max-turns stop message,
// used in assistant text through Goose 1.53.0 and in errors in Goose 1.60.2.
const stallStopPrefix = "I've reached the maximum number of actions I can do without user input"

// DefaultStallMatcher is the StallMatcher used by production runners. It
// matches only Goose's own injected stop message, not a model reply that
// merely quotes the same phrase: modelGenerated must be false (no
// metadata.inference seen on any fragment of the text) as well as the text
// itself matching.
func DefaultStallMatcher(finalAssistantText string, modelGenerated bool) bool {
	return !modelGenerated && matchesStallDiagnostic(finalAssistantText)
}

func matchesStallDiagnostic(message string) bool {
	return strings.HasPrefix(message, stallStopPrefix)
}
