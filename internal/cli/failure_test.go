package cli

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/JeremiahChurch/herd-msg/internal/broker"
)

func TestRetryHintOnlyWhenRetryCanHelp(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		hint bool
	}{
		{"definite refusal", &broker.Error{Code: 2, Message: "hop limit exceeded"}, false},
		{"retryable refusal", &broker.Error{Code: 2, Message: "pair rate limit exceeded", Retryable: true}, true},
		{"uncertain transport", errors.New("broker connection reset"), true},
		{"internal storage error", &broker.Error{Code: 1, Message: "disk I/O error"}, true},
		{"ambiguous address", &broker.Error{Code: 3, Message: "ambiguous recipient"}, false},
	} {
		var stderr bytes.Buffer
		c := &command{out: &bytes.Buffer{}, err: &stderr, knownID: "id-1", knownCreated: "2026-11-05T00:00:00Z"}
		c.failure(tc.err)
		if got := strings.Contains(stderr.String(), "Retry with --id id-1"); got != tc.hint {
			t.Fatalf("%s: retry hint shown=%v, want %v: %q", tc.name, got, tc.hint, stderr.String())
		}
	}
}
