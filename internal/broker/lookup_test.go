package broker

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

// The fake models only the peer-origin record shapes verified by the protocol probes.
func fakeClaudeLookup(record []byte, header string) (bool, error) {
	var row struct {
		Type   string `json:"type"`
		Origin struct {
			Kind string `json:"kind"`
		} `json:"origin"`
		Message struct {
			Content any `json:"content"`
		} `json:"message"`
		Attachment struct {
			Type   string `json:"type"`
			Prompt string `json:"prompt"`
			Origin struct {
				Kind string `json:"kind"`
			} `json:"origin"`
		} `json:"attachment"`
	}
	if e := json.Unmarshal(record, &row); e != nil {
		return false, e
	}
	if row.Type == "user" && row.Origin.Kind != "peer" {
		return false, nil
	}
	content := ""
	switch row.Type {
	case "user":
		s, ok := row.Message.Content.(string)
		if !ok {
			return false, nil
		}
		const wrapper = "Another Claude session sent a message:\n"
		if !strings.HasPrefix(s, wrapper) {
			return false, errors.New("unknown peer wrapper")
		}
		content = strings.TrimPrefix(s, wrapper)
	case "attachment":
		if row.Attachment.Type != "queued_command" || row.Attachment.Origin.Kind != "peer" {
			return false, nil
		}
		content = row.Attachment.Prompt
	default:
		return false, nil
	}
	line, _, _ := strings.Cut(content, "\n")
	return line == header, nil
}
func TestFakeLookupRequiresAnchoredPeerAcceptance(t *testing.T) {
	header := "[herd-msg] from builder.codex@local | id message-a | attempt attempt-a | hop 1"
	fixtures := []struct {
		record     map[string]any
		found, err bool
	}{
		{map[string]any{"type": "user", "origin": map[string]string{"kind": "peer"}, "isMeta": true, "message": map[string]any{"content": "Another Claude session sent a message:\n" + header + "\nbody"}}, true, false},
		{map[string]any{"type": "attachment", "origin": map[string]string{"kind": "peer"}, "attachment": map[string]any{"origin": map[string]string{"kind": "peer"}, "type": "queued_command", "prompt": header + "\nbody"}}, true, false},
		{map[string]any{"type": "user", "origin": map[string]string{"kind": "peer"}, "message": map[string]any{"content": "unknown wrapper\n" + header}}, false, true},
		{map[string]any{"type": "user", "message": map[string]any{"content": header}}, false, false},
		{map[string]any{"type": "user", "origin": map[string]string{"kind": "peer"}, "message": map[string]any{"content": []any{map[string]string{"type": "tool_result", "content": header}, map[string]string{"type": "text", "text": header}}}}, false, false},
		{map[string]any{"type": "assistant", "origin": map[string]string{"kind": "peer"}, "message": map[string]any{"content": header}}, false, false},
		{map[string]any{"type": "attachment", "origin": map[string]string{"kind": "peer"}, "attachment": map[string]any{"origin": map[string]string{"kind": "peer"}, "type": "queued_command", "prompt": "quoted:\n" + header}}, false, false},
		{map[string]any{"type": "attachment", "origin": map[string]string{"kind": "peer"}, "attachment": map[string]any{"origin": map[string]string{"kind": "peer"}, "type": "queued_command", "prompt": header + "0\nbody"}}, false, false},
	}
	for i, f := range fixtures {
		b, _ := json.Marshal(f.record)
		found, e := fakeClaudeLookup(b, header)
		if found != f.found || (e != nil) != f.err {
			t.Fatalf("fixture %d found=%v error=%v", i, found, e)
		}
	}
}
