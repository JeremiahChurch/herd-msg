package receiver

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"

	"github.com/JeremiahChurch/herd-msg/internal/broker"
	"github.com/JeremiahChurch/herd-msg/internal/store"
)

// Lookup reads only the paths pinned into an attempt and its explicit successors.
func Lookup(ctx context.Context, r broker.LookupRequest) (bool, error) {
	header, _, _ := strings.Cut(r.Attempt.Envelope, "\n")
	sessions := append([]store.Session{r.Attempt.Session}, r.Successors...)
	var last error
	for _, s := range sessions {
		path := s.Hints["transcript"]
		if path == "" {
			last = errors.New("receiver has no known transcript path")
			continue
		}
		f, e := os.Open(path)
		if e != nil {
			last = e
			continue
		}
		found, e := scan(ctx, f, s.ID, header)
		f.Close()
		if found {
			return true, nil
		}
		if e != nil {
			last = e
		}
	}
	return false, last
}
func scan(ctx context.Context, r io.Reader, session, header string) (bool, error) {
	br := bufio.NewReader(r)
	var last error
	for {
		if e := ctx.Err(); e != nil {
			return false, e
		}
		line, e := br.ReadBytes('\n')
		if e == io.EOF {
			return false, last
		}
		if e != nil {
			return false, e
		}
		if len(line) > 32<<20 {
			return false, errors.New("transcript record too large")
		}
		found, e := record(line, session, header)
		if found {
			return true, nil
		}
		if e != nil {
			last = e
		}
	}
}
func record(data []byte, session, header string) (bool, error) {
	var row struct {
		Type    string `json:"type"`
		Session string `json:"sessionId"`
		Origin  struct {
			Kind string `json:"kind"`
		} `json:"origin"`
		Message struct {
			Content json.RawMessage `json:"content"`
		} `json:"message"`
		Attachment struct {
			Type   string `json:"type"`
			Prompt string `json:"prompt"`
			Origin struct {
				Kind string `json:"kind"`
			} `json:"origin"`
		} `json:"attachment"`
	}
	if e := json.Unmarshal(data, &row); e != nil {
		return false, e
	}
	if row.Session != session {
		return false, nil
	}
	switch row.Type {
	case "user":
		if row.Origin.Kind != "peer" {
			return false, nil
		}
		var content string
		if json.Unmarshal(row.Message.Content, &content) != nil {
			return false, nil
		}
		const wrapper = "Another Claude session sent a message:\n"
		if !strings.HasPrefix(content, wrapper) {
			for _, line := range strings.Split(content, "\n") {
				if line == header {
					return false, errors.New("unrecognized peer wrapper for this attempt")
				}
			}
			return false, nil
		}
		line, _, _ := strings.Cut(strings.TrimPrefix(content, wrapper), "\n")
		return line == header, nil
	case "attachment":
		if row.Attachment.Type != "queued_command" || row.Attachment.Origin.Kind != "peer" {
			return false, nil
		}
		line, _, _ := strings.Cut(row.Attachment.Prompt, "\n")
		return line == header, nil
	}
	return false, nil
}
