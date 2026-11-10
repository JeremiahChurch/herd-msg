package muse

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"
	"time"

	"github.com/JeremiahChurch/herd-msg/internal/broker"
)

type record struct {
	Schema int `json:"schema_version"`
	Stream struct {
		Kind string `json:"kind"`
		ID   string `json:"id"`
	} `json:"stream"`
	Durability    string          `json:"durability"`
	Type          string          `json:"payload_type"`
	PayloadSchema int             `json:"payload_schema_version"`
	Payload       json.RawMessage `json:"payload"`
}

func accepted(raw []byte, session, body string, depth int) (bool, error) {
	if depth > 4 {
		return false, errors.New("Muse retained frame nesting exceeded")
	}
	var r record
	if e := json.Unmarshal(raw, &r); e != nil {
		return false, e
	}
	if r.Schema != 1 || r.Stream.Kind != "session" || r.Stream.ID != session || r.Durability != "durable" {
		return false, nil
	}
	if r.Type == "retained_frame" {
		var frame struct {
			Children []struct {
				JSON string `json:"record_json"`
			} `json:"children"`
		}
		if e := json.Unmarshal(r.Payload, &frame); e != nil {
			return false, e
		}
		for _, child := range frame.Children {
			found, e := accepted([]byte(child.JSON), session, body, depth+1)
			if found || e != nil {
				return found, e
			}
		}
	}
	if r.Type != "peer_message.admission.settled" || r.PayloadSchema != 2 {
		return false, nil
	}
	var admission struct {
		Schema   int    `json:"schema_version"`
		Cause    string `json:"cause"`
		Outcomes []struct {
			Status string `json:"status"`
			Item   struct {
				Body   string `json:"body"`
				Source struct {
					Schema     int    `json:"schema_version"`
					Authority  string `json:"authority"`
					Source     string `json:"source"`
					Provenance string `json:"ingress_provenance"`
					Target     string `json:"target_session_id"`
				} `json:"source"`
			} `json:"inbox_item"`
		} `json:"outcomes"`
	}
	if e := json.Unmarshal(r.Payload, &admission); e != nil {
		return false, e
	}
	if admission.Schema != 2 || admission.Cause != "authenticated_external_agent" || body == "" {
		return false, nil
	}
	for _, o := range admission.Outcomes {
		s := o.Item.Source
		if o.Status == "accepted" && o.Item.Body == body && s.Schema == 2 && s.Target == session && s.Source == "session_message" && s.Authority == "runtime_context" && s.Provenance == "external_agent" {
			return true, nil
		}
	}
	return false, nil
}

const transcriptReadLimit = 4 << 20

type transcriptCursor struct {
	Offset   int64
	Info     os.FileInfo
	Checked  time.Time
	Found    bool
	Proof    string
	Pending  []byte
	Dropping bool
}

// Save a file identity and byte offset before the native command. The proof
// survives broker restarts; replacements and truncations restart a bounded scan.
func (d *Driver) seedLookup(ctx context.Context, session, aid string) (string, error) {
	path, e := logPath(ctx, session)
	if e != nil {
		return "", e
	}
	file, e := os.Open(path)
	if e != nil {
		return "", e
	}
	defer file.Close()
	info, e := file.Stat()
	if e != nil {
		return "", e
	}
	partial := false
	if info.Size() > 0 {
		var last [1]byte
		if _, e = file.ReadAt(last[:], info.Size()-1); e != nil {
			return "", e
		}
		partial = last[0] != '\n'
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return "", errors.New("missing transcript file identity")
	}
	d.lookupMu.Lock()
	defer d.lookupMu.Unlock()
	if d.lookups == nil {
		d.lookups = map[string]*transcriptCursor{}
	}
	for key, scan := range d.lookups {
		if time.Since(scan.Checked) > time.Hour {
			delete(d.lookups, key)
		}
	}
	if len(d.lookups) >= 1024 {
		for key := range d.lookups {
			delete(d.lookups, key)
			break
		}
	}
	proof := fmt.Sprintf("muse-offset:%d:%d:%d:%t", stat.Dev, stat.Ino, info.Size(), partial)
	d.lookups[session+"/"+aid] = &transcriptCursor{Offset: info.Size(), Info: info, Checked: time.Now(), Dropping: partial, Proof: proof}
	return proof, nil
}
func (d *Driver) Lookup(ctx context.Context, r broker.LookupRequest) (bool, error) {
	path, e := logPath(ctx, r.Attempt.Session.ID)
	if e != nil {
		return false, e
	}
	f, e := os.Open(path)
	if e != nil {
		return false, e
	}
	defer f.Close()
	info, e := f.Stat()
	if e != nil {
		return false, e
	}
	d.lookupMu.Lock()
	defer d.lookupMu.Unlock()
	if d.lookups == nil {
		d.lookups = map[string]*transcriptCursor{}
	}
	for key, scan := range d.lookups {
		if time.Since(scan.Checked) > time.Hour {
			delete(d.lookups, key)
		}
	}
	key := r.Attempt.Session.ID + "/" + r.Attempt.ID
	defer d.trimPending(key)
	scan := d.lookups[key]
	if scan == nil {
		if len(d.lookups) >= 1024 {
			for key := range d.lookups {
				delete(d.lookups, key)
				break
			}
		}
		scan = &transcriptCursor{Info: info, Proof: r.Attempt.Proof}
		var dev, ino uint64
		var offset int64
		var partial bool
		fields, _ := fmt.Sscanf(r.Attempt.Proof, "muse-offset:%d:%d:%d:%t", &dev, &ino, &offset, &partial)
		if fields >= 3 {
			if stat, ok := info.Sys().(*syscall.Stat_t); ok && uint64(stat.Dev) == dev && stat.Ino == ino && offset >= 0 && offset <= info.Size() {
				scan.Offset = offset
				scan.Dropping = partial
			}
		}
		d.lookups[key] = scan
	}
	if !os.SameFile(scan.Info, info) || info.Size() < scan.Offset {
		scan.Offset = 0
		scan.Info = info
		scan.Found = false
		scan.Pending = nil
		scan.Dropping = false
	}
	scan.Checked = time.Now()
	if scan.Found {
		return true, nil
	}
	// Carry records across byte budgets and partial appends. Drop only an
	// incomplete pre-send record, or an oversized record, through its newline.
	reader := bufio.NewReader(io.NewSectionReader(f, scan.Offset, transcriptReadLimit))
	var last error
	for {
		if e := ctx.Err(); e != nil {
			return false, e
		}
		chunk, e := reader.ReadBytes('\n')
		scan.Offset += int64(len(chunk))
		if !scan.Dropping {
			if len(scan.Pending)+len(chunk) > 16<<20 {
				scan.Pending = nil
				scan.Dropping = true
				last = errors.New("Muse transcript record exceeds 16 MiB")
			} else {
				scan.Pending = append(scan.Pending, chunk...)
			}
		}
		complete := len(chunk) > 0 && chunk[len(chunk)-1] == '\n'
		if complete {
			if scan.Dropping {
				scan.Dropping = false
				scan.Pending = nil
			} else {
				found, parseErr := accepted(scan.Pending, r.Attempt.Session.ID, r.Attempt.Envelope, 0)
				scan.Pending = nil
				if found {
					scan.Found = true
					return true, nil
				}
				if parseErr != nil {
					last = parseErr
				}
			}
		}
		if e != nil {
			if e != io.EOF {
				return false, e
			}
			if len(scan.Pending) > 0 || scan.Dropping {
				return false, errors.New("incomplete Muse record; scan will resume at its saved offset")
			}
			return false, last
		}
	}
}

// The caller holds lookupMu. Bound retained partial-record memory across the
// entire driver; an evicted attempt can restart from its durable pre-send proof.
func (d *Driver) trimPending(keep string) {
	total := 0
	for _, scan := range d.lookups {
		total += cap(scan.Pending)
	}
	for key, scan := range d.lookups {
		if total <= 64<<20 {
			return
		}
		if key != keep {
			total -= cap(scan.Pending)
			delete(d.lookups, key)
		}
	}
}
