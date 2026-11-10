// Package wsunix implements bounded WebSocket messages over a Unix socket.
package wsunix

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

const MaxMessage = 32 << 20

type Conn struct {
	net.Conn
	reader  *bufio.Reader
	writeMu sync.Mutex
	done    chan struct{}
	once    sync.Once
}

func Dial(ctx context.Context, path string) (*Conn, error) {
	raw, err := (&net.Dialer{}).DialContext(ctx, "unix", path)
	if err != nil {
		return nil, err
	}
	w := &Conn{Conn: raw, reader: bufio.NewReader(raw), done: make(chan struct{})}
	go func() {
		select {
		case <-ctx.Done():
			w.Close()
		case <-w.done:
		}
	}()
	deadline := time.Now().Add(10 * time.Second)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	_ = raw.SetDeadline(deadline)
	keyBytes := make([]byte, 16)
	if _, err = rand.Read(keyBytes); err != nil {
		w.Close()
		return nil, err
	}
	key := base64.StdEncoding.EncodeToString(keyBytes)
	_, err = fmt.Fprintf(raw, "GET / HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n\r\n", key)
	if err != nil {
		w.Close()
		return nil, err
	}
	response, err := http.ReadResponse(w.reader, nil)
	if err != nil {
		w.Close()
		return nil, err
	}
	sum := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	if response.StatusCode != 101 || !token(response.Header.Get("Connection"), "upgrade") || !strings.EqualFold(response.Header.Get("Upgrade"), "websocket") || response.Header.Get("Sec-WebSocket-Accept") != base64.StdEncoding.EncodeToString(sum[:]) || response.Header.Get("Sec-WebSocket-Extensions") != "" {
		w.Close()
		return nil, errors.New("invalid WebSocket upgrade")
	}
	return w, nil
}
func token(v, want string) bool {
	for _, p := range strings.Split(v, ",") {
		if strings.EqualFold(strings.TrimSpace(p), want) {
			return true
		}
	}
	return false
}
func (w *Conn) Close() error { w.once.Do(func() { close(w.done) }); return w.Conn.Close() }
func (w *Conn) WriteMessage(data []byte) error {
	if len(data) > MaxMessage || !utf8.Valid(data) {
		return errors.New("invalid or oversized WebSocket text")
	}
	return w.write(1, data)
}
func (w *Conn) write(op byte, data []byte) error {
	w.writeMu.Lock()
	defer w.writeMu.Unlock()
	mask := make([]byte, 4)
	if _, err := rand.Read(mask); err != nil {
		return err
	}
	frame := []byte{0x80 | op}
	switch {
	case len(data) < 126:
		frame = append(frame, 0x80|byte(len(data)))
	case len(data) <= 65535:
		frame = append(frame, 0xfe, byte(len(data)>>8), byte(len(data)))
	default:
		frame = append(frame, 0xff)
		frame = binary.BigEndian.AppendUint64(frame, uint64(len(data)))
	}
	frame = append(frame, mask...)
	for i, b := range data {
		frame = append(frame, b^mask[i%4])
	}
	for len(frame) > 0 {
		n, err := w.Conn.Write(frame)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		frame = frame[n:]
	}
	return nil
}
func (w *Conn) ReadMessage() ([]byte, error) {
	var message []byte
	fragmented := false
	for {
		var h [2]byte
		if _, err := io.ReadFull(w.reader, h[:]); err != nil {
			return nil, err
		}
		fin, op := h[0]&128 != 0, h[0]&15
		if h[0]&0x70 != 0 || h[1]&128 != 0 {
			return nil, errors.New("invalid server WebSocket flags")
		}
		size := uint64(h[1] & 127)
		if size == 126 {
			var b [2]byte
			if _, err := io.ReadFull(w.reader, b[:]); err != nil {
				return nil, err
			}
			size = uint64(binary.BigEndian.Uint16(b[:]))
			if size < 126 {
				return nil, errors.New("noncanonical frame length")
			}
		} else if size == 127 {
			var b [8]byte
			if _, err := io.ReadFull(w.reader, b[:]); err != nil {
				return nil, err
			}
			size = binary.BigEndian.Uint64(b[:])
			if size < 65536 || size>>63 != 0 {
				return nil, errors.New("invalid frame length")
			}
		}
		control := op >= 8
		if size > MaxMessage || (!control && uint64(len(message))+size > MaxMessage) || (control && (!fin || size > 125)) {
			return nil, errors.New("invalid or oversized WebSocket frame")
		}
		data := make([]byte, int(size))
		if _, err := io.ReadFull(w.reader, data); err != nil {
			return nil, err
		}
		switch op {
		case 8:
			if len(data) == 1 {
				return nil, errors.New("invalid close frame")
			}
			if len(data) > 1 {
				code := binary.BigEndian.Uint16(data)
				if code < 1000 || code >= 5000 || code == 1004 || code == 1005 || code == 1006 || code == 1015 || !utf8.Valid(data[2:]) {
					return nil, errors.New("invalid close payload")
				}
			}
			_ = w.write(8, data)
			return nil, io.EOF
		case 9:
			if err := w.write(10, data); err != nil {
				return nil, err
			}
			continue
		case 10:
			continue
		case 1:
			if fragmented {
				return nil, errors.New("interleaved text messages")
			}
			fragmented = true
		case 0:
			if !fragmented {
				return nil, errors.New("unexpected continuation")
			}
		default:
			return nil, errors.New("unsupported WebSocket opcode")
		}
		message = append(message, data...)
		if fin {
			if !utf8.Valid(message) {
				return nil, errors.New("invalid UTF-8")
			}
			return message, nil
		}
	}
}
