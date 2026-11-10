package wsunix

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"
)

func frame(op byte, fin bool, b []byte) []byte {
	h := op
	if fin {
		h |= 128
	}
	out := []byte{h}
	switch {
	case len(b) < 126:
		out = append(out, byte(len(b)))
	case len(b) < 65536:
		out = append(out, 126, byte(len(b)>>8), byte(len(b)))
	default:
		out = append(out, 127)
		out = binary.BigEndian.AppendUint64(out, uint64(len(b)))
	}
	return append(out, b...)
}
func clientFrame(r io.Reader) (byte, []byte, error) {
	var h [2]byte
	if _, e := io.ReadFull(r, h[:]); e != nil {
		return 0, nil, e
	}
	if h[1]&128 == 0 {
		return 0, nil, fmt.Errorf("client frame unmasked")
	}
	n := int(h[1] & 127)
	if n == 126 {
		var b [2]byte
		io.ReadFull(r, b[:])
		n = int(binary.BigEndian.Uint16(b[:]))
	} else if n == 127 {
		var b [8]byte
		io.ReadFull(r, b[:])
		n = int(binary.BigEndian.Uint64(b[:]))
	}
	var m [4]byte
	if _, e := io.ReadFull(r, m[:]); e != nil {
		return 0, nil, e
	}
	b := make([]byte, n)
	_, e := io.ReadFull(r, b)
	for i := range b {
		b[i] ^= m[i%4]
	}
	return h[0] & 15, b, e
}
func TestFragmentsPingAndMaskedLengths(t *testing.T) {
	for _, n := range []int{0, 125, 126, 127, 65535, 65536} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			a, b := net.Pipe()
			defer a.Close()
			defer b.Close()
			w := &Conn{Conn: a, reader: bufio.NewReader(a), done: make(chan struct{})}
			payload := bytes.Repeat([]byte("x"), n)
			errs := make(chan error, 1)
			go func() {
				op, p, e := clientFrame(b)
				if e != nil || op != 1 || !bytes.Equal(p, payload) {
					errs <- fmt.Errorf("masked write op=%d len=%d error=%v", op, len(p), e)
					return
				}
				_, e = b.Write(frame(1, false, []byte("hel")))
				if e != nil {
					errs <- e
					return
				}
				_, e = b.Write(frame(9, true, []byte("ping")))
				if e != nil {
					errs <- e
					return
				}
				op, p, e = clientFrame(b)
				if e != nil || op != 10 || string(p) != "ping" {
					errs <- fmt.Errorf("pong %d %q %v", op, p, e)
					return
				}
				_, e = b.Write(frame(0, true, []byte("lo")))
				errs <- e
			}()
			if e := w.WriteMessage(payload); e != nil {
				t.Fatal(e)
			}
			p, e := w.ReadMessage()
			if e != nil || string(p) != "hello" {
				t.Fatalf("%q %v", p, e)
			}
			if e := <-errs; e != nil {
				t.Fatal(e)
			}
		})
	}
}
func TestMalformedFrames(t *testing.T) {
	for _, data := range [][]byte{{0x80, 0}, {0x81, 0x80}, {0xc1, 0}, {0x09, 0}, {0x83, 0}, {0x81, 126, 0, 1}, {0x81, 127, 0x80, 0, 0, 0, 0, 0, 0, 0}, {0x81, 1, 0xff}, {0x88, 1, 0}, {0x81, 127, 0, 0, 0, 0, 2, 0, 0, 1}} {
		a, b := net.Pipe()
		w := &Conn{Conn: a, reader: bufio.NewReader(a), done: make(chan struct{})}
		go func() { defer b.Close(); b.Write(data) }()
		a.SetDeadline(time.Now().Add(time.Second))
		if _, e := w.ReadMessage(); e == nil {
			t.Errorf("accepted malformed %x", data)
		}
		a.Close()
	}
}
func TestUpgradeAndCancellation(t *testing.T) {
	for _, valid := range []bool{false, true} {
		path := filepath.Join(t.TempDir(), "ws.sock")
		l, e := net.Listen("unix", path)
		if e != nil {
			t.Fatal(e)
		}
		defer l.Close()
		go func() {
			c, e := l.Accept()
			if e != nil {
				return
			}
			defer c.Close()
			r, e := http.ReadRequest(bufio.NewReader(c))
			if e != nil {
				return
			}
			sum := sha1.Sum([]byte(r.Header.Get("Sec-WebSocket-Key") + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
			accept := base64.StdEncoding.EncodeToString(sum[:])
			if !valid {
				accept = "bad"
			}
			fmt.Fprintf(c, "HTTP/1.1 101 Switching Protocols\r\nConnection: keep-alive, Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: %s\r\n\r\n", accept)
			io.Copy(io.Discard, c)
		}()
		ctx, cancel := context.WithCancel(context.Background())
		w, e := Dial(ctx, path)
		if !valid {
			if e == nil {
				w.Close()
				t.Fatal("bad accept")
			}
			cancel()
			continue
		}
		if e != nil {
			t.Fatal(e)
		}
		cancel()
		done := make(chan error, 1)
		go func() { _, e := w.ReadMessage(); done <- e }()
		select {
		case e := <-done:
			if e == nil {
				t.Fatal("cancel accepted")
			}
		case <-time.After(time.Second):
			t.Fatal("cancellation did not close")
		}
		w.Close()
	}
}

func TestExtendedServerLengthsKeepPayloadBytes(t *testing.T) {
	for _, size := range []int{126, 127, 128, 65535, 65536} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			payload := bytes.Repeat([]byte("x"), size)
			w := &Conn{reader: bufio.NewReader(bytes.NewReader(frame(1, true, payload)))}
			got, e := w.ReadMessage()
			if e != nil || !bytes.Equal(got, payload) {
				t.Fatalf("server length %d: got %d, %v", size, len(got), e)
			}
		})
	}
}
