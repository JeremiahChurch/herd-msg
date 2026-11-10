// Command codex sends JSON-RPC requests over a Unix WebSocket.
// Use only explicit throwaway thread IDs. Output may contain private data.
package main

import (
	"bufio"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"time"
)

type websocket struct {
	net.Conn
	reader *bufio.Reader
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: codex-probe socket [experimental]")
		os.Exit(2)
	}
	c, err := net.DialTimeout("unix", os.Args[1], 5*time.Second)
	if err != nil {
		panic(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(30 * time.Second))
	key := make([]byte, 16)
	if _, err := rand.Read(key); err != nil {
		panic(err)
	}
	_, err = fmt.Fprintf(c, "GET / HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n\r\n", base64.StdEncoding.EncodeToString(key))
	if err != nil {
		panic(err)
	}
	r := bufio.NewReader(c)
	response, err := http.ReadResponse(r, nil)
	if err != nil {
		panic(err)
	}
	if response.StatusCode != 101 {
		panic(response.Status)
	}
	w := websocket{c, r}
	call := func(id int, method string, params any) {
		data, err := json.Marshal(map[string]any{"id": id, "method": method, "params": params})
		if err != nil {
			panic(err)
		}
		if err := w.write(1, data); err != nil {
			panic(err)
		}
		for {
			data, err := w.read()
			if err != nil {
				panic(err)
			}
			fmt.Println(string(data))
			var message struct {
				ID *int `json:"id"`
			}
			_ = json.Unmarshal(data, &message)
			if message.ID != nil && *message.ID == id {
				break
			}
		}
	}
	call(1, "initialize", map[string]any{"clientInfo": map[string]string{"name": "herd-msg-probe", "version": "0.1.0"}, "capabilities": map[string]bool{"experimentalApi": len(os.Args) > 2 && os.Args[2] == "experimental"}})
	if err := w.write(1, []byte(`{"method":"initialized"}`)); err != nil {
		panic(err)
	}
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, 4096), 1024*1024)
	id := 2
	for scanner.Scan() {
		var request struct {
			Method string `json:"method"`
			Params any    `json:"params"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &request); err != nil {
			panic(err)
		}
		call(id, request.Method, request.Params)
		id++
	}
	if err := scanner.Err(); err != nil {
		panic(err)
	}
}

func (w websocket) write(op byte, data []byte) error {
	mask := make([]byte, 4)
	if _, err := rand.Read(mask); err != nil {
		return err
	}
	header := []byte{0x80 | op}
	switch {
	case len(data) < 126:
		header = append(header, 0x80|byte(len(data)))
	case len(data) <= 65535:
		header = append(header, 0x80|126, byte(len(data)>>8), byte(len(data)))
	default:
		header = append(header, 0x80|127)
		header = binary.BigEndian.AppendUint64(header, uint64(len(data)))
	}
	header = append(header, mask...)
	for i, v := range data {
		header = append(header, v^mask[i%4])
	}
	for len(header) > 0 {
		n, err := w.Conn.Write(header)
		if err != nil {
			return err
		}
		header = header[n:]
	}
	return nil
}

func (w websocket) read() ([]byte, error) {
	var message []byte
	for {
		header := make([]byte, 2)
		if _, err := io.ReadFull(w.reader, header); err != nil {
			return nil, err
		}
		n := uint64(header[1] & 127)
		if n == 126 {
			b := make([]byte, 2)
			if _, err := io.ReadFull(w.reader, b); err != nil {
				return nil, err
			}
			n = uint64(binary.BigEndian.Uint16(b))
		}
		if n == 127 {
			b := make([]byte, 8)
			if _, err := io.ReadFull(w.reader, b); err != nil {
				return nil, err
			}
			n = binary.BigEndian.Uint64(b)
		}
		if n > 64*1024*1024 {
			return nil, fmt.Errorf("frame exceeds probe limit")
		}
		var mask []byte
		if header[1]&128 != 0 {
			mask = make([]byte, 4)
			if _, err := io.ReadFull(w.reader, mask); err != nil {
				return nil, err
			}
		}
		data := make([]byte, n)
		if _, err := io.ReadFull(w.reader, data); err != nil {
			return nil, err
		}
		if mask != nil {
			for i := range data {
				data[i] ^= mask[i%4]
			}
		}
		switch header[0] & 15 {
		case 8:
			return nil, io.EOF
		case 9:
			if err := w.write(10, data); err != nil {
				return nil, err
			}
			continue
		case 10:
			continue
		}
		message = append(message, data...)
		if header[0]&128 != 0 {
			return message, nil
		}
	}
}
