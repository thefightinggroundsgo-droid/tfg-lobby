// Package websocket is a small RFC 6455 implementation (server and client) that needs nothing outside the Go
// standard library, so the lobby server builds without downloading modules and the same file also compiles into
// the Windows helper. It offers the part of the gorilla/websocket API that TFG uses.
package websocket

import (
	"bufio"
	"crypto/rand"
	"crypto/sha1"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	TextMessage   = 1
	BinaryMessage = 2
	CloseMessage  = 8
	PingMessage   = 9
	PongMessage   = 10
)

var ErrClosed = errors.New("websocket: closed")
var ErrTooBig = errors.New("websocket: message too big")

const guid = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

type Conn struct {
	c        net.Conn
	br       *bufio.Reader
	client   bool
	wmu      sync.Mutex
	limit    int64
	pong     func(string) error
	closed   bool
	cmu      sync.Mutex
	partKind int
	part     []byte
	idle     time.Duration // when set, every frame (pings too) pushes the read deadline out by this much
}

func newConn(c net.Conn, br *bufio.Reader, client bool) *Conn {
	return &Conn{c: c, br: br, client: client, limit: 1 << 20}
}

func (w *Conn) SetReadLimit(n int64)                { w.limit = n }
func (w *Conn) SetIdleTimeout(d time.Duration)      { w.idle = d }
func (w *Conn) SetReadDeadline(t time.Time)         { w.c.SetReadDeadline(t) }
func (w *Conn) SetWriteDeadline(t time.Time)        { w.c.SetWriteDeadline(t) }
func (w *Conn) SetPongHandler(h func(string) error) { w.pong = h }
func (w *Conn) RemoteAddr() net.Addr                { return w.c.RemoteAddr() }

func (w *Conn) Close() error {
	w.cmu.Lock()
	if w.closed {
		w.cmu.Unlock()
		return nil
	}
	w.closed = true
	w.cmu.Unlock()
	// best effort close frame, never block
	w.c.SetWriteDeadline(time.Now().Add(300 * time.Millisecond))
	w.writeFrame(CloseMessage, []byte{0x03, 0xe8})
	return w.c.Close()
}

func (w *Conn) WriteMessage(kind int, data []byte) error {
	if kind != TextMessage && kind != BinaryMessage && kind != PingMessage && kind != PongMessage && kind != CloseMessage {
		return errors.New("websocket: bad message type")
	}
	return w.writeFrame(kind, data)
}

func (w *Conn) writeFrame(op int, data []byte) error {
	w.wmu.Lock()
	defer w.wmu.Unlock()
	hdr := make([]byte, 0, 14)
	hdr = append(hdr, 0x80|byte(op))
	mask := byte(0)
	if w.client {
		mask = 0x80
	}
	n := len(data)
	switch {
	case n < 126:
		hdr = append(hdr, mask|byte(n))
	case n < 65536:
		hdr = append(hdr, mask|126, byte(n>>8), byte(n))
	default:
		hdr = append(hdr, mask|127)
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], uint64(n))
		hdr = append(hdr, b[:]...)
	}
	out := data
	if w.client {
		var k [4]byte
		rand.Read(k[:])
		hdr = append(hdr, k[:]...)
		out = make([]byte, n)
		for i := range data {
			out[i] = data[i] ^ k[i&3]
		}
	}
	buf := make([]byte, 0, len(hdr)+len(out))
	buf = append(buf, hdr...)
	buf = append(buf, out...)
	_, err := w.c.Write(buf)
	return err
}

// ReadMessage returns the next whole text or binary message. Pings are answered, pongs reported, close ends it.
func (w *Conn) ReadMessage() (int, []byte, error) {
	for {
		if w.idle > 0 {
			w.c.SetReadDeadline(time.Now().Add(w.idle))
		}
		var h [2]byte
		if _, err := io.ReadFull(w.br, h[:]); err != nil {
			return 0, nil, err
		}
		fin := h[0]&0x80 != 0
		op := int(h[0] & 0x0f)
		masked := h[1]&0x80 != 0
		n := int64(h[1] & 0x7f)
		if n == 126 {
			var b [2]byte
			if _, err := io.ReadFull(w.br, b[:]); err != nil {
				return 0, nil, err
			}
			n = int64(binary.BigEndian.Uint16(b[:]))
		} else if n == 127 {
			var b [8]byte
			if _, err := io.ReadFull(w.br, b[:]); err != nil {
				return 0, nil, err
			}
			n = int64(binary.BigEndian.Uint64(b[:]))
		}
		if n < 0 || n > w.limit || int64(len(w.part))+n > w.limit {
			return 0, nil, ErrTooBig
		}
		var key [4]byte
		if masked {
			if _, err := io.ReadFull(w.br, key[:]); err != nil {
				return 0, nil, err
			}
		}
		payload := make([]byte, n)
		if _, err := io.ReadFull(w.br, payload); err != nil {
			return 0, nil, err
		}
		if masked {
			for i := range payload {
				payload[i] ^= key[i&3]
			}
		}
		switch op {
		case 0x8:
			return 0, nil, ErrClosed
		case 0x9:
			// the write deadline the caller set earlier has long passed: give the pong its own
			w.wmu.Lock()
			w.c.SetWriteDeadline(time.Now().Add(10 * time.Second))
			w.wmu.Unlock()
			w.writeFrame(PongMessage, payload)
			continue
		case 0xa:
			if w.pong != nil {
				w.pong(string(payload))
			}
			continue
		case 0x0, 0x1, 0x2:
			if op != 0 {
				w.partKind = op
				w.part = payload
			} else {
				w.part = append(w.part, payload...)
			}
			if fin {
				m := w.part
				w.part = nil
				return w.partKind, m, nil
			}
		default:
			return 0, nil, errors.New("websocket: bad opcode")
		}
	}
}

// ---- server ------------------------------------------------------------------------------------------------------------
type Upgrader struct {
	ReadBufferSize, WriteBufferSize int
	CheckOrigin                     func(*http.Request) bool
}

func (u *Upgrader) Upgrade(rw http.ResponseWriter, r *http.Request, _ http.Header) (*Conn, error) {
	if !strings.Contains(strings.ToLower(r.Header.Get("Connection")), "upgrade") || !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		http.Error(rw, "websocket upgrade required", http.StatusBadRequest)
		return nil, errors.New("not a websocket request")
	}
	if u.CheckOrigin != nil && !u.CheckOrigin(r) {
		http.Error(rw, "origin", http.StatusForbidden)
		return nil, errors.New("origin")
	}
	key := r.Header.Get("Sec-WebSocket-Key")
	if key == "" {
		http.Error(rw, "no key", http.StatusBadRequest)
		return nil, errors.New("no key")
	}
	hj, ok := rw.(http.Hijacker)
	if !ok {
		http.Error(rw, "no hijack", http.StatusInternalServerError)
		return nil, errors.New("no hijack")
	}
	c, brw, err := hj.Hijack()
	if err != nil {
		return nil, err
	}
	sum := sha1.Sum([]byte(key + guid))
	resp := "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " +
		base64.StdEncoding.EncodeToString(sum[:]) + "\r\n\r\n"
	c.SetWriteDeadline(time.Now().Add(10 * time.Second))
	if _, err := c.Write([]byte(resp)); err != nil {
		c.Close()
		return nil, err
	}
	c.SetWriteDeadline(time.Time{})
	return newConn(c, brw.Reader, false), nil
}

// ---- client ------------------------------------------------------------------------------------------------------------
// Dial opens ws:// or wss:// (443 by default for wss).
func Dial(rawurl string, timeout time.Duration) (*Conn, error) {
	u, err := url.Parse(rawurl)
	if err != nil {
		return nil, err
	}
	host := u.Host
	if !strings.Contains(host, ":") {
		if u.Scheme == "wss" {
			host += ":443"
		} else {
			host += ":80"
		}
	}
	d := net.Dialer{Timeout: timeout}
	var c net.Conn
	if u.Scheme == "wss" {
		c, err = tls.DialWithDialer(&d, "tcp", host, &tls.Config{ServerName: u.Hostname()})
	} else {
		c, err = d.Dial("tcp", host)
	}
	if err != nil {
		return nil, err
	}
	var kb [16]byte
	rand.Read(kb[:])
	key := base64.StdEncoding.EncodeToString(kb[:])
	path := u.RequestURI()
	req := fmt.Sprintf("GET %s HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\nUser-Agent: TFG-helper\r\n\r\n", path, u.Host, key)
	c.SetDeadline(time.Now().Add(timeout))
	if _, err := c.Write([]byte(req)); err != nil {
		c.Close()
		return nil, err
	}
	br := bufio.NewReader(c)
	resp, err := http.ReadResponse(br, &http.Request{Method: "GET"})
	if err != nil {
		c.Close()
		return nil, err
	}
	if resp.StatusCode != 101 {
		c.Close()
		return nil, fmt.Errorf("server answered %s", resp.Status)
	}
	sum := sha1.Sum([]byte(key + guid))
	if resp.Header.Get("Sec-WebSocket-Accept") != base64.StdEncoding.EncodeToString(sum[:]) {
		c.Close()
		return nil, errors.New("bad handshake")
	}
	c.SetDeadline(time.Time{})
	return newConn(c, br, true), nil
}
