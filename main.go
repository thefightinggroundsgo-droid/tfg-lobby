// TFG lobby server: players open a lobby by name (and optional password), friends join it by name,
// and the server relays the game traffic between the two so nobody has to type an IP address or
// open ports on their router.
//
// One WebSocket per player at /ws. The first message is JSON:
//   {"op":"host","name":"v","lobby":"Friday","pass":"1234","v":1}
//   {"op":"join","name":"bob","lobby":"Friday","pass":"1234","v":1}
// Replies: {"op":"hosted"}, {"op":"joined","host":"v"}, to the host {"op":"guest","name":"bob"},
// {"op":"error","msg":"..."} and, when the other player leaves, {"op":"left"}.
// After the pair is made every frame (binary or text) from one player is passed to the other as is.
//
// GET /lobbies lists open lobbies (no passwords), GET /health answers "ok".
package main

import (
	"crypto/subtle"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/gorilla/websocket"
)

const (
	maxLobbies    = 500
	maxPerIP      = 4
	maxNameLen    = 20
	maxFrame      = 64 * 1024
	pingEvery     = 20 * time.Second
	readTimeout   = 60 * time.Second
	writeTimeout  = 10 * time.Second
	lobbyLifetime = 8 * time.Hour
	sendQueue     = 2048
)

type frame struct {
	kind int
	data []byte
}

type peer struct {
	conn    *websocket.Conn
	send    chan frame
	done    chan struct{}
	once    sync.Once
	name    string
	ip      string
	partner *peer // set once paired, guarded by mu
	lobby   *lobby
}

type lobby struct {
	key, name, pass string
	host, guest     *peer
	created         time.Time
}

var (
	mu      sync.Mutex
	lobbies = map[string]*lobby{}
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	CheckOrigin:     func(*http.Request) bool { return true }, // the game helper is not a browser
}

func clientIP(r *http.Request) string {
	if f := r.Header.Get("X-Forwarded-For"); f != "" {
		return strings.TrimSpace(strings.Split(f, ",")[0])
	}
	h, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return h
}

// cleanName trims a lobby or player name and rejects empty, too long or control-character names.
func cleanName(s string) (string, bool) {
	s = strings.TrimSpace(s)
	if s == "" || utf8.RuneCountInString(s) > maxNameLen || !utf8.ValidString(s) {
		return "", false
	}
	for _, r := range s {
		if r < 32 || r == 127 {
			return "", false
		}
	}
	return s, true
}

func (p *peer) close() {
	p.once.Do(func() {
		close(p.done)
		p.conn.Close()
	})
}

// queue hands a frame to the writer; a player that stops reading for writeTimeout is dropped.
func (p *peer) queue(f frame) bool {
	select {
	case p.send <- f:
		return true
	case <-p.done:
		return false
	case <-time.After(writeTimeout):
		p.close()
		return false
	}
}

func (p *peer) json(v any) bool {
	b, _ := json.Marshal(v)
	return p.queue(frame{websocket.TextMessage, b})
}

func (p *peer) writer() {
	t := time.NewTicker(pingEvery)
	defer t.Stop()
	for {
		select {
		case f := <-p.send:
			p.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			if err := p.conn.WriteMessage(f.kind, f.data); err != nil {
				p.close()
				return
			}
		case <-t.C:
			p.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			if err := p.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				p.close()
				return
			}
		case <-p.done:
			return
		}
	}
}

type hello struct {
	Op    string `json:"op"`
	Name  string `json:"name"`
	Lobby string `json:"lobby"`
	Pass  string `json:"pass"`
}

func fail(p *peer, msg string) {
	p.json(map[string]string{"op": "error", "msg": msg})
	time.Sleep(200 * time.Millisecond) // let the writer send it
	p.close()
}

func handleWS(w http.ResponseWriter, r *http.Request) {
	c, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	c.SetReadLimit(maxFrame)
	c.SetReadDeadline(time.Now().Add(readTimeout))
	c.SetPongHandler(func(string) error { c.SetReadDeadline(time.Now().Add(readTimeout)); return nil })
	p := &peer{conn: c, send: make(chan frame, sendQueue), done: make(chan struct{}), ip: clientIP(r)}
	go p.writer()
	defer leave(p)

	var h hello
	if _, b, err := c.ReadMessage(); err != nil || json.Unmarshal(b, &h) != nil {
		fail(p, "bad request")
		return
	}
	name, ok := cleanName(h.Name)
	if !ok {
		fail(p, "set a player name first (1-20 letters)")
		return
	}
	p.name = name
	lname, ok := cleanName(h.Lobby)
	if !ok {
		fail(p, "lobby name must be 1-20 letters")
		return
	}
	if len(h.Pass) > 40 {
		fail(p, "password is too long")
		return
	}
	key := strings.ToLower(lname)

	switch h.Op {
	case "host":
		mu.Lock()
		perIP := 0
		for _, l := range lobbies {
			if l.host.ip == p.ip {
				perIP++
			}
		}
		switch {
		case lobbies[key] != nil:
			mu.Unlock()
			fail(p, "that lobby name is taken, pick another")
			return
		case len(lobbies) >= maxLobbies:
			mu.Unlock()
			fail(p, "the server is full, try again later")
			return
		case perIP >= maxPerIP:
			mu.Unlock()
			fail(p, "you already have too many lobbies open")
			return
		}
		l := &lobby{key: key, name: lname, pass: h.Pass, host: p, created: time.Now()}
		lobbies[key] = l
		p.lobby = l
		mu.Unlock()
		log.Printf("lobby %q opened by %q", lname, name)
		p.json(map[string]string{"op": "hosted", "lobby": lname})
	case "join":
		mu.Lock()
		l := lobbies[key]
		switch {
		case l == nil:
			mu.Unlock()
			fail(p, "no lobby with that name is open")
			return
		case subtle.ConstantTimeCompare([]byte(l.pass), []byte(h.Pass)) != 1:
			mu.Unlock()
			time.Sleep(500 * time.Millisecond)
			fail(p, "wrong password")
			return
		case l.guest != nil:
			mu.Unlock()
			fail(p, "that lobby is already full")
			return
		}
		l.guest = p
		p.lobby = l
		p.partner = l.host
		l.host.partner = p
		host := l.host
		mu.Unlock()
		log.Printf("%q joined lobby %q", name, l.name)
		p.json(map[string]string{"op": "joined", "lobby": l.name, "host": host.name})
		host.json(map[string]string{"op": "guest", "name": name})
	default:
		fail(p, "bad request")
		return
	}

	// relay everything that follows to the partner
	for {
		kind, b, err := c.ReadMessage()
		if err != nil {
			return
		}
		c.SetReadDeadline(time.Now().Add(readTimeout))
		mu.Lock()
		other := p.partner
		mu.Unlock()
		if other == nil {
			continue // host talking before anyone joined: nothing to pass on
		}
		if !other.queue(frame{kind, b}) {
			return
		}
	}
}

// leave closes a player's lobby and tells the partner, so both helpers end the session.
func leave(p *peer) {
	p.close()
	mu.Lock()
	l := p.lobby
	other := p.partner
	if l != nil && lobbies[l.key] == l {
		delete(lobbies, l.key)
		log.Printf("lobby %q closed", l.name)
	}
	mu.Unlock()
	if other != nil {
		other.json(map[string]string{"op": "left"})
		go func() { time.Sleep(time.Second); other.close() }()
	}
}

type entry struct {
	Lobby  string `json:"lobby"`
	Host   string `json:"host"`
	Locked bool   `json:"locked"`
	Full   bool   `json:"full"`
}

func handleList(w http.ResponseWriter, r *http.Request) {
	mu.Lock()
	out := make([]entry, 0, len(lobbies))
	for _, l := range lobbies {
		out = append(out, entry{l.name, l.host.name, l.pass != "", l.guest != nil})
	}
	mu.Unlock()
	sort.Slice(out, func(i, j int) bool { return strings.ToLower(out[i].Lobby) < strings.ToLower(out[j].Lobby) })
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	json.NewEncoder(w).Encode(out)
}

// reaper closes lobbies older than lobbyLifetime so a forgotten one cannot hold a name forever.
func reaper() {
	for range time.Tick(time.Minute) {
		var old []*lobby
		mu.Lock()
		for _, l := range lobbies {
			if time.Since(l.created) > lobbyLifetime {
				old = append(old, l)
			}
		}
		mu.Unlock()
		for _, l := range old {
			l.host.close()
		}
	}
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	go reaper()
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", handleWS)
	mux.HandleFunc("/lobbies", handleList)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Write([]byte("TFG lobby server is running.\n"))
	})
	log.Printf("TFG lobby server listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}
