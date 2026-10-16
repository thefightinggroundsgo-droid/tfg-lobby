// TFG lobby server v2: ROOMS of up to 5 people. Two fight, the rest wait in a queue and watch.
// Nobody types an IP address or opens a router port: the server relays the game traffic.
//
// One WebSocket per player at /ws. The first message is JSON:
//
//	{"op":"create","v":2,"name":"v","id":"<client token>","lobby":"Friday","pass":"1234","rp":120,"w":4,"l":2,"bal":900}
//	{"op":"join",  ... same ...}
//
// Control messages are JSON text frames, game data are binary frames: [type byte][payload]
//
//	0x01  game traffic between the two fighters (relayed as is)
//	0x02  replay bytes of the running match (fighter 1 -> everyone watching)
//
// Op list (client -> server): ready, start, cfg, stepout, trade, tradeok, tradeno, terms, pick, accept, watch, result, ack, leave
// Replies: room (full snapshot, every change), go, settled, claims, trade, error.
//
// GET /lobbies lists open rooms (no passwords), GET /health answers "ok".
package main

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"tfglobby/websocket"
)

const (
	maxRooms     = 300
	maxPerIP     = 4
	maxNameLen   = 20
	maxFrame     = 256 * 1024
	maxMembers   = 5
	pingEvery    = 20 * time.Second
	readTimeout  = 70 * time.Second
	writeTimeout = 10 * time.Second
	roomLifetime = 10 * time.Hour
	sendQueue    = 4096
	maxReplay    = 48 << 20
	maxStake     = 100000
	resultWait   = 25 * time.Second
	claimKeep    = 48 * time.Hour

	tGame   = 0x01
	tReplay = 0x02
)

type frame struct {
	kind int
	data []byte
}

type member struct {
	id     string
	token  string
	name   string
	ip     string
	rp     int
	plate  int
	wins   int
	losses int
	bal    int
	conn   *websocket.Conn
	send   chan frame
	done   chan struct{}
	once   sync.Once
	room   *room
	// per match
	spec     bool // was present when the match started and is not fighting
	watching bool
	fighter  int // 0 none, 1, 2
	reported int // -1 nothing yet, else 0/1/2
	repAt    time.Time
}

type terms struct {
	Pts    int  `json:"pts"`
	Cstake bool `json:"cstake"`
}

type room struct {
	key, name, pass string
	members         []*member // join order
	queue           []*member // fight order: queue[0] and queue[1] fight next
	host            *member
	phase           string // lobby | bet | play
	winLimit        int
	streakID        string
	streakN         int
	terms           terms
	acc             [2]bool
	col             [2]string
	fit             [2]*member
	ready           [2]bool
	matchID         string
	matchBet        terms
	matchCol        [2]string
	replay          []byte
	tradeFrom       *member
	tradeTo         *member
	created         time.Time
	lastWinner      string
	settleTimer     *time.Timer
	build           string // fingerprint of the host's game files: everybody in the room must have the same one
}

var (
	mu       sync.Mutex
	rooms    = map[string]*room{}
	nextID   = 1
	nextM    = 1
	claims   = map[string][]claim{}
	upgrader = websocket.Upgrader{
		ReadBufferSize:  8192,
		WriteBufferSize: 8192,
		CheckOrigin:     func(*http.Request) bool { return true },
	}
)

type claim struct {
	Match string          `json:"match"`
	At    time.Time       `json:"-"`
	Msg   json.RawMessage `json:"msg"`
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

func clampI(x, a, b int) int {
	if x < a {
		return a
	}
	if x > b {
		return b
	}
	return x
}

// ---------------------------------------------------------------------------------------------------------------
// sending (never blocks: a client that stops reading is dropped)
// ---------------------------------------------------------------------------------------------------------------
func (m *member) close() {
	m.once.Do(func() {
		close(m.done)
		m.conn.Close()
	})
}

func (m *member) queueFrame(f frame) bool {
	select {
	case <-m.done:
		return false
	default:
	}
	select {
	case m.send <- f:
		return true
	default:
		log.Printf("dropping %q: send queue full", m.name)
		go m.close()
		return false
	}
}

func (m *member) json(v any) bool {
	b, _ := json.Marshal(v)
	return m.queueFrame(frame{websocket.TextMessage, b})
}

func (m *member) writer() {
	t := time.NewTicker(pingEvery)
	defer t.Stop()
	for {
		select {
		case f := <-m.send:
			m.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			if err := m.conn.WriteMessage(f.kind, f.data); err != nil {
				m.close()
				return
			}
		case <-t.C:
			m.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			if err := m.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				m.close()
				return
			}
		case <-m.done:
			return
		}
	}
}

func fail(m *member, msg string) {
	m.json(map[string]string{"op": "error", "msg": msg})
	time.Sleep(200 * time.Millisecond)
	m.close()
}

func warn(m *member, msg string) { m.json(map[string]string{"op": "warn", "msg": msg}) }

// ---------------------------------------------------------------------------------------------------------------
// snapshots
// ---------------------------------------------------------------------------------------------------------------
type mview struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	RP   int    `json:"rp"`
	Pl   int    `json:"pl"`
	W    int    `json:"w"`
	L    int    `json:"l"`
	Bal  int    `json:"bal"`
	Late bool   `json:"late,omitempty"` // arrived during the running match: can not watch it
}

func (r *room) fighterIDs() []string {
	out := []string{"", ""}
	if r.phase == "lobby" {
		if len(r.queue) > 0 {
			out[0] = r.queue[0].id
		}
		if len(r.queue) > 1 {
			out[1] = r.queue[1].id
		}
		return out
	}
	for i := 0; i < 2; i++ {
		if r.fit[i] != nil {
			out[i] = r.fit[i].id
		}
	}
	return out
}

func (r *room) snapshot(to *member) []byte {
	ms := make([]mview, 0, len(r.members))
	for _, m := range r.members {
		ms = append(ms, mview{ID: m.id, Name: m.name, RP: m.rp, Pl: m.plate, W: m.wins, L: m.losses, Bal: m.bal, Late: r.phase == "play" && !m.spec && m.fighter == 0})
	}
	q := make([]string, 0, len(r.queue))
	for _, m := range r.queue {
		q = append(q, m.id)
	}
	tr := map[string]string{}
	if r.tradeFrom != nil && r.tradeTo != nil {
		tr["from"], tr["to"] = r.tradeFrom.id, r.tradeTo.id
	}
	v := map[string]any{
		"op": "room", "lobby": r.name, "host": r.host.id, "you": to.id, "phase": r.phase, "winlimit": r.winLimit,
		"streak": map[string]any{"id": r.streakID, "n": r.streakN}, "members": ms, "queue": q, "fighters": r.fighterIDs(),
		"terms": r.terms, "acc": r.acc, "col": r.col, "ready": r.ready, "match": r.matchID, "trade": tr, "max": maxMembers,
		"build": r.build,
	}
	b, _ := json.Marshal(v)
	return b
}

func (r *room) broadcast() {
	for _, m := range r.members {
		m.queueFrame(frame{websocket.TextMessage, r.snapshot(m)})
	}
}

func (r *room) find(id string) *member {
	for _, m := range r.members {
		if m.id == id {
			return m
		}
	}
	return nil
}

func removeM(l []*member, m *member) []*member {
	out := l[:0:0]
	for _, x := range l {
		if x != m {
			out = append(out, x)
		}
	}
	return out
}

func (r *room) isFighter(m *member) int {
	for i := 0; i < 2; i++ {
		if r.fit[i] == m {
			return i + 1
		}
	}
	return 0
}

func (r *room) cancelBet() {
	r.terms = terms{}
	r.acc = [2]bool{}
	r.col = [2]string{}
	r.ready = [2]bool{}
	r.fit = [2]*member{}
	r.tradeFrom, r.tradeTo = nil, nil
	r.phase = "lobby"
}

// ---------------------------------------------------------------------------------------------------------------
// joining and leaving
// ---------------------------------------------------------------------------------------------------------------
type hello struct {
	Op    string `json:"op"`
	V     int    `json:"v"`
	Name  string `json:"name"`
	ID    string `json:"id"`
	Lobby string `json:"lobby"`
	Pass  string `json:"pass"`
	RP    int    `json:"rp"`
	Pl    int    `json:"pl"`
	W     int    `json:"w"`
	L     int    `json:"l"`
	Bal   int    `json:"bal"`
	Win   int    `json:"winlimit"`
	Build string `json:"build"`
}

func handleWS(w http.ResponseWriter, req *http.Request) {
	c, err := upgrader.Upgrade(w, req, nil)
	if err != nil {
		return
	}
	c.SetReadLimit(maxFrame)
	c.SetReadDeadline(time.Now().Add(readTimeout))
	c.SetPongHandler(func(string) error { c.SetReadDeadline(time.Now().Add(readTimeout)); return nil })
	m := &member{conn: c, send: make(chan frame, sendQueue), done: make(chan struct{}), ip: clientIP(req), reported: -1}
	go m.writer()
	defer leave(m)

	var h hello
	if _, b, err := c.ReadMessage(); err != nil || json.Unmarshal(b, &h) != nil {
		fail(m, "bad request")
		return
	}
	if h.V != 2 {
		fail(m, "this server needs the new TFG version (update your game files)")
		return
	}
	h.Build = strings.ToUpper(strings.TrimSpace(h.Build))
	if len(h.Build) < 4 || len(h.Build) > 16 {
		fail(m, "your game is out of date: install the LOBBY UPDATE to play online")
		return
	}
	name, ok := cleanName(h.Name)
	if !ok {
		fail(m, "set a player name first (1-20 letters)")
		return
	}
	lname, ok := cleanName(h.Lobby)
	if !ok {
		fail(m, "room name must be 1-20 letters")
		return
	}
	if len(h.Pass) > 40 || len(h.ID) > 64 {
		fail(m, "bad request")
		return
	}
	m.name, m.token = name, h.ID
	if m.token == "" {
		m.token = fmt.Sprintf("anon-%d", time.Now().UnixNano())
	}
	m.plate = clampI(h.Pl, 0, 99)
	m.rp, m.wins, m.losses, m.bal = clampI(h.RP, 0, 1000000), clampI(h.W, 0, 1000000), clampI(h.L, 0, 1000000), clampI(h.Bal, 0, 100000000)
	key := strings.ToLower(lname)

	mu.Lock()
	m.id = fmt.Sprintf("m%d", nextID)
	nextID++
	switch h.Op {
	case "create":
		perIP := 0
		for _, r := range rooms {
			if r.host.ip == m.ip {
				perIP++
			}
		}
		switch {
		case rooms[key] != nil:
			mu.Unlock()
			fail(m, "that room name is taken, pick another")
			return
		case len(rooms) >= maxRooms:
			mu.Unlock()
			fail(m, "the server is full, try again later")
			return
		case perIP >= maxPerIP:
			mu.Unlock()
			fail(m, "you already have too many rooms open")
			return
		}
		win := h.Win
		if win < 0 || win > 20 {
			win = 3
		}
		r := &room{key: key, name: lname, pass: h.Pass, host: m, phase: "lobby", winLimit: win, created: time.Now(), build: h.Build}
		r.members = []*member{m}
		r.queue = []*member{m}
		m.room = r
		rooms[key] = r
		log.Printf("room %q opened by %q", lname, name)
		m.json(map[string]string{"op": "hello", "you": m.id, "lobby": lname})
		sendClaims(m)
		r.broadcast()
	case "join":
		r := rooms[key]
		switch {
		case r == nil:
			mu.Unlock()
			fail(m, "no room with that name is open")
			return
		case subtle.ConstantTimeCompare([]byte(r.pass), []byte(h.Pass)) != 1:
			mu.Unlock()
			time.Sleep(500 * time.Millisecond)
			fail(m, "wrong password")
			return
		case len(r.members) >= maxMembers:
			mu.Unlock()
			fail(m, "that room is full (5 people)")
			return
		case r.build != h.Build:
			rb := r.build
			mu.Unlock()
			// a different copy of the game cannot play along: the fighters would not match and the screen stays black
			fail(m, fmt.Sprintf("YOUR GAME IS NOT THE SAME VERSION AS THIS ROOM (yours %s, the room %s). Everybody needs the same download of the game.", h.Build, rb))
			return
		}
		for _, o := range r.members {
			if strings.EqualFold(o.name, m.name) {
				m.name = fmt.Sprintf("%s%d", string([]rune(m.name)[:min(len([]rune(m.name)), maxNameLen-2)]), len(r.members)+1)
				break
			}
		}
		r.members = append(r.members, m)
		r.queue = append(r.queue, m)
		m.room = r
		log.Printf("%q joined room %q (%d)", name, r.name, len(r.members))
		m.json(map[string]string{"op": "hello", "you": m.id, "lobby": r.name})
		sendClaims(m)
		if r.phase == "bet" {
			// a newcomer shifts nobody: the two fighters are fixed for this match
		}
		r.broadcast()
	default:
		mu.Unlock()
		fail(m, "bad request")
		return
	}
	mu.Unlock()

	for {
		kind, b, err := c.ReadMessage()
		if err != nil {
			return
		}
		c.SetReadDeadline(time.Now().Add(readTimeout))
		if kind == websocket.BinaryMessage {
			relayBinary(m, b)
			continue
		}
		var msg map[string]any
		if json.Unmarshal(b, &msg) != nil {
			continue
		}
		op, _ := msg["op"].(string)
		mu.Lock()
		handle(m, op, msg)
		mu.Unlock()
	}
}

func sendClaims(m *member) {
	now := time.Now()
	list := claims[m.token]
	keep := list[:0]
	var out []json.RawMessage
	for _, cl := range list {
		if now.Sub(cl.At) > claimKeep {
			continue
		}
		keep = append(keep, cl)
		out = append(out, cl.Msg)
	}
	claims[m.token] = keep
	if len(out) > 0 {
		m.json(map[string]any{"op": "claims", "items": out})
	}
}

func relayBinary(m *member, b []byte) {
	if len(b) < 1 {
		return
	}
	mu.Lock()
	defer mu.Unlock()
	r := m.room
	if r == nil || r.phase != "play" {
		return
	}
	switch b[0] {
	case tGame:
		var other *member
		if m.fighter == 1 {
			other = r.fit[1]
		} else if m.fighter == 2 {
			other = r.fit[0]
		}
		if other != nil {
			other.queueFrame(frame{websocket.BinaryMessage, b})
		}
	case tReplay:
		if m.fighter != 1 {
			return
		}
		if len(r.replay)+len(b)-1 <= maxReplay {
			r.replay = append(r.replay, b[1:]...)
		}
		for _, s := range r.members {
			if s.spec && s.watching {
				s.queueFrame(frame{websocket.BinaryMessage, b})
			}
		}
	}
}

// leave removes a member; a fighter who leaves during a match forfeits.
func leave(m *member) {
	m.close()
	mu.Lock()
	defer mu.Unlock()
	r := m.room
	if r == nil {
		return
	}
	m.room = nil
	wasFighter := m.fighter
	if r.phase == "play" && wasFighter != 0 && m.reported < 0 {
		// forfeit: the other fighter wins unless a result was already agreed
		other := r.fit[2-wasFighter]
		r.members = removeM(r.members, m)
		r.queue = removeM(r.queue, m)
		if other != nil {
			settle(r, 3-wasFighter, true, false, m)
		}
		afterLeave(r, m)
		return
	}
	r.members = removeM(r.members, m)
	r.queue = removeM(r.queue, m)
	if r.phase == "bet" && (r.fit[0] == m || r.fit[1] == m) {
		r.cancelBet()
	}
	if r.tradeFrom == m || r.tradeTo == m {
		r.tradeFrom, r.tradeTo = nil, nil
	}
	afterLeave(r, m)
}

func afterLeave(r *room, m *member) {
	if len(r.members) == 0 {
		if r.settleTimer != nil {
			r.settleTimer.Stop()
		}
		if rooms[r.key] == r {
			delete(rooms, r.key)
			log.Printf("room %q closed", r.name)
		}
		return
	}
	if r.host == m {
		r.host = r.members[0]
		log.Printf("room %q: host is now %q", r.name, r.host.name)
	}
	if r.phase == "play" {
		// not enough fighters left
		if r.fit[0] == nil || r.fit[1] == nil || r.fit[0].room == nil || r.fit[1].room == nil {
			if r.settleTimer != nil {
				r.settleTimer.Stop()
			}
		}
	}
	r.broadcast()
}

// ---------------------------------------------------------------------------------------------------------------
// ops (mu is held)
// ---------------------------------------------------------------------------------------------------------------
func handle(m *member, op string, msg map[string]any) {
	r := m.room
	if r == nil {
		return
	}
	str := func(k string) string { s, _ := msg[k].(string); return s }
	num := func(k string) int { f, _ := msg[k].(float64); return int(f) }
	boolean := func(k string) bool { b, _ := msg[k].(bool); return b }
	if v, ok := msg["bal"].(float64); ok {
		m.bal = clampI(int(v), 0, 100000000)
	}
	switch op {
	case "ready":
		if r.phase != "lobby" || len(r.queue) < 2 {
			return
		}
		for i := 0; i < 2; i++ {
			if r.queue[i] == m {
				r.ready[i] = boolean("on") || msg["on"] == nil
			}
		}
		if r.ready[0] && r.ready[1] {
			enterBet(r)
		}
		r.broadcast()
	case "start":
		if r.host != m || r.phase != "lobby" || len(r.queue) < 2 {
			return
		}
		enterBet(r)
		r.broadcast()
	case "plate":
		m.plate = clampI(num("pl"), 0, 99)
		r.broadcast()
	case "rename":
		if n, ok := cleanName(str("name")); ok {
			for _, o := range r.members {
				if o != m && strings.EqualFold(o.name, n) {
					n = fmt.Sprintf("%s%d", string([]rune(n)[:min(len([]rune(n)), maxNameLen-2)]), len(r.members)+1)
					break
				}
			}
			m.name = n
			r.broadcast()
		}
	case "cfg":
		if r.host != m {
			return
		}
		if _, ok := msg["winlimit"]; ok {
			r.winLimit = clampI(num("winlimit"), 0, 20)
		}
		r.broadcast()
	case "stepout":
		if r.phase != "lobby" || len(r.queue) < 2 {
			return
		}
		r.queue = append(removeM(r.queue, m), m)
		r.ready = [2]bool{}
		r.tradeFrom, r.tradeTo = nil, nil
		r.broadcast()
	case "trade":
		if r.phase != "lobby" {
			return
		}
		to := r.find(str("to"))
		if to == nil || to == m {
			return
		}
		r.tradeFrom, r.tradeTo = m, to
		to.json(map[string]string{"op": "trade", "from": m.id, "name": m.name})
		r.broadcast()
	case "tradeok":
		if r.phase != "lobby" || r.tradeTo != m || r.tradeFrom == nil {
			return
		}
		a, b := -1, -1
		for i, x := range r.queue {
			if x == r.tradeFrom {
				a = i
			}
			if x == r.tradeTo {
				b = i
			}
		}
		if a >= 0 && b >= 0 {
			r.queue[a], r.queue[b] = r.queue[b], r.queue[a]
		}
		r.tradeFrom, r.tradeTo = nil, nil
		r.ready = [2]bool{}
		r.broadcast()
	case "tradeno":
		if r.tradeTo == m || r.tradeFrom == m {
			r.tradeFrom, r.tradeTo = nil, nil
			r.broadcast()
		}
	case "terms":
		if r.phase != "bet" || r.isFighter(m) == 0 {
			return
		}
		pts := clampI(num("pts"), 0, maxStake)
		lim := maxStake
		for i := 0; i < 2; i++ {
			if r.fit[i] != nil && r.fit[i].bal < lim {
				lim = r.fit[i].bal
			}
		}
		if pts > lim {
			pts = lim
		}
		r.terms = terms{pts, boolean("cstake")}
		r.acc = [2]bool{}
		if !r.terms.Cstake {
			r.col = [2]string{}
		}
		r.broadcast()
	case "pick":
		if r.phase != "bet" || r.isFighter(m) == 0 {
			return
		}
		c := str("colour")
		if len(c) > 120 {
			return
		}
		r.col[r.isFighter(m)-1] = c
		r.acc[r.isFighter(m)-1] = false
		r.broadcast()
	case "accept":
		if r.phase != "bet" || r.isFighter(m) == 0 {
			return
		}
		i := r.isFighter(m) - 1
		on := boolean("on")
		if on && r.terms.Cstake && r.col[i] == "" {
			warn(m, "pick the colour you put up first")
			return
		}
		if on && r.terms.Pts > m.bal {
			warn(m, "you do not have that many points")
			return
		}
		r.acc[i] = on
		if r.acc[0] && r.acc[1] {
			startMatch(r)
		}
		r.broadcast()
	case "cancel":
		// one of the two fighters backs out of the bet screen
		if r.phase != "bet" || r.isFighter(m) == 0 {
			return
		}
		r.cancelBet()
		r.broadcast()
	case "watch":
		if r.phase != "play" || !m.spec {
			return
		}
		m.watching = true
		// catch up with everything recorded so far
		buf := r.replay
		for off := 0; off < len(buf); off += 32 * 1024 {
			end := off + 32*1024
			if end > len(buf) {
				end = len(buf)
			}
			chunk := make([]byte, 0, end-off+1)
			chunk = append(chunk, tReplay)
			chunk = append(chunk, buf[off:end]...)
			m.queueFrame(frame{websocket.BinaryMessage, chunk})
		}
	case "result":
		if r.phase != "play" || m.fighter == 0 || str("match") != r.matchID {
			return
		}
		m.reported = clampI(num("winner"), 0, 2)
		m.repAt = time.Now()
		other := r.fit[2-m.fighter]
		if other != nil && other.reported >= 0 {
			finish(r)
		} else if r.settleTimer == nil {
			id := r.matchID
			r.settleTimer = time.AfterFunc(resultWait, func() {
				mu.Lock()
				defer mu.Unlock()
				if rooms[r.key] == r && r.phase == "play" && r.matchID == id {
					finish(r)
				}
			})
		}
	case "ack":
		id := str("match")
		list := claims[m.token]
		keep := list[:0]
		for _, cl := range list {
			if cl.Match != id {
				keep = append(keep, cl)
			}
		}
		claims[m.token] = keep
	}
}

func enterBet(r *room) {
	if len(r.queue) < 2 {
		return
	}
	r.phase = "bet"
	r.fit = [2]*member{r.queue[0], r.queue[1]}
	r.terms = terms{}
	r.acc = [2]bool{}
	r.col = [2]string{}
	r.tradeFrom, r.tradeTo = nil, nil
}

func startMatch(r *room) {
	r.phase = "play"
	r.matchID = fmt.Sprintf("%d-%d", time.Now().Unix(), nextM)
	nextM++
	r.matchBet = r.terms
	r.matchCol = r.col
	r.replay = r.replay[:0]
	if r.settleTimer != nil {
		r.settleTimer.Stop()
		r.settleTimer = nil
	}
	for _, x := range r.members {
		x.fighter, x.spec, x.watching, x.reported = 0, false, false, -1
	}
	r.fit[0].fighter, r.fit[1].fighter = 1, 2
	for _, x := range r.members {
		if x.fighter == 0 {
			x.spec = true
		}
	}
	info := func(x *member) map[string]any {
		return map[string]any{"id": x.id, "name": x.name, "rp": x.rp, "pl": x.plate}
	}
	for _, x := range r.members {
		role := "spec"
		if x.fighter == 1 {
			role = "p1"
		} else if x.fighter == 2 {
			role = "p2"
		}
		x.json(map[string]any{"op": "go", "match": r.matchID, "role": role, "p1": info(r.fit[0]), "p2": info(r.fit[1]),
			"bet":   map[string]any{"pts": r.matchBet.Pts, "cstake": r.matchBet.Cstake, "c1": r.matchCol[0], "c2": r.matchCol[1]},
			"specs": countSpecs(r)})
	}
	log.Printf("room %q: match %s %q vs %q bet %d/%v", r.name, r.matchID, r.fit[0].name, r.fit[1].name, r.matchBet.Pts, r.matchBet.Cstake)
}

func countSpecs(r *room) int {
	n := 0
	for _, x := range r.members {
		if x.spec {
			n++
		}
	}
	return n
}

// finish decides the match from the reports (mu held).
func finish(r *room) {
	if r.phase != "play" {
		return
	}
	a, b := r.fit[0], r.fit[1]
	ra, rb := -1, -1
	if a != nil {
		ra = a.reported
	}
	if b != nil {
		rb = b.reported
	}
	winner, void := 0, false
	switch {
	case ra >= 0 && rb >= 0:
		switch {
		case ra == rb:
			winner = ra
		case ra == 0:
			winner = rb // one side saw no result (it quit or lost the connection), the other one's word stands
		case rb == 0:
			winner = ra
		default:
			void = true // both claim a result and they disagree
		}
	case ra >= 0:
		winner = ra
	case rb >= 0:
		winner = rb
	default:
		void = true
	}
	if winner == 0 {
		void = true
	}
	settle(r, winner, false, void, nil)
}

func expected(ra, rb float64) float64 { return 1 / (1 + math.Pow(10, (rb-ra)/400)) }

// settle ends the match: rank points, bet, claims, queue rotation (mu held).
func settle(r *room, winner int, forfeit, void bool, quitter *member) {
	if r.phase != "play" {
		return
	}
	if r.settleTimer != nil {
		r.settleTimer.Stop()
		r.settleTimer = nil
	}
	a, b := r.fit[0], r.fit[1]
	res := map[string]any{"op": "settled", "match": r.matchID, "winner": winner, "forfeit": forfeit, "void": void,
		"bet": map[string]any{"pts": r.matchBet.Pts, "cstake": r.matchBet.Cstake, "c1": r.matchCol[0], "c2": r.matchCol[1]},
		"p1":  a.name, "p2": b.name}
	d1, d2 := 0, 0
	if !void && winner != 0 {
		ra, rb := 1000+float64(a.rp), 1000+float64(b.rp)
		ea := expected(ra, rb)
		eb := 1 - ea
		k := func(x *member) float64 {
			kk := 24.0
			if x.wins+x.losses < 5 {
				kk *= 2 // new players move fast to their real rank
			}
			if r.matchBet.Pts > 0 || r.matchBet.Cstake {
				kk *= 1.25 // a bet match is worth more
			}
			return kk
		}
		sa, sb := 0.0, 0.0
		if winner == 1 {
			sa = 1
			a.wins++
			b.losses++
		} else {
			sb = 1
			b.wins++
			a.losses++
		}
		d1 = int(math.Round(k(a) * (sa - ea)))
		d2 = int(math.Round(k(b) * (sb - eb)))
		if winner == 1 {
			if d1 < 1 {
				d1 = 1
			}
			if d2 > -1 {
				d2 = -1
			}
		} else {
			if d2 < 1 {
				d2 = 1
			}
			if d1 > -1 {
				d1 = -1
			}
		}
		a.rp = clampI(a.rp+d1, 0, 1000000)
		b.rp = clampI(b.rp+d2, 0, 1000000)
	}
	res["rp"] = map[string]int{"p1": d1, "p2": d2}
	res["newrp"] = map[string]int{"p1": a.rp, "p2": b.rp}
	raw, _ := json.Marshal(res)
	for _, x := range []*member{a, b} {
		if x != nil && x != quitter {
			claims[x.token] = append(claims[x.token], claim{Match: r.matchID, At: time.Now(), Msg: raw})
		}
	}
	// the quitter's client already paid its stake when the match began; remember the loss for a reconnect
	if quitter != nil {
		claims[quitter.token] = append(claims[quitter.token], claim{Match: r.matchID, At: time.Now(), Msg: raw})
	}
	for _, x := range r.members {
		x.queueFrame(frame{websocket.TextMessage, raw})
	}
	log.Printf("room %q: match %s settled winner=%d forfeit=%v void=%v rp=%d/%d", r.name, r.matchID, winner, forfeit, void, d1, d2)

	// queue rotation
	winM, loseM := a, b
	if winner == 2 {
		winM, loseM = b, a
	}
	if !void && winner != 0 {
		if r.streakID == winM.id {
			r.streakN++
		} else {
			r.streakID, r.streakN = winM.id, 1
		}
		r.queue = removeM(removeM(r.queue, winM), loseM)
		if loseM.room != nil {
			// loser steps to the back
		}
		out := []*member{}
		out = append(out, winM)
		out = append(out, r.queue...)
		if winM.room == nil {
			out = out[1:]
		}
		if loseM.room != nil {
			out = append(out, loseM)
		}
		if r.winLimit > 0 && r.streakN >= r.winLimit && winM.room != nil && len(out) > 2 {
			// the winner has had enough turns: they also step out and the next two fight
			out = removeM(out, winM)
			out = append(out, winM)
			r.streakID, r.streakN = "", 0
		}
		r.queue = out
	}
	r.cancelBet()
	for _, x := range r.members {
		x.fighter, x.spec, x.watching, x.reported = 0, false, false, -1
	}
	r.matchID = ""
	r.replay = r.replay[:0]
	r.broadcast()
}

// ---------------------------------------------------------------------------------------------------------------
// listing and housekeeping
// ---------------------------------------------------------------------------------------------------------------
type entry struct {
	Lobby   string `json:"lobby"`
	Host    string `json:"host"`
	Locked  bool   `json:"locked"`
	Full    bool   `json:"full"`
	Count   int    `json:"count"`
	Max     int    `json:"max"`
	Playing bool   `json:"playing"`
}

func handleList(w http.ResponseWriter, r *http.Request) {
	mu.Lock()
	out := make([]entry, 0, len(rooms))
	for _, x := range rooms {
		out = append(out, entry{x.name, x.host.name, x.pass != "", len(x.members) >= maxMembers, len(x.members), maxMembers, x.phase == "play"})
	}
	mu.Unlock()
	sort.Slice(out, func(i, j int) bool { return strings.ToLower(out[i].Lobby) < strings.ToLower(out[j].Lobby) })
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	json.NewEncoder(w).Encode(out)
}

func reaper() {
	for range time.Tick(time.Minute) {
		var old []*room
		mu.Lock()
		for _, x := range rooms {
			if time.Since(x.created) > roomLifetime {
				old = append(old, x)
			}
		}
		now := time.Now()
		for k, list := range claims {
			keep := list[:0]
			for _, cl := range list {
				if now.Sub(cl.At) <= claimKeep {
					keep = append(keep, cl)
				}
			}
			if len(keep) == 0 {
				delete(claims, k)
			} else {
				claims[k] = keep
			}
		}
		mu.Unlock()
		for _, x := range old {
			mu.Lock()
			ms := append([]*member(nil), x.members...)
			mu.Unlock()
			for _, m := range ms {
				m.close()
			}
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
		w.Write([]byte("TFG lobby server v2 is running.\n"))
	})
	log.Printf("TFG lobby server v2 listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}
