package main

// Practice in a room. Players who wait for their turn can train alone (the game's own training mode, played on their
// own PC; the server only hears that they are away) or play a practice match against each other. Neither one touches
// rank, wins, losses, bets or the line. A player who is practising when their fight is up (the other fighter is
// ready, or the host started the fight) is called back; if they are not back after callWait they are sent to the
// back of the line, so the room never waits for somebody who is busy.

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"tfglobby/websocket"
)

// how long a called player has to get back to the room (a variable so that the tests can shorten it)
var callWait = 30 * time.Second

const inviteLife = 60 * time.Second

// a practice match: a plays the host's part (p1) of the game's own connection.
// DLC Patch 1 - SIDE FIGHTS: the same thing with ranked set is a real match next to the line's fight, so several pairs
// fight in one room at once. It counts for rank and win/loss exactly like the line's fight (no bet, no spectators,
// the line is not touched). Both report the winner; it is settled like the line's fight; leaving is a forfeit.
type spar struct {
	id     string
	a, b   *member
	ranked bool
	rep    [2]int      // what a and b reported (-1 nothing yet)
	timer  *time.Timer // waits for the second report
	done   bool
}

func (s *spar) other(m *member) *member {
	if s.a == m {
		return s.b
	}
	if s.b == m {
		return s.a
	}
	return nil
}

// away: not at the room menu (training or in a practice match)
func away(m *member) bool { return m != nil && (m.practice || m.spar != nil) }

// what the room list shows next to the name: 1 training alone, 2 practice match
func practiceState(m *member) int {
	switch {
	case m.spar != nil && m.spar.ranked:
		return 3
	case m.spar != nil:
		return 2
	case m.practice:
		return 1
	}
	return 0
}

// inFight: one of the two who are in the bet screen or in the real fight
func (r *room) inFight(m *member) bool {
	return (r.phase == "bet" || r.phase == "play") && r.isFighter(m) != 0
}

// free: could train or start a practice match right now
func (r *room) free(m *member) bool {
	return m != nil && m.room == r && !r.inFight(m) && m.spar == nil && !m.practice
}

func practiceOp(m *member, r *room, op string, msg map[string]any) {
	str := func(k string) string { s, _ := msg[k].(string); return s }
	boolean := func(k string) bool { b, _ := msg[k].(bool); return b }
	switch op {
	case "practice":
		if boolean("on") {
			if m.spar != nil || r.inFight(m) {
				warn(m, "you can not train during your own fight")
				return
			}
			m.practice = true
		} else {
			m.practice = false
		}
		r.broadcast()
	case "spar":
		to := r.find(str("to"))
		if to == nil || to == m {
			return
		}
		if !r.free(m) {
			warn(m, "you can not start a practice match right now")
			return
		}
		if !r.free(to) {
			warn(m, to.name+" is busy right now")
			return
		}
		to.inv, to.invAt = m, time.Now()
		to.invRanked = str("kind") == "ranked"
		// the game's connector passes on only "from" and "name" of this message: a ranked request says so in
		// "from" ("ranked:<id>"), the game reads it there
		kind, from := "practice", m.id
		if to.invRanked {
			kind, from = "ranked", "ranked:"+m.id
		}
		to.json(map[string]string{"op": "sparreq", "from": from, "name": m.name, "kind": kind})
	case "sparno":
		inv := m.inv
		m.inv = nil
		if inv != nil && inv.room == r {
			inv.json(map[string]string{"op": "sparno", "name": m.name})
		}
	case "sparok":
		inv := m.inv
		m.inv = nil
		if inv == nil || inv.room != r || time.Since(m.invAt) > inviteLife {
			warn(m, "the invitation ran out")
			return
		}
		if !r.free(inv) || !r.free(m) {
			warn(m, "one of you is busy now")
			return
		}
		startSpar(r, inv, m, m.invRanked)
	case "sparend":
		sp := m.spar
		if sp == nil || str("match") != sp.id {
			return
		}
		if str("why") == "done" {
			// the fight ran to its end on this side: the other side finishes by itself and says so too
			m.spar = nil
			if sp.other(m) == nil || sp.other(m).spar != sp {
				endSpar(sp, m, false)
			}
		} else {
			endSpar(sp, m, true)
		}
		r.broadcast()
	}
}

func startSpar(r *room, a, b *member, ranked bool) {
	// a ranked side fight's id starts with "sd": the game's connector passes on only some fields of "go" (not
	// "side"), the game tells a side fight by its id
	pre := "sp"
	if ranked {
		pre = "sd"
	}
	id := fmt.Sprintf("%s%d-%d", pre, time.Now().Unix(), nextM)
	nextM++
	sp := &spar{id: id, a: a, b: b, ranked: ranked, rep: [2]int{-1, -1}}
	a.spar, b.spar = sp, sp
	a.practice, b.practice = false, false
	a.inv, b.inv = nil, nil
	info := func(x *member) map[string]any {
		return map[string]any{"id": x.id, "name": x.name, "rp": x.rp, "pl": x.plate}
	}
	for i, x := range []*member{a, b} {
		role := "p1"
		if i == 1 {
			role = "p2"
		}
		x.json(map[string]any{"op": "go", "practice": !ranked, "side": ranked, "match": id, "role": role, "p1": info(a), "p2": info(b),
			"bet": map[string]any{"pts": 0, "cstake": false, "c1": "", "c2": ""}, "specs": 0})
	}
	log.Printf("room %q: practice match %s %q vs %q ranked=%v", r.name, id, a.name, b.name, ranked)
	r.broadcast()
}

// sideReport: one of the two in a ranked side fight says who won (0 = it saw no result).
func sideReport(r *room, m *member, sp *spar, w int) {
	if sp.done {
		return
	}
	i := 0
	if sp.b == m {
		i = 1
	}
	sp.rep[i] = clampI(w, 0, 2)
	if sp.rep[0] >= 0 && sp.rep[1] >= 0 {
		sideSettle(r, sp, nil)
	} else if sp.timer == nil {
		sp.timer = time.AfterFunc(resultWait, func() {
			mu.Lock()
			defer mu.Unlock()
			if rooms[r.key] == r {
				sideSettle(r, sp, nil)
			}
		})
	}
}

// sideSettle ends a ranked side fight (mu held): rank points and record like the line's fight, then both are free.
// quitter: the one who left the room during it (the other one wins unless both had already agreed on a result).
func sideSettle(r *room, sp *spar, quitter *member) {
	if sp.done {
		return
	}
	sp.done = true
	if sp.timer != nil {
		sp.timer.Stop()
		sp.timer = nil
	}
	a, b := sp.a, sp.b
	ra, rb := sp.rep[0], sp.rep[1]
	winner, void, forfeit := 0, false, false
	switch {
	case quitter != nil && ra > 0 && ra == rb:
		winner = ra
	case quitter != nil:
		winner, forfeit = 1, true
		if quitter == a {
			winner = 2
		}
	case ra >= 0 && rb >= 0:
		switch {
		case ra == rb:
			winner = ra
		case ra == 0:
			winner = rb
		case rb == 0:
			winner = ra
		default:
			void = true
		}
	case ra >= 0:
		winner = ra
	case rb >= 0:
		winner = rb
	}
	if winner == 0 {
		void = true
	}
	d1, d2 := 0, 0
	if !void {
		d1, d2 = rate(a, b, winner, false)
	}
	res := map[string]any{"op": "settled", "match": sp.id, "winner": winner, "forfeit": forfeit, "void": void,
		"bet": map[string]any{"pts": 0, "cstake": false, "c1": "", "c2": ""}, "p1": a.name, "p2": b.name,
		"rp": map[string]int{"p1": d1, "p2": d2}, "newrp": map[string]int{"p1": a.rp, "p2": b.rp}}
	raw, _ := json.Marshal(res)
	for _, x := range []*member{a, b} {
		claims[x.token] = append(claims[x.token], claim{Match: sp.id, At: time.Now(), Msg: raw})
		if x.room == r && x != quitter {
			x.queueFrame(frame{websocket.TextMessage, raw})
		}
		if x.spar == sp {
			x.spar = nil
		}
	}
	log.Printf("room %q: side fight %s settled winner=%d forfeit=%v void=%v rp=%d/%d", r.name, sp.id, winner, forfeit, void, d1, d2)
	r.broadcast()
}

// endSpar lets both go; the other one is told when this was not the normal end of the fight.
func endSpar(sp *spar, by *member, tellOther bool) {
	a, b := sp.a, sp.b
	if a != nil && a.spar == sp {
		a.spar = nil
	}
	if b != nil && b.spar == sp {
		b.spar = nil
	}
	if tellOther {
		if o := sp.other(by); o != nil && o.room != nil {
			o.json(map[string]string{"op": "sparend", "match": sp.id})
		}
	}
	log.Printf("practice match %s ended", sp.id)
}

// due: the members the room is waiting for right now although they are away
func (r *room) due() map[*member]bool {
	d := map[*member]bool{}
	switch r.phase {
	case "lobby":
		if len(r.queue) >= 2 && r.queue[0].spar != nil && r.queue[0].spar == r.queue[1].spar {
			// the two who are up next are practising against each other: they are not called away from that
			break
		}
		if len(r.queue) >= 2 && (r.ready[0] || r.ready[1]) {
			for i := 0; i < 2; i++ {
				if !r.ready[i] && away(r.queue[i]) {
					d[r.queue[i]] = true
				}
			}
		}
	case "bet":
		for i := 0; i < 2; i++ {
			if away(r.fit[i]) {
				d[r.fit[i]] = true
			}
		}
	}
	return d
}

// checkCall (mu held, runs after every change of the room): starts the call for somebody who has just become due,
// ends it for somebody who is back or no longer needed.
func (r *room) checkCall() {
	d := r.due()
	for _, x := range r.members {
		if d[x] {
			if x.callT == nil {
				r.startCall(x)
			}
		} else if x.callT != nil {
			x.callT.Stop()
			x.callT = nil
			x.callGen++
			x.json(map[string]string{"op": "callend"})
		}
	}
}

func (r *room) startCall(x *member) {
	x.callGen++
	gen := x.callGen
	x.callT = time.AfterFunc(callWait, func() { r.callExpired(x, gen) })
	x.json(map[string]any{"op": "call", "sec": int(callWait / time.Second)})
}

func (r *room) callExpired(x *member, gen int) {
	mu.Lock()
	defer mu.Unlock()
	if rooms[r.key] != r || x.room != r || x.callGen != gen || x.callT == nil {
		return
	}
	x.callT = nil
	if !r.due()[x] {
		return
	}
	if len(r.queue) < 3 {
		// nobody could take the place: ask again
		r.startCall(x)
		return
	}
	ready := map[*member]bool{}
	if r.phase == "bet" {
		// both had agreed to fight: the one who is here stays ready
		ready[r.fit[0]], ready[r.fit[1]] = true, true
		r.cancelBet()
	} else {
		for i := 0; i < 2; i++ {
			ready[r.queue[i]] = r.ready[i]
		}
	}
	r.queue = append(removeM(r.queue, x), x)
	r.ready = [2]bool{}
	for i := 0; i < 2 && i < len(r.queue); i++ {
		r.ready[i] = ready[r.queue[i]]
	}
	r.tradeFrom, r.tradeTo = nil, nil
	log.Printf("room %q: %q was not back in %v and goes to the back of the line", r.name, x.name, callWait)
	x.json(map[string]string{"op": "skipped"})
	r.broadcast()
}
