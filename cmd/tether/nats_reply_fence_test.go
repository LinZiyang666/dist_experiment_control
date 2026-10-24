package main

// A frame-aware TCP relay between one ctl process and the real NATS server.
// It forwards every frame unchanged except the reply to a fenced request: the
// reply subject is fenced BEFORE that request is forwarded, so the original
// response is consumed at the socket boundary and provably never delivered.
// Nothing is forged; every retry or reconciliation is the client's own. The
// relay also retains each client publication, the oracle for "one transfer ID
// and one fixed object NUID" across the lost reply.
// origin: hot-update S7 plan §7 / §10 E4 / INTEG-D commit_lost carrier

import (
	"bufio"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
)

type fencedPublication struct {
	Subject string
	Body    []byte
}

type replyFence struct {
	ln       net.Listener
	upstream string
	match    func(subject string) bool
	// sever closes every relayed connection and the listener after the first
	// dropped reply: the ctl permanently loses its broker at that point.
	sever bool
	// request consumes the matched request itself instead of its reply: the
	// ctl saw its publish succeed, the broker never received it.
	request bool

	mu      sync.Mutex
	budget  int
	fenced  map[string]bool
	dropped []fencedPublication
	pubs    []fencedPublication
	conns   map[net.Conn]struct{}
	closed  bool
	wg      sync.WaitGroup
}

// fenceSpec: how many matched requests to fence, and whether to consume the
// request itself (request) or its reply and then cut the ctl off (sever).
type fenceSpec struct {
	drops          int
	request, sever bool
	match          func(string) bool
}

func startReplyFence(t *testing.T, upstream string, spec fenceSpec) (*replyFence, string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &replyFence{ln: ln, upstream: strings.TrimPrefix(upstream, "nats://"), match: spec.match, sever: spec.sever, request: spec.request,
		budget: spec.drops, fenced: map[string]bool{}, conns: map[net.Conn]struct{}{}}
	accepted := make(chan struct{})
	go func() {
		defer close(accepted)
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			if !f.track(c) {
				_ = c.Close()
				return
			}
			f.wg.Add(1)
			go f.serve(c)
		}
	}()
	t.Cleanup(func() {
		f.shutdown()
		<-accepted // no Add can race Wait after the accept owner has joined
		f.wg.Wait()
	})
	return f, "nats://" + ln.Addr().String()
}

func (f *replyFence) track(c net.Conn) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return false
	}
	f.conns[c] = struct{}{}
	return true
}

func (f *replyFence) shutdown() {
	f.mu.Lock()
	f.closed = true
	conns := make([]net.Conn, 0, len(f.conns))
	for c := range f.conns {
		conns = append(conns, c)
	}
	f.mu.Unlock()
	_ = f.ln.Close()
	for _, c := range conns {
		_ = c.Close()
	}
}

func (f *replyFence) publications() []fencedPublication {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]fencedPublication(nil), f.pubs...)
}

func (f *replyFence) drops() []fencedPublication {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]fencedPublication(nil), f.dropped...)
}

func (f *replyFence) serve(client net.Conn) {
	defer f.wg.Done()
	server, err := net.Dial("tcp", f.upstream)
	if err != nil || !f.track(server) {
		_ = client.Close()
		if server != nil {
			_ = server.Close()
		}
		return
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.pipe(client, server, true)
		_ = server.Close()
	}()
	f.pipe(server, client, false)
	_ = client.Close()
	<-done
}

// pipe copies one direction frame by frame. Client PUB/HPUB frames are
// recorded and may arm the fence; server MSG/HMSG frames to a fenced reply
// subject are consumed instead of forwarded.
func (f *replyFence) pipe(from, to net.Conn, fromClient bool) {
	rd := bufio.NewReaderSize(from, 64<<10)
	for {
		line, err := rd.ReadBytes('\n')
		if err != nil {
			return
		}
		fields := strings.Fields(string(line))
		frame := line
		var body []byte
		if len(fields) > 2 && (fromClient && (fields[0] == "PUB" || fields[0] == "HPUB") || !fromClient && (fields[0] == "MSG" || fields[0] == "HMSG")) {
			size, convErr := strconv.Atoi(fields[len(fields)-1])
			if convErr != nil || size < 0 {
				return
			}
			payload := make([]byte, size+2)
			if _, err := io.ReadFull(rd, payload); err != nil {
				return
			}
			frame = append(append([]byte(nil), line...), payload...)
			body = payload[:size]
			if fields[0] == "HPUB" || fields[0] == "HMSG" {
				if headers, convErr := strconv.Atoi(fields[len(fields)-2]); convErr == nil && headers <= size {
					body = payload[headers:size]
				}
			}
		}
		if fromClient && len(fields) > 2 && (fields[0] == "PUB" || fields[0] == "HPUB") {
			f.mu.Lock()
			f.pubs = append(f.pubs, fencedPublication{Subject: fields[1], Body: append([]byte(nil), body...)})
			reply := ""
			if (fields[0] == "PUB" && len(fields) == 4) || (fields[0] == "HPUB" && len(fields) == 5) {
				reply = fields[2]
			}
			swallow := false
			if reply != "" && f.budget > 0 && f.match(fields[1]) {
				f.budget--
				if f.request {
					swallow = true
					f.dropped = append(f.dropped, fencedPublication{Subject: fields[1], Body: append([]byte(nil), body...)})
				} else {
					f.fenced[reply] = true // armed before the request leaves the relay
				}
			}
			f.mu.Unlock()
			if swallow {
				continue
			}
		}
		if !fromClient && len(fields) > 2 && (fields[0] == "MSG" || fields[0] == "HMSG") {
			f.mu.Lock()
			drop := f.fenced[fields[1]]
			if drop {
				delete(f.fenced, fields[1])
				f.dropped = append(f.dropped, fencedPublication{Subject: fields[1], Body: append([]byte(nil), body...)})
			}
			sever := drop && f.sever
			f.mu.Unlock()
			if sever {
				f.shutdown()
				return
			}
			if drop {
				continue
			}
		}
		if _, err := to.Write(frame); err != nil {
			return
		}
	}
}
