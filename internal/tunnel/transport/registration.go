package transport

import (
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
)

// remoteHostOf is the source address a handshake is accounted to. A conn whose
// RemoteAddr cannot be parsed accounts to "" — one shared bucket, which is the
// conservative direction: an unattributable peer competes with other unattributable
// peers rather than getting an unmetered slot.
func RemoteHostOf(conn net.Conn) string {
	addr := conn.RemoteAddr()
	if addr == nil {
		return ""
	}
	host, _, err := net.SplitHostPort(addr.String())
	if err != nil {
		return addr.String()
	}
	return host
}

// regLogClasses is the fixed, coarse REGISTER-read failure taxonomy (review
// M1). NEVER err.Error(): variable detail (a port, a byte count) would defeat
// suppression. Each class carries its OWN Tracker so a class change is a
// different Tracker, not a logged transition — independent per-class pacing.
func RegLogClass(err error) string {
	var ne net.Error
	switch {
	case errors.Is(err, io.EOF):
		return "eof"
	case errors.As(err, &ne) && ne.Timeout():
		return "timeout"
	default:
		return "other"
	}
}

func ParseRegisterLine(line string) (sid, nid string, port int, token string, epoch int64, ok bool) {
	line = strings.TrimRight(line, "\r\n")
	parts := strings.Split(line, " ")
	// D6 §7.2(b): the v2 REGISTER grammar is EXACTLY 6 fields
	// (REGISTER <sid> <nid> <port> <token> <epoch>). A 5-field (pre-D6) or
	// 7-field line is rejected — never mis-bound.
	if len(parts) != 6 || parts[0] != "REGISTER" {
		return "", "", 0, "", 0, false
	}
	port, err := strconv.Atoi(parts[3])
	if err != nil {
		return "", "", 0, "", 0, false
	}
	// ParseInt(_, 10, 64) matches port_allocations.epoch (int64) and rejects
	// overflow that strconv.Atoi would silently truncate; negatives are rejected
	// (epoch is a non-negative monotone counter).
	epoch, err = strconv.ParseInt(parts[5], 10, 64)
	if err != nil || epoch < 0 {
		return "", "", 0, "", 0, false
	}
	return parts[1], parts[2], port, parts[4], epoch, true
}
