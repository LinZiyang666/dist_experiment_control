package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// Tier-A inline ceiling (mirrors broker + agent ceilings). Files
// <= this go inline; > this require JetStream tier B.
const cliTierAMaxBytes = 8 * 1024 * 1024

// Hard upper bound. Past this the user is expected to use
// `tether expose` + rsync (file-transfer-plan §Goals).
const cliMaxBytes = 200 * 1024 * 1024

// remoteSpec is one parsed `<node>:<remote-path>` argument.
type remoteSpec struct {
	Node string
	Path string
}

// parseRemoteSpec splits "<node>:<path>" into its parts. The leading
// segment up to the FIRST `:` is the node id; everything after is
// the path. We reject empty node, empty path, and a missing colon —
// `tether push foo` (no colon) is almost certainly user error and the
// helpful message is better than guessing.
func parseRemoteSpec(s string) (remoteSpec, error) {
	idx := strings.IndexByte(s, ':')
	if idx <= 0 {
		return remoteSpec{}, fmt.Errorf("missing 'node:' prefix in %q (expected <node>:<path>)", s)
	}
	nid := s[:idx]
	path := s[idx+1:]
	if path == "" {
		return remoteSpec{}, fmt.Errorf("empty remote path in %q", s)
	}
	if err := proto.ValidateNID(nid); err != nil {
		return remoteSpec{}, fmt.Errorf("invalid node id %q: %w", nid, err)
	}
	return remoteSpec{Node: nid, Path: path}, nil
}

// probeCaps issues `ctrl.by.<actor>.s.<sid>.caps.req` and returns the
// broker's reported capabilities. Used by chooseTier so we don't shoot
// a tier-B request at a broker without JetStream, or a tier-A request
// that exceeds the server max_payload. timeout is small (caps is a
// pre-flight; failing the probe simply means we fall back to
// conservative defaults).
func probeCaps(nc *nats.Conn, actor, sid string, timeout time.Duration) (proto.CapsResp, error) {
	body, _ := json.Marshal(proto.CapsReq{})
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	subj := proto.SubjCtrlCaps(actor, sid)
	msg, err := nc.RequestWithContext(ctx, subj, body)
	if err != nil {
		return proto.CapsResp{}, err
	}
	var resp proto.CapsResp
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		return proto.CapsResp{}, fmt.Errorf("caps: parse: %w", err)
	}
	return resp, nil
}

// chooseTier decides tier A vs B for a given file size + caps probe
// result. The plan's rules (file-transfer-plan §Tier selection):
//
//   - size > 200 MiB: refused outright before we get here.
//   - size > 8 MiB: tier B.
//   - size > caps.MaxPayload * 0.5: tier B (give NATS some headroom
//     for framing + headers).
//   - else: tier A.
//
// If caps.JetStreamReady=false, tier B isn't an option:
//   - size > 8 MiB: refuse with `jetstream_unavailable` so the user
//     gets a clean error instead of a cryptic mid-Put failure.
//
// Returns ("a"|"b", maxInline, err). maxInline is what we pass into
// PullPrepareReq.MaxInline for symmetric agent-side decision.
func chooseTier(size int64, caps proto.CapsResp) (string, int64, error) {
	if size > cliMaxBytes {
		return "", 0, fmt.Errorf("too_large: file size %d > %d (use `tether expose` + rsync)", size, cliMaxBytes)
	}
	maxInline := int64(cliTierAMaxBytes)
	if caps.MaxPayload > 0 {
		// Leave 1 KiB headroom for proto framing + base64.
		half := caps.MaxPayload/2 - 1024
		if half > 0 && half < maxInline {
			maxInline = half
		}
	}
	if size <= maxInline {
		return "a", maxInline, nil
	}
	if !caps.JetStreamReady {
		return "", maxInline, fmt.Errorf("jetstream_unavailable: broker has no JetStream so tier-B (>%d bytes) cannot be served; either bump nats max_payload >= %d or use `tether expose` + rsync",
			maxInline, size*2+2048)
	}
	return "b", maxInline, nil
}

// newTransferID makes a 16-hex random id. Not a ULID (we don't need
// time ordering for transfers) but identical shape for visual parity
// with the other audit columns.
func newTransferID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// hexSHA256 wraps the canonical sha256.Sum256+hex.EncodeToString pair.
func hexSHA256(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
