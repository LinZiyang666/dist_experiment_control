package broker

// reply_egress_test.go — guards for the h1 A2 single-egress rule: every broker
// control-plane reply leaves through respondLogged (directly or via
// Broker.respondBytes/replyJSON/the reply*Err helpers), so a failed Respond can
// never again be silent. The 2026-08-04 incident ran five days on a silently
// swallowed ErrMaxPayload (`_ = msg.Respond(payload)`), with every `tether ps`
// timing out against a broker that was answering everything else.
// origin: docs/reviews/h1-plan.md workstream A2.

import (
	"context"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

// replyEgressAPIs are the nats.Msg methods that put bytes on a reply subject.
//
// RespondMsg is here even though the production tree has ZERO of them today —
// that is exactly WHY. origin: docs/reviews/h1-external-review.md, "疑惑" #1:
// a census that names only `Respond` leaves the sibling API as a legal way to
// bypass the choke point, and the first person to reach for it gets no signal
// at all. Adding it while the count is zero costs nothing and closes the hole
// before it is used; adding it after would mean auditing sites that already
// shipped.
var replyEgressAPIs = map[string]bool{"Respond": true, "RespondMsg": true}

// TestReplyEgressSingleChokePoint is the census gate: it AST-scans every
// non-test file in this package for reply-egress call sites and requires each
// one to be inside respondLogged (the choke point itself — its first attempt +
// its fallback attempt) or inside the ONE pinned authcallout exemption (the
// consumer there is nats-server's auth-callout machinery, which must receive a
// signed JWT or nothing — a JSON fallback would be garbage to it).
//
// The exact per-function census is pinned, TLS-pairing-gate style: a NEW
// .Respond( site goes red even if it looks compliant, forcing the author to
// read this file and route through the choke point (or extend the pin with a
// written rationale).
func TestReplyEgressSingleChokePoint(t *testing.T) {
	// function name -> allowed .Respond( count inside it.
	allowed := map[string]int{
		"respondLogged":      2, // first attempt + reply_too_large fallback
		"installAuthCallout": 1, // pinned exemption — see authcallout.go
	}
	// Zero entries are NOT listed: `allowed` is consulted by lookup, so a
	// function absent from it is pinned at 0 automatically. The RespondMsg
	// arm therefore starts fully closed — any first use goes red.

	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]int{}
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Clean(name), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || !replyEgressAPIs[sel.Sel.Name] {
					return true
				}
				got[fn.Name.Name]++
				return true
			})
		}
	}
	for fnName, n := range got {
		if allowed[fnName] != n {
			t.Errorf("bare reply-egress census (Respond/RespondMsg): function %q has %d site(s), pinned %d — "+
				"route replies through respondLogged/respondBytes (h1 A2); if this site is a "+
				"genuine new exemption, pin it HERE with a written rationale",
				fnName, n, allowed[fnName])
		}
	}
	for fnName, n := range allowed {
		if got[fnName] != n {
			t.Errorf("pinned reply-egress census entry %q=%d no longer matches source (%d) — "+
				"update the pin alongside the code change", fnName, n, got[fnName])
		}
	}
}

// levelRecorder captures slog records so the test can assert on the LEVEL of
// egress logging (ERROR for max_payload, WARN for a closing conn).
type levelRecorder struct {
	mu      sync.Mutex
	records []slog.Record
}

func (r *levelRecorder) Enabled(context.Context, slog.Level) bool { return true }
func (r *levelRecorder) Handle(_ context.Context, rec slog.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = append(r.records, rec)
	return nil
}
func (r *levelRecorder) WithAttrs([]slog.Attr) slog.Handler { return r }
func (r *levelRecorder) WithGroup(string) slog.Handler      { return r }
func (r *levelRecorder) levels() []slog.Level {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]slog.Level, 0, len(r.records))
	for _, rec := range r.records {
		out = append(out, rec.Level)
	}
	return out
}

// TestRespondLoggedMaxPayloadFallback reproduces the incident shape against an
// embedded server with a tiny max_payload: the oversize reply must yield the
// typed reply_too_large fallback on the requester within one round trip (not a
// timeout), plus an ERROR log. Mutation check (h1): restoring the pre-h1
// `_ = msg.Respond(payload)` swallow in respondLogged turns the requester's
// reply into a timeout and this test red.
func TestRespondLoggedMaxPayloadFallback(t *testing.T) {
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	opts.MaxPayload = 1024
	ns := natstest.RunServer(&opts)
	defer func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	}()
	if !ns.ReadyForConnections(2 * time.Second) {
		t.Fatal("embedded nats-server not ready")
	}

	rec := &levelRecorder{}
	logger := slog.New(rec)

	ncA, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer ncA.Close()
	ncB, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer ncB.Close()

	oversize := make([]byte, 4096) // > MaxPayload 1024
	for i := range oversize {
		oversize[i] = 'x'
	}
	sub, err := ncA.Subscribe("h1.egress.req", func(msg *nats.Msg) {
		respondLogged(logger, ncA, msg, oversize)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := ncA.Flush(); err != nil {
		t.Fatal(err)
	}

	msg, err := ncB.Request("h1.egress.req", nil, 3*time.Second)
	if err != nil {
		t.Fatalf("requester saw %v — the typed fallback never arrived (the pre-h1 silent-drop shape)", err)
	}
	var fb proto.ReplyTooLarge
	if jerr := json.Unmarshal(msg.Data, &fb); jerr != nil {
		t.Fatalf("fallback not decodable: %v (%q)", jerr, msg.Data)
	}
	if fb.Code != proto.CodeReplyTooLarge {
		t.Fatalf("fallback code = %q, want %q", fb.Code, proto.CodeReplyTooLarge)
	}
	if !strings.Contains(fb.Error, "4096") || !strings.Contains(fb.Error, "1024") {
		t.Fatalf("fallback error should name reply size and max_payload, got %q", fb.Error)
	}
	if len(msg.Data) > 512 {
		t.Fatalf("fallback is %d bytes; want the bounded fallback pinned ≤512", len(msg.Data))
	}
	sawError := false
	for _, lv := range rec.levels() {
		if lv == slog.LevelError {
			sawError = true
		}
	}
	if !sawError {
		t.Fatal("oversize reply produced no ERROR log — the silence this file exists to forbid")
	}
}

// TestRespondLoggedClosedConnIsWarn pins the log-level split: a reply dropped
// because the broker's own conn is closing is WARN (teardown races every
// in-flight handler; an ERROR burst in the final log lines would read as a
// crash), while genuinely unexpected send failures stay ERROR.
func TestRespondLoggedClosedConnIsWarn(t *testing.T) {
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	ns := natstest.RunServer(&opts)
	defer func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	}()
	if !ns.ReadyForConnections(2 * time.Second) {
		t.Fatal("embedded nats-server not ready")
	}

	ncA, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	ncB, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer ncB.Close()

	sub, err := ncA.SubscribeSync("h1.egress.closed")
	if err != nil {
		t.Fatal(err)
	}
	if err := ncA.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := ncB.PublishRequest("h1.egress.closed", "h1.egress.closed.inbox", nil); err != nil {
		t.Fatal(err)
	}
	msg, err := sub.NextMsg(3 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ncA.Close() // now the captured msg's conn is closed

	rec := &levelRecorder{}
	respondLogged(slog.New(rec), ncA, msg, []byte("{}"))
	levels := rec.levels()
	if len(levels) != 1 || levels[0] != slog.LevelWarn {
		t.Fatalf("closed-conn reply drop logged as %v, want exactly one WARN", levels)
	}
}
