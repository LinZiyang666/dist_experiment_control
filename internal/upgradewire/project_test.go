// Real admin listeners, codecs, permission templates and signed bytes are used.
// The endpoint spy records routing and authorization; it does not fake a cut.
package upgradewire_test

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/upgradewire"
)

type endpointSpy struct {
	mu      sync.Mutex
	calls   []string
	key     upgradetx.Key
	actor   string
	stale   bool
	err     error
	input   upgradewire.Request
	foreign bool
}

var _ upgradeport.Endpoint = (*endpointSpy)(nil)

func (s *endpointSpy) record(op string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, op)
}
func (s *endpointSpy) count() int { s.mu.Lock(); defer s.mu.Unlock(); return len(s.calls) }
func (s *endpointSpy) view(nonce string) upgradetx.View {
	g := wireGrant(s.key)
	g.ActorFP = s.actor
	if s.foreign {
		g.Handle.TxID = strings.Repeat("f", 32)
	}
	if s.stale {
		nonce = "stale"
	}
	return upgradetx.View{Historical: upgradetx.Record{Key: s.key, Grant: g, Revision: 3, Forward: upgradetx.Attempt{Handle: g.Handle, Phase: upgradetx.PhasePrepared}}, Observation: upgradetx.Observation{Nonce: nonce, At: 123, OwnerKnown: true, RunningSHA: g.SourceSHA}}
}
func (s *endpointSpy) capture(r upgradewire.Request) { s.mu.Lock(); defer s.mu.Unlock(); s.input = r }
func (s *endpointSpy) Prepare(_ context.Context, key upgradetx.Key, p upgradetx.PrepareRequest) (upgradetx.Grant, error) {
	s.record(upgradewire.OpPrepare)
	s.capture(upgradewire.Request{Op: upgradewire.OpPrepare, Key: key, Prepare: p})
	return wireGrant(key), s.err
}
func (s *endpointSpy) Start(_ context.Context, g upgradetx.Grant, a upgradeartifact.Receipt) (upgradetx.View, error) {
	s.record(upgradewire.OpStart)
	s.capture(upgradewire.Request{Op: upgradewire.OpStart, Key: g.Handle.Key, Grant: g, Artifact: a})
	return s.view(""), s.err
}
func (s *endpointSpy) Query(_ context.Context, key upgradetx.Key, id, nonce string) (upgradetx.View, error) {
	s.record(upgradewire.OpQuery)
	s.capture(upgradewire.Request{Op: upgradewire.OpQuery, Key: key, TxID: id, QueryNonce: nonce})
	v := s.view(nonce)
	v.Historical.Key = key
	v.Historical.Forward.Handle.TxID = id
	return v, s.err
}
func (s *endpointSpy) Rollback(_ context.Context, f upgradetx.Fence, r upgradetx.ReverseRequest) (upgradetx.View, error) {
	s.record(upgradewire.OpRollback)
	s.capture(upgradewire.Request{Op: upgradewire.OpRollback, Key: f.Current.Key, Expected: f, Reverse: r})
	return s.view(""), s.err
}
func (s *endpointSpy) Cancel(_ context.Context, h upgradetx.Handle) (upgradetx.View, error) {
	s.record(upgradewire.OpCancel)
	s.capture(upgradewire.Request{Op: upgradewire.OpCancel, Key: h.Key, Expected: upgradetx.Fence{Current: h}})
	return s.view(""), s.err
}
func (s *endpointSpy) Archive(_ context.Context, h upgradetx.Handle) (upgradetx.View, error) {
	s.record(upgradewire.OpArchive)
	s.capture(upgradewire.Request{Op: upgradewire.OpArchive, Key: h.Key, Expected: upgradetx.Fence{Current: h}})
	return s.view(""), s.err
}
func (s *endpointSpy) CloseWindow(_ context.Context, h upgradetx.Handle) (upgradetx.View, error) {
	s.record(upgradewire.OpCloseWindow)
	s.capture(upgradewire.Request{Op: upgradewire.OpCloseWindow, Key: h.Key, Expected: upgradetx.Fence{Current: h}})
	return s.view(""), s.err
}

// adminEndpoint binds a test endpoint to the admin socket's port with the
// same contract as the production brokercontrol.BindAdminUpgrade (which this
// package cannot import without inverting its layering): the request context
// ends at the socket's absolute deadline or its stop signal, and the watcher
// is joined before the response returns. The admin port became this function
// type in S7 GATE-CONVERGE-layering F2; these tests had kept assigning the
// endpoint itself and no longer compiled.
// origin: S8 FIX-DESCRIBE (deviation: test-only adapter so the R1 describe tests can run)
func adminEndpoint(endpoint upgradeport.Endpoint) adminsock.UpgradeEndpoint {
	return func(body []byte, access upgradewire.Access, deadline controlbudget.Deadline, stop <-chan struct{}) upgradewire.Response {
		remaining, err := controlbudget.Remaining(deadline)
		if err != nil {
			return upgradewire.ErrorResponse(&upgradetx.Error{Code: upgradetx.CodeDeferred, Detail: "admin upgrade deadline expired"})
		}
		// ctx-none: the admin port supplies a deadline and owner stop, not a caller context.
		ctx, cancel := context.WithTimeout(context.Background(), remaining)
		joined := make(chan struct{})
		go func() {
			defer close(joined)
			select {
			case <-stop:
				cancel()
			case <-ctx.Done():
			}
		}()
		defer func() { cancel(); <-joined }()
		return upgradewire.Handle(ctx, endpoint, body, access)
	}
}

// origin: hot-update S8 plan §2.2 / §7.1 / P13
func TestUpgradeWireDispatchesOnlyAuthorizedOperations(t *testing.T) {
	for _, key := range []upgradetx.Key{wireKey(), brokerKey()} {
		for _, r := range wireRequests(key) {
			t.Run(string(key.Component)+r.Op, func(t *testing.T) {
				spy := &endpointSpy{key: key, actor: "actor"}
				resp := upgradewire.Handle(t.Context(), spy, requestBytes(t, r), upgradewire.Access{Key: key, ActorFP: "actor"})
				if resp.Code != "" || resp.Error != "" {
					t.Fatalf("authorized request refused: %+v", resp)
				}
				if !reflect.DeepEqual(spy.input, r) {
					t.Fatal("endpoint received a changed grant, handle or request")
				}
				calls := spy.calls
				if len(calls) == 0 || calls[len(calls)-1] != r.Op {
					t.Fatalf("wrong operation invoked: %v", calls)
				}
				if r.Op != upgradewire.OpQuery && r.Op != upgradewire.OpPrepare && r.Op != upgradewire.OpStart && len(calls) != 2 {
					t.Fatalf("mutation skipped original actor query: %v", calls)
				}
			})
		}
	}
	for _, r := range wireRequests(wireKey()) {
		for _, access := range []upgradewire.Access{{Key: upgradetx.Key{DomainID: "other", Component: upgradetx.ComponentAgent, SID: "session", ConfiguredNID: "node"}, ActorFP: "actor"}, {Key: brokerKey(), ActorFP: "actor"}, {Key: wireKey(), ActorFP: "other"}, {Key: wireKey()}} {
			spy := &endpointSpy{key: wireKey(), actor: "actor"}
			resp := upgradewire.Handle(t.Context(), spy, requestBytes(t, r), access)
			if resp.Code == "" {
				t.Fatalf("wrong actor/role authorized %s", r.Op)
			}
			for _, call := range spy.calls {
				if call != upgradewire.OpQuery {
					t.Fatalf("unauthorized side effect: %v", spy.calls)
				}
			}
		}
	}
	// A caller's chosen reverse actor cannot take somebody else's accepted cut.
	r := wireRequests(wireKey())[3]
	spy := &endpointSpy{key: wireKey(), actor: "other"}
	resp := upgradewire.Handle(t.Context(), spy, requestBytes(t, r), upgradewire.Access{Key: wireKey(), ActorFP: "actor"})
	if resp.Code != proto.CodeNotOwner || spy.count() != 1 {
		t.Fatal("reverse forged historical ownership")
	}
	for _, admin := range []bool{false, true} {
		spy := &endpointSpy{key: wireKey(), actor: "actor", stale: true}
		r := wireRequests(wireKey())[2]
		resp := upgradewire.Handle(t.Context(), spy, requestBytes(t, r), upgradewire.Access{Key: wireKey(), ActorFP: "actor", Administrator: admin})
		if resp.Code != upgradetx.CodeUnsettled {
			t.Fatal("stale query response signed current health")
		}
	}
	for _, admin := range []bool{false, true} {
		spy := &endpointSpy{key: wireKey(), actor: "actor", foreign: true}
		r := wireRequests(wireKey())[2]
		if resp := upgradewire.Handle(t.Context(), spy, requestBytes(t, r), upgradewire.Access{Key: wireKey(), ActorFP: "actor", Administrator: admin}); resp.Code != upgradetx.CodeUnsettled {
			t.Fatal("query returned another transaction's private view")
		}
	}

	r = wireRequests(wireKey())[2]
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	spy = &endpointSpy{key: wireKey(), actor: "actor"}
	if resp := upgradewire.Handle(ctx, spy, requestBytes(t, r), upgradewire.Access{Key: wireKey(), ActorFP: "actor"}); resp.Code == "" || spy.count() != 0 {
		t.Fatal("canceled request reached endpoint")
	}
	var missing *endpointSpy
	if resp := upgradewire.Handle(t.Context(), missing, requestBytes(t, r), upgradewire.Access{Key: wireKey(), ActorFP: "actor"}); resp.Code != upgradetx.CodeDeferred {
		t.Fatal("missing endpoint returned success")
	}
}

// origin: hot-update S8 plan §2.2 / §7.1 / P13
func TestUpgradeAdminSocketRoutesStrictTransactions(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	defer testharness.AssertNoGoroutineLeak(t, "upgrade admin", beforeG)
	defer testharness.AssertNoFDLeak(t, "upgrade admin", beforeFD, 0)
	for round := 0; round < 5; round++ {
		exerciseUpgradeAdminSocket(t)
	}
}
func exerciseUpgradeAdminSocket(t *testing.T) {
	t.Helper()
	key := brokerKey()
	spy := &endpointSpy{key: key, actor: "actor"}
	backend := adminsock.Backend{UpgradeEndpoint: adminEndpoint(spy), UpgradeKey: key}
	path := filepath.Join(t.TempDir(), "admin.sock")
	server := adminsock.New(path, backend)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	defer func() {
		if err := server.Close(); err != nil {
			t.Error(err)
		}
	}()
	if err := server.Start(ctx); err != nil {
		t.Fatal(err)
	}
	for _, r := range wireRequests(key) {
		body := requestBytes(t, r)
		reply := adminRoundTrip(t, path, body)
		resp, err := upgradewire.DecodeResponse(reply)
		if err != nil || resp.Code != "" || resp.Error != "" {
			t.Fatalf("admin did not dispatch %s: reply=%s err=%v", r.Op, reply, err)
		}
	}
	if spy.count() != 7 {
		t.Fatalf("admin operation count=%d want 7", spy.count())
	}
	before := spy.count()
	bad := bytesWithExtra(requestBytes(t, wireRequests(key)[2]), `"artifact":null`)
	if resp, err := upgradewire.DecodeResponse(adminRoundTrip(t, path, bad)); err != nil || resp.Code == "" {
		t.Fatal("admin accepted irrelevant transaction field")
	}
	wrong := requestBytes(t, wireRequests(wireKey())[2])
	if resp, err := upgradewire.DecodeResponse(adminRoundTrip(t, path, wrong)); err != nil || resp.Code == "" {
		t.Fatal("admin accepted agent role")
	}
	old := []byte(`{"op":"upgrade","url":"http://invalid","sha256":"x"}`)
	reply := adminRoundTrip(t, path, old)
	var oldResp adminsock.Response
	if err := json.Unmarshal(reply, &oldResp); err != nil || oldResp.Code != adminsock.CodeBadRequest {
		t.Fatalf("legacy op bypassed policy: %s %v", reply, err)
	}
	if spy.count() != before {
		t.Fatal("malformed/legacy request reached transaction endpoint")
	}
	// Existing read operations continue to use their original response shape.
	reply = adminRoundTrip(t, path, []byte(`{"op":"runtime"}`))
	if err := json.Unmarshal(reply, &oldResp); err != nil || oldResp.Op != adminsock.OpRuntime {
		t.Fatalf("legacy admin response changed: %s", reply)
	}
}
func bytesWithExtra(body []byte, extra string) []byte {
	return []byte(string(body[:len(body)-1]) + "," + extra + "}")
}
func adminRoundTrip(t *testing.T, path string, body []byte) []byte {
	t.Helper()
	conn, err := net.DialTimeout("unix", path, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(append(body, '\n')); err != nil {
		t.Fatal(err)
	}
	reply, err := bufio.NewReader(conn).ReadBytes('\n')
	if err != nil {
		t.Fatal(err)
	}
	return reply
}

// origin: hot-update S8 plan §2.2 / §7.1 / P13
func TestUpgradeTransactionACLStaysInActorSessionAndRole(t *testing.T) {
	actor, sid, nid := "actor", "session", "node"
	member := auth.PermissionsForActivatedMember(actor, sid, false)
	agent := auth.PermissionsForAgent(sid, nid, actor, false)
	broker := auth.PermissionsForBroker()
	unactivated := auth.PermissionsForUnactivated(actor, false)
	request := proto.SubjCmdBy(sid, actor, nid, "upgrade-tx")
	forwarded := proto.SubjCmdForwarded(sid, nid, "upgrade-tx")
	if !subjectsAllow(member.Pub.Allow, request) || !subjectsAllow(agent.Sub.Allow, forwarded) || !subjectsAllow(broker.Pub.Allow, forwarded) {
		t.Fatal("transaction command route is not granted")
	}
	for _, subject := range []string{proto.SubjCmdBy("other", actor, nid, "upgrade-tx"), proto.SubjCmdBy(sid, "other", nid, "upgrade-tx"), forwarded} {
		if subjectsAllow(member.Pub.Allow, subject) {
			t.Fatalf("member can impersonate actor/session/broker: %s", subject)
		}
	}
	if subjectsAllow(unactivated.Pub.Allow, request) || subjectsAllow(agent.Pub.Allow, request) || subjectsAllow(agent.Sub.Allow, proto.SubjCmdForwarded(sid, "other", "upgrade-tx")) {
		t.Fatal("wrong role granted transaction authority")
	}
	if !subjectsAllow(member.Pub.Allow, proto.SubjCmdBy(sid, actor, nid, "upgrade")) {
		t.Fatal("ordinary N-1 upgrade subject lost its grant")
	}
}
func subjectsAllow(patterns []string, subject string) bool {
	for _, pattern := range patterns {
		p, s := strings.Split(pattern, "."), strings.Split(subject, ".")
		match := true
		for i, token := range p {
			if token == ">" {
				if i < len(s) {
					return true
				}
				match = false
				break
			}
			if i >= len(s) || token != "*" && token != s[i] {
				match = false
				break
			}
		}
		if match && len(p) == len(s) {
			return true
		}
	}
	return false
}

// origin: hot-update S8 plan §2.2 / §7.1 / P13
func TestUpgradeTriggerSignsTransactionActorTargetAndGrant(t *testing.T) {
	r := proto.ClusterUpgradeReq{Op: "reload", TargetNode: "broker", IssuedAt: "2026-10-13T00:00:00Z"}
	old := "reload\nbroker\n\n\n\n\n2026-10-13T00:00:00Z"
	if got := string(proto.CanonicalUpgradeReqBytes(&r)); got != old {
		t.Fatalf("old canonical bytes changed: %q", got)
	}
	req := wireRequests(brokerKey())[0]
	r.Op = "upgrade-tx"
	r.ActorFP = "actor"
	r.Upgrade = requestBytes(t, req)
	canonical := string(proto.CanonicalUpgradeReqBytes(&r))
	if !strings.Contains(canonical, "\nupgrade-tx-v1\nactor\n") {
		t.Fatal("transaction extension is not versioned and signed")
	}
	for _, mutate := range []func(*proto.ClusterUpgradeReq){func(r *proto.ClusterUpgradeReq) { r.ActorFP = "other" }, func(r *proto.ClusterUpgradeReq) { r.TargetNode = "other" }, func(r *proto.ClusterUpgradeReq) {
		r.Upgrade = json.RawMessage(strings.Replace(string(r.Upgrade), `"actor_fp":"actor"`, `"actor_fp":"other"`, 1))
	}} {
		copy := r
		copy.Upgrade = append(json.RawMessage(nil), r.Upgrade...)
		mutate(&copy)
		if string(proto.CanonicalUpgradeReqBytes(&copy)) == canonical {
			t.Fatal("signature omitted actor/target/envelope")
		}
	}
	// json.Marshal compacts RawMessage fields and escapes HTML. Signers and
	// verifiers must bind the identical transported representation.
	copyEnvelope := r
	copyEnvelope.Upgrade = json.RawMessage(strings.Replace(string(r.Upgrade), `"actor_fp":"actor"`, `"actor_fp" : "actor"`, 1))
	signedBefore := proto.CanonicalUpgradeReqBytes(&copyEnvelope)
	body, err := json.Marshal(copyEnvelope)
	if err != nil {
		t.Fatal(err)
	}
	var transported proto.ClusterUpgradeReq
	if err := json.Unmarshal(body, &transported); err != nil {
		t.Fatal(err)
	}
	if string(signedBefore) != string(proto.CanonicalUpgradeReqBytes(&transported)) {
		t.Fatal("JSON marshal changed the signed envelope")
	}

	if _, err := upgradewire.DecodeClusterRequest(&r, "broker", "actor"); err != nil {
		t.Fatal(err)
	}
	for _, pair := range [][2]string{{"other", "actor"}, {"broker", "other"}} {
		if _, err := upgradewire.DecodeClusterRequest(&r, pair[0], pair[1]); err == nil {
			t.Fatal("signed request accepted different route")
		}
	}
	copy := r
	copy.Upgrade = requestBytes(t, wireRequests(wireKey())[0])
	if _, err := upgradewire.DecodeClusterRequest(&copy, "broker", "actor"); err == nil {
		t.Fatal("broker trigger authorized agent role")
	}
	for _, mutate := range []func(*proto.ClusterUpgradeReq){func(r *proto.ClusterUpgradeReq) { r.Op = "reload" }, func(r *proto.ClusterUpgradeReq) { r.ActorFP = "" }, func(r *proto.ClusterUpgradeReq) { r.ActorFP = "actor\nother" }, func(r *proto.ClusterUpgradeReq) { r.SID = "session" }, func(r *proto.ClusterUpgradeReq) { r.Upgrade = nil }} {
		copy := r
		mutate(&copy)
		if proto.ValidateUpgradeReq(&copy) == nil || proto.CanonicalUpgradeReqBytes(&copy) != nil {
			t.Fatal("invalid signed framing accepted")
		}
	}
}

// origin: hot-update S8 plan §2.2 / §7.1 / P13
func TestUpgradeSummaryContainsOnlyPublicProof(t *testing.T) {
	g := wireGrant(wireKey())
	view := upgradetx.View{Historical: upgradetx.Record{Key: g.Handle.Key, Grant: g, Reason: "PRIVATE", EffectiveSHA: "disk", Forward: upgradetx.Attempt{Handle: g.Handle, Phase: upgradetx.PhaseCompleted, Outcome: upgradetx.OutcomeSuccess}}, Observation: upgradetx.Observation{RunningSHA: g.SourceSHA, Current: upgradetx.Attempt{Handle: upgradetx.Handle{TxID: strings.Repeat("f", 32), CutID: strings.Repeat("e", 32), Generation: 3, Revision: 4}, Phase: upgradetx.PhaseMigrating, Outcome: upgradetx.OutcomePending}}, Detail: "PRIVATE", Code: upgradetx.CodeUnsettled}
	summary := upgradewire.Summary(view)
	if summary == nil || summary.TxID != view.Observation.Current.Handle.TxID || summary.RunningSHA != g.SourceSHA || summary.Phase != "migrating" || summary.Reason != upgradetx.CodeUnsettled {
		t.Fatalf("summary confused history/current/disk: %+v", summary)
	}
	b, _ := json.Marshal(summary)
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(b, &fields)
	for _, name := range []string{"tx_id", "cut_id", "phase", "outcome", "running_sha", "reason", "generation", "revision"} {
		if fields[name] == nil {
			t.Fatalf("public summary field %s is missing", name)
		}
	}
	if len(fields) != 8 || strings.Contains(string(b), "PRIVATE") || strings.Contains(string(b), "session") || strings.Contains(string(b), "actor") {
		t.Fatalf("private proof leaked: %s", b)
	}
	view.Code = "PRIVATE"
	if upgradewire.Summary(view).Reason != "" {
		t.Fatal("private detail promoted to public reason")
	}
	if upgradewire.Summary(upgradetx.View{}) != nil {
		t.Fatal("empty view advertises completed upgrade")
	}
	for _, specimen := range []any{proto.NodeRegisterReq{}, proto.NodeRegisterResp{}, proto.NodeLease{}, proto.ClusterHealthResp{}} {
		b, _ := json.Marshal(specimen)
		if strings.Contains(string(b), "incarnation") || strings.Contains(string(b), "\"upgrade\"") {
			t.Fatalf("N-1 zero shape changed: %s", b)
		}
	}
	types := []reflect.Type{reflect.TypeFor[proto.NodeRegisterReq](), reflect.TypeFor[proto.NodeRegisterResp](), reflect.TypeFor[proto.NodeLease]()}
	for _, typ := range types {
		f, ok := typ.FieldByName("Incarnation")
		if !ok || f.Type.Kind() != reflect.String || string(f.Tag) != `json:"incarnation,omitempty"` {
			t.Fatalf("authority field missing or changed on %s", typ)
		}
	}
}

// origin: hot-update S8 plan §7.1 / P13
func TestUpgradeErrorProjectionPreservesAcceptedOutcome(t *testing.T) {
	for _, row := range []struct {
		code string
		exit int
	}{{upgradetx.CodeUnsupportedComponent, 64}, {upgradetx.CodeGrantUnknown, 64}, {upgradetx.CodeGrantExpired, 64}, {upgradetx.CodeRejected, 64}, {upgradetx.CodeWindowClosed, 64}, {upgradetx.CodeDeferred, 75}, {upgradetx.CodeNotWired, 75}, {upgradetx.CodeStateCorrupt, 74}, {upgradetx.CodeUnsettled, 74}, {upgradetx.CodeSessionIncarnationMismatch, 77}, {proto.CodeNotOwner, 77}} {
		if exit, ok := upgradewire.ErrorExit(row.code); !ok || exit != row.exit {
			t.Errorf("unclassified %s: %d", row.code, exit)
		}
		resp := upgradewire.ErrorResponse(fmt.Errorf("wrapped: %w", &upgradetx.Error{Code: row.code, Detail: "public"}))
		if resp.Code != row.code || resp.Error != "public" {
			t.Fatalf("refusal projection changed: %+v", resp)
		}
		resp = upgradewire.ErrorResponse(&upgradetx.Error{Code: row.code, Detail: "public", Accepted: true})
		if row.exit != 77 && resp.Code != upgradetx.CodeUnsettled {
			t.Fatalf("accepted failure invites retry: %+v", resp)
		}
	}
	if _, ok := upgradewire.ErrorExit("unknown"); ok {
		t.Fatal("unknown code silently classified")
	}
	if r := upgradewire.ErrorResponse(errors.New("PIN_HASH_PRIVATE")); r.Code != upgradetx.CodeUnsettled || strings.Contains(r.Error, "PRIVATE") {
		t.Fatal("raw error material disclosed")
	}
	for _, row := range []struct{ code, want string }{{upgradetx.CodeRejected, proto.CodeBadRequest}, {upgradetx.CodeDeferred, "try_again"}, {upgradetx.CodeSessionIncarnationMismatch, proto.CodeNotOwner}} {
		r := upgradewire.LegacyError(&upgradetx.Error{Code: row.code, Detail: "legacy_busy"})
		if r.OK || r.Code != row.want || r.Error != "legacy_busy" {
			t.Fatalf("old ctl projection drift: %+v", r)
		}
	}
	r := upgradewire.LegacyError(&upgradetx.Error{Code: upgradetx.CodeDeferred, Accepted: true})
	if r.Code == "try_again" || r.OK {
		t.Fatal("old ctl invited to redispatch accepted operation")
	}
}

// origin: hot-update S8 plan §2.2 / §4.2 / P13
func TestUpgradeAdminClientKeepsTransactionAndLegacyShapes(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	defer testharness.AssertNoGoroutineLeak(t, "upgrade admin client", beforeG)
	defer testharness.AssertNoFDLeak(t, "upgrade admin client", beforeFD, 0)
	for round := 0; round < 5; round++ {
		exerciseUpgradeAdminClient(t)
	}
}
func exerciseUpgradeAdminClient(t *testing.T) {
	t.Helper()
	key := brokerKey()
	spy := &endpointSpy{key: key, actor: "actor"}
	path := filepath.Join(t.TempDir(), "admin.sock")
	server := adminsock.New(path, adminsock.Backend{UpgradeKey: key, UpgradeEndpoint: adminEndpoint(spy), RuntimeSnapshot: func() *adminsock.RuntimeReport {
		return &adminsock.RuntimeReport{Schema: "admin_runtime", SchemaVersion: 2}
	}})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	defer func() {
		if err := server.Close(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	if err := server.Start(ctx); err != nil {
		t.Fatal(err)
	}
	client := adminsock.Client{Path: path, Timeout: time.Second}
	for _, r := range wireRequests(key) {
		response, err := client.Call(adminsock.Request{Op: r.Op, Upgrade: &r})
		if err != nil || response.Upgrade == nil || response.Code != "" {
			t.Fatalf("admin client lost transaction result: %+v %v", response, err)
		}
	}
	if spy.count() != 7 {
		t.Fatalf("client dispatch count=%d", spy.count())
	}
	legacy, err := client.Call(adminsock.Request{Op: adminsock.OpRuntime})
	if err != nil || legacy.Runtime == nil || legacy.Op != adminsock.OpRuntime || legacy.Upgrade != nil {
		t.Fatalf("legacy client shape changed: %+v %v", legacy, err)
	}
	r := wireRequests(key)[2]
	for _, request := range []adminsock.Request{{Op: r.Op, Upgrade: &r, SID: "private"}, {Op: adminsock.OpEvict, Upgrade: &r}} {
		if _, err := json.Marshal(request); err == nil {
			t.Fatal("legacy fields smuggled into transaction envelope")
		}
	}
	missingPath := filepath.Join(t.TempDir(), "missing.sock")
	missing := adminsock.New(missingPath, adminsock.Backend{UpgradeKey: key})
	defer func() {
		if err := missing.Close(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	if err := missing.Start(ctx); err != nil {
		t.Fatal(err)
	}
	response, err := (&adminsock.Client{Path: missingPath, Timeout: time.Second}).Call(adminsock.Request{Upgrade: &r})
	if err != nil || response.Code != upgradetx.CodeDeferred {
		t.Fatalf("missing endpoint did not defer: %+v %v", response, err)
	}
}

// origin: hot-update S8 plan §6.1 / §7.1 / P13
func TestUpgradeVersionAndCodesRemainSingleSourced(t *testing.T) {
	if proto.ProtoVersion != 2 || proto.ImageABIVersion != 1 {
		t.Fatal("S8 changed an existing epoch")
	}
	// origin: S8 BROKER-EXPORTER R-11 — the S8 reader is advertised once every
	// exporter exists (upgrade-cut-v1 domain, three adapter features).
	want := []string{"image-domains exec-v1 pty-v1 flow-v1 ss-v1 broker-image-v1 transfer-v1 upgrade-cut-v1", "image-read-epochs 1",
		"image-features upgrade-tx-v1 authority-incarnation-v1 upgrade-cut-v1", "image-abi v1"}
	if !reflect.DeepEqual(proto.ImageVersionLines(), want) {
		t.Fatalf("reader epochs or N-1 seam changed: %v", proto.ImageVersionLines())
	}
	if proto.CapUpgradeTXV1 != upgradewire.Version || proto.CapAuthorityIncarnationV1 != "authority-incarnation-v1" || proto.CapUpgradeCutV1 != "upgrade-cut-v1" {
		t.Fatal("capability vocabulary changed")
	}
	for _, row := range [][2]string{{proto.CodeUpgradeNotWired, upgradetx.CodeNotWired}, {proto.CodeUnsupportedComponent, upgradetx.CodeUnsupportedComponent}, {proto.CodeUpgradeStateCorrupt, upgradetx.CodeStateCorrupt}, {proto.CodeUpgradeGrantUnknown, upgradetx.CodeGrantUnknown}, {proto.CodeUpgradeGrantExpired, upgradetx.CodeGrantExpired}, {proto.CodeUpgradeRejected, upgradetx.CodeRejected}, {proto.CodeUpgradeDeferred, upgradetx.CodeDeferred}, {proto.CodeUpgradeUnsettled, upgradetx.CodeUnsettled}, {proto.CodeUpgradeWindowClosed, upgradetx.CodeWindowClosed}, {proto.CodeSessionIncarnationMismatch, upgradetx.CodeSessionIncarnationMismatch}} {
		if row[0] != row[1] {
			t.Fatalf("neutral/wire code drift: %v", row)
		}
		if _, known := upgradewire.ErrorExit(row[0]); !known {
			t.Fatalf("wire code lacks exit projection: %s", row[0])
		}
	}
}

// origin: hot-update S8 plan §4.2 / P13
func TestUpgradeAdminTransactionCutKeepsReplyCustody(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("listener image requires Linux")
	}
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	defer testharness.AssertNoGoroutineLeak(t, "upgrade admin cut", beforeG)
	defer testharness.AssertNoFDLeak(t, "upgrade admin cut", beforeFD, 0)
	for round := 0; round < 5; round++ {
		exerciseUpgradeAdminCut(t)
	}
}

type cutEndpoint struct {
	endpointSpy
	server *adminsock.Server
}

func (e *cutEndpoint) Start(ctx context.Context, _ upgradetx.Grant, _ upgradeartifact.Receipt) (upgradetx.View, error) {
	e.record(upgradewire.OpStart)
	now, err := rawsocket.BootNow()
	if err != nil {
		return upgradetx.View{}, err
	}
	deadline := controlbudget.Deadline(now + int64(2*time.Second))
	if err := e.server.FenceImage(deadline); err != nil {
		return upgradetx.View{}, err
	}
	if err := ctx.Err(); err != nil {
		return upgradetx.View{}, err
	}
	if err := e.server.JoinImage(deadline); err != nil {
		return upgradetx.View{}, err
	}
	return e.view(""), nil
}
func exerciseUpgradeAdminCut(t *testing.T) {
	t.Helper()
	key := brokerKey()
	e := &cutEndpoint{endpointSpy: endpointSpy{key: key, actor: "actor"}}
	path := filepath.Join(t.TempDir(), "admin.sock")
	server := adminsock.New(path, adminsock.Backend{UpgradeKey: key, UpgradeEndpoint: adminEndpoint(e)})
	e.server = server
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	defer func() {
		if err := server.Close(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	if err := server.Start(ctx); err != nil {
		t.Fatal(err)
	}
	r := wireRequests(key)[1]
	resp, err := (&adminsock.Client{Path: path, Timeout: 3 * time.Second}).Call(adminsock.Request{Upgrade: &r})
	if err != nil || resp.Code != "" || resp.Upgrade == nil {
		t.Fatalf("initiating cut lost commit/reply custody: %+v %v", resp, err)
	}
	now, err := rawsocket.BootNow()
	if err != nil {
		t.Fatal(err)
	}
	if err := adminsock.JoinImageReplies(server, controlbudget.Deadline(now+int64(time.Second))); err != nil {
		t.Fatalf("armed reply was not joined: %v", err)
	}
	if e.count() != 1 {
		t.Fatal("initiating transaction redispatched")
	}
}

type shortWriter struct{}

func (shortWriter) Write(b []byte) (int, error) { return len(b) - 1, nil }

// origin: hot-update S8 plan §4.2 / P13
func TestUpgradeWireReplyRequiresFullWrite(t *testing.T) {
	if err := upgradewire.WriteLine(shortWriter{}, upgradewire.Response{}); err == nil {
		t.Fatal("short reply write signed as delivered")
	}
}

// origin: hot-update S8 plan §2.2 / §7.1 / P13
func TestUpgradeForwardedEnvelopeBindsBrokerActorAndTarget(t *testing.T) {
	for _, r := range wireRequests(wireKey()) {
		body, err := upgradewire.EncodeForwarded(r, "actor")
		if err != nil {
			t.Fatal(err)
		}
		decoded, access, err := upgradewire.DecodeForwarded(body, wireKey())
		if err != nil || !reflect.DeepEqual(decoded, r) || access.ActorFP != "actor" || access.Administrator {
			t.Fatalf("forward envelope changed authority: %+v %v", access, err)
		}
		spy := &endpointSpy{key: wireKey(), actor: "actor"}
		resp := upgradewire.HandleForwarded(t.Context(), spy, wireKey(), body)
		if resp.Code != "" || spy.count() == 0 {
			t.Fatalf("forwarded request not dispatched: %+v", resp)
		}
		wrong, _ := upgradewire.EncodeForwarded(r, "other")
		spy = &endpointSpy{key: wireKey(), actor: "actor"}
		resp = upgradewire.HandleForwarded(t.Context(), spy, wireKey(), wrong)
		if resp.Code != proto.CodeNotOwner {
			t.Fatal("forward envelope authorized another actor")
		}
		key := wireKey()
		key.DomainID = "other"
		if _, _, err := upgradewire.DecodeForwarded(body, key); err == nil {
			t.Fatal("forward envelope authorized another target")
		}
		if _, _, err := upgradewire.DecodeForwarded(body, brokerKey()); err == nil {
			t.Fatal("forward envelope authorized wrong role")
		}
		for _, poison := range []string{strings.Replace(string(body), `"actor_fp":"actor"`, `"actor_fp":"other","actor_fp":"actor"`, 1), strings.Replace(string(body), `"actor_fp":"actor"`, `"actor_fp":""`, 1), string(bytesWithExtra(body, `"administrator":true`))} {
			if _, _, err := upgradewire.DecodeForwarded([]byte(poison), wireKey()); err == nil {
				t.Fatal("ambiguous or privileged forward envelope accepted")
			}
		}
	}
	if _, err := upgradewire.EncodeForwarded(wireRequests(wireKey())[2], ""); err == nil {
		t.Fatal("forward encoder accepted missing actor")
	}
}
