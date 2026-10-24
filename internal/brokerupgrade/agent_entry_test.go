package brokerupgrade

import (
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/proc"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/upgradewire"
	"github.com/nats-io/nats.go"
)

type agentRouteFixture struct {
	db           *sql.DB
	actor, fp    string
	key          upgradetx.Key
	requests     []string
	envelopes    [][]byte
	reply        []byte
	requestError error
}

func newAgentRouteFixture(t *testing.T) *agentRouteFixture {
	t.Helper()
	seed, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatal(err)
	}
	actor, err := auth.PublicKeyFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}
	fp, err := auth.FingerprintFromActor(actor)
	if err != nil {
		t.Fatal(err)
	}
	db := testharness.OpenDB(t)
	if _, err := session.Create(db, "lab", "lab", fp, "pin-hash", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := node.Register(db, node.RegisterInput{SID: "lab", NID: "node", ProtoVersion: proto.ProtoVersion, ReleaseVersion: "v0.6.0"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	f := &agentRouteFixture{db: db, actor: actor, fp: fp,
		key: upgradetx.Key{Component: upgradetx.ComponentAgent, DomainID: strings.Repeat("d", 32), SID: "lab", ConfiguredNID: "node"}}
	f.reply, _ = upgradewire.EncodeResponse(upgradewire.Response{View: upgradetx.View{Historical: upgradetx.Record{Key: f.key}}})
	return f
}

func (f *agentRouteFixture) advertise(t *testing.T, capabilities ...string) {
	t.Helper()
	command, err := PlanAgentTransactionCapability(f.db, "lab", "node", capabilities)
	if err != nil {
		t.Fatal(err)
	}
	if command == nil {
		return
	}
	for _, statement := range command.Body {
		if _, err := f.db.Exec(statement.SQL, statement.Args...); err != nil {
			t.Fatal(err)
		}
	}
}

func (f *agentRouteFixture) prepare(t *testing.T, mutate func(*upgradewire.Request)) []byte {
	t.Helper()
	request := upgradewire.Request{Op: upgradewire.OpPrepare, Key: f.key, Prepare: upgradetx.PrepareRequest{
		ClientNonce: "nonce", ActorFP: f.fp, SourceRelease: "v1", TargetRelease: "v2", Mode: upgradetx.ModeHot,
		SourceSHA: strings.Repeat("a", 64), TargetSHA: strings.Repeat("b", 64), CapabilityDigest: strings.Repeat("c", 64),
		ExpiresAt: time.Now().Add(time.Minute).UnixNano(),
		Scope:     upgradetx.Scope{ID: "scope", Participants: []upgradetx.Key{f.key}, Domains: []upgradetx.Domain{upgradetx.DomainControl}}}}
	if mutate != nil {
		mutate(&request)
	}
	body, err := upgradewire.EncodeRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func (f *agentRouteFixture) cancel(t *testing.T) []byte {
	t.Helper()
	body, err := upgradewire.EncodeRequest(upgradewire.Request{Op: upgradewire.OpCancel, Key: f.key, Expected: upgradetx.Fence{Current: upgradetx.Handle{
		Key: f.key, TxID: strings.Repeat("1", 32), CutID: strings.Repeat("2", 32), Generation: 2, Revision: 3}}})
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func (f *agentRouteFixture) route(body []byte, reply string) (upgradewire.Response, string) {
	raw, code := RouteAgentTransaction(AgentRoute{DB: f.db, SID: "lab", NID: "node", Actor: f.actor, ActorFP: f.fp, Reply: reply, Body: body, Timeout: time.Second,
		Request: func(subject string, envelope []byte, _ time.Duration) ([]byte, error) {
			f.requests = append(f.requests, subject)
			f.envelopes = append(f.envelopes, envelope)
			return f.reply, f.requestError
		}})
	response, err := upgradewire.DecodeResponse(raw)
	if err != nil {
		return upgradewire.Response{Code: "undecodable:" + string(raw)}, code
	}
	return response, code
}

func (f *agentRouteFixture) privateReply() string { return auth.InboxPrefixFor(f.actor) + ".reply" }

// The new transaction hop is an upgrade entry like the old wire: a target that
// has not advertised the S8 route is legacy, so the same busy64/defer75 policy
// refuses before one byte reaches it. A capable target receives exactly the
// strict request with the broker-stamped actor, and only on the actor's own
// private reply root may a private View come back.
// origin: hot-update S8 plan §7.1 / §7.2 / §11.2 P15 (old-request direct-forward mutant)
func TestAgentTransactionHopRefusesLegacyTargetsBeforeForward(t *testing.T) {
	t.Run("legacy_idle_defers", func(t *testing.T) {
		f := newAgentRouteFixture(t)
		response, code := f.route(f.prepare(t, nil), f.privateReply())
		if response.Code != upgradetx.CodeDeferred || response.Error != "legacy_quiescence_unproved" || code != upgradetx.CodeDeferred || len(f.requests) != 0 {
			t.Fatalf("legacy idle target: %+v code=%s forwarded=%d", response, code, len(f.requests))
		}
	})
	t.Run("legacy_busy_rejects", func(t *testing.T) {
		f := newAgentRouteFixture(t)
		if err := proc.Insert(f.db, proc.Process{PID: "pid", SID: "lab", NID: "node", Argv: []string{"sleep", "60"}, StartedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
		response, _ := f.route(f.prepare(t, nil), f.privateReply())
		if response.Code != upgradetx.CodeRejected || response.Error != "legacy_busy" || len(f.requests) != 0 {
			t.Fatalf("legacy busy target: %+v forwarded=%d", response, len(f.requests))
		}
	})
	t.Run("downgraded_register_is_legacy_again", func(t *testing.T) {
		f := newAgentRouteFixture(t)
		f.advertise(t, proto.CapUpgradeTXV1)
		f.advertise(t, proto.CapProxyV1)
		if response, _ := f.route(f.prepare(t, nil), f.privateReply()); response.Code != upgradetx.CodeDeferred || len(f.requests) != 0 {
			t.Fatalf("a cold reinstall of an old agent still received transactions: %+v", response)
		}
	})
	t.Run("capable_forwards_once_with_broker_actor", func(t *testing.T) {
		f := newAgentRouteFixture(t)
		f.advertise(t, proto.CapProxyV1, proto.CapUpgradeTXV1)
		response, code := f.route(f.prepare(t, nil), f.privateReply())
		if response.Code != "" || code != "" || len(f.requests) != 1 || f.requests[0] != proto.SubjCmdForwarded("lab", "node", SignedTransactionOp) {
			t.Fatalf("capable target: %+v forwarded=%v", response, f.requests)
		}
		request, access, err := upgradewire.DecodeForwarded(f.envelopes[0], f.key)
		if err != nil || access.ActorFP != f.fp || access.Administrator || request.Op != upgradewire.OpPrepare {
			t.Fatalf("forwarded envelope does not bind the authenticated actor: %+v %+v %v", request.Op, access, err)
		}
	})
	for name, arm := range map[string]struct {
		reply  string
		mutate func(*upgradewire.Request)
		raw    []byte
		code   string
	}{
		"shared_inbox":         {reply: "_INBOX.reply", code: upgradetx.CodeRejected},
		"other_actors_inbox":   {reply: auth.InboxPrefixFor("UOTHER") + ".reply", code: upgradetx.CodeRejected},
		"other_session_target": {mutate: func(r *upgradewire.Request) { r.Key.SID = "other"; r.Prepare.Scope.Participants[0].SID = "other" }, code: proto.CodeNotOwner},
		"other_actor_in_body":  {mutate: func(r *upgradewire.Request) { r.Prepare.ActorFP = "someone-else" }, code: proto.CodeNotOwner},
		"agent_role_violation": {raw: []byte(`{"op":"upgrade.query"}`), code: upgradetx.CodeRejected},
	} {
		t.Run(name, func(t *testing.T) {
			f := newAgentRouteFixture(t)
			f.advertise(t, proto.CapUpgradeTXV1)
			body := arm.raw
			if body == nil {
				body = f.prepare(t, arm.mutate)
			}
			reply := arm.reply
			if reply == "" {
				reply = f.privateReply()
			}
			if response, _ := f.route(body, reply); response.Code != arm.code || len(f.requests) != 0 {
				t.Fatalf("%s: %+v forwarded=%d", name, response, len(f.requests))
			}
		})
	}
	t.Run("lost_replies", func(t *testing.T) {
		f := newAgentRouteFixture(t)
		f.advertise(t, proto.CapUpgradeTXV1)
		// Nobody subscribed: nothing was delivered, so a retry is safe.
		f.requestError = nats.ErrNoResponders
		if response, _ := f.route(f.cancel(t), f.privateReply()); response.Code != upgradetx.CodeDeferred {
			t.Fatalf("no responder = %+v", response)
		}
		// A lost Prepare reply leaves only an inert, nonce-idempotent grant.
		f.requestError = nats.ErrTimeout
		if response, _ := f.route(f.prepare(t, nil), f.privateReply()); response.Code != upgradetx.CodeDeferred {
			t.Fatalf("lost prepare reply = %+v", response)
		}
		// A lost mutation reply may follow an accepted effect: query, never retry.
		if response, _ := f.route(f.cancel(t), f.privateReply()); response.Code != upgradetx.CodeUnsettled {
			t.Fatalf("lost mutation reply = %+v", response)
		}
		f.requestError, f.reply = nil, []byte(`{"view":`)
		if response, _ := f.route(f.cancel(t), f.privateReply()); response.Code != upgradetx.CodeUnsettled {
			t.Fatalf("unreadable mutation reply = %+v", response)
		}
		if len(f.requests) != 4 {
			t.Fatalf("each request was sent exactly once: %d", len(f.requests))
		}
	})
}

// The capability record follows the LATEST authenticated register and is read
// afresh per request, including on a queue-group member that never saw it.
// origin: hot-update S8 plan §6.1 / §7.2 / §11.2 P15
func TestAgentTransactionCapabilityRecordFollowsLatestRegister(t *testing.T) {
	f := newAgentRouteFixture(t)
	if AgentTransactionCapable(f.db, "lab", "node") || AgentTransactionCapable(nil, "lab", "node") {
		t.Fatal("absence or an unreadable store proved capability")
	}
	if command, err := PlanAgentTransactionCapability(f.db, "lab", "node", []string{proto.CapProxyV1}); err != nil || command != nil {
		t.Fatalf("a legacy register on an empty record wrote %v %v", command, err)
	}
	f.advertise(t, proto.CapUpgradeTXV1)
	if !AgentTransactionCapable(f.db, "lab", "node") || AgentTransactionCapable(f.db, "lab", "other") || AgentTransactionCapable(f.db, "other", "node") {
		t.Fatal("capability is not scoped to the registering session/node")
	}
	if command, err := PlanAgentTransactionCapability(f.db, "lab", "node", []string{proto.CapUpgradeTXV1}); err != nil || command != nil {
		t.Fatalf("an unchanged register proposed a write: %v %v", command, err)
	}
	f.advertise(t)
	if AgentTransactionCapable(f.db, "lab", "node") {
		t.Fatal("a downgraded register kept the transaction route")
	}
	if _, err := f.db.Exec(`DROP TABLE cluster_meta`); err != nil {
		t.Fatal(err)
	}
	if _, err := PlanAgentTransactionCapability(f.db, "lab", "node", nil); err == nil {
		t.Fatal("an unreadable record was treated as unchanged")
	}
}

// The old wire keeps its historical gate order and audit text, then always
// refuses through the shared legacy policy: no allowlisted URL, valid SHA or
// account signature turns the old envelope into an execution authority.
// origin: hot-update S8 plan §7.1 / §7.2 / §12.1 (P15 move of upgrade.go gates)
func TestLegacyAgentRequestRefusesInHistoricalGateOrder(t *testing.T) {
	f := newAgentRouteFixture(t)
	valid := proto.UpgradeReq{URL: "https://releases.example/t", SHA256: strings.Repeat("a", 64), ProtoVersion: proto.ProtoVersion}
	allow := []string{"https://releases.example/"}
	body := func(mutate func(*proto.UpgradeReq)) []byte {
		req := valid
		if mutate != nil {
			mutate(&req)
		}
		raw, _ := json.Marshal(req)
		return raw
	}
	for name, arm := range map[string]struct {
		body  []byte
		allow []string
		want  LegacyRefusal
	}{
		"malformed_json": {body: []byte("{"), allow: allow, want: LegacyRefusal{Code: "json_parse", Detail: "unexpected end of JSON input"}},
		"proto_epoch":    {body: body(func(r *proto.UpgradeReq) { r.ProtoVersion++ }), allow: allow, want: LegacyRefusal{Code: "proto_bump_requires_reinstall", Audit: "proto_bump_requires_reinstall"}},
		"sha_shape":      {body: body(func(r *proto.UpgradeReq) { r.SHA256 = "abc" }), allow: allow, want: LegacyRefusal{Code: "sha256_invalid", Detail: "expected 64 lowercase hex chars", Audit: "sha256_invalid"}},
		"no_allowlist":   {body: body(nil), want: LegacyRefusal{Code: "url_not_allowed", Detail: valid.URL, Audit: "url_not_allowed"}},
		"unfenced_idle":  {body: body(nil), allow: allow, want: LegacyRefusal{Code: "try_again", Detail: "legacy_quiescence_unproved", Audit: "legacy_quiescence_unproved"}},
	} {
		got := CheckLegacyAgentRequest(arm.body, arm.allow, f.db, "lab", "node")
		if got.Code != arm.want.Code || got.Audit != arm.want.Audit || arm.want.Detail != "" && got.Detail != arm.want.Detail {
			t.Fatalf("%s: got %+v want %+v", name, got, arm.want)
		}
	}
	// Another node's live work cannot label this target busy.
	if err := node.Register(f.db, node.RegisterInput{SID: "lab", NID: "other", ProtoVersion: proto.ProtoVersion}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := proc.Insert(f.db, proc.Process{PID: "pid", SID: "lab", NID: "other", Argv: []string{"x"}, StartedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if got := CheckLegacyAgentRequest(body(nil), allow, f.db, "lab", "node"); got.Code != "try_again" {
		t.Fatalf("another node's live work labelled this target busy: %+v", got)
	}
	if got := CheckLegacyAgentRequest(body(nil), allow, f.db, "lab", "other"); got.Code != proto.CodeBadRequest || got.Detail != "legacy_busy" {
		t.Fatalf("live work on the target was not a permanent refusal: %+v", got)
	}
	// A lease-shaped sibling makes the shared binary's family unprovable.
	if err := node.Register(f.db, node.RegisterInput{SID: "lab", NID: "node-02", ProtoVersion: proto.ProtoVersion}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if got := CheckLegacyAgentRequest(body(nil), allow, f.db, "lab", "node"); got.Code != "clone_family_upgrade_unsupported" {
		t.Fatalf("clone family was not refused before the policy: %+v", got)
	}
}
