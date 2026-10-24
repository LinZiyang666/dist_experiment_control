// Real canonical/signature and projection code; no infrastructure process is
// spawned. Previous-release artifacts and production SUB installation require
// P26/P30 receipts and are not established by these pure protocol checks.
package proto

import (
	"encoding/hex"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/nats-io/nkeys"
)

func infraSignedFixture(t *testing.T) (InfraUpgradeReq, string, nkeys.KeyPair) {
	t.Helper()
	account, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(account.Wipe)
	user, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(user.Wipe)
	actor, _ := user.PublicKey()
	pub, _ := account.PublicKey()
	r := InfraUpgradeReq{Request: InfraCommand{Version: 1, Purpose: "infra-request-v1", Op: "start", Component: "nats", Action: "upgrade", TxID: strings.Repeat("01", 16), ToVersion: "2.14.6", ExpectSHA256: strings.Repeat("02", 32), RemainingNS: uint64(time.Minute)}, TargetNode: "b1", Actor: actor, IssuedAt: "2026-10-13T12:00:00Z", Nonce: strings.Repeat("03", 16)}
	data, err := InfraUpgradeSigningBytes(r)
	if err != nil {
		t.Fatal(err)
	}
	sig, err := account.Sign(data)
	if err != nil {
		t.Fatal(err)
	}
	r.Sig = hex.EncodeToString(sig)
	return r, pub, account
}

// origin: hot-update S9 plan §7.1 / T46 / M23
func TestInfraWireRejectsOldReloadConfusion(t *testing.T) {
	r, pub, _ := infraSignedFixture(t)
	otherUser, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	defer otherUser.Wipe()
	otherActor, _ := otherUser.PublicKey()
	signing, err := InfraUpgradeSigningBytes(r)
	wantSigning := "tether-infra-request-v1\n" +
		`{"request":{"version":1,"purpose":"infra-request-v1","op":"start","component":"nats","action":"upgrade","tx_id":"` + strings.Repeat("01", 16) +
		`","to_version":"2.14.6","expect_sha256":"` + strings.Repeat("02", 32) +
		`","config_sha256":"","remaining_ns":60000000000},"target_node":"b1","actor":"` + r.Actor +
		`","issued_at":"2026-10-13T12:00:00Z","nonce":"` + strings.Repeat("03", 16) + `","sig":""}`
	if err != nil || string(signing) != wantSigning {
		t.Fatalf("infra signing domain/order/spelling changed: %v", err)
	}
	prefix := "_TINBOX.testidentity"
	now := time.Date(2026, 10, 13, 12, 0, 1, 0, time.UTC)
	verify := func(r InfraUpgradeReq) error {
		return VerifyInfraUpgradeRequest(r, pub, r.Actor, r.TargetNode, prefix, prefix+".response", now)
	}
	if err := verify(r); err != nil {
		t.Fatal(err)
	}
	body, err := EncodeInfraUpgradeRequest(r)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeInfraUpgradeRequest(body)
	if err != nil || !reflect.DeepEqual(decoded, r) || verify(decoded) != nil {
		t.Fatalf("canonical round trip lost signed identity: %v", err)
	}
	for _, edit := range []func(*InfraUpgradeReq){
		func(r *InfraUpgradeReq) { r.Actor = "root" },
		func(r *InfraUpgradeReq) { r.TargetNode = "*" },
		func(r *InfraUpgradeReq) { r.Nonce = "" },
		func(r *InfraUpgradeReq) { r.Nonce = strings.Repeat("00", 16) },
		func(r *InfraUpgradeReq) { r.IssuedAt = "2026-10-13T12:00:00+00:00" },
		func(r *InfraUpgradeReq) { r.IssuedAt = "not-a-time" },
		func(r *InfraUpgradeReq) { r.Sig = "bad-signature" },
	} {
		changed := r
		edit(&changed)
		if _, err := EncodeInfraUpgradeRequest(changed); err == nil {
			t.Fatal("invalid unsigned transport identity admitted")
		}
	}
	for _, change := range []struct {
		name string
		edit func(*InfraUpgradeReq)
	}{
		{"version", func(r *InfraUpgradeReq) { r.Request.Version++ }},
		{"purpose", func(r *InfraUpgradeReq) { r.Request.Purpose = "broker-image-v1" }},
		{"legacy_reload", func(r *InfraUpgradeReq) { r.Request.Op = "reload" }},
		{"component", func(r *InfraUpgradeReq) { r.Request.Component = "caddy" }},
		{"root_support", func(r *InfraUpgradeReq) { r.Request.Component = "host-support" }},
		{"action", func(r *InfraUpgradeReq) {
			r.Request.Action = "rollback"
			r.Request.ToVersion = ""
			r.Request.ExpectSHA256 = ""
		}},
		{"transaction", func(r *InfraUpgradeReq) { r.Request.TxID = strings.Repeat("04", 16) }},
		{"version_selector", func(r *InfraUpgradeReq) { r.Request.ToVersion = "2.14.5" }},
		{"binary_digest", func(r *InfraUpgradeReq) { r.Request.ExpectSHA256 = strings.Repeat("04", 32) }},
		{"config_digest", func(r *InfraUpgradeReq) { r.Request.ConfigSHA256 = strings.Repeat("04", 32) }},
		{"budget", func(r *InfraUpgradeReq) { r.Request.RemainingNS-- }},
		{"host", func(r *InfraUpgradeReq) { r.TargetNode = "b2" }},
		{"actor", func(r *InfraUpgradeReq) { r.Actor = otherActor }},
		{"time", func(r *InfraUpgradeReq) { r.IssuedAt = "2026-10-13T12:00:00.5Z" }},
		{"nonce", func(r *InfraUpgradeReq) { r.Nonce = strings.Repeat("04", 16) }},
		{"signature", func(r *InfraUpgradeReq) { r.Sig = strings.Repeat("04", 64) }},
	} {
		t.Run(change.name, func(t *testing.T) {
			changed := r
			change.edit(&changed)
			if verify(changed) == nil {
				t.Fatal("altered signed operand authorized")
			}
		})
	}
	for _, bad := range []string{
		strings.Replace(string(body), `"version":1`, `"version":1,"version":1`, 1),
		strings.Replace(string(body), `"version":1`, `"Version":1`, 1),
		strings.Replace(string(body), `"version":1`, `"version":-1`, 1),
		strings.Replace(string(body), `"version":1`, `"version":65536`, 1),
		strings.Replace(string(body), `"version":1,`, "", 1),
		strings.Replace(string(body), `"version":1`, `"version":1,"root_path":"/tmp/key"`, 1),
		strings.Replace(string(body), `"remaining_ns":60000000000`, `"remaining_ns":18446744073709551616`, 1),
		string(body) + " ", string(body) + "{}", strings.Repeat(" ", 65537),
		strings.Replace(string(body), `"target_node":"b1"`, `"target_node":"b\u0031"`, 1),
		strings.Replace(string(body), `"target_node":"b1"`, `"target_node":"b1","target_node":"b1"`, 1),
		strings.Replace(string(body), `"target_node":"b1"`, "\"target_node\":\"\xff\"", 1),
	} {
		if _, err := DecodeInfraUpgradeRequest([]byte(bad)); err == nil {
			t.Fatalf("ambiguous or unbounded request admitted: %.200s", bad)
		}
	}
	for _, tc := range []struct {
		account, actor, node, prefix, reply string
		now                                 time.Time
	}{
		{pub, "another-actor", "b1", prefix, prefix + ".response", now},
		{pub, r.Actor, "b2", prefix, prefix + ".response", now},
		{pub, r.Actor, "b1", prefix, "_TINBOX.victim.response", now},
		{pub, r.Actor, "b1", "_INBOX.test", "_INBOX.test.response", now},
		{pub, r.Actor, "b1", prefix, prefix + ".*", now},
		{pub, r.Actor, "b1", prefix, prefix + ".", now},
		{pub, r.Actor, "b1", prefix, prefix + ".response", now.Add(3 * time.Minute)},
		{pub, r.Actor, "b1", prefix, prefix + ".response", now.Add(-2 * time.Second)},
		{r.Actor, r.Actor, "b1", prefix, prefix + ".response", now},
	} {
		if VerifyInfraUpgradeRequest(r, tc.account, tc.actor, tc.node, tc.prefix, tc.reply, tc.now) == nil {
			t.Fatal("wrong authority/target/reply/time authorized")
		}
	}
	legacy := &ClusterUpgradeReq{Op: "reload", TargetNode: "b1", ToVersion: "v1.0.0", ExpectSHA256: "disk", IssuedAt: "2026-10-13T12:00:00Z"}
	if got, want := string(CanonicalUpgradeReqBytes(legacy)), "reload\nb1\n\nv1.0.0\ndisk\n\n2026-10-13T12:00:00Z"; got != want {
		t.Fatalf("legacy zero-extension bytes changed: %q", got)
	}
	if strings.HasPrefix(string(CanonicalUpgradeReqBytes(legacy)), InfraRequestSigningDomain) {
		t.Fatal("infra signing domain aliases legacy reload")
	}
	if ProtoVersion != 2 || ImageABIVersion != 1 {
		t.Fatal("infra changed an existing wire/image epoch")
	}
}

// origin: hot-update S9 plan §2.2 / T46
func TestInfraCodecBoundsAllocationBeforeTypedDecode(t *testing.T) {
	body := []byte(`{"text":"` + strings.Repeat("a", 65536) + `"}`)
	var target struct {
		Text string `json:"text"`
	}
	target.Text = "unmodified"
	if decodeInfraCanonical(body, &target) == nil || target.Text != "unmodified" {
		t.Fatal("oversized body reached the typed allocator")
	}
}

// origin: hot-update S9 plan §7.1 / T46
func TestInfraOperationSelectorsAreClosed(t *testing.T) {
	r, _, _ := infraSignedFixture(t)
	for _, edit := range []func(*InfraCommand){
		func(r *InfraCommand) { r.Version = 0 },
		func(r *InfraCommand) { r.Purpose = "infra-move-v1" },
		func(r *InfraCommand) { r.RemainingNS = 0 },
		func(r *InfraCommand) { r.RemainingNS = uint64(120*time.Second) + 1 },
		func(r *InfraCommand) { r.Op = "update-support" },
		func(r *InfraCommand) { r.Component = "host-support" },
		func(r *InfraCommand) { r.TxID = strings.Repeat("00", 16) },
		func(r *InfraCommand) { r.ExpectSHA256 = "not-binary-sha" },
		func(r *InfraCommand) { r.ToVersion = "--root-path"; r.ConfigSHA256 = "unexpected" },
		func(r *InfraCommand) { r.Action = "reload" },
		func(r *InfraCommand) {
			r.Action, r.ToVersion, r.ExpectSHA256 = "reload", "", ""
			r.ConfigSHA256 = strings.Repeat("04", 32)
		},
		func(r *InfraCommand) { r.Action = "rollback" },
		func(r *InfraCommand) { r.Action = "exec" },
		func(r *InfraCommand) { r.Component = "" },
	} {
		changed := r.Request
		edit(&changed)
		if ValidateInfraCommand(changed) == nil {
			t.Fatalf("invalid selector admitted: %+v", changed)
		}
	}
	for _, op := range []string{"status", "recover", "probe"} {
		command := InfraCommand{Version: 1, Purpose: "infra-request-v1", Op: op, RemainingNS: uint64(time.Second)}
		if op != "probe" {
			command.TxID = r.Request.TxID
		}
		if ValidateInfraCommand(command) != nil {
			t.Fatalf("valid %s rejected", op)
		}
		command.ToVersion = "unrelated-version"
		if ValidateInfraCommand(command) == nil {
			t.Fatalf("%s admitted new-plan operands", op)
		}
	}
	for _, action := range []string{"reload", "rollback"} {
		command := r.Request
		command.Component, command.Action, command.ToVersion, command.ExpectSHA256 = "caddy", action, "", ""
		if action == "reload" {
			command.ConfigSHA256 = strings.Repeat("04", 32)
		}
		if ValidateInfraCommand(command) != nil {
			t.Fatalf("valid caddy %s rejected", action)
		}
	}
}

// origin: hot-update S9 plan §2.2 / §3.3 / T46
func TestInfraPublicReportsOmitPrivateState(t *testing.T) {
	secret := "private-key-canary"
	s := c.Status{Tx: c.ID{1}, Step: 3, Phase: c.Completed, Revision: 9, Detail: secret, Before: c.Process{ExeSHA256: c.Digest{2}}, Current: c.Process{ExeSHA256: c.Digest{3}, BootID: secret, InvocationID: secret, Cgroup: secret}, Census: c.Census{LiveBusiness: 5, Processes: []c.Process{{BootID: secret}}, Sockets: []c.Socket{{Local: secret, Remote: secret}}}, Proof: c.Digest{4}}
	report := InfraStatusReport(s)
	if report.RunningSHA256 != hex.EncodeToString(s.Current.ExeSHA256[:]) || report.RunningSHA256 == hex.EncodeToString(s.Before.ExeSHA256[:]) || report.LiveBusiness != 5 || report.Phase != "completed" {
		t.Fatalf("public running evidence changed: %+v", report)
	}
	var health ClusterHealthResp
	capability := &InfraCapability{Version: 1, NATS: true}
	SetInfraClusterHealth(&health, capability, &s)
	capability.NATS = false
	if !health.InfraCapability.NATS || health.InfraUpgradeVersion != 1 {
		t.Fatal("collector retained a mutable capability pointer")
	}
	body, err := json.Marshal(health)
	if err != nil || strings.Contains(string(body), secret) || strings.Contains(string(body), "sockets") {
		t.Fatalf("private status escaped health: %v", err)
	}
	var fields map[string]any
	if err := json.Unmarshal(mustInfraJSON(t, report), &fields); err != nil || len(fields) != 7 {
		t.Fatalf("public report allowlist changed: %v %v", fields, err)
	}
	SetInfraClusterHealth(&health, nil, nil)
	body = mustInfraJSON(t, health)
	if strings.Contains(string(body), `"infra"`) || strings.Contains(string(body), `"infra_capability"`) {
		t.Fatal("old/unmeasured health inferred infra readiness")
	}
	SetInfraClusterHealth(&health, &InfraCapability{Version: 1}, nil)
	if health.InfraUpgradeVersion != 0 || health.InfraCapability != nil {
		t.Fatal("empty capability advertised an upgrade runtime")
	}
	s.Code = secret
	if InfraStatusReport(s).Code != "infra_internal" {
		t.Fatal("unknown private error escaped projection")
	}
}

func mustInfraJSON(t *testing.T, value any) []byte {
	t.Helper()
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

// origin: hot-update S9 plan §7.1 / T46 / T47
func TestInfraSubjectsAreExactAndIndependent(t *testing.T) {
	if got := SubjClusterInfra("b1"); got != "tether.v2.cluster.infra.b1.req" {
		t.Fatalf("wrong component subject: %s", got)
	}
	if got := SubjInfraMove("lab", "lab-1"); got != "tether.v2.ctrl.s.lab.node.lab-1.infra-move.req" {
		t.Fatalf("wrong move subject: %s", got)
	}
	if node, ok := ParseClusterInfra(SubjClusterInfra("b1")); !ok || node != "b1" {
		t.Fatal("target node lost")
	}
	if sid, nid, ok := ParseInfraMove(SubjInfraMove("lab", "lab-1")); !ok || sid != "lab" || nid != "lab-1" {
		t.Fatal("move scope lost")
	}
	for _, bad := range []string{"tether.v1.cluster.infra.b1.req", "tether.v2.cluster.infra.*.req", "tether.v2.cluster.infra.b1.req.forwarded", "tether.v2.cluster.infra..req", SubjCtrlClusterUpgrade("actor")} {
		if _, ok := ParseClusterInfra(bad); ok {
			t.Fatalf("non-infra subject parsed: %s", bad)
		}
	}
	for _, bad := range []string{"tether.v1.ctrl.s.lab.node.lab-1.infra-move.req", "tether.v2.ctrl.s.*.node.lab-1.infra-move.req", "tether.v2.ctrl.s.lab.node.*.infra-move.req", "tether.v2.ctrl.s.lab.node.lab-1.infra-move.req.forwarded", "tether.v2.ctrl.s.lab.node.lab-1.register.req"} {
		if _, _, ok := ParseInfraMove(bad); ok {
			t.Fatalf("non-move subject parsed: %s", bad)
		}
	}
	if len(InfraSubscriptions("b1", "lab", "lab-1")) != 2 || len(InfraSubscriptions("*", "*", "*")) != 0 {
		t.Fatal("descriptor is incomplete or admits wildcard identities")
	}
}

// origin: hot-update S9 plan §2.2 / §7.1 / T46
func TestInfraMoveCannotBypassFrozenProofCodec(t *testing.T) {
	process := c.Process{BootID: "boot", PID: 42, StartTicks: 9, InvocationID: "unit", ExeDevice: 1, ExeInode: 2, ExeSHA256: c.Digest{1}, Cgroup: "/fixture"}
	old := c.Attachment{Participant: c.ID{1}, Generation: 1, ServerID: "server", CID: 3, URL: "wss://fixture", Ingress: process, HealthGeneration: 1, Raw: c.Socket{NetNS: 1, Device: 2, Inode: 3, Cookie: 4, Local: "private-local", Remote: "private-remote", Owner: process}}
	r := InfraMoveReq{Version: 1, Purpose: "infra-move-v1", SID: "lab", NID: "lab-1", Proof: c.RouteProof{Purpose: c.RoutePrepare, Direction: c.RouteOutbound, Target: c.RouteTarget{URL: "wss://target", NodeID: "b1", Component: c.NATS, Ingress: process}, Version: 1, Tx: c.ID{2}, Participant: old.Participant, Old: old, HomeDigest: c.Digest{3}, AuthorityRevision: 1, PolicyRevision: 1, RemainingNS: uint64(time.Second), Nonce: c.ID{4}, KeyID: "key", Signature: make([]byte, 64)}}
	body, err := EncodeInfraMoveRequest(r)
	if err != nil {
		t.Fatal(err)
	}
	if decoded, err := DecodeInfraMoveRequest(body, "lab", "lab-1"); err != nil || !reflect.DeepEqual(decoded, r) {
		t.Fatalf("valid typed route lost: %v", err)
	}
	for _, scope := range [][2]string{{"other", "lab-1"}, {"lab", "lab-2"}} {
		if _, err := DecodeInfraMoveRequest(body, scope[0], scope[1]); err == nil {
			t.Fatal("move authorized for another owner")
		}
	}
	for _, edit := range []func(*InfraMoveReq){
		func(r *InfraMoveReq) { r.Purpose = "infra-request-v1" },
		func(r *InfraMoveReq) { r.Version = 2 },
		func(r *InfraMoveReq) { r.SID = "*" },
		func(r *InfraMoveReq) { r.NID = "*" },
		func(r *InfraMoveReq) { r.Proof.Signature = nil },
		func(r *InfraMoveReq) { r.Proof.Purpose = 255 },
		func(r *InfraMoveReq) { r.Proof.Old.Raw = c.Socket{} },
		func(r *InfraMoveReq) { r.Proof.Participant = c.ID{99} },
	} {
		changed := r
		edit(&changed)
		bad := mustInfraJSON(t, changed)
		if _, err := DecodeInfraMoveRequest(bad, "lab", "lab-1"); err == nil {
			t.Fatal("malformed private proof bypassed codec")
		}
	}
	for _, bad := range []string{string(body) + "{}", strings.Replace(string(body), `"BootID":"boot"`, `"BootID":"boot","BootID":"boot"`, 1), strings.Replace(string(body), `"BootID":"boot"`, `"BootID":"boot","root_path":"/root"`, 1)} {
		if _, err := DecodeInfraMoveRequest([]byte(bad), "lab", "lab-1"); err == nil {
			t.Fatal("ambiguous nested private proof admitted")
		}
	}
}

// Fuzz the hand-written subject layout, not encoding/json. A parsed subject
// must be exactly the versioned builder's output with validated scope tokens.
// origin: hot-update S9 plan §7.1 / T46 / T47
func FuzzInfraSubjectScope(f *testing.F) {
	for _, subject := range []string{"", "tether.v2.cluster.infra.b1.req", "tether.v1.cluster.infra.b1.req", "tether.v2.ctrl.s.lab.node.lab-1.infra-move.req", "tether.v1.ctrl.s.lab.node.lab-1.infra-move.req", "tether.v2.cluster.infra.*.req", "tether.v2.ctrl.s.*.node.*.infra-move.req"} {
		f.Add(subject)
	}
	f.Fuzz(func(t *testing.T, subject string) {
		if node, ok := ParseClusterInfra(subject); ok && (ValidateClusterNodeID(node) != nil || SubjClusterInfra(node) != subject) {
			t.Fatal("component parser admitted an alias or invalid scope")
		}
		if sid, nid, ok := ParseInfraMove(subject); ok && (ValidateSID(sid) != nil || ValidateNID(nid) != nil || SubjInfraMove(sid, nid) != subject) {
			t.Fatal("move parser admitted an alias or invalid scope")
		}
	})
}
