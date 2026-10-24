package natsconf

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

// origin: hot-update S9 plan §3.3 / T28
func TestSystemPrincipalStepdownIsLeastPrivilege(t *testing.T) {
	t.Run("config authority", checkSystemPrincipalConfig)
	t.Run("real meta stepdown", exerciseSystemStepdown)
}

func checkSystemPrincipalConfig(t *testing.T) {
	dir := t.TempDir()
	_, readPub := systemTestKey(t)
	_, stepPub := systemTestKey(t)
	principals := SystemPrincipals{ReadNkey: readPub, StepNkey: stepPub}
	block, err := SystemPrincipalBlock(principals)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "nats.conf")
	// Account names retain case, while directive names retain the server's
	// case-insensitive parsing behavior. Explicit default selection is preserved.
	mixed := strings.NewReplacer("accounts", "ACCOUNTS", "users:", "Users:", "permissions", "Permissions", "publish", "Publish").Replace(block)
	systemTestWrite(t, path, mixed+"SYSTEM_ACCOUNT: \"$SYS\"\n")
	own, err := Preflight(path)
	if err != nil {
		t.Fatal(err)
	}
	got, err := SystemPrincipalIdentity(own)
	if err != nil || got != principals {
		t.Fatalf("managed SYS block did not roundtrip: %+v %v", got, err)
	}
	if table := OwnershipTable(own); !strings.Contains(table, "accounts") || !strings.Contains(table, "system_account") {
		t.Fatal("owned SYS directives are absent from the ownership table")
	}
	preserved, err := preserveSystemPrincipals(own, "port: -1\n")
	if err != nil || !strings.HasSuffix(preserved, "system_account: \"$SYS\"\n") {
		t.Fatalf("explicit system account option was erased: %v", err)
	}
	systemTestWrite(t, path, preserved)
	opts, err := natsserver.ProcessConfigFile(path)
	if err != nil || opts.SystemAccount != "$SYS" || len(opts.Nkeys) != 2 {
		t.Fatalf("real config parser disagrees with the managed identity: %v", err)
	}
	descriptors, err := SystemPrincipalDescriptors(principals)
	if err != nil {
		t.Fatal(err)
	}
	// Independent oracle: broad $SYS/$JS permissions and a shared reply inbox are
	// never legal, even if both the parser and the renderer drift together.
	wantRead := []string{"$SYS.REQ.SERVER.*.JSZ", "$SYS.REQ.SERVER.*.VARZ", "$SYS.REQ.ACCOUNT.*.INFO"}
	for i, descriptor := range descriptors {
		wantPub := append([]string(nil), wantRead...)
		key, file := readPub, "infra-sys-read.nk"
		if i == 1 {
			wantPub = append(wantPub, "$JS.API.META.LEADER.STEPDOWN")
			key, file = stepPub, "infra-sys-step.nk"
		}
		wantPrefix := "_INBOX.tether.infra.sys." + key
		if descriptor.Nkey != key || descriptor.SeedFile != file || descriptor.ReplyPrefix != wantPrefix ||
			!reflect.DeepEqual(descriptor.Publish, wantPub) || !reflect.DeepEqual(descriptor.Subscribe, []string{wantPrefix + ".>"}) {
			t.Fatalf("managed descriptor has excess or missing authority: %+v", descriptor)
		}
		var user *natsserver.NkeyUser
		for _, candidate := range opts.Nkeys {
			if candidate.Nkey == key {
				user = candidate
			}
		}
		if user == nil || user.Account == nil || user.Account.Name != "$SYS" || user.Permissions == nil ||
			!reflect.DeepEqual(user.Permissions.Publish.Allow, wantPub) ||
			!reflect.DeepEqual(user.Permissions.Subscribe.Allow, []string{wantPrefix + ".>"}) ||
			len(user.Permissions.Publish.Deny) != 0 || len(user.Permissions.Subscribe.Deny) != 0 || user.Permissions.Response != nil {
			t.Fatal("server did not assign the exact managed permissions inside SYS")
		}
	}
	descriptors[0].Publish[0] = ">"
	descriptors[0].Subscribe[0] = ">"
	again, err := SystemPrincipalDescriptors(principals)
	if err != nil || again[0].Publish[0] != wantRead[0] || again[0].Subscribe[0] == ">" {
		t.Fatal("descriptor mutation widened future renders")
	}
	negative := map[string]string{
		"global impersonates SYS": strings.Replace(block, "$SYS {", "$G {", 1),
		"different case account":  strings.Replace(block, "$SYS {", "$sys {", 1),
		"foreign account":         strings.Replace(block, "accounts {", "accounts { OTHER {}", 1),
		"changed SystemAccount":   block + "system_account: OTHER\n",
		"disabled SystemAccount":  block + "no_system_account: true\n",
		"unknown system option":   strings.Replace(block, "$SYS {", "$SYS { jetstream: true", 1),
		"missing users":           "accounts { $SYS {} }\n",
		"accounts array":          "accounts: [SYS]\n",
		"non-map system user":     "accounts { $SYS { users: [1,2] } }\n",
		"users wrong shape":       "accounts { $SYS { users: {} } }\n",
		"partial managed pair":    strings.Replace(block, "users: [", "users: [] #", 1),
		"duplicate read role":     strings.Replace(block, fmt.Sprintf("            %q\n", SystemStepdownSubject), "", 1),
		"duplicate nkey":          strings.ReplaceAll(block, stepPub, readPub),
		"invalid nkey":            strings.ReplaceAll(block, readPub, "not-a-public-user-key"),
		"seed in config":          strings.Replace(block, "permissions {", "seed: SECRET_CANARY permissions {", 1),
		"broad SYS":               strings.Replace(block, "$SYS.REQ.SERVER.*.JSZ", "$SYS.>", 1),
		"broad JS":                strings.Replace(block, "$JS.API.META.LEADER.STEPDOWN", "$JS.API.>", 1),
		"purge granted":           strings.Replace(block, "$SYS.REQ.SERVER.*.JSZ", "$JS.API.STREAM.PURGE.*", 1),
		"foreign inbox":           strings.Replace(block, "_INBOX.tether.infra.sys."+readPub+".>", "_INBOX.>", 1),
		"wrong user inbox":        strings.Replace(block, "_INBOX.tether.infra.sys."+readPub+".>", "_INBOX.tether.infra.sys."+stepPub+".>", 1),
		"extra response grant":    strings.Replace(block, "permissions {", "permissions { allow_responses: true", 1),
		"global identity reuse":   block + fmt.Sprintf("authorization { users: [{ nkey: %q }] }\n", readPub),
	}
	for name, text := range negative {
		t.Run(name, func(t *testing.T) {
			systemTestWrite(t, path, text)
			if _, err := Preflight(path); err == nil {
				t.Fatal("unsupported SYS config was accepted")
			} else if strings.Contains(err.Error(), "SECRET_CANARY") || strings.Contains(err.Error(), readPub) || strings.Contains(err.Error(), stepPub) {
				t.Fatal("principal error exposed credential material")
			}
		})
	}
	// The conf lexer can reject scalar arrays before the ownership boundary. Exercise
	// the public ownership consumer too, so that earlier parser refusal cannot hide
	// a missing guard against malformed decoded users.
	malformed := &Ownership{Parsed: map[string]any{"accounts": map[string]any{
		"$SYS": map[string]any{"users": []any{int64(1), int64(2)}},
	}}}
	if _, err := WithSystemPrincipals(malformed, principals); err == nil {
		t.Fatal("provisioning accepted scalar users in the decoded ownership view")
	}
	for name, invalid := range map[string]SystemPrincipals{"missing read": {StepNkey: stepPub}, "missing step": {ReadNkey: readPub}, "same user": {ReadNkey: readPub, StepNkey: readPub}, "invalid user": {ReadNkey: "SECRET_CANARY", StepNkey: stepPub}} {
		t.Run(name, func(t *testing.T) {
			if _, err := SystemPrincipalBlock(invalid); err == nil || strings.Contains(err.Error(), "SECRET_CANARY") {
				t.Fatal("invalid principals rendered or appeared in the error")
			}
		})
	}
	if _, err := SystemPrincipalIdentity(nil); err == nil {
		t.Fatal("missing ownership was accepted")
	}
	if _, err := preserveSystemPrincipals(own, fmt.Sprintf("authorization { users: [{ nkey: %q }] }", readPub)); err == nil {
		t.Fatal("generated global identity collision was accepted")
	}
	_, newRead := systemTestKey(t)
	_, newStep := systemTestKey(t)
	rotated, err := WithSystemPrincipals(own, SystemPrincipals{ReadNkey: newRead, StepNkey: newStep})
	if err != nil {
		t.Fatal(err)
	}
	original, err := SystemPrincipalIdentity(own)
	if err != nil || original != principals {
		t.Fatal("explicit rotation mutated the original config view")
	}
	identity, err := SystemPrincipalIdentity(rotated)
	if err != nil || identity.ReadNkey != newRead || identity.StepNkey != newStep {
		t.Fatal("explicit rotation did not replace both public identities")
	}
}

func exerciseSystemStepdown(t *testing.T) {
	dir := t.TempDir()
	brokerKey, brokerPub := systemTestKey(t)
	memberKey, memberPub := systemTestKey(t)
	agentKey, agentPub := systemTestKey(t)
	readKey, readPub := systemTestKey(t)
	stepKey, stepPub := systemTestKey(t)
	account, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatal(err)
	}
	issuer, err := account.PublicKey()
	account.Wipe()
	if err != nil {
		t.Fatal(err)
	}
	ca, cert, key := writeClusterCertPEMs(t, dir)
	var ports []net.Listener
	var peers []Broker
	for i := 0; i < 3; i++ {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = listener.Close() })
		ports = append(ports, listener)
		port := listener.Addr().(*net.TCPAddr).Port
		peers = append(peers, Broker{ServerName: fmt.Sprintf("principal-%d", i), NkeyPub: brokerPub, RouteURL: fmt.Sprintf("nats://localhost:%d", port)})
	}
	principals := SystemPrincipals{ReadNkey: readPub, StepNkey: stepPub}
	var servers []*natsserver.Server
	for i, peer := range peers {
		cfg := Config{Local: peer, Peers: peers, AccountIssuer: issuer, JSStoreDir: filepath.Join(dir, peer.ServerName),
			ClientListen: "127.0.0.1:-1", ClusterName: "principal-cluster", ClusterListen: ports[i].Addr().String(), CAFile: ca, CertFile: cert, KeyFile: key}
		own, err := WithSystemPrincipals(&Ownership{Parsed: map[string]any{}}, principals)
		if err != nil {
			t.Fatal(err)
		}
		text, err := BuildMergedConf(own, cfg)
		if err != nil {
			t.Fatal(err)
		}
		// This L1 test isolates stock SYS authorization. The original config-mode
		// AuthCallout live migration remains a separate mandatory T27 proof, which
		// currently fails; omitting it here does not establish that migration.
		start := strings.Index(text, "  auth_callout {\n")
		end := strings.Index(text[start:], "  }\n") + start + len("  }\n")
		text = text[:start] + text[end:]
		// Ordinary identities use the product's normal least-privilege ACL builders;
		// none gets a system account or a special stepdown exemption in this fixture.
		var ordinary strings.Builder
		for _, identity := range []struct {
			key      string
			pub, sub []string
		}{
			{memberPub, auth.PermissionsForActivatedMember("member", "principal", true).Pub.Allow, auth.PermissionsForActivatedMember("member", "principal", true).Sub.Allow},
			{agentPub, auth.PermissionsForAgent("principal", "node", "member", true).Pub.Allow, auth.PermissionsForAgent("principal", "node", "member", true).Sub.Allow},
		} {
			fmt.Fprintf(&ordinary, "    { nkey: %q\n      permissions {\n", identity.key)
			writeAllowBlock(&ordinary, "publish", identity.pub)
			writeAllowBlock(&ordinary, "subscribe", identity.sub)
			ordinary.WriteString("    } }\n")
		}
		text = strings.Replace(text, "authorization {\n  users: [\n", "authorization {\n  users: [\n"+ordinary.String(), 1)
		path := filepath.Join(dir, peer.ServerName+".conf")
		systemTestWrite(t, path, text)
		if err := ports[i].Close(); err != nil {
			t.Fatal(err)
		}
		servers = append(servers, startSystemTestServer(t, path))
	}
	var leader, successor *natsserver.Server
	if !testharness.WaitFor(t, 15*time.Second, 10*time.Millisecond, func() bool {
		var leaders []*natsserver.Server
		for _, ns := range servers {
			if !ns.JetStreamIsCurrent() {
				return false
			}
			if ns.JetStreamIsLeader() {
				leaders = append(leaders, ns)
			}
		}
		if len(leaders) != 1 {
			return false
		}
		leader = leaders[0]
		return true
	}) {
		t.Fatal("three-server embedded meta group never became current")
	}
	for _, ns := range servers {
		if ns != leader {
			successor = ns
			break
		}
	}
	descriptors, err := SystemPrincipalDescriptors(principals)
	if err != nil {
		t.Fatal(err)
	}
	readErrors, stepErrors := make(chan error, 32), make(chan error, 32)
	reader := systemTestConnect(t, leader, readKey, descriptors[0].ReplyPrefix, func(_ *nats.Conn, _ *nats.Subscription, err error) { readErrors <- err })
	stepper := systemTestConnect(t, leader, stepKey, descriptors[1].ReplyPrefix, func(_ *nats.Conn, _ *nats.Subscription, err error) { stepErrors <- err })
	global := systemTestConnect(t, leader, brokerKey, "", nil)
	cid, err := global.GetClientID()
	if err != nil {
		t.Fatal(err)
	}
	js, err := global.JetStream(nats.MaxWait(2 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	subject := proto.SubjectPrefix + ".s.principal.audit.event"
	if _, err := js.AddStream(&nats.StreamConfig{Name: "ORIGINAL", Subjects: []string{subject}, Storage: nats.FileStorage, Replicas: 3}); err != nil {
		t.Fatal(err)
	}
	if _, err := js.Publish(subject, []byte("durable")); err != nil {
		t.Fatal(err)
	}
	assertSystemReadRequests(t, reader, leader)
	assertSystemReadRequests(t, stepper, leader)
	for _, subject := range []string{SystemStepdownSubject, "$JS.API.STREAM.PURGE.ORIGINAL", "$JS.API.STREAM.DELETE.ORIGINAL", "$SYS.REQ.SERVER." + leader.ID() + ".RELOAD"} {
		assertSystemPublishDenied(t, reader, readErrors, subject)
	}
	for _, subject := range []string{"$JS.API.STREAM.PURGE.ORIGINAL", "$JS.API.STREAM.DELETE.ORIGINAL", "$JS.API.ACCOUNT.PURGE.$G", "$SYS.REQ.SERVER." + leader.ID() + ".RELOAD"} {
		assertSystemPublishDenied(t, stepper, stepErrors, subject)
	}
	assertSystemSubscribeDenied(t, reader, readErrors, descriptors[1].ReplyPrefix+".foreign")
	assertSystemSubscribeDenied(t, stepper, stepErrors, descriptors[0].ReplyPrefix+".foreign")
	assertSystemSubscribeDenied(t, reader, readErrors, "_INBOX.>")
	// The real broker already has $JS.API.> yet remains in $G. Subject permission
	// alone cannot authorize meta stepdown; this request reaches the server and is refused.
	if _, err := global.Request(SystemStepdownSubject, nil, 150*time.Millisecond); !errors.Is(err, nats.ErrTimeout) && !errors.Is(err, nats.ErrNoResponders) {
		t.Fatalf("global-account user was not refused meta stepdown: %v", err)
	}
	for role, ordinaryKey := range map[string]nkeys.KeyPair{"member": memberKey, "agent": agentKey} {
		t.Run(role+" refuses stepdown", func(t *testing.T) {
			violations := make(chan error, 1)
			ordinary := systemTestConnect(t, leader, ordinaryKey, "", func(_ *nats.Conn, _ *nats.Subscription, err error) { violations <- err })
			assertSystemPublishDenied(t, ordinary, violations, SystemStepdownSubject)
		})
	}
	if !leader.JetStreamIsLeader() {
		t.Fatal("refused global/read actions changed the real meta leader")
	}
	body, err := json.Marshal(natsserver.JSApiLeaderStepdownRequest{Placement: &natsserver.Placement{Preferred: successor.Name()}})
	if err != nil {
		t.Fatal(err)
	}
	response, err := stepper.Request(SystemStepdownSubject, body, 2*time.Second)
	if err != nil {
		t.Fatalf("narrow SYS action identity cannot step down the real meta leader: %v", err)
	}
	var receipt natsserver.JSApiLeaderStepDownResponse
	if err := json.Unmarshal(response.Data, &receipt); err != nil || !receipt.Success || receipt.Error != nil {
		t.Fatalf("real meta stepdown was not accepted: %+v %v", receipt, err)
	}
	if !testharness.WaitFor(t, 10*time.Second, 10*time.Millisecond, func() bool { return successor.JetStreamIsLeader() && !leader.JetStreamIsLeader() }) {
		t.Fatal("successful stepdown receipt did not install the requested successor")
	}
	info, err := js.StreamInfo("ORIGINAL")
	if err != nil || info.State.Msgs != 1 {
		t.Fatalf("denied mutations or stepdown damaged the original stream: %+v %v", info, err)
	}
	message, err := js.GetMsg("ORIGINAL", 1)
	if err != nil || !bytes.Equal(message.Data, []byte("durable")) {
		t.Fatalf("original global stream ACK prefix changed: %v", err)
	}
	afterCID, err := global.GetClientID()
	if err != nil || afterCID != cid || global.Stats().Reconnects != 0 {
		t.Fatal("stepdown replaced the original global business connection")
	}
}

func assertSystemPublishDenied(t *testing.T, nc *nats.Conn, violations <-chan error, subject string) {
	t.Helper()
	if err := nc.Publish(subject, []byte("{}")); err != nil {
		t.Fatal(err)
	}
	if err := nc.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	assertSystemViolation(t, violations, "Publish", subject)
}

func assertSystemSubscribeDenied(t *testing.T, nc *nats.Conn, violations <-chan error, subject string) {
	t.Helper()
	sub, err := nc.SubscribeSync(subject)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	assertSystemViolation(t, violations, "Subscription", subject)
}

func assertSystemViolation(t *testing.T, violations <-chan error, operation, subject string) {
	t.Helper()
	select {
	case err := <-violations:
		if !errors.Is(err, nats.ErrPermissionViolation) || !strings.Contains(err.Error(), operation) || !strings.Contains(err.Error(), subject) {
			t.Fatalf("denial came from the wrong permission guard: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatalf("excess %s authority was accepted for %s", operation, subject)
	}
}

// The callout admits SYS identities only from the managed block of the config
// file itself: none before provisioning, exactly the descriptors after it, and
// an error (hence no SYS admission at all) for a broadened or unreadable file.
// origin: hot-update S9 plan §3.3 / P12 / T27 — normal-queue SYS admission
func TestLoadedSystemPrincipalDescriptorsFollowTheConfigFile(t *testing.T) {
	dir := t.TempDir()
	_, readPub := systemTestKey(t)
	_, stepPub := systemTestKey(t)
	principals := SystemPrincipals{ReadNkey: readPub, StepNkey: stepPub}
	block, err := SystemPrincipalBlock(principals)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "nats.conf")
	systemTestWrite(t, path, "port: -1\n")
	if got, err := LoadedSystemPrincipalDescriptors(path); err != nil || got != nil {
		t.Fatalf("a config without the managed block yielded SYS identities: %+v %v", got, err)
	}
	systemTestWrite(t, path, "port: -1\n"+block)
	want, err := SystemPrincipalDescriptors(principals)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := LoadedSystemPrincipalDescriptors(path); err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("loaded SYS descriptors differ from the managed authority: %+v %v", got, err)
	}
	for name, text := range map[string]string{
		"broadened publish": strings.Replace(block, "$SYS.REQ.SERVER.*.JSZ", "$SYS.>", 1),
		"shared reply root": strings.Replace(block, "_INBOX.tether.infra.sys."+readPub+".>", "_INBOX.tether.infra.sys.>", 1),
	} {
		systemTestWrite(t, path, "port: -1\n"+text)
		if got, err := LoadedSystemPrincipalDescriptors(path); err == nil || got != nil {
			t.Fatalf("%s was admitted as SYS authority: %+v", name, got)
		}
	}
	if got, err := LoadedSystemPrincipalDescriptors(filepath.Join(dir, "missing.conf")); err == nil || got != nil {
		t.Fatalf("an unreadable config yielded SYS identities: %+v", got)
	}
}
