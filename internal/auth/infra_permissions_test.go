// Embedded NATS enforces the real role ACLs and direct subscriptions. Handlers
// return a marker only; this test does not certify a production infrastructure
// cut, account-callout role minting or previous-release runtime compatibility.
package auth_test

import (
	"errors"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

// origin: hot-update S9 plan §7.1 / T47 / M23
func TestInfraACLMatchesLiveSubscriptions(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseInfraSubscriptions(t)
	}
	testharness.AssertNoGoroutineLeak(t, "infra live subscriptions", beforeG)
	testharness.AssertNoFDLeak(t, "infra live subscriptions", beforeFD, 0)
}

func exerciseInfraSubscriptions(t *testing.T) {
	t.Helper()
	key, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	defer key.Wipe()
	actor, _ := key.PublicKey()
	roles := map[string]jwt.Permissions{
		"manager":     auth.PermissionsForInfraManager(actor, "b1"),
		"broker":      auth.PermissionsForInfraBroker(),
		"agent":       auth.PermissionsForInfraAgent("lab", "lab-1", actor, false),
		"other_agent": auth.PermissionsForInfraAgent("other", "lab-1", actor, false),
		"member":      auth.PermissionsForActivatedMember(actor, "lab", false),
		"unactivated": auth.PermissionsForUnactivated(actor, false),
		"root_caller": {}, // Local UID alone carries no remote management authority.
	}
	var users []*server.User
	for role, perms := range roles {
		users = append(users, &server.User{Username: role, Password: "fixture", Permissions: &server.Permissions{
			Publish:   &server.SubjectPermission{Allow: perms.Pub.Allow, Deny: perms.Pub.Deny},
			Subscribe: &server.SubjectPermission{Allow: perms.Sub.Allow, Deny: perms.Sub.Deny},
		}})
	}
	// Empty NATS allow slices mean unrestricted, unlike an absent product
	// grant. Give the root canary an explicit unreachable allow on both sides.
	for _, user := range users {
		if user.Username == "root_caller" {
			user.Permissions.Publish.Allow = []string{"fixture.noauthority"}
			user.Permissions.Subscribe.Allow = []string{"fixture.noauthority"}
		}
	}
	s, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true, Users: users})
	if err != nil {
		t.Fatal(err)
	}
	s.Start()
	defer func() { s.Shutdown(); s.WaitForShutdown() }()
	if !s.ReadyForConnections(3 * time.Second) {
		t.Fatal("embedded NATS did not become ready")
	}
	conns := map[string]*nats.Conn{}
	errorsByRole := map[string]chan error{}
	defer func() {
		for _, nc := range conns {
			nc.Close()
		}
	}()
	for role := range roles {
		errorCh := make(chan error, 32)
		errorsByRole[role] = errorCh
		nc, err := nats.Connect(s.ClientURL(), nats.UserInfo(role, "fixture"), nats.NoReconnect(), nats.CustomInboxPrefix(auth.InboxPrefixFor(actor)), nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) { errorCh <- err }))
		if err != nil {
			t.Fatal(err)
		}
		conns[role] = nc
	}
	var requests atomic.Int32
	rows := proto.InfraSubscriptions("b1", "lab", "lab-1")
	if len(rows) != 2 {
		t.Fatal("infra descriptor omitted a live subscription")
	}
	installed := map[string]bool{}
	for _, row := range rows {
		if row.Subscriber != "broker" && row.Subscriber != "agent" {
			t.Fatalf("unknown subscription owner: %s", row.Subscriber)
		}
		if !infraAllowed(roles[row.Subscriber].Sub.Allow, row.Subject) || !infraAllowed(roles[row.Publisher].Pub.Allow, row.Subject) {
			t.Fatalf("descriptor does not reconcile role ACLs: %+v", row)
		}
		nc := conns[row.Subscriber]
		sub, err := nc.Subscribe(row.Subject, func(m *nats.Msg) {
			requests.Add(1)
			if err := m.Respond([]byte("direct-owner")); err != nil {
				errorCh := errorsByRole[row.Subscriber]
				errorCh <- err
			}
		})
		if err != nil || sub == nil || sub.Queue != "" {
			t.Fatalf("direct subscription failed or became queue grouped: %v", err)
		}
		if err := nc.FlushTimeout(time.Second); err != nil {
			t.Fatal(err)
		}
		installed[row.Subject] = true
		msg, err := conns[row.Publisher].Request(row.Subject, nil, time.Second)
		if err != nil || string(msg.Data) != "direct-owner" {
			t.Fatalf("authorized owner unreachable: %v", err)
		}
	}
	// Every manager grant must have a concrete direct owner. A future grant
	// without a subscriber must turn red even if all existing requests work.
	for _, grant := range roles["manager"].Pub.Allow {
		if !installed[grant] {
			t.Fatalf("management grant has no subscriber: %s", grant)
		}
	}
	for _, role := range []string{"member", "unactivated", "agent", "other_agent", "root_caller"} {
		for _, subject := range []string{proto.SubjClusterInfra("b1"), proto.SubjInfraMove("lab", "lab-1"), "tether.v2.cluster.infra-support.b1.req"} {
			assertInfraPublishDenied(t, conns[role], errorsByRole[role], subject)
		}
	}
	for _, subject := range []string{proto.SubjClusterInfra("b2"), proto.SubjInfraMove("lab", "lab-1"), "tether.v2.cluster.infra-support.b1.req"} {
		assertInfraPublishDenied(t, conns["manager"], errorsByRole["manager"], subject)
	}
	if requests.Load() != 2 {
		t.Fatalf("unauthorized publish reached an owner: requests=%d", requests.Load())
	}
	for _, tc := range []struct{ role, subject string }{
		{"agent", proto.SubjInfraMove("other", "lab-1")},
		{"agent", proto.SubjInfraMove("lab", "lab-2")},
		{"member", proto.SubjInfraMove("lab", "lab-1")},
		{"manager", proto.SubjClusterInfra("b1")},
		{"manager", auth.InboxPrefixFor("victim") + ".>"},
		{"manager", "_INBOX.>"},
	} {
		sub, err := conns[tc.role].SubscribeSync(tc.subject)
		if err != nil {
			t.Fatal(err)
		}
		if err := conns[tc.role].FlushTimeout(time.Second); err != nil {
			t.Fatal(err)
		}
		assertInfraPermissionError(t, errorsByRole[tc.role])
		if err := sub.Unsubscribe(); err != nil {
			t.Fatal(err)
		}
	}
}

func assertInfraPublishDenied(t *testing.T, nc *nats.Conn, errorCh <-chan error, subject string) {
	t.Helper()
	if err := nc.Publish(subject, []byte("unauthorized-canary")); err != nil {
		t.Fatal(err)
	}
	if err := nc.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	assertInfraPermissionError(t, errorCh)
}

func assertInfraPermissionError(t *testing.T, errorCh <-chan error) {
	t.Helper()
	select {
	case err := <-errorCh:
		if !errors.Is(err, nats.ErrPermissionViolation) && !strings.Contains(err.Error(), "Permissions Violation") {
			t.Fatalf("expected real permissions violation, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("unauthorized operation did not receive a NATS permissions violation")
	}
}

// origin: hot-update S9 plan §7.1 / T47
func TestInfraPermissionTemplatesRejectMalformedIdentity(t *testing.T) {
	for _, p := range []jwt.Permissions{auth.PermissionsForInfraManager("root", "b1"), auth.PermissionsForInfraManager("UABCDEFGHIJKLMNOPQRSTUVWXYZ234567ABCDEFGHIJKLMNOPQRSTUV", "*"), auth.PermissionsForInfraAgent("*", "lab-1", "UABCDEFGHIJKLMNOPQRSTUVWXYZ234567ABCDEFGHIJKLMNOPQRSTUV", false), auth.PermissionsForInfraAgent("lab", "*", "UABCDEFGHIJKLMNOPQRSTUVWXYZ234567ABCDEFGHIJKLMNOPQRSTUV", false)} {
		if len(p.Pub.Allow) != 0 || len(p.Sub.Allow) != 0 {
			t.Fatal("malformed identity received infrastructure authority")
		}
		if len(p.Pub.Deny) != 1 || p.Pub.Deny[0] != ">" || len(p.Sub.Deny) != 1 || p.Sub.Deny[0] != ">" {
			t.Fatal("empty allow lists became unrestricted NATS authority")
		}
	}
	// Ordinary permissions retain their prior actor/session and legacy inbox
	// semantics, including S7 settlement and S8 transaction routes.
	if infraAllowed(auth.PermissionsForActivatedMember("UABCDEFGHIJKLMNOPQRSTUVWXYZ234567ABCDEFGHIJKLMNOPQRSTUV", "lab", false).Pub.Allow, proto.SubjClusterInfra("b1")) || infraAllowed(auth.PermissionsForAgent("lab", "lab-1", "UABCDEFGHIJKLMNOPQRSTUVWXYZ234567ABCDEFGHIJKLMNOPQRSTUV", false).Sub.Allow, proto.SubjInfraMove("lab", "lab-1")) {
		t.Fatal("an ordinary connection acquired an uninstalled infra grant")
	}
}

func infraAllowed(patterns []string, subject string) bool {
	for _, pattern := range patterns {
		parts, tokens := strings.Split(pattern, "."), strings.Split(subject, ".")
		match := true
		for i, token := range parts {
			if token == ">" && len(tokens) > i {
				return true
			}
			if i >= len(tokens) || (token != "*" && token != tokens[i]) {
				match = false
				break
			}
		}
		if match && len(parts) == len(tokens) {
			return true
		}
	}
	return false
}
