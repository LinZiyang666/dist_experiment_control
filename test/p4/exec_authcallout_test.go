// exec_authcallout_test.go exercises the P4 control plane END-TO-END with
// the P3 auth_callout boundary enabled — the gap the P4 review's F1
// flagged. The other file in this package (exec_e2e_test.go) keeps using
// the anonymous harness on purpose: it tests app-layer logic in isolation
// without the JWT scaffolding noise.
//
// What this file proves additionally:
//   - the broker auth_callout grants ctl the activated-member role and
//     grants the agent the "tether-agent:<sid>:<nid>" role only after a
//     PIN-bootstrap binds the agent fp into agent_provisioning;
//   - exec / ps round-trips work through the secure stack;
//   - an agent without the binding (no --pin on first connect) is denied
//     at CONNECT time, surfacing as a fail-fast error from agent.Run.
//
// The architecture-mandated "ctl directly publishes .req.forwarded → NATS
// permission denied" case is already covered in test/p3 (see
// permissions_e2e_test.go); we don't re-implement it here.
package p4_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agent"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/nats-io/jwt/v2"
	natsserver "github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

// ----- harness: NATS with auth_callout + broker wired in --------------

func startAuthNATS(t *testing.T) (url string, brokerSeed, accountSeed []byte) {
	t.Helper()

	brokerKp, _ := nkeys.CreateUser()
	brokerPub, _ := brokerKp.PublicKey()
	rawBroker, _ := brokerKp.Seed()
	brokerSeed = append([]byte(nil), rawBroker...)
	brokerKp.Wipe()

	accountKp, _ := nkeys.CreateAccount()
	accountPub, _ := accountKp.PublicKey()
	rawAcct, _ := accountKp.Seed()
	accountSeed = append([]byte(nil), rawAcct...)
	accountKp.Wipe()

	opts := natstest.DefaultTestOptions
	opts.Port = -1
	opts.Nkeys = []*natsserver.NkeyUser{
		{
			Nkey:        brokerPub,
			Permissions: jwtToServerPerms(auth.PermissionsForBroker()),
		},
	}
	opts.AuthCallout = &natsserver.AuthCallout{
		Issuer:    accountPub,
		Account:   "$G",
		AuthUsers: []string{brokerPub},
	}

	ns := natstest.RunServer(&opts)
	t.Cleanup(func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	})
	if !ns.ReadyForConnections(2 * time.Second) {
		t.Fatal("embedded nats-server (auth_callout) not ready")
	}
	return ns.ClientURL(), brokerSeed, accountSeed
}

func startBrokerWithAuth(t *testing.T, url string, db *sql.DB, brokerSeed, accountSeed []byte) func() {
	t.Helper()

	b, err := broker.New(broker.Config{
		NATSURL:           url,
		DB:                db,
		Logger:            silentLog(),
		ReconcileInterval: 50 * time.Millisecond,
		StaleAfter:        300 * time.Millisecond,
		OfflineAfter:      900 * time.Millisecond,
		AuthCallout: &broker.AuthCalloutConfig{
			BrokerNkeySeed: brokerSeed,
			AccountSeed:    accountSeed,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()

	waitAuthCalloutReady(t, url)

	return func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("broker did not exit on cancel")
		}
	}
}

func waitAuthCalloutReady(t *testing.T, url string) {
	t.Helper()
	probeKp, _ := nkeys.CreateUser()
	probePub, _ := probeKp.PublicKey()
	rawSeed, _ := probeKp.Seed()
	seed := append([]byte(nil), rawSeed...)
	probeKp.Wipe()

	sigCB := func(nonce []byte) ([]byte, error) {
		kp, err := nkeys.FromSeed(seed)
		if err != nil {
			return nil, err
		}
		defer kp.Wipe()
		return kp.Sign(nonce)
	}

	// Per-attempt timeout was 200ms, which on a slow CI runner can
	// be shorter than the auth_callout round-trip itself; tighten
	// the outer deadline but loosen the per-attempt timeout to 1s
	// so we don't burn deadline on connect timeouts faster than
	// the subscribe-then-handle path can serve them (audit shard 05
	// F10).
	deadline := time.Now().Add(5 * time.Second)
	for {
		nc, err := nats.Connect(url,
			nats.Nkey(probePub, sigCB),
			nats.Name(cli.CtlNameUnactivated),
			nats.Timeout(1*time.Second),
		)
		if err == nil {
			nc.Close()
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("broker auth_callout not ready: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func jwtToServerPerms(p jwt.Permissions) *natsserver.Permissions {
	return &natsserver.Permissions{
		Publish: &natsserver.SubjectPermission{
			Allow: p.Pub.Allow,
			Deny:  p.Pub.Deny,
		},
		Subscribe: &natsserver.SubjectPermission{
			Allow: p.Sub.Allow,
			Deny:  p.Sub.Deny,
		},
	}
}

// ----- helpers ----------------------------------------------------------

// freshCtlIdentity creates a per-test ctl nkey identity (separate from the
// agent identity).
func freshCtlIdentity(t *testing.T) *cli.Identity {
	t.Helper()
	id, err := cli.EnsureIdentity(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// freshAgentIdentity creates a per-test agent nkey identity at the K.1
// path layout under a temp HOME.
func freshAgentIdentity(t *testing.T, sid string) (*cli.Identity, string) {
	t.Helper()
	home := t.TempDir()
	id, err := cli.EnsureAgentIdentity(home, sid)
	if err != nil {
		t.Fatal(err)
	}
	return id, home
}

// seedSessionWithPIN creates an ACTIVE sessions row whose pin_hash matches
// the supplied raw PIN. Owner is set to ownerFP and added as a member, so
// a ctl signed by ownerFP can immediately use the activated-member role.
func seedSessionWithPIN(t *testing.T, db *sql.DB, sid, ownerFP, pin string) {
	t.Helper()
	hash, err := auth.HashPIN(pin)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Create(db, sid, sid, ownerFP, hash, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
}

// startAgentSecure boots an in-process agent that connects via nkey +
// auth_callout. The first call for a given (sid, nid) supplies pin so the
// broker provisions the agent fp.
func startAgentSecure(t *testing.T, url, sid, nid, pin string, id *cli.Identity) func() {
	t.Helper()
	a, err := agent.New(agent.Config{
		NATSURL:           url,
		SID:               sid,
		NID:               nid,
		Identity:          id,
		PIN:               pin,
		Logger:            silentLog(),
		HeartbeatInterval: 100 * time.Millisecond,
		RegisterTimeout:   2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	// Give register + subscribe a moment to settle.
	time.Sleep(300 * time.Millisecond)

	return func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("agent did not exit on cancel")
		}
	}
}

// ----- tests ------------------------------------------------------------

// Happy path: secure stack runs an exec end-to-end. Validates all of:
//   - agent CONNECT through auth_callout (PIN-bootstrap path),
//   - ctl CONNECT through auth_callout (activated-member role),
//   - broker forwards exec.req → exec.req.forwarded with preserved Reply,
//   - agent streams chunks back to ctl's _INBOX.
func TestExecHappyPathThroughAuthCallout(t *testing.T) {
	url, brokerSeed, accountSeed := startAuthNATS(t)
	db := openDB(t)

	ctlID := freshCtlIdentity(t)
	seedSessionWithPIN(t, db, "lab", ctlID.Fingerprint, "test-pin")

	defer startBrokerWithAuth(t, url, db, brokerSeed, accountSeed)()

	agentID, _ := freshAgentIdentity(t, "lab")
	defer startAgentSecure(t, url, "lab", "lab-1", "test-pin", agentID)()

	nc, err := cli.ConnectNATSWithNkey(url, ctlID,
		nats.Name(cli.CtlNameForSession("lab")))
	if err != nil {
		t.Fatalf("ctl connect: %v", err)
	}
	defer nc.Close()

	res := runExec(t, nc, "lab", ctlID.PublicKey, "lab-1", []string{"echo", "secure-hello"})
	if res.Err != "" {
		t.Fatalf("exec error: %s", res.Err)
	}
	if res.ExitCode != 0 {
		t.Errorf("exit code: got %d want 0", res.ExitCode)
	}
	if res.PID == "" {
		t.Error("started chunk had no PID")
	}
}

// Re-connect path: once a (sid, nid) is bound, a fresh agent process can
// CONNECT without supplying --pin. The binding is durable across agent
// restarts as long as the same nkey is presented.
func TestAgentReconnectsWithoutPINAfterBootstrap(t *testing.T) {
	url, brokerSeed, accountSeed := startAuthNATS(t)
	db := openDB(t)

	ctlID := freshCtlIdentity(t)
	seedSessionWithPIN(t, db, "lab", ctlID.Fingerprint, "test-pin")

	defer startBrokerWithAuth(t, url, db, brokerSeed, accountSeed)()

	agentID, _ := freshAgentIdentity(t, "lab")
	// Bootstrap: PIN supplied.
	stop1 := startAgentSecure(t, url, "lab", "lab-1", "test-pin", agentID)
	stop1()

	// Re-connect: no PIN, same identity. Must succeed.
	stop2 := startAgentSecure(t, url, "lab", "lab-1", "" /* no PIN */, agentID)
	defer stop2()

	nc, err := cli.ConnectNATSWithNkey(url, ctlID,
		nats.Name(cli.CtlNameForSession("lab")))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	res := runExec(t, nc, "lab", ctlID.PublicKey, "lab-1", []string{"echo", "rebind"})
	if res.Err != "" || res.ExitCode != 0 {
		t.Fatalf("exec after pinless re-connect failed: %+v", res)
	}
}

// Negative: a never-provisioned (sid, nid) without --pin fails at CONNECT
// time. The error must surface fast-fail through agent.Run rather than
// silently flapping forever.
func TestAgentWithoutPINIsRejectedAtConnect(t *testing.T) {
	url, brokerSeed, accountSeed := startAuthNATS(t)
	db := openDB(t)

	ctlID := freshCtlIdentity(t)
	seedSessionWithPIN(t, db, "lab", ctlID.Fingerprint, "test-pin")

	defer startBrokerWithAuth(t, url, db, brokerSeed, accountSeed)()

	agentID, _ := freshAgentIdentity(t, "lab")
	a, err := agent.New(agent.Config{
		NATSURL:           url,
		SID:               "lab",
		NID:               "lab-99",
		Identity:          agentID,
		PIN:               "",
		Logger:            silentLog(),
		HeartbeatInterval: 100 * time.Millisecond,
		RegisterTimeout:   2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err = a.Run(ctx)
	if err == nil {
		t.Fatal("agent.Run must fail-fast when nid is unprovisioned and no PIN supplied")
	}
}

// Sanity: the test file compiles against the same imports the e2e file
// uses. Refers to runExec/openDB/silentLog from exec_e2e_test.go to make
// the dependency explicit (Go's test build links everything in the
// directory together so this also keeps the go vet happy if those names
// ever drift).
var (
	_ = json.Marshal
	_ = os.Getenv
	_ = proto.SubjCmdBy
)
