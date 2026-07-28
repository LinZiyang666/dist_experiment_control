package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/clusterroster"
	"github.com/LinZiyang666/tether/internal/proto"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

func startCLIExternalReviewNATS(t *testing.T) string {
	t.Helper()
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	ns := natstest.RunServer(&opts)
	t.Cleanup(func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	})
	if !ns.ReadyForConnections(2 * time.Second) {
		t.Fatal("embedded nats-server not ready")
	}
	return ns.ClientURL()
}

func cliExternalAccount(t *testing.T) ([]byte, string) {
	t.Helper()
	kp, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatal(err)
	}
	seed, err := kp.Seed()
	if err != nil {
		t.Fatal(err)
	}
	pub, err := kp.PublicKey()
	if err != nil {
		t.Fatal(err)
	}
	return seed, pub
}

func cliExternalManifestServer(t *testing.T, seed []byte, pub string, endpoint string) *httptest.Server {
	t.Helper()
	now := time.Now().UTC()
	r, err := clusterroster.Build(seed, pub, 1,
		[]proto.RosterBroker{{NodeID: "b1", PublicHost: "b1.example.com", Phase: proto.RosterPhaseVoter}},
		now.Format(time.RFC3339Nano), now.Add(time.Hour).Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}
	s, err := clusterroster.BuildSeeds(seed, pub, 1, []string{endpoint}, "",
		now.Format(time.RFC3339Nano), now.Add(time.Hour).Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(proto.ClusterManifest{SchemaVersion: proto.ClusterManifestSchemaVersion, Roster: r, Seeds: s})
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(body)
	}))
}

func seedPinnedFailoverCache(t *testing.T, home, floor, survivor string) {
	t.Helper()
	_, pub := cliExternalAccount(t)
	if err := cli.WriteDefaultBrokerURL(home, floor); err != nil {
		t.Fatal(err)
	}
	if err := cli.WriteClusterEndpoints(home, &cli.ClusterEndpoints{
		PinAccountPub: pub,
		FloorURL:      floor,
		InviteSeeds:   []string{survivor},
	}); err != nil {
		t.Fatal(err)
	}
}

func TestCLIExternalReviewBootstrapOnlyPinWarmsManifest(t *testing.T) {
	t.Setenv(cli.DevNoAuthEnv, "1")
	t.Setenv(cli.DefaultBrokerURLEnv, "")
	t.Setenv(cli.NoDiscoverEnv, "")

	home := t.TempDir()
	base := startCLIExternalReviewNATS(t)
	seed, pub := cliExternalAccount(t)
	manifest := cliExternalManifestServer(t, seed, pub, "nats://survivor.example:4222")
	defer manifest.Close()
	if err := cli.WriteClusterEndpoints(home, &cli.ClusterEndpoints{
		PinAccountPub: pub,
		FloorURL:      base,
		BootstrapURL:  manifest.URL,
	}); err != nil {
		t.Fatal(err)
	}
	id, err := cli.EnsureIdentity(home)
	if err != nil {
		t.Fatal(err)
	}
	cmd := newSessionCmd()
	cmd.SetContext(context.Background())
	nc, err := connectCtl(cmd, "external review", home, base, id, nats.Name(cli.CtlNameUnactivated))
	if err != nil {
		t.Fatal(err)
	}
	nc.Close()
	ce, err := cli.ReadClusterEndpoints(home)
	if err != nil {
		t.Fatal(err)
	}
	if ce == nil || ce.Seeds == nil || ce.SeedGen != 1 {
		t.Fatalf("bootstrap-only pinned cache should be warmed from manifest; cache=%+v", ce)
	}
}

func TestCLIExternalReviewSessionCreateUsesFailoverDial(t *testing.T) {
	t.Setenv(cli.DevNoAuthEnv, "1")
	t.Setenv(cli.DefaultBrokerURLEnv, "")
	t.Setenv(cli.NoDiscoverEnv, "")

	home := t.TempDir()
	dead := "nats://127.0.0.1:1"
	survivor := startCLIExternalReviewNATS(t)
	seedPinnedFailoverCache(t, home, dead, survivor)
	id, err := cli.EnsureIdentity(home)
	if err != nil {
		t.Fatal(err)
	}
	nc, err := nats.Connect(survivor)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	sub, err := nc.Subscribe(proto.SubjCtrlSessionCreate(id.PublicKey), func(msg *nats.Msg) {
		body, _ := json.Marshal(proto.SessionCreateResp{SID: "lab", OwnerFP: id.Fingerprint, CreatedAt: time.Now()})
		_ = msg.Respond(body)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	cmd := newSessionCmd()
	cmd.SetContext(context.Background())
	cmd.SetArgs([]string{"--home", home, "create", "lab", "--pin", "1234"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("session create should use failover dial when broker_url floor is down: %v", err)
	}
}

func TestCLIExternalReviewLoginActivationUsesFailoverDial(t *testing.T) {
	t.Setenv(cli.DevNoAuthEnv, "1")
	t.Setenv(cli.DefaultBrokerURLEnv, "")
	t.Setenv(cli.NoDiscoverEnv, "")

	home := t.TempDir()
	dead := "nats://127.0.0.1:1"
	survivor := startCLIExternalReviewNATS(t)
	seedPinnedFailoverCache(t, home, dead, survivor)

	cmd := newLoginCmd()
	cmd.SetContext(context.Background())
	cmd.SetArgs([]string{"--home", home, "--session", "lab"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("login -s should use failover dial when broker_url floor is down: %v", err)
	}
	if got := cli.ReadCurrentSession(home); got != "lab" {
		t.Fatalf("current_session=%q, want lab", got)
	}
}
