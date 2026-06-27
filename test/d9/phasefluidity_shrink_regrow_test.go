//go:build d9_integration

package d9_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/natscluster"
	"github.com/LinZiyang666/tether/internal/natsconf"
	"github.com/LinZiyang666/tether/internal/natsreconcile"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nkeys"
)

func pfNkeys(t *testing.T) (accountPub, userPub string) {
	t.Helper()
	acc, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatal(err)
	}
	accountPub, _ = acc.PublicKey()
	acc.Wipe()
	usr, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	userPub, _ = usr.PublicKey()
	usr.Wipe()
	return
}

func pfWriteConf(t *testing.T, conf string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "nats.conf")
	if err := os.WriteFile(path, []byte(conf), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestPhaseFluidityShrinkRegrowLifecycle (external-review R5) proves the post-shrink N=1 STANDALONE
// JetStream conf the shrink path renders is actually loaded + run by a REAL nats-server (JS standalone
// works), and that the automatic reconciler PRESERVES each mode against real confs: a standalone N=1
// stays standalone (F4, converged), a still-clustered N=1 stays clustered (R3) — it never auto-crosses
// the destructive de-cluster boundary.
func TestPhaseFluidityShrinkRegrowLifecycle(t *testing.T) {
	acc, usr := pfNkeys(t)
	self := natscluster.Broker{ServerName: "pf-solo", NkeyPub: usr, RouteURL: "nats://127.0.0.1:6222"}
	store := filepath.Join(t.TempDir(), "js")

	// (1) post-shrink: the rendered STANDALONE conf loads in a REAL nats-server with JS standalone.
	standaloneConf, err := natscluster.Render(natscluster.Config{
		Standalone: true, Local: self, Peers: []natscluster.Broker{self}, AccountIssuer: acc,
		JSStoreDir: store, ClientListen: "127.0.0.1:-1",
	})
	if err != nil {
		t.Fatalf("render standalone: %v", err)
	}
	sPath := pfWriteConf(t, standaloneConf)
	opts, err := natsserver.ProcessConfigFile(sPath)
	if err != nil {
		t.Fatalf("real nats-server rejected the shrink's standalone conf: %v", err)
	}
	opts.NoLog, opts.NoSigs = true, true
	srv, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("NewServer(standalone): %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(20 * time.Second) {
		t.Fatal("post-shrink standalone server never became ready")
	}
	t.Cleanup(func() { srv.Shutdown(); srv.WaitForShutdown() })
	if !srv.JetStreamEnabled() {
		t.Fatal("post-shrink standalone server must have JetStream enabled")
	}
	if srv.JetStreamIsClustered() {
		t.Fatal("post-shrink JetStream must be STANDALONE, not clustered (a lone node can never reach the meta quorum)")
	}

	// (2) the reconciler keeps the standalone N=1 conf converged (F4) — it does NOT rewrite it.
	out := natsreconcile.ReconcileOnce(natsreconcile.Inputs{
		SelfServerName: "pf-solo", Peers: []natscluster.Broker{self}, AccountIssuer: acc,
		ConfPath: sPath, NatsServerBin: "/bin/true", DesiredGen: 3,
	}, 0, 0, nil, func() (time.Time, error) { return time.Now().Add(time.Hour), nil })
	if post, _ := natsconf.Preflight(sPath); post == nil || !post.IsStandaloneJetStream() {
		t.Fatalf("reconciler rewrote a converged standalone N=1 conf; outcome=%+v", out)
	}

	// (3) a still-CLUSTERED N=1 conf is NOT auto-de-clustered by the reconciler (R3) — the destructive
	// transition requires an explicit operator `reconcile nats --to-standalone`.
	clusteredConf, err := natscluster.Render(natscluster.Config{
		Local: self, Peers: []natscluster.Broker{self}, AccountIssuer: acc,
		JSStoreDir: filepath.Join(t.TempDir(), "js2"), ClientListen: "127.0.0.1:-1",
		ClusterListen: "127.0.0.1:6222", ClusterName: "tether",
		CAFile: "/run/tether/ca.pem", CertFile: "/run/tether/cert.pem", KeyFile: "/run/tether/key.pem",
	})
	if err != nil {
		t.Fatalf("render clustered: %v", err)
	}
	cPath := pfWriteConf(t, clusteredConf)
	_ = natsreconcile.ReconcileOnce(natsreconcile.Inputs{
		SelfServerName: "pf-solo", Peers: []natscluster.Broker{self}, AccountIssuer: acc,
		ConfPath: cPath, NatsServerBin: "/bin/true", DesiredGen: 4,
	}, 0, 0, nil, func() (time.Time, error) { return time.Now().Add(time.Hour), nil })
	post, err := natsconf.Preflight(cPath)
	if err != nil {
		t.Fatalf("preflight clustered post-reconcile: %v", err)
	}
	if !post.IsClusteredJetStream() {
		t.Fatal("reconciler auto-removed cluster{} from a still-clustered N=1 conf without explicit de-cluster intent (R3)")
	}
}
