package natsconf

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestExternalSinglePeerStandaloneRemainsConverged verifies that the supported post-shrink shape is
// also a desired state of the automatic reconciler. A one-off CLI rewrite is not sufficient: the
// daemon keeps running its topology loop, and later generation changes/restarts must continue to
// treat N=1 standalone JetStream as converged rather than permanently STUCK.
func TestExternalSinglePeerStandaloneRemainsConverged(t *testing.T) {
	self := Broker{
		ServerName: "solo", NkeyPub: "UBROKER", RouteURL: "nats://127.0.0.1:6222",
	}
	conf, err := Render(Config{
		Standalone: true, Local: self, Peers: []Broker{self},
		AccountIssuer: "AISSUER", JSStoreDir: filepath.Join(t.TempDir(), "js"),
		ClientListen: "127.0.0.1:4222",
	})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "nats.conf")
	if err := os.WriteFile(path, []byte(conf), 0o600); err != nil {
		t.Fatal(err)
	}

	out := ReconcileOnce(Inputs{
		SelfServerName: "solo", Peers: []Broker{self}, AccountIssuer: "AISSUER",
		ConfPath: path, NatsServerBin: "nats-server", DesiredGen: 7,
	}, 0, 0, nil, func() (time.Time, error) { return time.Now().Add(time.Hour), nil })
	if out.Err != nil || out.Action != ActionNoop || out.AppliedGen != 7 || out.ObservedGen != 7 {
		t.Fatalf("N=1 standalone must remain a converged desired topology; got %+v", out)
	}
}

// TestExternalSinglePeerClusteredConfRequiresExplicitDeclusterIntent pins the other half of the N=1
// state transition. Topology reconciliation has no operator-intent input, so merely observing one
// peer must not silently remove cluster{} from an existing clustered-JetStream configuration. Doing
// so bypasses `--confirm-single`, the backup warning, and the required clustered->standalone store
// reset; the next ordinary restart could load standalone JS against an unprepared clustered store.
func TestExternalSinglePeerClusteredConfRequiresExplicitDeclusterIntent(t *testing.T) {
	self := Broker{
		ServerName: "solo", NkeyPub: "UBROKER", RouteURL: "nats://127.0.0.1:6222",
	}
	// The FIXTURE is a VALID clustered conf (self + a former peer) — the shape left after an N=2
	// cluster lost a node down to N=1. (A lone-self clustered render is unbootable and now refused by
	// Render — audit D — so we cannot use it as the fixture.)
	gonePeer := Broker{ServerName: "gone", NkeyPub: "UGONE", RouteURL: "nats://127.0.0.2:6222"}
	conf, err := Render(Config{
		Local: self, Peers: []Broker{self, gonePeer}, AccountIssuer: "AISSUER",
		JSStoreDir: filepath.Join(t.TempDir(), "js"), ClientListen: "127.0.0.1:4222",
		ClusterListen: "127.0.0.1:6222", CAFile: "/run/tether/ca.pem",
		CertFile: "/run/tether/cert.pem", KeyFile: "/run/tether/key.pem",
	})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "nats.conf")
	if err := os.WriteFile(path, []byte(conf), 0o600); err != nil {
		t.Fatal(err)
	}

	out := ReconcileOnce(Inputs{
		SelfServerName: "solo", Peers: []Broker{self}, AccountIssuer: "AISSUER",
		ConfPath: path, NatsServerBin: "/bin/true", DesiredGen: 8,
	}, 0, 0, nil, func() (time.Time, error) { return time.Now().Add(time.Hour), nil })
	post, err := Preflight(path)
	if err != nil {
		t.Fatalf("post-reconcile preflight: %v (outcome=%+v)", err, out)
	}
	if !post.IsClusteredTopology() {
		t.Fatalf("automatic reconcile removed cluster{} without explicit de-cluster intent; outcome=%+v", out)
	}
	// audit D: at N=1 the reconciler must REFUSE (ActionRejected/STUCK) to swap in a lone-self clustered
	// conf (empty routes = unbootable) rather than silently bricking the broker — the existing clustered
	// conf is preserved untouched, pending an explicit `reconcile nats --to-standalone --confirm-single`.
	if out.Action != ActionRejected {
		t.Fatalf("N=1 with a still-clustered conf must be ActionRejected (refuse the unbootable empty-routes re-render), got %q (outcome=%+v)", out.Action, out)
	}
}
