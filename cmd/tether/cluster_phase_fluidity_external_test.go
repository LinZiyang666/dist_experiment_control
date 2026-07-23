package main

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/nats-io/nkeys"
	"github.com/spf13/cobra"
)

func externalClusteredNATSConf(t *testing.T, withStoreDir bool) string {
	t.Helper()
	account, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatal(err)
	}
	accountPub, err := account.PublicKey()
	if err != nil {
		t.Fatal(err)
	}
	account.Wipe()
	user, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	userPub, err := user.PublicKey()
	if err != nil {
		t.Fatal(err)
	}
	user.Wipe()

	js := "jetstream {}"
	if withStoreDir {
		js = fmt.Sprintf("jetstream { store_dir: %q }", filepath.Join(t.TempDir(), "js"))
	}
	body := fmt.Sprintf(`server_name: "solo"
listen: "127.0.0.1:4222"
%s
cluster {
  name: "tether"
  listen: "127.0.0.1:6222"
  routes: []
  tls {
    ca_file: "/run/tether/ca.pem"
    cert_file: "/run/tether/cert.pem"
    key_file: "/run/tether/key.pem"
    verify: true
  }
}
authorization {
  auth_callout { issuer: %q, account: "$G", auth_users: [%q] }
  users: [{ nkey: %q }]
}
`, js, accountPub, userPub, userPub)
	path := filepath.Join(t.TempDir(), "nats.conf")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func externalStandaloneCmd() *cobra.Command {
	cmd := &cobra.Command{}
	cmd.SetOut(&bytes.Buffer{})
	cmd.SetErr(&bytes.Buffer{})
	return cmd
}

// TestExternalToStandaloneFailsClosedWithoutLiveN1Proof locks the destructive boundary: typed
// intent is not proof of cluster size. If the admin socket cannot produce a live voter count, the
// command must not rewrite this node out of the route mesh.
func TestExternalToStandaloneFailsClosedWithoutLiveN1Proof(t *testing.T) {
	conf := externalClusteredNATSConf(t, true)
	orig := fetchClusterStatusReport
	fetchClusterStatusReport = func(string) (*adminsock.ClusterStatusReport, error) {
		return nil, errors.New("admin socket unavailable")
	}
	t.Cleanup(func() { fetchClusterStatusReport = orig })

	f := &natsconfTakeoverFlags{
		confPath: conf, serverName: "solo", takeoverSocket: "/unavailable",
		plan: true, skipDryRun: true,
	}
	if err := runReconcileToStandalone(externalStandaloneCmd(), f, true, false); err == nil {
		t.Fatal("--to-standalone must fail closed when live N=1 cannot be proved")
	}
}

// TestExternalToStandaloneRefusesMissingStoreDir mirrors the existing grow-side fail-closed guard.
// The source conf still enables JetStream, but Render omits jetstream{} when JSStoreDir is empty;
// allowing the rewrite would silently disable JetStream on the next restart.
func TestExternalToStandaloneRefusesMissingStoreDir(t *testing.T) {
	conf := externalClusteredNATSConf(t, false)
	orig := fetchClusterStatusReport
	fetchClusterStatusReport = func(string) (*adminsock.ClusterStatusReport, error) {
		return &adminsock.ClusterStatusReport{Nodes: []adminsock.ClusterNodeStatus{{NodeID: "solo", Role: "leader"}}}, nil
	}
	t.Cleanup(func() { fetchClusterStatusReport = orig })

	f := &natsconfTakeoverFlags{
		confPath: conf, serverName: "solo", takeoverSocket: "/unused",
		plan: true, skipDryRun: true,
	}
	err := runReconcileToStandalone(externalStandaloneCmd(), f, true, false)
	if err == nil || !strings.Contains(err.Error(), "store_dir") {
		t.Fatalf("--to-standalone must refuse JetStream-without-store_dir, got %v", err)
	}
}

// TestExternalToStandaloneRequiresCompleteLeaderN1Proof verifies that "one visible voter" is not
// by itself a destructive authorization. A follower may hold a stale raft configuration and a
// partial/errored report explicitly says its node list is incomplete. Only a complete leader view
// can prove that removing cluster{} will not tear a still-live route mesh.
func TestExternalToStandaloneRequiresCompleteLeaderN1Proof(t *testing.T) {
	reports := []struct {
		name string
		rep  *adminsock.ClusterStatusReport
	}{
		{
			name: "partial leader view",
			rep: &adminsock.ClusterStatusReport{IsLeaderView: true, Partial: true,
				Nodes: []adminsock.ClusterNodeStatus{{NodeID: "solo", Role: "leader"}}},
		},
		{
			name: "errored leader view",
			rep: &adminsock.ClusterStatusReport{IsLeaderView: true, Errors: []string{"raft config incomplete"},
				Nodes: []adminsock.ClusterNodeStatus{{NodeID: "solo", Role: "leader"}}},
		},
		{
			name: "non-leader view",
			rep: &adminsock.ClusterStatusReport{IsLeaderView: false,
				Nodes: []adminsock.ClusterNodeStatus{{NodeID: "solo", Role: "voter"}}},
		},
		{
			name: "unknown role hidden beside one voter",
			rep: &adminsock.ClusterStatusReport{IsLeaderView: true,
				Nodes: []adminsock.ClusterNodeStatus{{NodeID: "solo", Role: "leader"}, {NodeID: "mystery", Role: "unexpected"}}},
		},
	}

	orig := fetchClusterStatusReport
	t.Cleanup(func() { fetchClusterStatusReport = orig })
	for _, tc := range reports {
		t.Run(tc.name, func(t *testing.T) {
			conf := externalClusteredNATSConf(t, true)
			fetchClusterStatusReport = func(string) (*adminsock.ClusterStatusReport, error) { return tc.rep, nil }
			f := &natsconfTakeoverFlags{
				confPath: conf, serverName: "solo", takeoverSocket: "/unused",
				plan: true, skipDryRun: true,
			}
			if err := runReconcileToStandalone(externalStandaloneCmd(), f, true, false); err == nil {
				t.Fatal("--to-standalone accepted a non-authoritative/incomplete N=1 report")
			}
		})
	}
}
