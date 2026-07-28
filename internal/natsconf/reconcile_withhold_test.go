package natsconf

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// g4_withhold_test.go (G4 #3/#10/#4) — the FIRST standalone→clustered grow: with a secrets dir the render
// now SUCCEEDS (no harvest hard-fail), but the reconciler must WITHHOLD the swap (ActionAwaitingClusteredCutover)
// so a SIGHUP never crosses the destructive cutover; only `cluster add` performs the coordinated restart.

// a standalone (post-init, pre-grow) conf: jetstream + auth_callout, NO cluster{} block.
const reconcileStandaloneConf = `server_name: "brk-a"
host: "0.0.0.0"
port: 4222
http: "127.0.0.1:8223"
jetstream {
  store_dir: "/var/lib/tether/js"
}
authorization {
  auth_callout {
    issuer: "ABROKERACCOUNTPUB"
    account: "$G"
    auth_users: [ "UBUSNKEYA" ]
  }
  users: [
    { nkey: "UBUSNKEYA" }
  ]
}
`

// fakeNatsServer writes an executable that ignores its args and exits 0, so DryRun's
// `nats-server -c <tmp> -t` validation passes without a real nats-server. Skips on non-unix.
func fakeNatsServer(t *testing.T) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("fake nats-server shell script is unix-only")
	}
	dir := t.TempDir()
	bin := filepath.Join(dir, "nats-server")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	return bin
}

func g4GrowPeers() []Broker {
	return []Broker{
		{ServerName: "brk-a", NkeyPub: "UBUSNKEYA", RouteURL: "nats://10.0.0.1:6222"},
		{ServerName: "brk-b", NkeyPub: "UBUSNKEYB", RouteURL: "nats://10.0.0.2:6222"},
	}
}

func TestReconcileWithholdsClusteredCutoverSwap(t *testing.T) {
	dir := t.TempDir()
	conf := filepath.Join(dir, "nats.conf")
	if err := os.WriteFile(conf, []byte(reconcileStandaloneConf), 0o600); err != nil {
		t.Fatal(err)
	}
	in := Inputs{
		SelfServerName: "brk-a",
		Peers:          g4GrowPeers(),
		AccountIssuer:  "ABROKERACCOUNTPUB",
		ConfPath:       conf,
		NatsServerBin:  fakeNatsServer(t),
		DesiredGen:     7,
		SecretsDir:     "/etc/tether/secrets", // #3 fallback: render clustered from the secrets dir
	}
	out := ReconcileOnce(in, 3, 3, func() error {
		t.Fatal("WITHHOLD must NOT reload — the swap is deferred to `cluster add`")
		return nil
	}, nil)
	if out.Action != ActionAwaitingClusteredCutover {
		t.Fatalf("first standalone→clustered grow must WITHHOLD (ActionAwaitingClusteredCutover), got %q (%v)", out.Action, out.Err)
	}
	// Applied/observed must NOT advance — topology is honestly not converged until the orchestrated cutover.
	if out.AppliedGen != 3 || out.ObservedGen != 3 {
		t.Fatalf("withhold must not advance applied/observed: %+v", out)
	}
	// The on-disk conf is UNCHANGED (still standalone — no cluster{} swapped in).
	after, _ := os.ReadFile(conf)
	if string(after) != reconcileStandaloneConf {
		t.Fatalf("withhold must NOT swap the conf; it was modified:\n%s", after)
	}
}

func TestReconcileFirstGrowWithoutSecretsDirStaysRejected(t *testing.T) {
	dir := t.TempDir()
	conf := filepath.Join(dir, "nats.conf")
	if err := os.WriteFile(conf, []byte(reconcileStandaloneConf), 0o600); err != nil {
		t.Fatal(err)
	}
	// No SecretsDir → the harvest hard-fail is preserved (fail-closed), NOT a silent success.
	out := ReconcileOnce(Inputs{
		SelfServerName: "brk-a",
		Peers:          g4GrowPeers(),
		AccountIssuer:  "ABROKERACCOUNTPUB",
		ConfPath:       conf,
		NatsServerBin:  fakeNatsServer(t),
		DesiredGen:     7,
	}, 3, 3, func() error { t.Fatal("must not reload"); return nil }, nil)
	if out.Action != ActionRejected {
		t.Fatalf("first grow with no secrets dir must stay fail-closed (ActionRejected via the harvest hard-fail), got %q", out.Action)
	}
	after, _ := os.ReadFile(conf)
	if string(after) != reconcileStandaloneConf {
		t.Fatal("a rejected render must not touch the conf")
	}
}
