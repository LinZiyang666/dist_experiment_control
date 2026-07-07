package broker

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
)

// g2_banner_test.go (G2 #20) — the DATA-PLANE-DEGRADED banner is the ANTI-SILENT-FAILURE guard: after
// force-single, a survivor whose on-disk nats.conf still carries a cluster{} block wedges JetStream at 503
// SILENTLY (the alert path itself rides JetStream — this is what let racknerd rot for days). StatusReport
// must surface it OUT-OF-BAND on the banner, keyed on the LIVE conf, and be BEST-EFFORT (a missing /
// unreadable / non-force-single conf must never add the banner and never fail the report).

const g2ClusteredConf = `server_name: "brk-a"
host: "0.0.0.0"
port: 4222
http: "127.0.0.1:8223"
jetstream {
  store_dir: "/var/lib/tether/js"
}
cluster {
  name: "tether"
  listen: "0.0.0.0:6222"
  routes: [ "nats://10.0.0.2:6222" ]
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

const g2StandaloneConf = `server_name: "brk-a"
host: "0.0.0.0"
port: 4222
jetstream {
  store_dir: "/var/lib/tether/js"
}
`

const dataPlaneBanner = "DATA-PLANE DEGRADED"

func writeConfFile(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "nats.conf")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatalf("write conf: %v", err)
	}
	return p
}

func TestG2DataPlaneDegradedBanner(t *testing.T) {
	n, addr := d7SingleNode(t, "brk-a")
	admin := NewClusterAdmin(n, nil)
	caughtUp := func(barrier uint64) (bool, error) { cur, err := n.AppliedIndex(); return cur >= barrier, err }
	if err := admin.AddNode(d7JoinInput(t, "brk-a", addr), addr, caughtUp, 5*time.Second); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	clustered := writeConfFile(t, g2ClusteredConf)

	// GATE PROOF: with a CLUSTERED conf but force-single NOT active, the banner must be ABSENT — the
	// degraded signal is specific to the force-single wedge, not to any clustered conf.
	admin.SetNatsConfPath(clustered)
	rep, err := admin.StatusReport("ctl-nats")
	if err != nil {
		t.Fatalf("StatusReport (pre-force-single): %v", err)
	}
	if strings.Contains(rep.Banner, dataPlaneBanner) {
		t.Fatalf("banner must NOT fire before force-single is active: %q", rep.Banner)
	}

	// Activate force-single. NOW a still-clustered conf is the silent-503 trap → the banner MUST fire.
	setCmd, perr := cluster.PlanSetForceSingle(time.Now())
	if perr != nil {
		t.Fatalf("PlanSetForceSingle: %v", perr)
	}
	proposeCmd(t, n, setCmd)

	rep, err = admin.StatusReport("ctl-nats")
	if err != nil {
		t.Fatalf("StatusReport (clustered): %v", err)
	}
	if !strings.Contains(rep.Banner, dataPlaneBanner) {
		t.Fatalf("force-single + clustered conf MUST raise the DATA-PLANE DEGRADED banner; got %q", rep.Banner)
	}

	// De-clustered (standalone) conf → the wedge is gone → banner must DISAPPEAR (proves it is keyed on
	// the LIVE conf, not a latched marker).
	admin.SetNatsConfPath(writeConfFile(t, g2StandaloneConf))
	rep, err = admin.StatusReport("ctl-nats")
	if err != nil {
		t.Fatalf("StatusReport (standalone): %v", err)
	}
	if strings.Contains(rep.Banner, dataPlaneBanner) {
		t.Fatalf("a standalone conf must clear the DATA-PLANE DEGRADED banner; got %q", rep.Banner)
	}

	// BEST-EFFORT: an unreadable/missing conf must neither add the banner nor fail the report (the
	// force-single verdict itself must still render).
	admin.SetNatsConfPath(filepath.Join(t.TempDir(), "does-not-exist.conf"))
	rep, err = admin.StatusReport("ctl-nats")
	if err != nil {
		t.Fatalf("StatusReport with a missing conf must NOT fail the report (best-effort): %v", err)
	}
	if strings.Contains(rep.Banner, dataPlaneBanner) {
		t.Fatalf("a missing conf must not add the banner; got %q", rep.Banner)
	}

	// Unwired path ("" ⇒ the banner check is skipped entirely).
	admin.SetNatsConfPath("")
	rep, err = admin.StatusReport("ctl-nats")
	if err != nil {
		t.Fatalf("StatusReport with no conf path: %v", err)
	}
	if strings.Contains(rep.Banner, dataPlaneBanner) {
		t.Fatalf("empty conf path must skip the banner; got %q", rep.Banner)
	}
}
