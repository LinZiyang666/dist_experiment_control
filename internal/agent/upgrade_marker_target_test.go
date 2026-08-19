package agent

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

// origin: docs/reviews/cloned-credential-instances-plan.md — I2 ledger review.
//
// The upgrade marker's target identity (external review F1) is the ONLY gate on
// the TERMINAL (rolled_back / rollback_failed) report-and-clear arm: the other
// three parts of the commit proof (BootCount, the process-local boot proof, the
// running-image sha) guard the pending -> committed arm only. Its discriminator
// is (cfg.SID, cfg.NID), and the cloned-credential premise is that co-hosted
// instances share BOTH — plus, per install.sh (BIN_DIR=$HOME/.local/bin) and the
// plan's §0.6 spike (~/.tether is one shared NFS mount), they share the binary
// directory the marker lives in.
//
// So the basename holder can report AND ERASE the outcome of an upgrade the
// operator ran against `<basename>-NN`: the outcome is attributed to the wrong
// node and the instance that actually rolled back never reports it.
func TestTerminalUpgradeOutcomeIsNotReportedByAShareBinarySibling(t *testing.T) {
	exePath, markerPath := bootFixture(t, "OLD", "", nil)

	// The marker the LEASED instance wrote for its own `node upgrade jup-02`.
	// TargetNID is a.cfg.NID by construction (upgrade.go), i.e. the BASENAME.
	m := testMarker(upgradeStateRolledBack, 1, time.Now())
	m.TargetSID, m.TargetNID = "lab", "jup"
	// TargetNID is the BASENAME by construction, so it cannot discriminate
	// between clones. The instance that ran the upgrade stamps its lineage here
	// (upgrade.go), and that is what stops a sibling claiming the outcome.
	m.TargetInstance = "bbbbbbbbbbbbbbbbbbbbbbbbbb"
	m.Detail = "install failed: sha256 mismatch"
	if err := writeUpgradeMarker(markerPath, m); err != nil {
		t.Fatal(err)
	}

	// The BASENAME holder: a different process that never installed anything.
	// Same sid, same agent.yaml nid — that is the clone premise.
	holder := &Agent{cfg: Config{
		SID: "lab", NID: "jup",
		Logger:                slog.New(slog.DiscardHandler),
		UpgradeExecutablePath: exePath,
	}}

	state, detail := holder.upgradeRegisterReport()
	if state != "" {
		t.Errorf("the basename holder reported another instance's upgrade outcome: state=%q detail=%q "+
			"(the broker records the rollback against nid %q, which was never upgraded)",
			state, detail, holder.cfg.NID)
	}

	holder.commitUpgradeAfterRegister(upgradeStateRolledBack)
	if _, err := os.Stat(markerPath); os.IsNotExist(err) {
		t.Error("the basename holder ERASED the leased instance's terminal marker; " +
			"the instance that actually rolled back can never report it")
	}
}

// The same collapse, stated directly: for two clones of one image the marker's
// instance-identity check cannot discriminate at all, because the routing name
// (which does differ) is not what it compares.
func TestUpgradeMarkerTargetDistinguishesTwoInstancesOfOneImage(t *testing.T) {
	exePath, _ := bootFixture(t, "OLD", "", nil)
	// Each instance carries its own lineage — that is what a real process has
	// (instance.go mints it at startup), and it is the ONLY thing that differs
	// between two clones of one image on the marker's terms: the routing name
	// deliberately is not compared, because a lease is transient and comparing
	// against it would strand the marker across an adoption.
	mk := func(routing, iid string) *Agent {
		a := &Agent{
			instanceID: iid,
			cfg: Config{
				SID: "lab", NID: "jup",
				Logger:                slog.New(slog.DiscardHandler),
				UpgradeExecutablePath: exePath,
			},
		}
		if routing != "" {
			a.routingNID.Store(&routing)
		}
		return a
	}
	holder := mk("", "aaaaaaaaaaaaaaaaaaaaaaaaaa")
	leased := mk("jup-02", "bbbbbbbbbbbbbbbbbbbbbbbbbb")
	m := testMarker(upgradeStatePending, 1, time.Now().Add(time.Minute))
	m.TargetSID, m.TargetNID = "lab", "jup"
	// The upgrade the LEASED instance started.
	m.TargetInstance = "bbbbbbbbbbbbbbbbbbbbbbbbbb"

	if holder.markerTargetsThisAgent(m) == leased.markerTargetsThisAgent(m) {
		t.Errorf("markerTargetsThisAgent gives the SAME verdict (%v) for the basename holder "+
			"(routing %q) and the leased instance (routing %q) — the F1 sibling guard has no "+
			"discriminating power for the population this increment creates",
			holder.markerTargetsThisAgent(m), nidOf(holder), nidOf(leased))
	}
}

func TestLeasedInstanceRefusesRemoteUpgrade(t *testing.T) {
	a := &Agent{cfg: Config{NID: "gpu1"}}
	leased := "gpu1-02"
	a.routingNID.Store(&leased)
	if !leasedInstanceRefusesUpgrade(a) {
		t.Fatal("a leased instance was allowed to replace a potentially shared binary")
	}
	a.routingNID.Store(nil)
	if leasedInstanceRefusesUpgrade(a) {
		t.Fatal("a fixed-identity agent was classified as leased")
	}
}

// origin: docs/reviews/cloned-credential-instances-external-review-tasklist.md D7
//
// A contested reply is explicitly not a successful register: the broker
// short-circuits before registerNode, reconciliation and events. It therefore
// cannot be the health check-in that commits an upgrade marker. The marker has
// to remain pending until the rebuilt session registers successfully under the
// assigned name; otherwise an auth failure during adoption disables rollback
// while the upgraded agent is still offline.
func TestContestedRegisterReplyDoesNotCommitAnUpgrade(t *testing.T) {
	url := testharness.StartNATS(t)
	exePath, markerPath := bootFixture(t, "NEW", "", nil)
	const instanceID = "bbbbbbbbbbbbbbbbbbbbbbbbbb"
	t.Setenv(instanceIDEnv, instanceID)

	a, err := New(Config{
		NATSURL:                 url,
		SID:                     "lab",
		NID:                     "gpu1",
		Logger:                  testharness.SilentLog(),
		RegisterTimeout:         2 * time.Second,
		UpgradeExecutablePath:   exePath,
		UpgradeRunningImagePath: exePath,
		UpgradeBootProofID:      "test-upgrade-id",
	})
	if err != nil {
		t.Fatal(err)
	}
	m := testMarker(upgradeStatePending, 1, time.Now().Add(time.Minute))
	m.NewSHA = mustSHA(t, exePath)
	m.TargetSID, m.TargetNID, m.TargetInstance = "lab", "gpu1", instanceID
	if err := writeUpgradeMarker(markerPath, m); err != nil {
		t.Fatal(err)
	}

	serverNC, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer serverNC.Close()
	sub, err := serverNC.Subscribe(proto.SubjNodeRegister("lab", "gpu1"), func(msg *nats.Msg) {
		body, _ := json.Marshal(proto.NodeRegisterResp{
			OK:    true,
			Lease: &proto.NodeLease{AssignedNID: "gpu1-02", Basename: "gpu1"},
		})
		_ = msg.Respond(body)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := serverNC.Flush(); err != nil {
		t.Fatal(err)
	}

	agentNC, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer agentNC.Close()
	resp, err := a.register(context.Background(), agentNC)
	if err != nil || resp.Lease == nil {
		t.Fatalf("register did not return the contested verdict: resp=%+v err=%v", resp, err)
	}
	if got := readMarkerState(t, markerPath).State; got != upgradeStatePending {
		t.Fatalf("contested reply committed the upgrade before the assigned-name register succeeded: state=%q", got)
	}
}
