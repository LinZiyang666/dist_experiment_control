package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/proto"
)

// g4_external_review_fixes_test.go — adversarial regressions for the G4 external-review fixes (B1/B2/M1/M2/M3).
// The end-to-end path is a deploy-tier drill (network-coupled), so each fix's DECISION is extracted into a pure
// predicate and pinned here — a hermetic guard that survives without a live cluster.

// B1a: a SINGLE-mode broker answers the local admin socket too (OK=false / Cluster=nil / Code=cluster_not_enabled).
// Accepting any reply misjudged it as "cluster-up + joined"; only a genuine clustered status may pass.
func TestAdminStatusIsClustered_RejectsSingleMode(t *testing.T) {
	cases := []struct {
		name string
		resp *adminsock.Response
		want bool
	}{
		{"nil (no reply)", nil, false},
		{"single-mode refusal", &adminsock.Response{OK: false, Code: adminsock.CodeClusterNotEnabled}, false},
		{"ok but no cluster report (impossible-but-defensive)", &adminsock.Response{OK: true, Cluster: nil}, false},
		{"cluster report but not OK", &adminsock.Response{OK: false, Cluster: &adminsock.ClusterStatusReport{}}, false},
		{"genuine clustered status", &adminsock.Response{OK: true, Cluster: &adminsock.ClusterStatusReport{}}, true},
	}
	for _, c := range cases {
		if got := adminStatusIsClustered(c.resp); got != c.want {
			t.Errorf("%s: adminStatusIsClustered=%v want %v", c.name, got, c.want)
		}
	}
}

// B1b: verifyClusterSeam must fail-closed when broker.cluster is absent (the joiner would boot single mode) and
// pass only when the seam decodes. This backstops applyClusterSeam's own verify from the driver side.
func TestVerifyClusterSeam(t *testing.T) {
	dir := t.TempDir()

	seamed := filepath.Join(dir, "clustered.yaml")
	if err := os.WriteFile(seamed, []byte("broker:\n  domain: x\nstorage:\n  scratch: /tmp\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := applyClusterSeam(seamed, "/var/lib/tether", "brk-a:7400", "/etc/tether/secrets"); err != nil {
		t.Fatalf("apply seam: %v", err)
	}
	if err := verifyClusterSeam(seamed, "brk-a:7400", "/var/lib/tether", "/etc/tether/secrets", "/etc/tether/nats.d/nats.conf"); err != nil {
		t.Errorf("a complete matching seamed config must verify: %v", err)
	}

	// R2-B1: a seam whose raft_addr matches a DIFFERENT node must fail-closed (stale/wrong seam), showing both.
	if err := verifyClusterSeam(seamed, "brk-b:7400", "/var/lib/tether", "/etc/tether/secrets", "/etc/tether/nats.d/nats.conf"); err == nil {
		t.Error("a seam whose raft_addr != this cluster add's --raft-addr must fail-closed (stale/wrong seam)")
	} else if !strings.Contains(err.Error(), "brk-a:7400") || !strings.Contains(err.Error(), "brk-b:7400") {
		t.Errorf("the mismatch error must show both actual and expected raft_addr, got: %v", err)
	}

	// R4-B1: a full-looking seam whose data_dir is present but WRONG (a stale/copied broker.yaml) must
	// fail-closed — serve would probe raft state at the wrong path. Names data_dir + the stale value.
	wrongDD := filepath.Join(dir, "wrongdd.yaml")
	if err := os.WriteFile(wrongDD, []byte("broker:\n  cluster:\n    data_dir: /stale/dd\n    raft_addr: brk-a:7400\n    secrets_dir: /etc/tether/secrets\n    nats_conf_path: /etc/tether/nats.d/nats.conf\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := verifyClusterSeam(wrongDD, "brk-a:7400", "/var/lib/tether", "/etc/tether/secrets", "/etc/tether/nats.d/nats.conf"); err == nil {
		t.Error("a full-looking seam with a wrong data_dir must fail-closed (R4-B1)")
	} else if !strings.Contains(err.Error(), "data_dir") || !strings.Contains(err.Error(), "/stale/dd") {
		t.Errorf("the wrong-data_dir error must name data_dir + the stale value, got: %v", err)
	}

	// R3-B1: a partial seam with the RIGHT raft_addr but a missing trigger field (secrets_dir here — a different
	// field than the reviewer's data_dir case) must ALSO fail-closed, naming the field.
	incomplete := filepath.Join(dir, "incomplete.yaml")
	if err := os.WriteFile(incomplete, []byte("broker:\n  cluster:\n    data_dir: /var/lib/tether\n    raft_addr: brk-a:7400\n    nats_conf_path: /etc/tether/nats.d/nats.conf\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := verifyClusterSeam(incomplete, "brk-a:7400", "/var/lib/tether", "/etc/tether/secrets", "/etc/tether/nats.d/nats.conf"); err == nil {
		t.Error("a partial seam missing secrets_dir must fail-closed")
	} else if !strings.Contains(err.Error(), "secrets_dir") {
		t.Errorf("the incomplete-seam error must name the missing field (secrets_dir), got: %v", err)
	}

	single := filepath.Join(dir, "single.yaml")
	if err := os.WriteFile(single, []byte("broker:\n  domain: x\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := verifyClusterSeam(single, "brk-a:7400", "/var/lib/tether", "/etc/tether/secrets", "/etc/tether/nats.d/nats.conf"); err == nil {
		t.Error("a config with NO broker.cluster seam must fail-closed (joiner would boot single mode)")
	} else if !strings.Contains(err.Error(), "SINGLE mode") {
		t.Errorf("the HALT must name the single-mode risk, got: %v", err)
	}

	if err := verifyClusterSeam(filepath.Join(dir, "absent.yaml"), "brk-a:7400", "/var/lib/tether", "/etc/tether/secrets", "/etc/tether/nats.d/nats.conf"); err == nil {
		t.Error("a missing config must fail-closed for a grow (the joiner has no config to boot clustered)")
	}
}

// R2-B1: applyClusterSeam must NOT treat a MISMATCHED existing seam as idempotent — a stale/wrong broker.yaml
// (a copied or leftover one) must be a HARD error, not a silent skip that leaves the wrong raft_addr in place.
func TestApplyClusterSeam_RejectsMismatchedExistingSeam(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "broker.yaml")
	if err := os.WriteFile(cfg, []byte("broker:\n  cluster:\n    raft_addr: stale:7400\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	applied, err := applyClusterSeam(cfg, "/var/lib/tether", "brk2:7400", "/etc/tether/secrets")
	if err == nil {
		t.Fatal("applyClusterSeam must reject an existing seam whose raft_addr != this node's --raft-addr")
	}
	if applied {
		t.Error("a rejected stale seam must NOT report applied=true")
	}
	if !strings.Contains(err.Error(), "stale:7400") || !strings.Contains(err.Error(), "brk2:7400") {
		t.Errorf("error should show both the stale and the requested raft_addr, got: %v", err)
	}
	// R3-B1: a PARTIAL existing seam (right raft_addr but missing data_dir etc.) must ALSO hard-error — it is not
	// idempotent, because it would boot the joiner in single mode. A raft_addr-only seam is the exact case.
	partial := filepath.Join(dir, "partial.yaml")
	if err := os.WriteFile(partial, []byte("broker:\n  cluster:\n    raft_addr: brk2:7400\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if applied, err := applyClusterSeam(partial, "/var/lib/tether", "brk2:7400", "/etc/tether/secrets"); err == nil || applied {
		t.Errorf("a partial existing seam (missing data_dir) must hard-error, not be idempotent; got applied=%v err=%v", applied, err)
	}
	// Only a COMPLETE matching seam stays idempotent (no double-append, no error).
	match := filepath.Join(dir, "match.yaml")
	if err := os.WriteFile(match, []byte("broker:\n  cluster:\n    data_dir: /var/lib/tether\n    raft_addr: brk2:7400\n    secrets_dir: /etc/tether/secrets\n    nats_conf_path: /etc/tether/nats.d/nats.conf\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if applied, err := applyClusterSeam(match, "/var/lib/tether", "brk2:7400", "/etc/tether/secrets"); err != nil || applied {
		t.Errorf("a complete matching seam must be idempotent (applied=false, err=nil), got applied=%v err=%v", applied, err)
	}
}

// B2: the start-joiner hint must say `restart nats-server`, NOT a bare `start` (a no-op on a running standalone
// nats that never loads the freshly-rendered clustered conf).
func TestStartJoinerHint_SaysRestartNotStart(t *testing.T) {
	h := startJoinerHint("brk2")
	if !strings.Contains(h, "systemctl restart nats-server") {
		t.Errorf("start-joiner hint must instruct `restart nats-server`, got:\n%s", h)
	}
	if strings.Contains(h, "systemctl start nats-server") {
		t.Errorf("start-joiner hint must NOT say `start nats-server` (a no-op that never loads the clustered conf):\n%s", h)
	}
	if !strings.Contains(h, "brk2") {
		t.Errorf("hint must name the joiner: %s", h)
	}
}

// M3: only CATCHING_UP / SERVING clear the AddNonvoter barrier; a terminal non-SERVING op is a hard error so the
// driver never proceeds into cutover on a dead op.
func TestCatchupBarrier(t *testing.T) {
	cases := []struct {
		name    string
		resp    *proto.ClusterGrowResp
		wantMet bool
		wantErr bool
	}{
		{"nil", nil, false, false},
		{"not ok (transient)", &proto.ClusterGrowResp{OK: false}, false, false},
		{"catching up", &proto.ClusterGrowResp{OK: true, OpState: "CATCHING_UP"}, true, false},
		{"serving", &proto.ClusterGrowResp{OK: true, OpState: "SERVING", Terminal: true}, true, false},
		{"still rostering (not terminal)", &proto.ClusterGrowResp{OK: true, OpState: "ROSTER_COMMITTED"}, false, false},
		{"terminal ABORTED before catch-up", &proto.ClusterGrowResp{OK: true, OpState: "ABORTED", Terminal: true, LastError: "boom"}, false, true},
	}
	for _, c := range cases {
		met, err := catchupBarrier(c.resp)
		if met != c.wantMet || (err != nil) != c.wantErr {
			t.Errorf("%s: met=%v err=%v want met=%v err=%v", c.name, met, err, c.wantMet, c.wantErr)
		}
	}
}

// M2: an EXPLICIT broker refusal (non-OK reply, not a transport error) is stable and must HALT; a transport
// error is the expected post-SIGKILL lost reply and must NOT be treated as a stable refusal.
func TestStableCutoverRefusal(t *testing.T) {
	if stableCutoverRefusal(nil, errStub("transport down")) {
		t.Error("a transport error is the expected post-SIGKILL lost reply — NOT a stable refusal")
	}
	if stableCutoverRefusal(&proto.ClusterGrowResp{OK: true}, nil) {
		t.Error("an OK reply is not a refusal")
	}
	if stableCutoverRefusal(&proto.ClusterGrowResp{AlreadyDone: true}, nil) {
		t.Error("an AlreadyDone reply is not a refusal")
	}
	if !stableCutoverRefusal(&proto.ClusterGrowResp{OK: false, Code: adminsock.CodeBadRequest, Error: "non-empty store"}, nil) {
		t.Error("a non-OK broker reply (non-empty store without ack) MUST be a stable refusal that HALTs")
	}
	if !stableCutoverRefusal(&proto.ClusterGrowResp{Code: "cutover_revival_failed"}, nil) {
		t.Error("a revival-failure reply MUST be a stable refusal")
	}
}
