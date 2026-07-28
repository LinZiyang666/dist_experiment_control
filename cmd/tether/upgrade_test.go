package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/clusterupgrade"
	"github.com/LinZiyang666/tether/internal/proto"
)

// g5_upgrade_test.go — G5 #13/#14 W4: the testable orchestrator cores. The live drive loop (signed
// triggers over NATS + converge waits) is exercised by the N=3 sim drill; here we pin the dry-run plan
// render and — critically — that the ctl signer produces a signature the broker verifier accepts.

func TestRenderUpgradePlan(t *testing.T) {
	var buf bytes.Buffer
	plan := clusterupgrade.Plan{
		Steps: []clusterupgrade.Step{
			{Kind: clusterupgrade.StepSkip, NodeID: "a"},
			{Kind: clusterupgrade.StepUpgrade, NodeID: "c"},
			{Kind: clusterupgrade.StepUpgrade, NodeID: "b", TransferTo: "c"},
		},
		N2WriteFence: true,
	}
	renderUpgradePlan(&buf, plan, "v2")
	s := buf.String()
	for _, want := range []string{"SKIP    a", "UPGRADE c", "transfer to c first", "N=2"} {
		if !strings.Contains(s, want) {
			t.Fatalf("plan render missing %q:\n%s", want, s)
		}
	}
}

// TestSignUpgradeTriggerVerifiable is the cross-package auth contract: the ctl signer (W4) must produce a
// signature the broker responder (W2b) accepts, and any field tamper must break it.
func TestSignUpgradeTriggerVerifiable(t *testing.T) {
	seed, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatal(err)
	}
	pub, err := auth.PublicKeyFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}
	req := &proto.ClusterUpgradeReq{Op: "reload", TargetNode: "brk-a", ToVersion: "v2", ExpectSHA256: "abc"}
	data, err := signUpgradeTrigger(seed, req, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	var parsed proto.ClusterUpgradeReq
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatal(err)
	}
	if parsed.IssuedAt == "" {
		t.Fatal("the signer must stamp issued_at (replay bound)")
	}
	sig, err := hex.DecodeString(parsed.Sig)
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.VerifySignature(pub, proto.CanonicalUpgradeReqBytes(&parsed), sig); err != nil {
		t.Fatalf("the ctl signer must produce a broker-verifiable signature: %v", err)
	}
	parsed.ToVersion = "v3-tampered"
	if auth.VerifySignature(pub, proto.CanonicalUpgradeReqBytes(&parsed), sig) == nil {
		t.Fatal("a field tampered after signing must invalidate the signature")
	}
}
