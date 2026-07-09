package main

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/clusterroster"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// TestExternalReviewBuildUpgradeNodesRejectsResponderAbsentFromRosterEvenPreG5 pins the mixed-version
// stale-manifest hole: IsVoter is additive, so a real pre-G5 voter can answer health without setting it.
// If the signed roster cache omits that responding broker, the upgrade planner must still fail closed
// instead of silently planning over only the stale roster's voters.
func TestExternalReviewBuildUpgradeNodesRejectsResponderAbsentFromRosterEvenPreG5(t *testing.T) {
	url := startCLIExternalReviewNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer nc.Close()

	seed, pub := cliExternalAccount(t)
	now := time.Now().UTC()
	roster, err := clusterroster.Build(seed, pub, 1,
		[]proto.RosterBroker{{NodeID: "brk-a", PublicHost: "a.example.com", Phase: proto.RosterPhaseVoter}},
		now.Format(time.RFC3339Nano), now.Add(time.Hour).Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}
	manifest, _ := json.Marshal(proto.ClusterManifest{SchemaVersion: proto.ClusterManifestSchemaVersion, Roster: roster})
	actor, sid := "Uactor-g5-review", "lab"
	if _, err := nc.Subscribe(proto.SubjCtrlClusterRoster(actor), func(m *nats.Msg) {
		if m.Reply != "" {
			_ = m.Respond(manifest)
		}
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := nc.Subscribe(proto.SubjCtrlNodeList(actor, sid), func(m *nats.Msg) {
		body, _ := json.Marshal(proto.NodeListResp{})
		_ = m.Respond(body)
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := nc.Subscribe(proto.SubjCtrlClusterHealth(actor), func(m *nats.Msg) {
		a, _ := json.Marshal(proto.ClusterHealthResp{NodeID: "brk-a", ReleaseVersion: "v1", IsVoter: true})
		// brk-b models a pre-G5 broker: it is a real broker health responder, but the additive IsVoter
		// field is absent/false. A stale roster that omits it must be treated as unsafe.
		b, _ := json.Marshal(proto.ClusterHealthResp{NodeID: "brk-b", ReleaseVersion: "v1"})
		_ = m.Respond(a)
		_ = m.Respond(b)
	}); err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	nodes, err := buildUpgradeNodes(context.Background(), nc, actor, sid, pub, io.Discard)
	if err == nil {
		t.Fatalf("expected stale roster refusal for responder absent from signed roster, got nodes=%+v", nodes)
	}
	if !strings.Contains(err.Error(), "absent from the signed roster") {
		t.Fatalf("refusal should explain the responder/roster mismatch, got %v", err)
	}
}
