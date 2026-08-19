package main

import (
	"context"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

// origin: docs/reviews/cloned-credential-instances-plan.md §6 N-1 matrix
// ("旧 agent × 新 broker ⇒ 与今天逐字节相同") + §4.1 ("Leased ... 缺失 ⇒
// multiplicity-1 的 --json 逐字节不变").
//
// A device an OPERATOR named `<something>-NN` — `gpu-02`, `rack-15`, `dgx-04`
// — is not a lease. It has its own agent.yaml, its own nkey, its own
// agent_provisioning row, and it may well predate this increment entirely
// (a pre-feature agent never sends an instance id, so the broker never
// adjudicates anything for it). Deriving NodeListEntry.Leased from the NAME
// SHAPE alone makes the broker call it ephemeral anyway, and
// `tether node upgrade --all` then silently drops it from the fleet rollout.
//
// MUTATION that must turn this red once fixed: re-derive Leased from
// proto.SplitLeaseName(n.NID) alone in internal/broker/exec.go's
// handleNodeListReq.
func TestOperatorNamedNodeEndingInDigitsIsNotTreatedAsALease(t *testing.T) {
	url := testharness.StartNATS(t)
	db := testharness.OpenDB(t)
	pub, fp := testharness.FreshUserPub(t)
	now := time.Now().UTC()

	if _, err := session.Create(db, "lab", "lab", fp, "pin-hash", now); err != nil {
		t.Fatalf("session.Create: %v", err)
	}
	// One device. Nobody else in the session — there is no `gpu` basename row,
	// so this cannot be a lease under any reading.
	if err := node.Register(db, node.RegisterInput{
		SID:            "lab",
		NID:            "gpu-02",
		ProtoVersion:   proto.ProtoVersion,
		ReleaseVersion: proto.ReleaseVersion,
		OS:             "linux",
		Arch:           "amd64",
		BootID:         "boot-1",
	}, now); err != nil {
		t.Fatalf("node.Register: %v", err)
	}

	b, err := broker.New(broker.Config{
		NATSURL:           url,
		DB:                db,
		Logger:            testharness.SilentLog(),
		ReconcileInterval: time.Hour,
		StaleAfter:        time.Hour,
		OfflineAfter:      2 * time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("broker did not exit")
		}
	})
	time.Sleep(150 * time.Millisecond)

	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	got, skippedLeased, err := listOnlineNIDs(context.Background(), nc, "lab", pub)
	if err != nil {
		t.Fatal(err)
	}
	if skippedLeased != 0 {
		t.Errorf("an operator-named device must not be reported as an excluded lease; skipped=%d", skippedLeased)
	}
	if len(got) != 1 || got[0] != "gpu-02" {
		t.Errorf("`node upgrade --all` silently dropped a real device whose name merely LOOKS like a lease; got %v, want [gpu-02]", got)
	}
}
