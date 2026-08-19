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

// origin: docs/reviews/cloned-credential-instances-plan.md §1.1 I1
//
// I1: a device that only ever runs ONE agent must be untouched. `Leased` is
// inferred from the NAME's SHAPE, and `<word>-NN` is an ordinary operator
// naming convention — docs/usage.md ships `gpu-01 gpu-02 gpu-03` as the
// example fleet. A real, single-instance `gpu-02` is therefore classified as an
// ephemeral lease and silently dropped from `node upgrade --all`, told it
// "reverts to the image's binary on restart". It never gets upgraded again.
func TestUpgradeAllStillTargetsARealDeviceNamedLikeALease(t *testing.T) {
	url := testharness.StartNATS(t)
	db := testharness.OpenDB(t)
	pub, fp := testharness.FreshUserPub(t)
	now := time.Now().UTC()

	if _, err := session.Create(db, "lab", "lab", fp, "pin-hash", now); err != nil {
		t.Fatalf("session.Create: %v", err)
	}
	// One real device per usage.md's own example fleet. Each runs exactly one agent.
	for _, nid := range []string{"gpu-01", "gpu-02", "gpu-03"} {
		if err := node.Register(db, node.RegisterInput{
			SID: "lab", NID: nid, ProtoVersion: proto.ProtoVersion,
			ReleaseVersion: proto.ReleaseVersion, OS: "linux", Arch: "amd64",
		}, now); err != nil {
			t.Fatalf("node.Register %s: %v", nid, err)
		}
	}

	b, err := broker.New(broker.Config{
		NATSURL: url, DB: db, Logger: testharness.SilentLog(),
		ReconcileInterval: time.Hour, StaleAfter: time.Hour, OfflineAfter: 2 * time.Hour,
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

	got, skipped, err := listOnlineNIDs(context.Background(), nc, "lab", pub)
	if err != nil {
		t.Fatal(err)
	}
	if skipped != 0 || len(got) != 3 {
		t.Fatalf("upgrade --all dropped real single-agent devices: targets=%v skipped=%d; "+
			"every one of these is a genuine device, none is a clone", got, skipped)
	}
}
