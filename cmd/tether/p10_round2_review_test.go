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

func TestReviewUpgradeAllIncludesOnlineNodesWithoutProcesses(t *testing.T) {
	url := testharness.StartNATS(t)
	db := testharness.OpenDB(t)
	pub, fp := testharness.FreshUserPub(t)
	now := time.Now().UTC()

	if _, err := session.Create(db, "lab", "lab", fp, "pin-hash", now); err != nil {
		t.Fatalf("session.Create: %v", err)
	}
	if err := node.Register(db, node.RegisterInput{
		SID:            "lab",
		NID:            "lab-1",
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

	got, err := listOnlineNIDs(context.Background(), nc, "lab", pub)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != "lab-1" {
		t.Fatalf("upgrade --all should include ONLINE nodes even before any process has run; got %v", got)
	}
}
