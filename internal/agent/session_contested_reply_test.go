package agent

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

// origin: docs/reviews/cloned-credential-instances-plan.md §3.1
//
// A CONTESTED register reply is not a successful register: the broker returned
// before registerNode, so it carries no AcceptedProcesses, no reconcile
// directives and no roster. onNATSReconnect now returns on ANY non-nil Lease
// for exactly that reason. Agent.session does not: when the offered name fails
// acceptableLeaseName it logs a Warn and FALLS THROUGH to
// courier.onRegisterSuccess, whose rule is "an exit the broker did not accept
// has already been delivered" — so every pending proc.exit is deleted and the
// rc is lost forever, leaving the row RUNNING in `tether ps`.
//
// This is reachable on a device running exactly ONE agent: any device whose
// configured nid is itself lease-shaped (`gpu-02`, `worker-07` — docs/usage.md's
// own example fleet) can only ever be offered a name under the `gpu` basename,
// which acceptableLeaseName correctly refuses.
func TestContestedRegisterReplyDoesNotSettleThePendingExitQueue(t *testing.T) {
	url := testharness.StartNATS(t)
	a, err := New(Config{
		NATSURL:           url,
		SID:               "lab",
		NID:               "gpu-02", // a REAL device whose name is lease-shaped
		Logger:            testharness.SilentLog(),
		HeartbeatInterval: time.Hour,
		RegisterTimeout:   2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	// An exit that has not yet been delivered to the broker.
	a.courier.enqueueExit("01hpid", 7)
	if len(a.courier.pendingExitSnapshot()) != 1 {
		t.Fatalf("harness: pending exit not enqueued")
	}

	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	// A broker that contests: it assigns a name under the `gpu` basename, which
	// this agent must refuse, and — per the O2 ordering — it TOUCHED NOTHING,
	// so the reply carries no AcceptedProcesses.
	sub, err := nc.Subscribe(proto.SubjNodeRegister("lab", "gpu-02"), func(m *nats.Msg) {
		body, _ := json.Marshal(proto.NodeRegisterResp{
			OK:    true,
			Lease: &proto.NodeLease{AssignedNID: "gpu-03", Basename: "gpu"},
		})
		_ = m.Respond(body)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = a.session(ctx)
	}()
	<-done

	if got := len(a.courier.pendingExitSnapshot()); got != 1 {
		t.Fatalf("the pending proc.exit was discarded by a CONTESTED register reply (pending=%d).\n"+
			"Agent.session's refusal arm logs a Warn and then falls through to "+
			"courier.onRegisterSuccess with a reply that has no AcceptedProcesses, so every "+
			"undelivered exit is deleted: the ctl waiting on it never gets an rc and `tether ps` "+
			"keeps the row RUNNING. onNATSReconnect already returns on any non-nil Lease; "+
			"session() must do the same.", got)
	}
}
