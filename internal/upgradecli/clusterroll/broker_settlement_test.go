package clusterroll

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/brokermaint"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/upgradewire"
)

// This is a deterministic transport-boundary test of the real S6/S8 driver.
// It supplies topology observations, never product completion receipts. A
// transient election after one child must not discard its accepted parent or
// repeat an unsafe Start while waiting for the next original child.
// origin: S8 internal review ROLL-SETTLEMENT F3; hot-update S8 plan §3.1 / §5.1
func TestBrokerRollSettlesTopologyBeforeDispatchingOriginalChild(t *testing.T) {
	g, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseBrokerTopologySettlement(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "broker topology settlement x5", g)
	testharness.AssertNoFDLeak(t, "broker topology settlement x5", fds, 0)
}

func exerciseBrokerTopologySettlement(t *testing.T) {
	t.Helper()
	for _, fault := range []string{"incomplete", "election", "stale-contact", "accepted-unarmed"} {
		t.Run(fault, func(t *testing.T) {
			key := upgradetx.Key{Component: upgradetx.ComponentBroker, DomainID: "original-domain", ConfigPath: "/fixture/broker.yaml"}
			grant := upgradetx.Grant{Handle: upgradetx.Handle{Key: key, TxID: strings.Repeat("a", 32), CutID: strings.Repeat("b", 32), Generation: 2, Revision: 1},
				Issuer:     upgradetx.Owner{BootID: "original-boot", PID: 123, StartTicks: 456, InstanceID: "original-instance"},
				ParentTxID: strings.Repeat("c", 32), ParentSlot: "target:original-slot", ActorFP: "SHA256:actor", ClientNonce: "original-client",
				SourceRelease: "v1", TargetRelease: "v2", SourceSHA: strings.Repeat("1", 64), TargetSHA: strings.Repeat("2", 64),
				ScopeDigest: strings.Repeat("3", 64), CapabilityDigest: strings.Repeat("4", 64), Mode: upgradetx.ModeHot, ExpiresAt: time.Now().Add(time.Minute).UnixNano()}
			artifact := upgradeartifact.Receipt{Grant: grant, SourceSHA: grant.SourceSHA, TargetSHA: grant.TargetSHA, ManifestDigest: strings.Repeat("5", 64), CanonicalPath: "/fixture/tether", SourcePath: "/fixture/source", TargetPath: "/fixture/target"}
			reads, starts, holds, ends := 0, 0, 0, 0
			c := &clusterClient{parent: upgradetx.Grant{Handle: upgradetx.Handle{TxID: grant.ParentTxID}}}
			c.ports.Health = func(context.Context) ([]proto.ClusterHealthResp, error) {
				reads++
				rows := []proto.ClusterHealthResp{
					{NodeID: "target", IsVoter: true, LeaderID: "leader", AppliedIndex: 10, BrokerImage: &proto.BrokerImageStatus{Generation: 1, RunningSHA256: grant.SourceSHA, Phase: "serving", Mode: "quorum"}},
					{NodeID: "leader", IsVoter: true, LeaderID: "leader", WritableLeaderConfirmed: true, AppliedIndex: 10, BrokerImage: &proto.BrokerImageStatus{Generation: 1}},
					{NodeID: "peer", IsVoter: true, LeaderID: "leader", AppliedIndex: 10, BrokerImage: &proto.BrokerImageStatus{Generation: 1}},
				}
				// The first read establishes the configured voters. The next read
				// races the preceding broker's role convergence before any effect.
				if reads == 2 {
					switch fault {
					case "incomplete":
						return nil, cli.ErrIncompleteBrokerHealth
					case "election":
						rows[1].WritableLeaderConfirmed = false
					case "stale-contact":
						rows[0].LeaderContactStale = true
					}
				}
				if starts != 0 {
					rows[0].BrokerImage = &proto.BrokerImageStatus{Generation: 2, RunningSHA256: grant.TargetSHA, Phase: "serving", Mode: "quorum", CutID: grant.Handle.CutID}
				}
				return rows, nil
			}
			c.ports.Trigger = func(_ context.Context, r *proto.ClusterUpgradeReq) (*proto.ClusterUpgradeResp, error) {
				if reads < 3 && fault != "accepted-unarmed" {
					t.Fatal("effect dispatched before topology settled")
				}
				switch r.Op {
				case "maintenance":
					if r.Holder != grant.ParentTxID || r.CutID != grant.Handle.CutID || r.TargetNode != "leader" {
						t.Fatal("maintenance changed the accepted parent or child")
					}
					if r.Maintenance != nil {
						ends++
						return &proto.ClusterUpgradeResp{OK: true}, nil
					}
					holds++
					return &proto.ClusterUpgradeResp{OK: true, Maintenance: &brokermaint.Record{Phase: brokermaint.MaintActive, Holder: r.Holder, CutID: r.CutID, Home: "target", Generation: 1}}, nil
				case "upgrade-tx":
					var request upgradewire.Request
					if err := json.Unmarshal(r.Upgrade, &request); err != nil {
						t.Fatal(err)
					}
					if request.Op != upgradewire.OpStart || request.Grant != grant || request.Artifact != artifact || r.Holder != grant.ParentTxID || r.TargetNode != "target" {
						t.Fatal("settlement changed the original Start authorization")
					}
					starts++
					detail := "broker_activation_dispatched"
					if fault == "accepted-unarmed" {
						detail = "broker store cut was not armed"
					}
					body, err := upgradewire.EncodeResponse(upgradewire.Response{Code: upgradetx.CodeUnsettled, Error: detail})
					if err != nil {
						t.Fatal(err)
					}
					return &proto.ClusterUpgradeResp{Code: upgradetx.CodeUnsettled, Upgrade: body}, nil
				default:
					t.Fatalf("unexpected effect: %s", r.Op)
					return nil, nil
				}
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			_, err := c.startBroker(ctx, grant, artifact)
			if fault == "accepted-unarmed" {
				var accepted *upgradetx.Error
				if !errors.As(err, &accepted) || !accepted.Accepted || accepted.Code != upgradetx.CodeUnsettled || accepted.Detail != "broker store cut was not armed" || starts != 1 || ends != 0 {
					t.Fatalf("accepted child became an untyped terminal instead of a queryable result: starts=%d ends=%d err=%v", starts, ends, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("original parent abandoned a transient topology: %v", err)
			}
			if starts != 1 || holds != 1 || ends != 1 {
				t.Fatalf("original child was not settled exactly once: starts=%d holds=%d ends=%d", starts, holds, ends)
			}
		})
	}
}
