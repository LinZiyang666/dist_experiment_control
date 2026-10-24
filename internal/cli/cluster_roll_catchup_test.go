package cli

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// The acquire-lock ACK can precede its application on either survivor. Every
// retry must use a fresh leader cursor, keep the configured voter set, and send
// at most one transfer to the destination whose leadership is later proved.
// origin: hot-update S6 plan §5 / T38 / HARDGATE-B request 3
func TestBrokerRollWaitsForLockCommitCatchUp(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseBrokerRollLockCommitCatchUp(t)
	}
	testharness.AssertNoGoroutineLeak(t, "broker roll lock commit catch-up", g)
	testharness.AssertNoFDLeak(t, "broker roll lock commit catch-up", fd, 0)
}

func exerciseBrokerRollLockCommitCatchUp(t *testing.T) {
	t.Helper()
	for _, scenario := range []string{"catch-up", "elected-survivor", "elected-survivor-unconfirmed", "membership-drift", "health-error", "observer-lost", "deadline", "cancelled-caught-up", "wrong-destination", "old-follower-unconfirmed"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			deadline, _ := ctx.Deadline()
			reads, transfers, reloads := 0, 0, 0
			leader, reloaded := "b", false
			var ops []string
			io := ClusterRollIO{
				Health: func(callCtx context.Context) ([]proto.ClusterHealthResp, error) {
					if got, _ := callCtx.Deadline(); got != deadline {
						t.Fatalf("health reset the absolute step deadline: %s != %s", got, deadline)
					}
					reads++
					hs := rollHealth(leader)
					if transfers == 0 && !reloaded && leader == "b" {
						// Index 11 is the just-ACKed holder command. A further
						// commit reaches 12 while a catches up only to 11.
						hs[1].AppliedIndex = 11
						if reads > 1 {
							switch scenario {
							case "elected-survivor":
								leader = "a"
								hs = rollHealth(leader)
							case "elected-survivor-unconfirmed":
								hs = rollHealth("a")
								hs[1].LeaderID = "b"
								if reads == 3 {
									cancel()
								}
							case "membership-drift":
								hs[2].NodeID = "replacement"
							case "health-error":
								return nil, errors.New("fresh read failed")
							case "observer-lost":
								hs[0].BrokerImage = nil
							case "deadline":
								// Cancellation is deterministic and must interrupt
								// lag polling before it sends any unsafe request.
								cancel()
							case "cancelled-caught-up":
								hs[0].AppliedIndex = hs[1].AppliedIndex
								cancel() // a qualifying reply arrived after the caller canceled
							default:
								if reads == 2 {
									return nil, ErrIncompleteBrokerHealth
								}
								if reads == 3 {
									hs[1].WritableLeaderConfirmed = false
								}
								hs[1].AppliedIndex, hs[0].AppliedIndex = 12, 11
								if reads >= 5 {
									hs[0].AppliedIndex = 12
								}
								hs[2].AppliedIndex = 1 // stale plan hint c remains unsafe
							}
						}
					}
					if scenario == "wrong-destination" && transfers > 0 {
						hs = rollHealth("c")
						hs[1].LeaderID = "a" // the old follower alone still reports the requested a
						if reads == 7 {
							cancel() // one live-budget mismatch must already have prevented the cut
						}
					}
					if scenario == "old-follower-unconfirmed" && transfers > 0 {
						hs[1].LeaderID = "c"
						if reads == 7 {
							cancel() // a's proof alone cannot prove b has followed it
						}
					}
					if reloaded {
						hs[1].BrokerImage = &proto.BrokerImageStatus{RunningSHA256: rollNewSHA, Generation: 8, CutID: "cut-b", Mode: "quorum", Phase: "serving"}
					}
					return hs, nil
				},
				Trigger: func(callCtx context.Context, req *proto.ClusterUpgradeReq) (*proto.ClusterUpgradeResp, error) {
					if scenario == "elected-survivor-unconfirmed" {
						t.Fatal("roll sent a request before the old target followed the elected survivor")
					}
					if got, _ := callCtx.Deadline(); got != deadline || callCtx.Err() != nil {
						t.Fatalf("trigger escaped the original deadline: %s %v", got, callCtx.Err())
					}
					ops = append(ops, req.Op)
					if (scenario == "wrong-destination" || scenario == "old-follower-unconfirmed") && req.Op != "transfer-leader" {
						t.Fatal("roll sent a request before both leadership proofs agreed on the requested destination")
					}
					switch req.Op {
					case "transfer-leader":
						transfers++
						if reads != 5 || req.TargetNode != "b" || req.TransferTo != "a" {
							t.Fatalf("transfer used stale leader cursor or destination: reads=%d req=%+v", reads, req)
						}
						leader = req.TransferTo
						return &proto.ClusterUpgradeResp{OK: true}, nil
					case "maintenance":
						return rollMaintenanceResponse(t, req), nil
					case "reload":
						if leader != "a" || req.TargetNode != "b" {
							t.Fatalf("reload bypassed the fresh leadership proof: %s %+v", leader, req)
						}
						reloads++
						reloaded = true
						return &proto.ClusterUpgradeResp{OK: true, Reloading: true, NextGeneration: 8}, nil
					default:
						return nil, fmt.Errorf("unexpected roll operation %s", req.Op)
					}
				},
			}
			err := RollBrokerStep(ctx, io, rollConfig())
			if scenario == "catch-up" || scenario == "elected-survivor" {
				want := "transfer-leader,maintenance,reload,maintenance"
				if scenario == "elected-survivor" {
					want = "maintenance,reload,maintenance"
				}
				if err != nil || reloads != 1 || strings.Join(ops, ",") != want {
					t.Fatalf("lock commit lag refused a safe roll: err=%v reads=%d ops=%v", err, reads, ops)
				}
				return
			}
			proofRefusal := scenario == "wrong-destination" || scenario == "old-follower-unconfirmed"
			if err == nil || reloads != 0 || (!proofRefusal && transfers != 0) {
				t.Fatalf("unsafe poll result: err=%v reads=%d transfers=%d reloads=%d", err, reads, transfers, reloads)
			}
			if (scenario == "deadline" || scenario == "cancelled-caught-up") && (!errors.Is(err, context.Canceled) || reads != 2) {
				t.Fatalf("lag wait did not preserve cancellation: reads=%d err=%v", reads, err)
			}
			for kind, detail := range map[string]string{"membership-drift": "configured voter set changed", "health-error": "fresh read failed", "observer-lost": "maintenance_observer_unsupported"} {
				if scenario == kind && (reads != 2 || !strings.Contains(err.Error(), detail)) {
					t.Fatalf("terminal %s was retried as apply lag: reads=%d err=%v", kind, reads, err)
				}
			}
			if proofRefusal && (transfers != 1 || reads != 7) {
				t.Fatalf("transfer destination was not held through verification: transfers=%d reads=%d err=%v", transfers, reads, err)
			}
		})
	}
}
