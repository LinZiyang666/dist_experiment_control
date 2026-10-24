package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// origin: hot-update S6 plan §5 / T45 / lead L3
func TestBrokerScopeAndTransfersDeferred(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseBrokerScopeAndTransfers(t)
	}
	testharness.AssertNoGoroutineLeak(t, "broker command IO", beforeG)
	testharness.AssertNoFDLeak(t, "broker command IO", beforeFD, 0)
}

func exerciseBrokerScopeAndTransfers(t *testing.T) {
	t.Helper()
	for _, verdict := range []string{"busy", "census-error", "terminal", "runtime-unknown", "unknown-result"} {
		ctx, cancel := context.WithCancel(t.Context())
		path := filepath.Join(t.TempDir(), "admin.sock")
		ln, err := net.Listen("unix", path)
		if err != nil {
			t.Fatal(err)
		}
		oldSHA, newSHA := strings.Repeat("a", 64), strings.Repeat("b", 64)
		var mu sync.Mutex
		reloads, reads := 0, 0
		var ops []string
		done := make(chan struct{})
		go func() {
			defer close(done)
			for {
				c, err := ln.Accept()
				if err != nil {
					return
				}
				_ = c.SetDeadline(time.Now().Add(time.Second))
				var req adminsock.Request
				err = json.NewDecoder(c).Decode(&req)
				if err != nil {
					_ = c.Close()
					continue
				}
				mu.Lock()
				ops = append(ops, req.Op)
				image := &proto.BrokerImageStatus{RunningSHA256: oldSHA, Generation: 4, Mode: "single", Phase: "serving"}
				resp := adminsock.Response{OK: true}
				dropReply := false
				switch req.Op {
				case adminsock.OpBrokerUpgradeReload:
					reloads++
					if req.ExpectSHA256 != newSHA {
						resp = adminsock.Response{Code: adminsock.CodeBadRequest, Error: "wrong candidate"}
						break
					}
					switch verdict {
					case "unknown-result":
						dropReply = true
					case "busy":
						resp = adminsock.Response{Code: adminsock.CodeUpgradeDeferred, Error: "deferred(transfers_in_flight)"}
					case "census-error":
						resp = adminsock.Response{Code: adminsock.CodeStoreError, Error: "transfer census read failed"}
					case "terminal":
						image.CutID = "local-cut"
						resp.Reloading = true
						resp.NextGeneration = 5
						resp.BrokerImage = image
					}
				case adminsock.OpRuntime:
					reads++
					if reloads > 0 && verdict != "unknown-result" {
						image.RunningSHA256 = newSHA
						image.Generation = 5
						image.CutID = "local-cut"
					}
					if verdict == "runtime-unknown" {
						image.Generation = 0
					}
					resp.Runtime = &adminsock.RuntimeReport{BrokerImage: image}
					if verdict == "unknown-result" && reloads > 0 {
						cancel()
					}
				default:
					resp = adminsock.Response{Code: adminsock.CodeBadRequest, Error: "unexpected op"}
				}
				mu.Unlock()
				if !dropReply {
					_ = json.NewEncoder(c).Encode(resp)
				}
				_ = c.Close()
			}
		}()
		cmd := newRootCmd()
		var output bytes.Buffer
		cmd.SetOut(&output)
		cmd.SetErr(&output)
		cmd.SetArgs([]string{"broker", "upgrade", "reload", "--socket", path, "--to-version", "v2", "--expect-sha256", newSHA})
		err = cmd.ExecuteContext(ctx)
		cancel()
		_ = ln.Close()
		<-done
		mu.Lock()
		gotReloads, gotReads := reloads, reads
		gotOps := append([]string(nil), ops...)
		mu.Unlock()
		if verdict != "runtime-unknown" && gotReloads != 1 {
			t.Fatalf("%s: unsafe reload sends=%d", verdict, gotReloads)
		}
		for _, op := range gotOps {
			if op != adminsock.OpRuntime && op != adminsock.OpBrokerUpgradeReload {
				t.Fatalf("scope touched another process: %s", op)
			}
		}
		switch verdict {
		case "runtime-unknown":
			if err == nil || gotReloads != 0 || gotReads != 1 {
				t.Fatalf("unknown runtime entered cut: err=%v reloads=%d reads=%d", err, gotReloads, gotReads)
			}
		case "busy":
			var ee *ExitError
			if err == nil || !strings.Contains(err.Error(), "transfers_in_flight") || !errors.As(err, &ee) || ee.Class != exitTransient || gotReads != 1 {
				t.Fatalf("busy census: err=%v reads=%d", err, gotReads)
			}
		case "unknown-result":
			var ee *ExitError
			if err == nil || !strings.Contains(err.Error(), "control_result_unknown") || !errors.As(err, &ee) || ee.Class != exitUnsettled || ee.Code != proto.CodeAgentNoResponders {
				t.Fatalf("sent result was retryable: %v", err)
			}
		case "census-error":
			var ee *ExitError
			if !errors.As(err, &ee) || ee.Class != exitInternal || ee.Code != adminsock.CodeStoreError {
				t.Fatalf("store refusal lost its namespace: %v", err)
			}
			if err == nil || !strings.Contains(err.Error(), "store_error") || strings.Contains(err.Error(), "deferred") || gotReads != 1 {
				t.Fatalf("census read failure hidden: err=%v reads=%d", err, gotReads)
			}
		case "terminal":
			if err != nil {
				t.Fatalf("terminal transfer did not allow reload: %v", err)
			}
			var image proto.BrokerImageStatus
			if err := json.Unmarshal(output.Bytes(), &image); err != nil {
				t.Fatal(err)
			}
			if image.RunningSHA256 != newSHA || image.Generation != 5 || image.Phase != "serving" || gotReads != 2 {
				t.Fatalf("armed reply falsely completed: %+v reads=%d", image, gotReads)
			}
		}
	}
	root := newRootCmd()
	for _, path := range [][]string{{"broker", "upgrade", "reload"}, {"broker", "upgrade", "status"}} {
		found, args, err := root.Find(path)
		if err != nil || len(args) != 0 || found.Name() != path[2] {
			t.Fatalf("missing local command %v", path)
		}
	}
	node, _, err := root.Find([]string{"node", "upgrade"})
	if err != nil || node.Flags().Lookup("broker") != nil {
		t.Fatal("node upgrade acquired a broker alias")
	}
	if newClusterUpgradeCmd().Flags().Lookup("broker-only") == nil {
		t.Fatal("cluster upgrade lacks product scope")
	}
}

// origin: hot-update S6 plan §9.1 / T46
func TestBrokerUpgradeSignerValidatesExtensions(t *testing.T) {
	seed, pub := cliExternalAccount(t)
	req := &proto.ClusterUpgradeReq{Op: "reload", TargetNode: "b", Holder: "h", Scope: "broker-daemon", CutID: "cut"}
	body, err := signUpgradeTrigger(seed, req, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	var signed proto.ClusterUpgradeReq
	if err := json.Unmarshal(body, &signed); err != nil {
		t.Fatal(err)
	}
	sig, err := hex.DecodeString(signed.Sig)
	if err != nil || auth.VerifySignature(pub, proto.CanonicalUpgradeReqBytes(&signed), sig) != nil {
		t.Fatal("extensions are not verifiable")
	}
	for _, invalid := range []*proto.ClusterUpgradeReq{nil, {Scope: "broker"}, {Holder: "first\nsecond"}, {CutID: "bad\xff"}} {
		if _, err := signUpgradeTrigger(seed, invalid, time.Now()); err == nil {
			t.Fatalf("signer accepted ambiguous request: %+v", invalid)
		}
	}
}

// origin: hot-update S6 plan §5 / T45
// origin: S8 internal review upgrade-maketest-2 F5; hot-update S8 plan §5.1 / §9.4
// origin: S8 internal review SIGNED-ROLL-FIXTURES F5
func TestBrokerDaemonRollLeavesAgentUntouched(t *testing.T) {
	// origin: hot-update S10 plan L17 / §11.1 / POLISH-B
	if runIsolatedBinaryTest(t) {
		return
	}
	binary := signedRollBuild(t)
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	// Each round owns its mutable homes, ports and journals. Join two lanes
	// before the shared census; serial construction exceeded the 420s limit.
	// origin: S10 internal review E2E-PARALLEL-REAL-FAILS F5; plan L17 / §11.1
	lanes := make(chan struct{}, 2)
	t.Run("rounds", func(t *testing.T) {
		for round := 0; round < 5; round++ {
			t.Run(fmt.Sprint(round), func(t *testing.T) {
				t.Parallel()
				lanes <- struct{}{}
				defer func() { <-lanes }()
				exerciseBrokerDaemonRoll(t, binary)
			})
		}
	})
	testharness.AssertNoGoroutineLeak(t, "broker daemon roll", beforeG)
	testharness.AssertNoFDLeak(t, "broker daemon roll", beforeFD, 0)
}

func exerciseBrokerDaemonRoll(t *testing.T, binary string) {
	t.Helper()
	exerciseSignedBrokerDaemonRoll(t, binary)
}
