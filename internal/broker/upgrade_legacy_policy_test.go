package broker

import (
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/port"
	"github.com/LinZiyang666/tether/internal/proc"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

// origin: S8 internal review rv-n1 F1
func TestLegacyUpgradeRefusalUsesPositiveBusyEvidence(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseLegacyUpgradeEvidence(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "legacy busy evidence x5", g)
	testharness.AssertNoFDLeak(t, "legacy busy evidence x5", fd, 0)
}

func legacyPolicyTestBroker(t *testing.T) *Broker {
	t.Helper()
	db := testharness.OpenDB(t)
	if _, err := session.Create(db, "lab", "lab", "owner", "pin-hash", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := node.Register(db, node.RegisterInput{SID: "lab", NID: "node", ProtoVersion: proto.ProtoVersion, ReleaseVersion: "v0.6.0"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	return &Broker{cfg: Config{DB: db}}
}

func exerciseLegacyUpgradeEvidence(t *testing.T) {
	t.Helper()
	for _, arm := range []string{"empty", "process", "port", "exited-process", "freed-port", "missing-process-domain", "missing-port-domain", "closed-store", "no-store"} {
		t.Run(arm, func(t *testing.T) {
			b := legacyPolicyTestBroker(t)
			wantCode, wantDetail := "try_again", "legacy_quiescence_unproved"
			switch arm {
			case "process", "missing-port-domain", "exited-process":
				if err := proc.Insert(b.cfg.DB, proc.Process{PID: "pid", SID: "lab", NID: "node", Argv: []string{"sleep", "60"}, StartedAt: time.Now()}); err != nil {
					t.Fatal(err)
				}
				wantCode, wantDetail = "bad_request", "legacy_busy"
				if arm == "exited-process" {
					if err := proc.MarkExited(b.cfg.DB, "pid", "lab", 0, time.Now()); err != nil {
						t.Fatal(err)
					}
					wantCode, wantDetail = "try_again", "legacy_quiescence_unproved"
				}
			case "port", "missing-process-domain", "freed-port":
				allocation, err := port.Allocate(b.cfg.DB, "lab", "node", "live", 8000, 0, "owner", false, nil)
				if err != nil {
					t.Fatal(err)
				}
				wantCode, wantDetail = "bad_request", "legacy_busy"
				if arm == "freed-port" {
					if err := port.Free(b.cfg.DB, allocation.Port, time.Now()); err != nil {
						t.Fatal(err)
					}
					wantCode, wantDetail = "try_again", "legacy_quiescence_unproved"
				}
			case "closed-store":
				if err := b.cfg.DB.Close(); err != nil {
					t.Fatal(err)
				}
			case "no-store":
				b.cfg.DB = nil
			}
			if arm == "missing-process-domain" || arm == "missing-port-domain" {
				table := "processes"
				if arm == "missing-port-domain" {
					table = "port_allocations"
				}
				if _, err := b.cfg.DB.Exec("ALTER TABLE " + table + " RENAME TO unavailable_domain"); err != nil {
					t.Fatal(err)
				}
			}
			if code, detail := legacyUpgradeRefusal(b, "lab", "node"); code != wantCode || detail != wantDetail {
				t.Fatalf("legacy evidence projection = %s/%s, want %s/%s", code, detail, wantCode, wantDetail)
			}
			// Another session/node's live resources cannot label this target busy.
			for _, target := range [][2]string{{"other", "node"}, {"lab", "other"}} {
				if code, detail := legacyUpgradeRefusal(b, target[0], target[1]); code != "try_again" || detail != "legacy_quiescence_unproved" {
					t.Fatalf("foreign busy resource changed projection: %s/%s", code, detail)
				}
			}
		})
	}
}

// The signed cluster child is the production source of ReExecOnly. Signature,
// target selection and the colocated-NID fallback must not authorize bare exec.
// origin: S8 internal review rv-n1 F1
func TestLegacyClusterReexecRequiresPolicyBeforeDispatch(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseLegacyClusterReexec(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "legacy cluster policy x5", g)
	testharness.AssertNoFDLeak(t, "legacy cluster policy x5", fd, 0)
}

func exerciseLegacyClusterReexec(t *testing.T) {
	t.Helper()
	for _, arm := range []string{"idle-unfenced", "busy", "fallback-nid"} {
		t.Run(arm, func(t *testing.T) {
			b := legacyPolicyTestBroker(t)
			seed, err := auth.GenerateUserSeed()
			if err != nil {
				t.Fatal(err)
			}
			b.selfID = "node"
			b.cfg.AuthCallout = &AuthCalloutConfig{AccountSeed: seed}
			if arm != "fallback-nid" {
				b.selfID, b.cfg.ColocatedAgentNID = "broker", "node"
			}
			wantCode, wantDetail := "try_again", "legacy_quiescence_unproved"
			if arm == "busy" {
				if err := proc.Insert(b.cfg.DB, proc.Process{PID: "pid", SID: "lab", NID: "node", Argv: []string{"sleep", "60"}, StartedAt: time.Now()}); err != nil {
					t.Fatal(err)
				}
				wantCode, wantDetail = "bad_request", "legacy_busy"
			}
			nc, err := nats.Connect(testharness.StartNATS(t))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(nc.Close)
			b.nc.Store(nc)
			var forwards atomic.Int64
			_, err = nc.Subscribe(proto.SubjCmdForwarded("lab", "node", "upgrade"), func(m *nats.Msg) {
				forwards.Add(1)
				_ = m.Respond([]byte(`{"ok":true}`))
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := nc.FlushTimeout(time.Second); err != nil {
				t.Fatal(err)
			}
			var adminCalls int
			b.setClusterAdminHandle(func(adminsock.Request) adminsock.Response {
				adminCalls++
				return adminsock.Response{OK: true}
			})
			req := &proto.ClusterUpgradeReq{Op: "reexec-agent", TargetNode: b.selfID, SID: "lab", IssuedAt: triggerNow.Format(time.RFC3339)}
			resp := b.handleUpgradeTrigger(signUpgradeReq(t, seed, req), triggerNow)
			if resp == nil || resp.OK || resp.Code != wantCode || resp.Error != wantDetail || forwards.Load() != 0 || adminCalls != 0 {
				t.Fatalf("legacy cluster child bypassed policy: response=%+v forwarded=%d admin=%d", resp, forwards.Load(), adminCalls)
			}
		})
	}
}
