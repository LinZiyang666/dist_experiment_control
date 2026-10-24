package broker

// Copy into internal/broker. The adapter is an effect counter; the tested
// signature verification and production routing are the real implementation.
import (
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/port"
	"github.com/LinZiyang666/tether/internal/proc"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// origin: S8 internal review rv3-leaves F2
func TestLegacySignedReloadRequiresPolicyBeforeRouting(t *testing.T) {
	seed, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatal(err)
	}
	var routed []adminsock.Request
	b := newTriggerTestBroker("broker", seed, triggerNow, &routed)
	req := &proto.ClusterUpgradeReq{Op: "reload", TargetNode: "broker", ToVersion: "v0.6.0", ExpectSHA256: strings.Repeat("a", 64), IssuedAt: triggerNow.Format(time.RFC3339)}
	resp := b.handleUpgradeTrigger(signUpgradeReq(t, seed, req), triggerNow)
	if resp == nil || resp.OK || len(routed) != 0 {
		t.Fatalf("unfenced legacy reload reached execution adapter: response=%+v routed=%d", resp, len(routed))
	}
}

// origin: S8 internal review rv3-leaves F2
func TestLegacyReloadPolicyCoversLocalAndSignedEntries(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseLegacyReloadPolicy(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "legacy reload policy x5", g)
	testharness.AssertNoFDLeak(t, "legacy reload policy x5", fd, 0)
}

func exerciseLegacyReloadPolicy(t *testing.T) {
	t.Helper()
	for _, arm := range []string{"busy-process", "busy-port", "idle-unfenced", "unknown-store", "missing-reader"} {
		t.Run(arm, func(t *testing.T) {
			b := legacyPolicyTestBroker(t)
			wantCode, wantDetail := "try_again", "legacy_quiescence_unproved"
			switch arm {
			case "busy-process":
				if err := proc.Insert(b.cfg.DB, proc.Process{PID: "original", SID: "lab", NID: "node", Argv: []string{"sleep", "60"}, StartedAt: triggerNow}); err != nil {
					t.Fatal(err)
				}
				wantCode, wantDetail = "bad_request", "legacy_busy"
			case "busy-port":
				if _, err := port.Allocate(b.cfg.DB, "lab", "node", "original", 8000, 0, "owner", false, nil); err != nil {
					t.Fatal(err)
				}
				wantCode, wantDetail = "bad_request", "legacy_busy"
			case "unknown-store":
				if err := b.cfg.DB.Close(); err != nil {
					t.Fatal(err)
				}
			}
			seed, err := auth.GenerateUserSeed()
			if err != nil {
				t.Fatal(err)
			}
			b.selfID, b.cfg.AuthCallout = "broker", &AuthCalloutConfig{AccountSeed: seed}
			routed := 0
			b.clusterAdminHandle = func(adminsock.Request) adminsock.Response {
				routed++
				return adminsock.Response{OK: true, Reloading: true}
			}
			local := (&brokerReloadBackend{broker: b}).HandleCluster(adminsock.Request{Op: adminsock.OpBrokerUpgradeReload, ToVersion: "v0.6.0", ExpectSHA256: strings.Repeat("a", 64)})
			req := &proto.ClusterUpgradeReq{Op: "reload", TargetNode: b.selfID, ToVersion: "v0.6.0", ExpectSHA256: strings.Repeat("a", 64), IssuedAt: triggerNow.Format(time.RFC3339)}
			signed := b.handleUpgradeTrigger(signUpgradeReq(t, seed, req), triggerNow)
			if signed == nil || signed.OK || signed.Reloading || local.OK || local.Reloading || routed != 0 || b.reloadSignal.Requested() ||
				local.Code != wantCode || signed.Code != wantCode || local.Error != wantDetail || signed.Error != wantDetail {
				t.Fatalf("legacy %s policy differs by entry: local=%+v signed=%+v routed=%d", arm, local, signed, routed)
			}
		})
	}
}
