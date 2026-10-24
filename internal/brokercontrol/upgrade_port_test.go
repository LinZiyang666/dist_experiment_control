package brokercontrol

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/upgradewire"
)

type adminUpgradeEngine struct {
	upgradeport.Endpoint
	calls   atomic.Int64
	entered chan context.Context
}

func (e *adminUpgradeEngine) Prepare(ctx context.Context, _ upgradetx.Key, _ upgradetx.PrepareRequest) (upgradetx.Grant, error) {
	e.calls.Add(1)
	e.entered <- ctx
	<-ctx.Done()
	return upgradetx.Grant{}, ctx.Err()
}

// origin: S7 internal review GATE-CONVERGE-layering F2
func TestAdminUpgradePortJoinsOwnedCancellation(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseAdminUpgradePort(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "admin upgrade port", beforeG)
	testharness.AssertNoFDLeak(t, "admin upgrade port", beforeFD, 0)
}

func exerciseAdminUpgradePort(t *testing.T) {
	key := upgradetx.Key{Component: upgradetx.ComponentBroker, DomainID: strings.Repeat("d", 32), ConfigPath: "/fixture/broker.yaml"}
	request := upgradewire.Request{Op: upgradewire.OpPrepare, Key: key, Prepare: upgradetx.PrepareRequest{
		ClientNonce: "nonce", ActorFP: "actor", SourceRelease: "v1", TargetRelease: "v2", Mode: upgradetx.ModeHot,
		SourceSHA: strings.Repeat("a", 64), TargetSHA: strings.Repeat("b", 64), CapabilityDigest: strings.Repeat("c", 64), ExpiresAt: time.Now().Add(time.Minute).UnixNano(),
		Scope: upgradetx.Scope{ID: "scope", Participants: []upgradetx.Key{key}, Domains: []upgradetx.Domain{upgradetx.DomainControl}},
	}}
	body, err := upgradewire.EncodeRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	access := upgradewire.Access{Key: key, Administrator: true}
	for _, mode := range []string{"already_stopped", "stop", "deadline", "expired"} {
		t.Run(mode, func(t *testing.T) {
			engine := &adminUpgradeEngine{entered: make(chan context.Context, 1)}
			port := BindAdminUpgrade(engine)
			stop := make(chan struct{})
			defer closeIfOpen(stop)
			deadline, err := controlbudget.After(time.Second)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "already_stopped" {
				close(stop)
			}
			if mode == "expired" {
				deadline = 1
			}
			if mode == "deadline" {
				deadline, err = controlbudget.After(50 * time.Millisecond)
				if err != nil {
					t.Fatal(err)
				}
			}
			done := make(chan upgradewire.Response, 1)
			go func() { done <- port(body, access, deadline, stop) }()
			var admitted context.Context
			if mode == "stop" || mode == "deadline" {
				select {
				case ctx := <-engine.entered:
					admitted = ctx
					if _, bounded := ctx.Deadline(); !bounded {
						t.Error("engine received an unbounded lifetime")
					}
					if mode == "stop" {
						close(stop)
					}
				case <-time.After(2 * time.Second):
					t.Fatal("engine did not receive the original request")
				}
			}
			select {
			case response := <-done:
				if response.Code == "" {
					t.Fatal("canceled upgrade was reported successful")
				}
				if mode == "expired" && response.Code != upgradetx.CodeDeferred {
					t.Fatalf("unadmitted budget refusal lost its transient classification: %s", response.Code)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("admin port did not join after its owner stopped")
			}
			if (mode == "already_stopped" || mode == "expired") && engine.calls.Load() != 0 {
				t.Fatal("fenced request entered the upgrade engine")
			}
			if mode == "stop" && !errors.Is(admitted.Err(), context.Canceled) {
				t.Fatalf("owner stop waited for a different lifetime: %v", admitted.Err())
			}
			if mode == "deadline" && !errors.Is(admitted.Err(), context.DeadlineExceeded) {
				t.Fatalf("explicit budget did not expire: %v", admitted.Err())
			}
		})
	}
	if BindAdminUpgrade(nil) != nil {
		t.Fatal("missing engine was hidden by an adapter")
	}
}

func closeIfOpen(stop chan struct{}) {
	select {
	case <-stop:
	default:
		close(stop)
	}
}
