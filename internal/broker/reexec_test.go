package broker

import (
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/proto"
)

// reexec_test.go — G5 #13: the reload-primitive ARMING logic (the syscall.Exec trampoline itself is
// exercised by the N=3 sim drill, since it replaces the process image). RequestReExec must arm the
// post-shutdown flag AND unwind Run via the injected reload trigger, and must be nil-trigger-safe.

func TestRequestReExecArmsAndTriggers(t *testing.T) {
	b := &Broker{}
	if b.ReExecRequested() {
		t.Fatal("a fresh broker must not have re-exec armed")
	}
	triggered := 0
	b.SetReloadTrigger(func() { triggered++ })
	b.RequestReExec()
	if !b.ReExecRequested() {
		t.Fatal("RequestReExec must arm the trampoline flag serve.go checks after Run")
	}
	if triggered != 1 {
		t.Fatalf("RequestReExec must call the reload trigger exactly once, got %d", triggered)
	}
}

func TestRequestReExecNilTriggerSafe(t *testing.T) {
	b := &Broker{}
	b.RequestReExec() // no trigger set (e.g. a test / pre-Run) → must not panic
	if !b.ReExecRequested() {
		t.Fatal("re-exec must still be armed even with no trigger wired")
	}
}

// --- G5 #13 W2: the OpBrokerUpgradeReload gate order (idempotency → leader-refuse → sha → arm). ---

func TestReloadAlreadyAtVersionSkips(t *testing.T) {
	b := &clusterAdminBackend{} // idempotency is checked FIRST, before the leader / wiring gates
	resp := b.handleBrokerUpgradeReload(adminsock.Request{Op: adminsock.OpBrokerUpgradeReload, ToVersion: proto.ReleaseVersion})
	if !resp.OK || !resp.AlreadyAtVersion || resp.Reloading {
		t.Fatalf("already-at-version must be an idempotent no-op skip: %+v", resp)
	}
}

func TestReloadRefusesOnLeader(t *testing.T) {
	b, _ := fsTestBackend(t, "brk-a", "brk-b", "127.0.0.1:1") // single node → it IS the raft leader
	resp := b.handleBrokerUpgradeReload(adminsock.Request{Op: adminsock.OpBrokerUpgradeReload, ToVersion: "v-new-999"})
	if resp.Code != adminsock.CodeIsLeader {
		t.Fatalf("the leader must refuse to reload (transfer leadership off first); got %+v", resp)
	}
}

func TestReloadShaMismatchRefuses(t *testing.T) {
	b := &clusterAdminBackend{requestReExec: func() {}} // admin nil → non-leader path; reaches the sha gate
	resp := b.handleBrokerUpgradeReload(adminsock.Request{
		Op: adminsock.OpBrokerUpgradeReload, ToVersion: "v-new-999",
		ExpectSHA256: "0000000000000000000000000000000000000000000000000000000000000000",
	})
	if resp.Code != adminsock.CodeShaMismatch {
		t.Fatalf("a stale/unstaged on-disk binary must refuse the reload; got %+v", resp)
	}
}

func TestReloadArmsReExecOnSuccess(t *testing.T) {
	sha, err := onDiskBinarySHA256() // the on-disk digest MUST match (the sha guard is mandatory)
	if err != nil {
		t.Fatalf("hash self: %v", err)
	}
	fired := make(chan struct{})
	b := &clusterAdminBackend{requestReExec: func() { close(fired) }}
	resp := b.handleBrokerUpgradeReload(adminsock.Request{Op: adminsock.OpBrokerUpgradeReload, ToVersion: "v-new-999", ExpectSHA256: sha})
	if !resp.OK || !resp.Reloading {
		t.Fatalf("a valid reload must reply OK+Reloading; got %+v", resp)
	}
	select {
	case <-fired:
	case <-time.After(2 * time.Second):
		t.Fatal("reload must arm the re-exec after the reply drain")
	}
}

func TestReloadUnwiredReplies(t *testing.T) {
	sha, err := onDiskBinarySHA256()
	if err != nil {
		t.Fatalf("hash self: %v", err)
	}
	b := &clusterAdminBackend{} // requestReExec nil (single mode / not wired)
	resp := b.handleBrokerUpgradeReload(adminsock.Request{Op: adminsock.OpBrokerUpgradeReload, ToVersion: "v-new-999", ExpectSHA256: sha})
	if resp.Code != adminsock.CodeClusterNotEnabled {
		t.Fatalf("an unwired reload must reply cluster_not_enabled, not arm a restart; got %+v", resp)
	}
}

// TestReloadRefusesEmptySHA pins the External-review fail-closed fix: an unguarded reload (no expect_sha256)
// must be REFUSED, never re-exec whatever happens to be on disk. Idempotency/leader gates fire first, so
// use a non-idempotent ToVersion and a non-leader (admin nil) backend to reach the sha gate.
func TestReloadRefusesEmptySHA(t *testing.T) {
	armed := false
	b := &clusterAdminBackend{requestReExec: func() { armed = true }}
	resp := b.handleBrokerUpgradeReload(adminsock.Request{Op: adminsock.OpBrokerUpgradeReload, ToVersion: "v-new-999"})
	if resp.Code != adminsock.CodeBadRequest {
		t.Fatalf("an unguarded reload (empty expect_sha256) must be refused with bad_request; got %+v", resp)
	}
	if armed {
		t.Fatal("an unguarded reload must NOT arm the re-exec")
	}
}
