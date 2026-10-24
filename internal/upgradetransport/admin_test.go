package upgradetransport

import (
	"context"
	"encoding/json"
	"net"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/upgradewire"
)

// origin: S8 internal review activation-and-route-migration F7
func TestBrokerAdminTransactionKeepsArmingReply(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseBrokerAdminArming(t)
	}
	testharness.AssertNoGoroutineLeak(t, "transaction admin reply x5", g)
	testharness.AssertNoFDLeak(t, "transaction admin reply x5", fd, 0)
}

func exerciseBrokerAdminArming(t *testing.T) {
	t.Helper()
	for _, op := range []string{upgradewire.OpStart, upgradewire.OpRollback} {
		if brokerAdminBudget(adminsock.Request{Upgrade: &upgradewire.Request{Op: op}}) != upgradebudget.ComponentHandover {
			t.Fatal("transaction mutation has no arming-reply budget")
		}
	}
	for _, op := range []string{upgradewire.OpPrepare, upgradewire.OpQuery, upgradewire.OpCancel} {
		if brokerAdminBudget(adminsock.Request{Upgrade: &upgradewire.Request{Op: op}}) != upgradebudget.RequestLatency {
			t.Fatal("read-only or ordinary request gained the transaction budget")
		}
	}
	path := filepath.Join(t.TempDir(), "admin.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	served := make(chan error, 1)

	key := upgradetx.Key{DomainID: strings.Repeat("d", 32), Component: upgradetx.ComponentBroker, ConfigPath: "/sandbox/broker.yaml", UID: 1000}
	grant := upgradetx.Grant{Handle: upgradetx.Handle{Key: key, TxID: strings.Repeat("1", 32), CutID: strings.Repeat("2", 32), Generation: 2, Revision: 1}, Issuer: upgradetx.Owner{BootID: "boot", PID: 123, StartTicks: 1, InstanceID: strings.Repeat("3", 32)}, ActorFP: "actor", ClientNonce: "nonce", SourceRelease: "A", TargetRelease: "B", Mode: upgradetx.ModeHot, ExpiresAt: time.Now().Add(time.Minute).UnixNano(), SourceSHA: strings.Repeat("a", 64), TargetSHA: strings.Repeat("b", 64), ScopeDigest: strings.Repeat("c", 64), CapabilityDigest: strings.Repeat("e", 64)}
	artifact := upgradeartifact.Receipt{Grant: grant, SourceSHA: grant.SourceSHA, TargetSHA: grant.TargetSHA, ManifestDigest: strings.Repeat("f", 64), CanonicalPath: "/sandbox/tether", SourcePath: "/sandbox/A", TargetPath: "/sandbox/B"}
	request := adminsock.Request{Op: upgradewire.OpStart, Upgrade: &upgradewire.Request{Op: upgradewire.OpStart, Key: key, Grant: grant, Artifact: artifact}}
	if _, err := json.Marshal(request); err != nil {
		t.Fatal(err)
	}
	// An earlier caller deadline must win even for an authorized mutation. This
	// also exercises cancellation, connection closure and joined reader cleanup.
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			served <- err
			return
		}
		defer func() { _ = conn.Close() }()
		var got json.RawMessage
		if err = json.NewDecoder(conn).Decode(&got); err != nil {
			served <- err
			return
		}
		var data [1]byte
		_, err = conn.Read(data[:])
		served <- err
	}()
	if _, err = CallBrokerAdmin(ctx, path, request); err == nil {
		t.Fatal("caller cancellation did not interrupt transaction reply")
	}
	select {
	case <-served:
	case <-time.After(time.Second):
		t.Fatal("cancelled transaction retained its reply connection")
	}
}
