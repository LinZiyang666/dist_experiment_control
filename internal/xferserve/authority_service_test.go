package xferserve

import (
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// origin: S7 internal review s7-p9m-open F1 / hot-update S7 plan §8.2
func TestTransferAuthorityJoinIncludesLeaderVerification(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round", func(t *testing.T) { exerciseAuthorityLeaderJoin(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "authority leader verification join", g)
	testharness.AssertNoFDLeak(t, "authority leader verification join", fd, 0)
}

func exerciseAuthorityLeaderJoin(t *testing.T) {
	h, js := assemblyHost(t, t.TempDir())
	directory, err := openDirectory(js, 1, assemblyDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	h.Leader = func() bool { close(entered); <-release; return false }
	a := Assembly{Directory: directory, Host: h}
	service, err := a.StartCluster(js.Conn())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
		if err := service.Stop(assemblyDeadline(t)); err != nil {
			t.Error(err)
		}
	}()
	if err := js.Conn().FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	if err := js.Conn().PublishRequest(continuationAuthoritySubject, js.Conn().NewInbox(), marshal(authorityRequest{Operation: "roster", BudgetNS: int64(time.Second)})); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("leader verification did not enter")
	}
	short, err := controlbudget.After(30 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Join(short); err == nil {
		t.Fatal("authority service joined while leader verification still borrowed the backend")
	}
	close(release)
	if err := service.Join(assemblyDeadline(t)); err != nil {
		t.Fatal(err)
	}
}
